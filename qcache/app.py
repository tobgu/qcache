import base64
import functools
import json
import re
import ssl
import time

from tornado import httpserver
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError

from qcache.cache_common import QueryResult, InsertResult
from qcache.compression import CompressedContentEncoding, decoded_body
from qcache.constants import CONTENT_TYPE_JSON, CONTENT_TYPE_CSV
from qcache.qframe import FILTER_ENGINE_NUMEXPR
from qcache.sharded_cache import ShardedCache


class ResponseCode(object):
    OK = 200
    CREATED = 201

    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    NOT_FOUND = 404
    NOT_ACCEPTABLE = 406
    UNSUPPORTED_MEDIA_TYPE = 415

ACCEPTED_TYPES = {CONTENT_TYPE_JSON, CONTENT_TYPE_CSV}  # text/*, */*?
CHARSET_REGEX = re.compile('charset=([A-Za-z0-9_-]+)')

auth_user = None
auth_password = None


def auth_enabled():
    return auth_user is not None and auth_password is not None


def credentials_correct(provided_user, provided_password):
    return provided_user == auth_user and provided_password == auth_password


def http_auth(handler_class):
    """
    Basic auth decorator. Based on the decorator found here:
    https://simplapi.wordpress.com/2014/03/26/python-tornado-and-decorator/
    """

    def set_401(handler):
        handler.set_status(ResponseCode.UNAUTHORIZED)
        handler.set_header('WWW-Authenticate', 'Basic realm=Restricted')
        handler._transforms = []
        handler.finish()

    def wrap_execute(handler_execute):
        def is_authenticated(handler):
            if not auth_enabled():
                return True

            auth_header = handler.request.headers.get('Authorization')
            if auth_header is None or not auth_header.startswith('Basic '):
                set_401(handler)
                return False

            auth_decoded = base64.decodebytes(auth_header[6:].encode('utf-8')).decode('utf-8')
            user, password = auth_decoded.split(':', 2)

            if not credentials_correct(user, password):
                set_401(handler)
                return False

            return True

        def _execute(self, transforms, *args, **kwargs):
            if not is_authenticated(self):
                return False

            return handler_execute(self, transforms, *args, **kwargs)

        return _execute

    handler_class._execute = wrap_execute(handler_class._execute)
    return handler_class


def measured(method):
    @functools.wraps(method)
    def _execute(self, *args, **kwargs):
        t0 = time.time()
        result = method(self, *args, **kwargs)
        self.set_header('X-QCache-execution-duration', '{}'.format(round(time.time() - t0, 4)))
        return result

    return _execute


@http_auth
class DatasetHandler(RequestHandler):
    def initialize(self, cache):
        self.cache = cache

    def prepare(self):
        self.request_start = time.time()

    def accept_type(self):
        accept_types = [t.strip() for t in self.request.headers.get('Accept', CONTENT_TYPE_JSON).split(',')]
        for t in accept_types:
            if t in ACCEPTED_TYPES:
                return t

        raise HTTPError(ResponseCode.NOT_ACCEPTABLE)

    def content_type(self):
        header = self.request.headers.get("Content-Type", CONTENT_TYPE_CSV).split(';')
        content_type = header[0]
        if content_type not in ACCEPTED_TYPES:
            raise HTTPError(ResponseCode.UNSUPPORTED_MEDIA_TYPE,
                            "Content-Type '{content_type}' not supported".format(content_type=content_type))

        if len(header) > 1:
            m = CHARSET_REGEX.match(header[1].strip())
            if m and m.group(1) != 'utf-8':
                raise HTTPError(ResponseCode.UNSUPPORTED_MEDIA_TYPE,
                                "charset={charset} not supported, only utf-8".format(charset=m.group(1)))

        return content_type

    def header_to_key_values(self, header_name):
        header_value = self.request.headers.get(header_name, None)
        if not header_value:
            return None

        key_values = []
        for key_value in header_value.split(';'):
            key_values.append(tuple(s.strip() for s in key_value.split('=')))

        return key_values

    def dtypes(self):
        types = self.header_to_key_values('X-QCache-types')
        if not types:
            return None

        dtypes = {}
        for column_name, type_name in types:
            if type_name == 'string':
                dtypes[column_name] = 'object'
            elif type_name == 'enum':
                dtypes[column_name] = 'category'
            else:
                raise HTTPError(ResponseCode.BAD_REQUEST,
                                'Unrecognized type name "{type_name}" for column "{column_name}"'.format(
                                    type_name=type_name, column_name=column_name))

        return dtypes

    def stand_in_columns(self):
        return self.header_to_key_values('X-QCache-stand-in-columns')

    def query(self, dataset_key, q):
        accept_type = self.accept_type()
        result = self.cache.query(dataset_key=dataset_key,
                                  q=q,
                                  filter_engine=self.request.headers.get('X-QCache-filter-engine', None),
                                  stand_in_columns=self.stand_in_columns(),
                                  accept_type=accept_type)

        if result.status == QueryResult.STATUS_SUCCESS:
            self.set_header("Content-Type", "{content_type}; charset=utf-8".format(content_type=result.content_type))
            self.set_header("X-QCache-unsliced-length", result.unsliced_length)
            self.write(result.data)
        elif result.status == QueryResult.STATUS_NOT_FOUND:
            raise HTTPError(ResponseCode.NOT_FOUND)
        elif result.status == QueryResult.STATUS_MALFORMED_QUERY:
            self.write(json.dumps({'error': result.data}))
            self.set_status(ResponseCode.BAD_REQUEST)
        else:
            raise Exception("Unknown query status: {}".format(result.status))

    def q_json_to_dict(self, q_json):
        try:
            return json.loads(q_json)
        except ValueError:
            self.write(json.dumps({'error': 'Could not load JSON: {json}'.format(json=json)}))
            self.set_status(ResponseCode.BAD_REQUEST)

        return None

    @measured
    def get(self, dataset_key, optional_q):
        if optional_q:
            # There should not be a q URL for the GET method, it's supposed to take
            # q as a query parameter
            raise HTTPError(ResponseCode.NOT_FOUND)

        q_dict = self.q_json_to_dict(self.get_argument('q', default=''))
        if q_dict is not None:
            self.query(dataset_key, q_dict)

    @measured
    def post(self, dataset_key, optional_q):
        if optional_q:
            q_dict = self.q_json_to_dict(decoded_body(self.request))
            if q_dict is not None:
                self.query(dataset_key, q_dict)
            return

        result = self.cache.insert(dataset_key=dataset_key,
                                   data=decoded_body(self.request),
                                   content_type=self.content_type(),
                                   data_types=self.dtypes(),
                                   stand_in_columns=self.stand_in_columns())

        if result.status == InsertResult.STATUS_SUCCESS:
            self.set_status(ResponseCode.CREATED)
            self.write("")
        else:
            raise Exception("Unknown insert status: {}".format(result.status))

    @measured
    def delete(self, dataset_key, optional_q):
        if optional_q:
            # There should not be a q parameter for the delete method
            raise HTTPError(ResponseCode.NOT_FOUND)

        self.cache.delete(dataset_key)
        self.write("")


@http_auth
class StatusHandler(RequestHandler):
    def initialize(self, cache):
        self.cache = cache

    def get(self):
        if self.cache.status() == "OK":
            self.write("OK")
        else:
            raise Exception("Caches not OK")




@http_auth
class StatisticsHandler(RequestHandler):
    def initialize(self, cache):
        self.cache = cache

    @measured
    def get(self):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(json.dumps(self.cache.statistics()))


def make_cache(max_cache_size=1000000000,
               max_age=0,
               statistics_buffer_size=1000,
               default_filter_engine=FILTER_ENGINE_NUMEXPR,
               cache_shards=1,
               l2_cache_size=0):
    return ShardedCache(statistics_buffer_size=statistics_buffer_size,
                        max_cache_size=max_cache_size,
                        max_age=max_age,
                        default_filter_engine=default_filter_engine,
                        shard_count=cache_shards)


def make_app(cache, url_prefix='/qcache', debug=False, basic_auth=None):
    if basic_auth:
        global auth_user, auth_password
        auth_user, auth_password = basic_auth.split(':', 2)

    return Application([
                           url(r"{url_prefix}/dataset/([A-Za-z0-9\-_]+)/?(q)?".format(url_prefix=url_prefix),
                               DatasetHandler,
                               dict(cache=cache),
                               name="dataset"),
                           url(r"{url_prefix}/status".format(url_prefix=url_prefix),
                               StatusHandler,
                               dict(cache=cache),
                               name="status"),
                           url(r"{url_prefix}/statistics".format(url_prefix=url_prefix),
                               StatisticsHandler,
                               dict(cache=cache),
                               name="statistics")
                       ], debug=debug, transforms=[CompressedContentEncoding])


def ssl_options(certfile, cafile=None):
    if certfile:
        print("Enabling TLS")
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH, cafile=cafile)
        ssl_context.load_cert_chain(certfile)

        if cafile:
            print("Enabling client certificate verification")
            ssl_context.verify_mode = ssl.CERT_REQUIRED
        return dict(ssl_options=ssl_context)

    return {}


def run(port=8888,
        max_cache_size=1000000000,
        max_age=0,
        statistics_buffer_size=1000,
        debug=False,
        certfile=None,
        cafile=None,
        basic_auth=None,
        default_filter_engine=FILTER_ENGINE_NUMEXPR,
        api_workers=1,
        cache_shards=1,
        l2_cache_size=0):

    if basic_auth and not certfile:
        print("TLS must be enabled to use basic auth!")
        return

    print("Starting...")
    print("port={}".format(port))
    print("max_cache_size={} bytes".format(max_cache_size))
    print("max_age={} seconds".format(max_age))
    print("statistics_buffer_size={}".format(statistics_buffer_size))
    print("debug={}".format(debug))
    print("default_filter_engine={}".format(default_filter_engine))
    print("api_workers={}".format(api_workers))
    print("cache_shards={}".format(cache_shards))
    print("l2_cache_size={} bytes".format(l2_cache_size))

    cache = make_cache(max_cache_size=max_cache_size,
                       max_age=max_age,
                       default_filter_engine=default_filter_engine,
                       statistics_buffer_size=statistics_buffer_size,
                       cache_shards=cache_shards,
                       l2_cache_size=l2_cache_size)

    app = make_app(cache, debug=debug, basic_auth=basic_auth)

    args = {}
    args.update(ssl_options(certfile=certfile, cafile=cafile))
    http_server = httpserver.HTTPServer(app, max_buffer_size=max_cache_size)
    http_server.bind(port)
    http_server.start(api_workers)
    IOLoop.current().start()


if __name__ == "__main__":
    run()
