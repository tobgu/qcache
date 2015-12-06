import base64
import json
import re
import time
import gc

from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError

from qcache.dataset_cache import DatasetCache
from qcache.qframe import MalformedQueryException, QFrame
from qcache.statistics import Statistics


class ResponseCode(object):
    OK = 200
    CREATED = 201

    BAD_REQUEST = 400
    NOT_FOUND = 404
    NOT_ACCEPTABLE = 406
    UNSUPPORTED_MEDIA_TYPE = 415

CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_CSV = 'text/csv'
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
        handler.set_status(401)
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

            auth_decoded = base64.decodestring(auth_header[6:])
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


class UTF8JSONDecoder(json.JSONDecoder):
    def decode(self, json_string):
        obj = super(UTF8JSONDecoder, self).decode(json_string)
        assert isinstance(obj, list), "Must pass a list of objects"

        for r in obj:
            yield {k: v.encode(encoding='utf-8') if isinstance(v, unicode) else v for k, v in r.items()}


class AppState(object):
    def __init__(self):
        self.query_count = 0


@http_auth
class DatasetHandler(RequestHandler):
    def initialize(self, dataset_cache, state, stats):
        self.dataset_cache = dataset_cache
        self.state = state
        self.stats = stats

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

    def dtypes(self):
        types = self.request.headers.get('X-QCache-types', None)
        if not types:
            return None

        dtypes = {}
        for type_spec in types.split(';'):
            column_name, type_name = [s.strip() for s in type_spec.split('=')]
            if type_name == 'string':
                dtypes[column_name] = 'object'
            else:
                raise HTTPError(ResponseCode.BAD_REQUEST,
                                'Unrecognized type name "{type_name}" for column "{column_name}"'.format(
                                    type_name=type_name, column_name=column_name))

        return dtypes

    def get(self, dataset_key):
        t0 = time.time()
        accept_type = self.accept_type()
        if dataset_key not in self.dataset_cache:
            self.stats.inc('miss_count')
            raise HTTPError(ResponseCode.NOT_FOUND)

        if self.dataset_cache.evict_if_too_old(dataset_key):
            self.stats.inc('miss_count')
            self.stats.inc('age_evict_count')
            raise HTTPError(ResponseCode.NOT_FOUND)

        try:
            q_json = self.get_argument('q', default='')
            q = json.loads(q_json)
        except ValueError:
            self.write(json.dumps({'error': 'Could not load JSON: {json}'.format(json=json)}))
            self.set_status(ResponseCode.BAD_REQUEST)
            return

        qf = self.dataset_cache[dataset_key]
        try:
            result_frame = qf.query(q)
        except MalformedQueryException as e:
            self.write(json.dumps({'error': e.message}))
            self.set_status(ResponseCode.BAD_REQUEST)
            return

        self.set_header("Content-Type", "{content_type}; charset=utf-8".format(content_type=accept_type))
        self.set_header("X-QCache-unsliced-length", result_frame.unsliced_df_len)
        if accept_type == CONTENT_TYPE_CSV:
            self.write(result_frame.to_csv())
        else:
            self.write(result_frame.to_json())

        self.post_query_processing()
        self.stats.inc('hit_count')
        self.stats.append('query_durations', time.time() - t0)

    def post_query_processing(self):
        if self.state.query_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact query performance negatively.
            gc.collect()

        self.state.query_count += 1

    def post(self, dataset_key):
        t0 = time.time()
        if dataset_key in self.dataset_cache:
            self.stats.inc('replace_count')
            del self.dataset_cache[dataset_key]

        content_type = self.content_type()
        if content_type == CONTENT_TYPE_CSV:
            evict_count = self.dataset_cache.ensure_free(len(self.request.body))
            qf = QFrame.from_csv(self.request.body, column_types=self.dtypes())
        else:
            # This is a waste of CPU cycles, first the JSON decoder decodes all strings
            # from UTF-8 then we immediately encode them back into UTF-8. Couldn't
            # find an easy solution to this though.
            evict_count = self.dataset_cache.ensure_free(len(self.request.body)/2)
            data = json.loads(self.request.body, cls=UTF8JSONDecoder)
            qf = QFrame.from_dicts(data)

        self.dataset_cache[dataset_key] = qf
        self.set_status(ResponseCode.CREATED)
        self.stats.inc('size_evict_count', count=evict_count)
        self.stats.inc('store_count')
        self.stats.append('store_row_counts', len(qf))
        self.stats.append('store_durations', time.time() - t0)
        self.write("")

    def delete(self, dataset_key):
        if dataset_key in self.dataset_cache:
            del self.dataset_cache[dataset_key]

        self.write("")


@http_auth
class StatusHandler(RequestHandler):
    def get(self):
        self.write("OK")


@http_auth
class StatisticsHandler(RequestHandler):
    def initialize(self, stats):
        self.stats = stats

    def get(self):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(self.stats.to_json())
        self.stats.reset()


def make_app(url_prefix='/qcache', debug=False, max_cache_size=1000000000, max_age=0, statistics_buffer_size=1000, basic_auth=None):
    if basic_auth:
        global auth_user, auth_password
        auth_user, auth_password = basic_auth.split(':', 2)

    stats = Statistics(buffer_size=statistics_buffer_size)
    return Application([
        url(r"{url_prefix}/dataset/([A-Za-z0-9\-_]+)".format(url_prefix=url_prefix),
            DatasetHandler,
            dict(dataset_cache=DatasetCache(max_size=max_cache_size, max_age=max_age),
                 state=AppState(),
                 stats=stats),
            name="dataset"),
        url(r"{url_prefix}/status".format(url_prefix=url_prefix), StatusHandler, {}, name="status"),
        url(r"{url_prefix}/statistics".format(url_prefix=url_prefix), StatisticsHandler, dict(stats=stats), name="statistics")
    ], debug=debug)


def run(port=8888, max_cache_size=1000000000, max_age=0, statistics_buffer_size=1000, debug=False, certfile=None, basic_auth=None):
    if basic_auth and not certfile:
        print "SSL must be enbabled to use basic auth!"
        return

    print("Starting on port {port}, max cache size {max_cache_size} bytes, max age {max_age} seconds,"
          " statistics_buffer_size {statistics_buffer_size}, debug={debug}".format(
        port=port, max_cache_size=max_cache_size, max_age=max_age, statistics_buffer_size=statistics_buffer_size, debug=debug))

    app = make_app(
        debug=debug, max_cache_size=max_cache_size, max_age=max_age, statistics_buffer_size=statistics_buffer_size, basic_auth=basic_auth)

    args = {}
    if certfile:
        args['ssl_options'] = {'certfile': certfile, 'ciphers': 'ECDH+AESGCM:DH+AESGCM:ECDH+AES256:DH+AES256:ECDH+AES128:DH+AES:ECDH+3DES:DH+3DES:RSA+AESGCM:RSA+AES:RSA+3DES:!aNULL:!MD5:!DSS'}

    app.listen(port, max_buffer_size=max_cache_size, **args)
    IOLoop.current().start()

if __name__ == "__main__":
    run()
