from typing import Dict, Optional, List, Tuple

import base64
import functools
import json
import re
import ssl
import time

from tornado import httpserver
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError

from qcache.cache.cache_common import QueryResult, InsertResult, Result
from qcache.compression import CompressedContentEncoding, decoded_body
from qcache.constants import CONTENT_TYPE_JSON, CONTENT_TYPE_CSV
from qcache.qframe import DTypes
from qcache.cache.sharded_cache import ShardedCache


class ResponseCode:
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


def auth_enabled() -> bool:
    return auth_user is not None and auth_password is not None


def credentials_correct(provided_user, provided_password) -> bool:
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
        def is_authenticated(handler) -> bool:
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
        add_stats_header(self, 'request_duration', round(time.time() - t0, 4))
        return result

    return _execute


def add_stats_header(handler, key: str, value: float):
    handler.add_header('X-QCache-stats', '{}={}'.format(key, value))


@http_auth
class DatasetHandler(RequestHandler):
    def initialize(self, cache: ShardedCache):
        self.cache = cache

    def prepare(self):
        self.request_start: float = time.time()

    def accept_type(self) -> str:
        accept_types = [t.strip() for t in self.request.headers.get('Accept', CONTENT_TYPE_JSON).split(',')]
        for t in accept_types:
            if t in ACCEPTED_TYPES:
                return t

        raise HTTPError(ResponseCode.NOT_ACCEPTABLE)

    def content_type(self) -> str:
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

    def header_to_key_values(self, header_name: str) -> Optional[List[Tuple[str, ...]]]:
        header_value = self.request.headers.get(header_name, None)
        if not header_value:
            return None

        key_values = []
        if ";" in header_value:
            # This is left for backwards compatibility, requests should now use "," to separate multiple
            # header values for the same header field.
            keys_values = header_value.split(';')
        else:
            # This could be improved, it currently does not maintain quoted strings
            # for example
            keys_values = header_value.split(',')

        for key_value in keys_values:
            key_values.append(tuple(s.strip() for s in key_value.split('=')))

        return key_values

    def add_stats_header(self, result):
        if isinstance(result, Result):
            for k, v in result.stats.items():
                add_stats_header(self, k, v)

    def dtypes(self) -> DTypes:
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

    def query(self, dataset_key: str, q: dict) -> QueryResult:
        accept_type = self.accept_type()
        result = self.cache.query(dataset_key=dataset_key,
                                  q=q,
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

        return result

    def q_json_to_dict(self, q_json: str) -> Optional[dict]:
        if not q_json:
            return {}

        try:
            return json.loads(q_json)
        except ValueError:
            self.write(json.dumps({'error': 'Could not load JSON: {json}'.format(json=q_json)}))
            self.set_status(ResponseCode.BAD_REQUEST)

        return None

    @measured
    def get(self, dataset_key: str, optional_q: str):
        if optional_q:
            # There should not be a q URL for the GET method, it's supposed to take
            # q as a query parameter
            raise HTTPError(ResponseCode.NOT_FOUND)

        q_dict = self.q_json_to_dict(self.get_argument('q', default=''))
        if q_dict is not None:
            result = self.query(dataset_key, q_dict)
            self.add_stats_header(result)

    @measured
    def post(self, dataset_key: str, optional_q: str):
        if optional_q:
            q_dict = self.q_json_to_dict(decoded_body(self.request))
            if q_dict is not None:
                result = self.query(dataset_key, q_dict)
                self.add_stats_header(result)
            return

        result = self.cache.insert(dataset_key=dataset_key,
                                   data=decoded_body(self.request),
                                   content_type=self.content_type(),
                                   data_types=self.dtypes(),
                                   stand_in_columns=self.stand_in_columns())

        self.add_stats_header(result)

        if result.status == InsertResult.STATUS_SUCCESS:
            self.set_status(ResponseCode.CREATED)
            self.write("")
        else:
            raise Exception("Unknown insert status: {}".format(result.status))

    @measured
    def delete(self, dataset_key: str, optional_q: str):
        if optional_q:
            # There should not be a q parameter for the delete method
            raise HTTPError(ResponseCode.NOT_FOUND)

        result = self.cache.delete(dataset_key)
        self.add_stats_header(result)
        self.write("")



@http_auth
class StatusHandler(RequestHandler):
    def initialize(self, cache: ShardedCache):
        self.cache = cache

    def get(self):
        status = self.cache.status()
        if status == "OK":
            self.write("OK")
        else:
            raise Exception("Cache not OK: {}".format(status))


@http_auth
class StatisticsHandler(RequestHandler):
    def initialize(self, cache: ShardedCache):
        self.cache = cache

    @measured
    def get(self):
        self.set_header("Content-Type", "application/json; charset=utf-8")
        self.write(json.dumps(self.cache.statistics()))


def make_app(cache: ShardedCache, url_prefix: str='/qcache', debug: bool=False, basic_auth: Optional[str]=None):
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


def ssl_options(certfile: str, cafile: str=None) -> dict:
    if certfile:
        print("Enabling TLS")
        ssl_context = ssl.create_default_context(purpose=ssl.Purpose.CLIENT_AUTH, cafile=cafile)
        ssl_context.load_cert_chain(certfile)

        if cafile:
            print("Enabling client certificate verification")
            ssl_context.verify_mode = ssl.CERT_REQUIRED
        return dict(ssl_options=ssl_context)

    return {}


def run(port: int=8888,
        max_cache_size: int=1000000000,
        max_age: int=0,
        statistics_buffer_size: int=1000,
        debug: bool=False,
        certfile: Optional[str]=None,
        cafile: Optional[str]=None,
        basic_auth: Optional[str]=None,
        api_workers: int=1,
        cache_shards: int=1,
        l2_cache_size: int=0):
    if basic_auth and not certfile:
        print("TLS must be enabled to use basic auth!")
        return

    print("Starting...")
    print("port={}".format(port))
    print("max_cache_size={} bytes".format(max_cache_size))
    print("max_age={} seconds".format(max_age))
    print("statistics_buffer_size={}".format(statistics_buffer_size))
    print("debug={}".format(debug))
    print("api_workers={}".format(api_workers))
    print("cache_shards={}".format(cache_shards))
    print("l2_cache_size={} bytes".format(l2_cache_size))
    print("certfile={}".format(certfile))
    print("cafile={}".format(cafile))

    cache = ShardedCache(max_cache_size=max_cache_size,
                         max_age=max_age,
                         statistics_buffer_size=statistics_buffer_size,
                         shard_count=cache_shards,
                         l2_cache_size=l2_cache_size)

    app = make_app(cache, debug=debug, basic_auth=basic_auth)

    args = dict(max_buffer_size=max_cache_size)
    args.update(ssl_options(certfile=certfile, cafile=cafile))
    http_server = httpserver.HTTPServer(app, **args)
    http_server.bind(port)
    http_server.start(api_workers)
    IOLoop.current().start()


if __name__ == "__main__":
    run()
