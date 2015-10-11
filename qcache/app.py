import json
from StringIO import StringIO
import gc
import pandas
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError
from qcache.dataset_cache import DatasetCache
from qcache.query import query, MalformedQueryException
import re


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


class UTF8JSONDecoder(json.JSONDecoder):
    def decode(self, json_string):
        obj = super(UTF8JSONDecoder, self).decode(json_string)
        assert isinstance(obj, list), "Must pass a list of objects"

        for r in obj:
            yield {k: v.encode(encoding='utf-8') if isinstance(v, unicode) else v for k, v in r.items()}


class AppState(object):
    def __init__(self):
        self.query_count = 0


class DatasetHandler(RequestHandler):
    def initialize(self, dataset_cache, state):
        self.dataset_cache = dataset_cache
        self.state = state

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

    def get(self, dataset_key):
        accept_type = self.accept_type()
        if not dataset_key in self.dataset_cache:
            raise HTTPError(ResponseCode.NOT_FOUND)

        q = self.get_argument('q', default='')
        df = self.dataset_cache[dataset_key]
        try:
            response = query(df, q)
        except MalformedQueryException as e:
            self.write(json.dumps({'error': e.message}))
            self.set_status(ResponseCode.BAD_REQUEST)
            return

        self.set_header("Content-Type", "{content_type}; charset=utf-8".format(content_type=accept_type))
        if accept_type == CONTENT_TYPE_CSV:
            self.write(response.to_csv(index=False))
        else:
            self.write(response.to_json(orient='records'))

        self.post_query_processing()

    def post_query_processing(self):
        if self.state.query_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact query performance negatively.
            gc.collect()

        self.state.query_count += 1

    def post(self, dataset_key):
        if dataset_key in self.dataset_cache:
            del self.dataset_cache[dataset_key]

        content_type = self.content_type()
        if content_type == CONTENT_TYPE_CSV:
            self.dataset_cache.ensure_free(len(self.request.body))
            df = pandas.read_csv(StringIO(self.request.body))
        else:
            # This is a waste of CPU cycles, first the JSON decoder decodes all strings
            # from UTF-8 then we immediately encode them back into UTF-8. Couldn't
            # find an easy solution to this though.
            self.dataset_cache.ensure_free(len(self.request.body)/2)
            data = json.loads(self.request.body, cls=UTF8JSONDecoder)
            df = pandas.DataFrame.from_records(data)

        self.dataset_cache[dataset_key] = df
        self.set_status(ResponseCode.CREATED)
        self.write("")


def make_app(url_prefix='/qcache', debug=False, max_cache_size=1000000000):
    # /dataset/{key}
    # /dataset/{namespace}/{key}
    # /stat
    # /stat/{namespace}
    #
    #
    return Application([
        url(r"{url_prefix}/dataset/([A-Za-z0-9\-_]+)".format(url_prefix=url_prefix),
            DatasetHandler, dict(dataset_cache=DatasetCache(max_size=max_cache_size), state=AppState()),
            name="dataset")
    ], debug=debug)


def run():
    make_app(debug=True).listen(8888, max_buffer_size=1000000000)
    IOLoop.current().start()

if __name__ == "__main__":
    run()
