import json
import datetime
from StringIO import StringIO
import pandas
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError
from qcache.query import query


class ResponseCode(object):
    OK = 200
    CREATED = 201

    NOT_FOUND = 404
    NOT_ACCEPTABLE = 406
    UNSUPPORTED_MEDIA_TYPE = 415


class CacheItem(object):
    def __init__(self, dataset):
        self.creation_time = datetime.datetime.utcnow()
        self.last_access_time = self.creation_time
        self._dataset = dataset
        self.access_count = 0

    @property
    def dataset(self):
        self.last_access_time = datetime.datetime.utcnow()
        self.access_count += 1
        return self._dataset

CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_CSV = 'text/csv'
ACCEPTED_TYPES = {CONTENT_TYPE_JSON, CONTENT_TYPE_CSV}  # text/*, */*?


class DatasetHandler(RequestHandler):
    def initialize(self, key_to_dataset):
        self.key_to_dataset = key_to_dataset

    def accept_type(self):
        accept_types = [t.strip() for t in self.request.headers.get('Accept', CONTENT_TYPE_JSON).split(',')]
        for t in accept_types:
            if t in ACCEPTED_TYPES:
                return t

        raise HTTPError(ResponseCode.NOT_ACCEPTABLE)

    def content_type(self):
        content_type = self.request.headers.get("Content-Type", CONTENT_TYPE_CSV)
        if content_type not in ACCEPTED_TYPES:
            raise HTTPError(ResponseCode.UNSUPPORTED_MEDIA_TYPE,
                            "Content-Type '{content_type}' not supported".format(content_type=content_type))

        return content_type

    def get(self, dataset_key):
        accept_type = self.accept_type()
        if not dataset_key in self.key_to_dataset:
            raise HTTPError(ResponseCode.NOT_FOUND)

        q = self.get_argument('q', default='')
        df = self.key_to_dataset[dataset_key]
        response = query(df, json.loads(q))

        # encoding="utf-8"
        self.set_header("Content-Type", accept_type)
        if accept_type == CONTENT_TYPE_CSV:
            self.write(response.to_csv(index=False))
        else:
            self.write(response.to_json(orient='records'))

    def post(self, dataset_key):
        content_type = self.content_type()
        if content_type == CONTENT_TYPE_CSV:
            df = pandas.read_csv(StringIO(self.request.body))
        else:
            df = pandas.DataFrame.from_records(json.loads(self.request.body))

        self.key_to_dataset[dataset_key] = df
        self.set_status(ResponseCode.CREATED)
        self.write("")


def make_app(url_prefix='/qcache'):
    # /dataset/{key}
    # /dataset/{namespace}/{key}
    # /stat
    # /stat/{namespace}
    #
    #
    return Application([
        url(r"{url_prefix}/dataset/([A-Za-z0-9\-_]+)".format(url_prefix=url_prefix),
            DatasetHandler, dict(key_to_dataset={}), name="dataset")
    ], debug=True)


def run():
    make_app().listen(8888, max_buffer_size=1104857600)
    IOLoop.current().start()

if __name__ == "__main__":
    run()
