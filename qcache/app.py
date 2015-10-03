from StringIO import StringIO
import json
import pandas
from tornado.ioloop import IOLoop
from tornado.web import RequestHandler, Application, url, HTTPError
from qcache.query import query


class DatasetHandler(RequestHandler):
    def initialize(self, key_to_dataset):
        self.key_to_dataset = key_to_dataset

    def get(self, dataset_key):
        if not dataset_key in self.key_to_dataset:
            raise HTTPError(404)

        # TODO: Support for mutable datasets by adding timeout/max age to evict from cache
        q = self.get_argument('q', default='')
        df = self.key_to_dataset[dataset_key]
        response = query(df, json.loads(q))
        self.write(response.to_json(orient='records'))

    def post(self, dataset_key):
        df = pandas.read_csv(StringIO(self.request.body))
        self.key_to_dataset[dataset_key] = df

        # Respond 201
        self.set_status(201)
        self.write("")


def make_app():
    return Application([
        url(r"/url_prefix/([A-Za-z0-9\-_]+)", DatasetHandler, dict(key_to_dataset={}), name="dataset")
    ], debug=True)

def run():
    make_app().listen(8888, max_buffer_size=1104857600)
    IOLoop.current().start()

if __name__ == "__main__":
    run()
