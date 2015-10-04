# coding=utf-8
import json
import pytest
from tornado.httputil import url_concat
from tornado.testing import AsyncHTTPTestCase
import qcache.app as app
import csv
from StringIO import StringIO


def to_json(data):
    return json.dumps(data)


def to_csv(data):
    if not data:
        return ""

    out = StringIO()
    writer = csv.DictWriter(out, data[0].keys())
    writer.writeheader()

    for entry in data:
        writer.writerow(entry)

    return out.getvalue()


def from_csv(text):
    input_data = StringIO(text)
    return list(csv.DictReader(input_data))


class TestSingleServer(AsyncHTTPTestCase):
    def get_app(self):
        return app.make_app(url_prefix='')

    def post_json(self, url, data):
        body = to_json(data)
        return self.fetch(url, method='POST', body=body, headers={'Content-Type': 'application/json'})

    def query_json(self, url, query):
        url = url_concat(url, {'q': json.dumps(query)})
        return self.fetch(url, headers={'Accept': 'application/json, text/csv'})

    def post_csv(self, url, data):
        body = to_csv(data)
        return self.fetch(url, method='POST', body=body, headers={'Content-Type': 'text/csv'})

    def query_csv(self, url, query):
        url = url_concat(url, {'q': json.dumps(query)})
        return self.fetch(url, headers={'Accept': 'text/csv, application/json'})

    def test_404_when_item_is_missing(self):
        response = self.fetch('/dataset/abc')
        assert response.code == 404

    def test_upload_json_query_json(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['==', 'foo', 1]})
        assert response.code == 200
        assert json.loads(response.body) == [{'foo': 1, 'bar': 10}]

    def test_upload_csv_query_csv(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}, {'baz': 2, 'bar': 20}])
        assert response.code == 201

        response = self.query_csv('/dataset/cba', {'where': ['==', 'baz', 1]})
        assert response.code == 200
        assert from_csv(response.body) == [{'baz': '1', 'bar': '10'}]  # NB: Strings for numbers here

    @pytest.mark.skipif(True, reason="JSON + unicode problem")
    def test_upload_json_query_json_unicode_characters(self):
        response = self.post_json('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiøn'}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiøn"']})
        assert response.code == 200
        assert json.loads(response.body) == [{'foo': u'Iñtërnâtiônàližætiøn'}]

    def test_upload_csv_query_csv_unicode_characters_encoded_as_utf8(self):
        # TODO
        response = self.post_csv('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_csv('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiønåäö"']})
        assert response.code == 200
        assert from_csv(response.body) == [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}]

    def test_upload_and_query_json_unicode_characters_encoded_as_utf8(self):
        response = self.post_csv('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiønåäö"']})
        assert response.code == 200
        assert json.loads(response.body) == [{'foo': u'Iñtërnâtiônàližætiønåäö'}]


# Error cases:
# - Malformed query
#   * Structure
#   * Impossible to parse
#   * Invalid function
#   * Invalid operator
#   * Missing column
# - Malformed input data
#   * No data sent in => error
#   * Wrong format specified
#   * Accepted format specified but cannot be encoded
#   * Non-uniform JSON and CSV
# - Non fitting data
#   * The data is too large to be fitted into memory of the current instance.