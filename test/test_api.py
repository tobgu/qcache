# coding=utf-8
import json
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


class SharedTest(AsyncHTTPTestCase):
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


class TestBaseCases(SharedTest):

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


class TestCharacterEncoding(SharedTest):
    def test_upload_json_query_json_unicode_characters(self):
        response = self.post_json('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiøn'}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiøn"']})

        assert response.code == 200
        response_data = json.loads(response.body)
        assert response_data == [{'foo': u'Iñtërnâtiônàližætiøn'}]
        assert type(response_data[0]['foo']) is unicode

    def test_upload_csv_query_csv_unicode_characters_encoded_as_utf8(self):
        response = self.post_csv('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_csv('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiønåäö"']})
        assert response.code == 200
        assert from_csv(response.body) == [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}]

    def test_upload_csv_query_json_unicode_characters_encoded_as_utf8(self):
        response = self.post_csv('/dataset/abc', [{'foo': u'Iñtërnâtiônàližætiønåäö'.encode('utf-8')}, {'foo': 'qux'}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['==', 'foo', u'"Iñtërnâtiônàližætiønåäö"']})

        assert response.code == 200
        response_data = json.loads(response.body)
        assert json.loads(response.body) == [{'foo': u'Iñtërnâtiônàližætiønåäö'}]
        assert type(response_data[0]['foo']) is unicode

    def test_upload_invalid_content_type(self):
        response = self.fetch('/dataset/abc', method='POST', body='', headers={'Content-Type': 'text/html'})
        assert response.code == 415

    def test_upload_invalid_charset(self):
        response = self.fetch('/dataset/abc', method='POST', body='', headers={'Content-Type': 'text/csv; charset=iso-123'})
        assert response.code == 415


class TestInvalidQueries(SharedTest):
    def setUp(self):
        super(TestInvalidQueries, self).setUp()
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

    def do_invalid(self, q):
        response = self.query_json('/dataset/abc', q)
        assert response.code == 400
        return response

    def test_list_instead_of_dict(self):
        self.do_invalid(['where', ['==', 'foo', 1]])

    def test_json_not_possible_to_parse(self):
        response = self.fetch(url_concat('/dataset/abc', {'q': 'foo'}))
        assert response.code == 400

    def test_invalid_filter_format(self):
        response = self.do_invalid({'where': ['==', 'foo', 1, 2]})
        assert 'Invalid number of arguments' in json.loads(response.body)['error']

    def test_unknown_filter_operator(self):
        response = self.query_json('/dataset/abc', {'where': ['<>', 'foo', 1]})
        assert 'Unknown operator' in json.loads(response.body)['error']

    def test_unknown_select_operator(self):
        response = self.query_json('/dataset/abc', {'select': [['baz', 'foo']]})
        assert 'Unknown function' in json.loads(response.body)['error']

    def test_missing_column_in_select(self):
        response = self.query_json('/dataset/abc', {'select': ['baz', 'foo']})
        assert 'Selected column not in table' in json.loads(response.body)['error']

    def test_missing_column_in_filter(self):
        response = self.query_json('/dataset/abc', {'where': ['>', 'baz', 1]})
        assert 'is not defined' in json.loads(response.body)['error']

    def test_missing_column_in_group_by(self):
        response = self.query_json('/dataset/abc', {'group_by': ['baz']})
        assert 'Group by column not in table' in json.loads(response.body)['error']

    def test_missing_column_in_order_by(self):
        response = self.query_json('/dataset/abc', {'order_by': ['baz']})
        assert 'Order by column not in table' in json.loads(response.body)['error']

    def test_malformed_order_by(self):
        response = self.query_json('/dataset/abc', {'order_by': [['baz']]})
        assert 'Invalid order by format' in json.loads(response.body)['error']

    def test_wrong_type_for_offset(self):
        response = self.query_json('/dataset/abc', {'offset': 4.3})
        assert 'Invalid type' in json.loads(response.body)['error']

    def test_group_by_not_list(self):
        response = self.query_json('/dataset/abc', {'group_by': {'foo': 4.3}})
        assert 'Invalid format' in json.loads(response.body)['error']

# Error cases:
# - Malformed query
#   * Still some edge cases left in projection and filter.
# - Malformed input data
#   * No data sent in => error
#   * Wrong format specified
#   * Accepted format specified but cannot be encoded
#   * Non-uniform JSON and CSV
# - Non fitting data
#   * The data is too large to be fitted into memory of the current instance.


class TestCacheEviction(SharedTest):
    def get_app(self):
        # A cache size of 200 is trimmed for the below test cases
        return app.make_app(url_prefix='', max_cache_size=200)

    def test_evicts_entry_when_too_much_space_occupied(self):
        data = [{'some_longish_key': 'some_fairly_longish_value_that_needs_to_be_stuffed_in'},
                {'some_longish_key': 'another_fairly_longish_value_that_also_should_be_fitted'}]

        # Post data and assure available
        response = self.post_json('/dataset/abc', data)
        assert response.code == 201
        assert self.query_json('/dataset/abc', {}).code == 200

        response = self.post_json('/dataset/cba', data)
        assert response.code == 201

        # The old dataset has been evicted, the new one has taken its place
        assert self.query_json('/dataset/abc', {}).code == 404
        assert self.query_json('/dataset/cba', {}).code == 200

    def test_can_insert_more_entries_with_smaller_values(self):
        data = [{'some_longish_key': 'short'},
                {'some_longish_key': 'another_short'}]

        self.post_json('/dataset/abc', data)
        self.post_json('/dataset/cba', data)

        # Both datasets co-exist in the cache
        assert self.query_json('/dataset/abc', {}).code == 200
        assert self.query_json('/dataset/cba', {}).code == 200


class TestStatusEndpoint(SharedTest):
    def test_status_endpoint_returns_200_ok(self):
        response = self.fetch('/status')

        assert response.code == 200
        assert response.body == "OK"


class TestDatasetDelete(SharedTest):
    def test_post_data_then_delete(self):
        data = [{'some_key': '123456'}]
        self.post_json('/dataset/abc', data)

        assert self.query_json('/dataset/abc', {}).code == 200
        assert self.fetch('/dataset/abc', method='DELETE').code == 200
        assert self.query_json('/dataset/abc', {}).code == 404
