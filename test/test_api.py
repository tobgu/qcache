# coding=utf-8
import json
import os

import lz4 as lz4
import ssl

from tornado.httputil import url_concat
from tornado.testing import AsyncHTTPTestCase
from freezegun import freeze_time

import qcache
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
        return app.make_app(url_prefix='', debug=True)

    def post_json(self, url, data, extra_headers=None):
        if not isinstance(data, basestring):
            body = to_json(data)
        else:
            # Data already prepared by calling function
            body = data

        headers = {'Content-Type': 'application/json'}

        if extra_headers:
            headers.update(extra_headers)

        return self.fetch(url, method='POST', body=body, headers=headers, use_gzip=False)

    def query_json(self, url, query, extra_headers=None):
        url = url_concat(url, {'q': json.dumps(query)})
        headers = {'Accept': 'application/json, text/csv'}
        if extra_headers:
            headers.update(extra_headers)
        return self.fetch(url, headers=headers, use_gzip=False)

    def post_csv(self, url, data, types=None, extra_headers=None):
        headers = {'Content-Type': 'text/csv'}
        if types:
            headers['X-QCache-types'] = '; '.join('{column_name}={type_name}'.format(column_name=c, type_name=t)
                                                  for c, t in types.items())
        if extra_headers:
            headers.update(extra_headers)

        body = to_csv(data)
        return self.fetch(url, method='POST', body=body, headers=headers, use_gzip=False)

    def query_csv(self, url, query):
        url = url_concat(url, {'q': json.dumps(query)})
        return self.fetch(url, headers={'Accept': 'text/csv, application/json'}, use_gzip=False)

    def get_statistics(self):
        response = self.fetch('/statistics', use_gzip=False)
        assert response.code == 200
        return json.loads(response.body)


class TestBaseCases(SharedTest):
    def test_404_when_item_is_missing(self):
        url = url_concat('/dataset/abc', {'q': json.dumps('{}')})
        response = self.fetch(url)
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

    def test_division_by_zero(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 0}])
        assert response.code == 201

        # Result of division by 0 will be transmitted as null/None
        response = self.query_json('/dataset/abc', {'select': [['=', 'baz', ['/', 'foo', 'bar']]]})
        assert response.code == 200
        assert json.loads(response.body) == [{'baz': None}]


class TestQueryWithPost(SharedTest):
    def post_query_json(self, url, query):
        return self.fetch(url, headers={'Accept': 'application/json, text/csv', 'Content-Type': 'application/json'},
                          method="POST", body=to_json(query))

    def test_upload_json_post_query_json(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.post_query_json('/dataset/abc/q', {'where': ['==', 'foo', 1]})
        assert response.code == 200
        assert json.loads(response.body) == [{'foo': 1, 'bar': 10}]

    def test_upload_json_post_query_json_malformed_query(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.post_query_json('/dataset/abc/q', {'blabb': ['==', 'foo', 1]})
        assert response.code == 400

    def test_delete_against_q_endpoint_is_404(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.fetch('/dataset/abc/q', method='DELETE')
        assert response.code == 404

        response = self.fetch('/dataset/abc', method='DELETE')
        assert response.code == 200

    def test_get_against_q_endpoint_is_404(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.query_json('/dataset/abc/q', query={})
        assert response.code == 404

        response = self.query_json('/dataset/abc', query={})
        assert response.code == 200


class TestSlicing(SharedTest):
    def test_unsliced_size_header_indicates_the_dataset_size_before_slicing_it(self):
        # This helps out in pagination of data
        self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}, {'baz': 2, 'bar': 20}])

        # Fetch all data, the header value should be the same as the length of the response
        response = self.query_json('/dataset/cba', {})
        assert response.code == 200
        assert len(json.loads(response.body)) == 2
        assert response.headers['X-QCache-unsliced-length'] == '2'

        response = self.query_json('/dataset/cba', {"offset": 1})
        assert response.code == 200
        assert len(json.loads(response.body)) == 1
        assert response.headers['X-QCache-unsliced-length'] == '2'


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
        response = self.fetch('/dataset/abc', method='POST', body='',
                              headers={'Content-Type': 'text/csv; charset=iso-123'})
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
        assert 'Unknown aggregation function' in json.loads(response.body)['error']

    def test_missing_column_in_select(self):
        response = self.query_json('/dataset/abc', {'select': ['baz', 'foo']})
        assert 'Selected columns not in table' in json.loads(response.body)['error']

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
# * Still some edge cases left in projection and filter.
# - Malformed input data
#   * No data sent in => error
#   * Wrong format specified
#   * Accepted format specified but cannot be encoded
#   * Non-uniform JSON and CSV
# - Non fitting data
#   * The data is too large to be fitted into memory of the current instance.


class TestBitwiseQueries(SharedTest):
    def test_bitwise_query_succeeds(self):
        response = self.post_json('/dataset/abc', [{'foo': 1, 'bar': 10}, {'foo': 2, 'bar': 20}])
        assert response.code == 201

        response = self.query_json('/dataset/abc', {'where': ['all_bits', 'foo', 1]})
        assert response.code == 200


class TestCacheEvictionOnSize(SharedTest):
    def get_app(self):
        # A cache size of 200 is trimmed for the below test cases
        just_enough_to_fit_smaller_values = 315
        return app.make_app(url_prefix='', max_cache_size=just_enough_to_fit_smaller_values, debug=True)

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

        # Check statistics
        stats = self.get_statistics()

        assert stats['dataset_count'] == 1
        assert stats['cache_size'] == 370
        assert stats['hit_count'] == 2
        assert stats['miss_count'] == 1
        assert stats['size_evict_count'] == 1
        assert stats['store_count'] == 2
        assert stats['statistics_duration'] > 0.0
        assert stats['statistics_buffer_size'] == 1000
        assert len(stats['store_durations']) == 2
        assert len(stats['store_row_counts']) == 2
        assert sum(stats['store_row_counts']) == 4
        assert len(stats['query_durations']) == 2
        assert len(stats['durations_until_eviction']) == 1
        assert stats['durations_until_eviction'][0] > 0.0

        # Check stats again, this time it should have been cleared
        assert set(self.get_statistics().keys()) == \
               {'dataset_count', 'cache_size', 'statistics_duration', 'statistics_buffer_size'}

    def test_can_insert_more_entries_with_smaller_values(self):
        data = [{'some_longish_key': 'short'},
                {'some_longish_key': 'another_short'}]

        self.post_json('/dataset/abc', data)
        self.post_json('/dataset/cba', data)

        # Both datasets co-exist in the cache
        assert self.query_json('/dataset/abc', {}).code == 200
        assert self.query_json('/dataset/cba', {}).code == 200

    def test_query_stand_in_columns_do_not_interfere_with_cache_eviction(self):
        # Executing a query with stand in columns can increase the dataset
        # size after insert. This should not lead to failed handling of the
        # current cache size where the measured cache size gets smaller and
        # smaller which causes the actual cache size to grow. See #15.
        data = [{'some_longish_key': 'some_fairly_longish_value_that_needs_to_be_stuffed_in'},
                {'some_longish_key': 'another_fairly_longish_value_that_also_should_be_fitted'}]

        repetitions = 10
        for i in range(repetitions):
            response = self.post_json('/dataset/{i}'.format(i=i), data)
            assert response.code == 201
            assert self.query_json('/dataset/{i}'.format(i=i),
                                   {},
                                   extra_headers={'X-QCache-stand-in-columns': 'foo="bar_baz_qux"'}).code == 200

        stats = self.get_statistics()
        assert stats['dataset_count'] == 1
        assert stats['size_evict_count'] == repetitions - 1
        assert stats['cache_size'] == 370


class TestCacheEvictionOnAge(SharedTest):
    def get_app(self):
        # A cache size of 200 is trimmed for the below test cases
        return app.make_app(url_prefix='', max_age=5, debug=True)

    def test_evicts_dataset_when_data_too_old(self):
        with freeze_time('2015-10-22 00:00:00'):
            data = [{'some_longish_key': 'short'}]
            self.post_json('/dataset/abc', data)

        with freeze_time('2015-10-22 00:00:04'):
            assert self.query_json('/dataset/abc', {}).code == 200

        with freeze_time('2015-10-22 00:00:06'):
            assert self.query_json('/dataset/abc', {}).code == 404


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


class TestColumnTyping(SharedTest):
    def get(self, q, response_code=200):
        response = self.query_json('/dataset/abc', q)
        assert response.code == response_code
        return json.loads(response.body)

    def test_type_hint_string_on_column_with_only_integers(self):
        data = [
            {'some_key': '123456', 'another_key': 1111},
            {'some_key': 'abcdef', 'another_key': 2222}]

        self.post_csv('/dataset/abc', data, types={'another_key': 'string'})

        assert self.get({'where': ['==', 'another_key', '"2222"']}) == \
               [{'some_key': 'abcdef', 'another_key': '2222'}]

        # No matching item when querying by integer
        assert not self.get({'where': ['==', 'another_key', 2222]})

    def test_type_hinting_with_invalid_type_results_in_bad_request(self):
        # It's currently only possible to type hint strings and enums.
        # Is there ever a need for other type hints?

        data = [{'some_key': '123456', 'another_key': 1111}]
        response = self.post_csv('/dataset/abc', data, types={'another_key': 'int'})
        assert response.code == 400

    def test_type_hinting_with_enum(self):
        data = [{'some_key': 'aaa'}]
        response = self.post_csv('/dataset/abc', data, types={'some_key': 'enum'})
        assert response.code == 201

        assert self.get({'where': ['==', 'some_key', '"aaa"']}) == [
            {'some_key': 'aaa'}
        ]

    def test_type_int_to_string(self):
        def get(q, response_code=200):
            response = self.query_json('/dataset/abc', q)
            assert response.code == response_code
            return json.loads(response.body)

        data = [
            {'some_key': '123456', 'another_key': 1111},
            {'some_key': 'abcdef', 'another_key': 2222}]

        self.post_csv('/dataset/abc', data)

        # Querying on integer field
        assert get({'where': ['==', 'another_key', 2222]}) == \
               [{'some_key': 'abcdef', 'another_key': 2222}]

        get({'where': ['==', 'another_key', '2222']}, response_code=400)
        get({'where': ['==', 'another_key', '"2222"']}, response_code=400)

        # Querying on string field
        assert not get({'where': ['==', 'some_key', 123456]})
        get({'where': ['==', 'some_key', '123456']}, response_code=400)

        # Matching string
        assert get({'where': ['==', 'some_key', '"123456"']}) == \
               [{'some_key': '123456', 'another_key': 1111}]

        # Here abcdef is interpreted as another column. Since column abcdef
        # doesn't exist a 400, Bad request will be returned.
        get({'where': ['==', 'some_key', 'abcdef']}, response_code=400)


class TestStandInColumns(SharedTest):
    def test_stand_in_column_with_numeric_value(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                 extra_headers={'X-QCache-stand-in-columns': 'foo=13'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {'where': ['==', 'foo', 13]})
        assert response.code == 200
        result = json.loads(response.body)
        assert result == [{'baz': 1, 'bar': 10, 'foo': 13}]
        assert type(result[0]['foo']) == int

        response = self.query_json('/dataset/cba', {'where': ['==', 'foo', 14]})
        assert response.code == 200
        assert json.loads(response.body) == []

    def test_stand_in_column_with_string_value(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                 extra_headers={'X-QCache-stand-in-columns': 'foo="13"'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {'where': ['==', 'foo', '"13"']})
        assert response.code == 200
        assert json.loads(response.body) == [{'baz': 1, 'bar': 10, 'foo': "13"}]

    def test_stand_in_column_with_other_column(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}, {'baz': 2, 'bar': 20}],
                                 extra_headers={'X-QCache-stand-in-columns': 'foo=bar'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {'where': ['==', 'foo', 20]})
        assert response.code == 200
        assert json.loads(response.body) == [{'baz': 2, 'bar': 20, 'foo': 20}]

    def test_multiple_stand_in_columns(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                 extra_headers={'X-QCache-stand-in-columns': 'foo=bar; qux=13'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {})
        assert response.code == 200
        assert json.loads(response.body) == [{'baz': 1, 'bar': 10, 'foo': 10, 'qux': 13}]

    def test_chained_stand_in_columns(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                 extra_headers={'X-QCache-stand-in-columns': 'foo=13; qux=foo'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {})
        assert response.code == 200
        assert json.loads(response.body) == [{'baz': 1, 'bar': 10, 'foo': 13, 'qux': 13}]

    def test_json_stand_in_columns(self):
        response = self.post_json('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                  extra_headers={'X-QCache-stand-in-columns': 'foo=13'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {})
        assert json.loads(response.body) == [{'baz': 1, 'bar': 10, 'foo': 13}]

    def test_stand_in_column_not_applied_when_column_exists_in_submitted_data(self):
        response = self.post_csv('/dataset/cba', [{'baz': 1, 'bar': 10}],
                                 extra_headers={'X-QCache-stand-in-columns': 'bar=13'})
        assert response.code == 201

        response = self.query_json('/dataset/cba', {})
        assert json.loads(response.body) == [{'baz': 1, 'bar': 10}]

    def test_stand_in_columns_in_query(self):
        response = self.post_csv('/dataset/cba', [{'foo': 1}])
        assert response.code == 201

        response = self.query_json('/dataset/cba', {}, extra_headers={'X-QCache-stand-in-columns': 'bar=13;baz=foo'})
        assert json.loads(response.body) == [{'foo': 1, 'bar': 13, 'baz': 1}]


class TestCompression(SharedTest):
    def call_api_with_compression(self, accept_encoding, content_encoding, decoding_fn, encoding_fn, expected_encoding):
        input_data = 10000 * [{'foo': 1, 'bar': 10}]
        data = encoding_fn(to_json(input_data))

        response = self.post_json('/dataset/abc', data, extra_headers={'Content-Encoding': content_encoding})
        assert response.code == 201

        response = self.query_json('/dataset/abc', query={}, extra_headers={'Accept-Encoding': accept_encoding})

        assert response.code == 200
        assert response.headers.get('Content-Encoding') == expected_encoding
        assert json.loads(decoding_fn(response.body)) == input_data

    def test_upload_gzip_accept_gzip(self):
        self.call_api_with_compression(accept_encoding='gzip',
                                       content_encoding='gzip',
                                       decoding_fn=qcache.compression.gzip_loads,
                                       encoding_fn=qcache.compression.gzip_dumps,
                                       expected_encoding='gzip')

    def test_upload_lz4_accept_lz4(self):
        self.call_api_with_compression(accept_encoding='lz4',
                                       content_encoding='lz4',
                                       decoding_fn=lz4.block.decompress,
                                       encoding_fn=lz4.block.compress,
                                       expected_encoding='lz4')

    def test_upload_lz4_accept_gzip(self):
        self.call_api_with_compression(accept_encoding='lz4',
                                       content_encoding='gzip',
                                       decoding_fn=lz4.block.decompress,
                                       encoding_fn=qcache.compression.gzip_dumps,
                                       expected_encoding='lz4')

    def test_prefer_lz4_if_multiple_supported_encodings_exists(self):
        self.call_api_with_compression(accept_encoding='compress,gzip,lz4',
                                       content_encoding='gzip',
                                       decoding_fn=lz4.block.decompress,
                                       encoding_fn=qcache.compression.gzip_dumps,
                                       expected_encoding='lz4')

    def test_unknown_accept_encoding_results_in_no_response_compression(self):
        self.call_api_with_compression(accept_encoding='foo,bar',
                                       content_encoding='lz4',
                                       decoding_fn=lambda x: x,
                                       encoding_fn=lz4.block.compress,
                                       expected_encoding=None)

    def test_upload_with_unknown_encoding_results_in_400(self):
        data = to_json([{'foo': 'bar'}])
        response = self.post_json('/dataset/abc', data, extra_headers={'Content-Encoding': 'baz'})
        assert response.code == 400
        assert 'Unrecognized encoding' in response.body

    def test_only_200_responses_are_compressed(self):
        data = to_json([{'foo': 'bar'}])
        response = self.post_json('/dataset/abc', data)
        assert response.code == 201

        response = self.query_json('/dataset/non_present_dataset', query={}, extra_headers={'Accept-Encoding': 'lz4'})
        assert response.code == 404
        assert response.headers.get('Content-Encoding') is None


class TestStatistics(SharedTest):
    def test_store_and_query_durations(self):
        assert self.post_json('/dataset/abc', [{'foo': 123}]).code == 201
        assert self.query_json('/dataset/abc', query={}).code == 200

        stats = self.get_statistics()

        assert len(stats['query_durations']) == 1
        assert len(stats['store_durations']) == 1
        assert len(stats['query_request_durations']) == 1
        assert len(stats['store_request_durations']) == 1

        assert stats['query_durations'][0] < stats['query_request_durations'][0]
        assert stats['store_durations'][0] < stats['store_request_durations'][0]


class SSLTestBase(AsyncHTTPTestCase):
    TLS_DIR = os.path.join(os.path.dirname(__file__), '../tls/')

    def get_app(self):
        return app.make_app(url_prefix='', debug=True)

    def get_protocol(self):
        return 'https'

    def get_ssl_version(self):
        raise NotImplementedError()

    def get_httpserver_options(self):
        # By default don't require client certificate. Override in subclasses where client
        # certs are tested.
        return app.ssl_options(certfile=self.TLS_DIR + 'host.pem')

    def fetch(self, path, **kwargs):
        if 'validate_cert' not in kwargs:
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH,
                                                     cafile=os.path.join(self.TLS_DIR, 'ca.pem'))

            if 'client_cert' in kwargs:
                ssl_context.load_cert_chain(kwargs['client_cert'])

            kwargs['ssl_options'] = ssl_context

        return super(SSLTestBase, self).fetch(path=path, **kwargs)


class TestSSLServerWithSSL(SSLTestBase):
    def test_fetch_status(self):
        response = self.fetch('/status')
        assert response.code == 200

    def test_fetch_status_no_cert_validation(self):
        response = self.fetch('/status', validate_cert=False)
        assert response.code == 200


class TestSSLServerWithSSLClientCertVerification(SSLTestBase):
    def get_httpserver_options(self):
        return app.ssl_options(certfile=self.TLS_DIR + 'host.pem',
                               cafile=self.TLS_DIR + 'ca.pem')

    def test_fetch_status(self):
        response = self.fetch('/status', client_cert=self.TLS_DIR + 'host.pem')
        assert response.code == 200

    def test_fetch_status_no_client_cert_supplied(self):
        response = self.fetch('/status')
        assert response.code == 599


class TestSSLServerWithoutSSL(SSLTestBase):
    def get_protocol(self):
        return 'http'

    def test_fetch_status(self):
        response = self.fetch('/status')
        assert response.code == 599


class TestSSLServerWithSSLAndBasicAuth(SSLTestBase):
    def get_app(self):
        return app.make_app(url_prefix='', debug=True, basic_auth='foo:bar')

    def test_fetch_status_correct_credentials(self):
        response = self.fetch('/status', auth_username='foo', auth_password='bar')
        assert response.code == 200

    def test_fetch_status_incorrect_password(self):
        response = self.fetch('/status', auth_username='foo', auth_password='ba')
        assert response.code == 401

    def test_fetch_status_unknown_user(self):
        response = self.fetch('/status', auth_username='fo', auth_password='bar')
        assert response.code == 401

    def test_fetch_status_missing_credentials(self):
        response = self.fetch('/status')
        assert response.code == 401

    def test_fetch_data_missing_credentials(self):
        response = self.fetch('/dataset/XYZ')
        assert response.code == 401

    def test_fetch_data_correct_credentials(self):
        url = url_concat('/dataset/XYZ', {'q': json.dumps('{}')})
        response = self.fetch(url, auth_username='foo', auth_password='bar')
        assert response.code == 404

    def test_fetch_statistics_missing_credentials(self):
        response = self.fetch('/statistics')
        assert response.code == 401

    def test_fetch_statistics_correct_credentials(self):
        response = self.fetch('/statistics', auth_username='foo', auth_password='bar')
        assert response.code == 200

        # Delete against a Q endpoint is a 404
        # Get against a Q endpoint is a 404
