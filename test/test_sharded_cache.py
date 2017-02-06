import json
import random
import string
from contextlib import contextmanager

import pytest

from qcache.cache_common import QueryResult, InsertResult, DeleteResult
from qcache.constants import CONTENT_TYPE_CSV, CONTENT_TYPE_JSON
from qcache.in_process_cache import InProcessCache
from qcache.qframe import QFrame
from qcache.qframe.constants import FILTER_ENGINE_NUMEXPR

from qcache.sharded_cache import ShardedCache


@pytest.fixture
def basic_csv_frame():
    return """
index,foo,bar,baz,qux
1,bbb,1.25,5,qqq
2,aaa,3.25,7,qqq
3,ccc,,9,www"""


@contextmanager
def sharded_cache(cache_type,
                  statistics_buffer_size=100,
                  max_cache_size=100000,
                  max_age=100,
                  default_filter_engine=FILTER_ENGINE_NUMEXPR,
                  cache_count=3):
    if cache_type == 'sharded':
        cache = ShardedCache(statistics_buffer_size=statistics_buffer_size,
                             max_cache_size=max_cache_size,
                             max_age=max_age,
                             default_filter_engine=default_filter_engine,
                             cache_count=cache_count)
    else:
        cache = InProcessCache(statistics_buffer_size=statistics_buffer_size,
                               max_cache_size=max_cache_size,
                               max_age=max_age,
                               default_filter_engine=default_filter_engine)
    try:
        yield cache
    finally:
        cache.stop()


@pytest.fixture(scope='session', params=['sharded', 'in_process'])
def cache_type(request):
    return request.param


def random_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))


def test_insert_query_delete(cache_type, basic_csv_frame):
    with sharded_cache(cache_type) as cache:
        key = random_key()
        limit = 2
        insert_result = cache.insert(dataset_key=key,
                                     data=basic_csv_frame,
                                     content_type=CONTENT_TYPE_CSV,
                                     data_types={'index': 'string'},
                                     stand_in_columns=[('extra_insert', '42')])

        assert insert_result.status == InsertResult.STATUS_SUCCESS

        query_result = cache.query(dataset_key=key,
                                   q={'limit': limit},
                                   filter_engine=None,
                                   stand_in_columns=[('extra_query', '24')],
                                   accept_type=CONTENT_TYPE_JSON)

        assert query_result.status == QueryResult.STATUS_SUCCESS
        assert query_result.content_type == CONTENT_TYPE_JSON
        assert query_result.unsliced_length == 3
        result = json.loads(query_result.data)
        assert result == [{u'bar': 1.25,
                           u'baz': 5,
                           u'extra_insert': 42.0,
                           u'extra_query': 24.0,
                           u'foo': u'bbb',
                           u'index': u'1',
                           u'qux': u'qqq'},
                          {u'bar': 3.25,
                           u'baz': 7,
                           u'extra_insert': 42.0,
                           u'extra_query': 24.0,
                           u'foo': u'aaa',
                           u'index': u'2',
                           u'qux': u'qqq'}]


def test_insert_delete(cache_type, basic_csv_frame):
    with sharded_cache(cache_type) as cache:
        key = random_key()
        limit = 2
        insert_result = cache.insert(dataset_key=key,
                                     data=basic_csv_frame,
                                     content_type=CONTENT_TYPE_CSV,
                                     data_types={'index': 'string'},
                                     stand_in_columns=[('extra_insert', '42')])
        assert insert_result.status == InsertResult.STATUS_SUCCESS

        query_result = cache.query(dataset_key=key,
                                   q={'limit': limit},
                                   filter_engine=None,
                                   stand_in_columns=[('extra_query', '24')],
                                   accept_type=CONTENT_TYPE_JSON)
        assert query_result.status == QueryResult.STATUS_SUCCESS

        delete_result = cache.delete(dataset_key=key)
        assert delete_result.status == DeleteResult.STATUS_SUCCESS

        query_result = cache.query(dataset_key=key,
                                   q={},
                                   filter_engine=None,
                                   stand_in_columns=[],
                                   accept_type=CONTENT_TYPE_JSON)
        assert query_result.status == QueryResult.STATUS_NOT_FOUND


@pytest.fixture
def large_csv_frame():
    d = [{'aaa': random.randint(0, 10000000), 'bbb': random_key(), 'ccc': random.random() * 100000} for _ in range(100)]
    return QFrame.from_dicts(d).to_csv()


def test_statistics(cache_type, large_csv_frame):
    with sharded_cache(cache_type, max_cache_size=100000) as cache:
        for _ in range(100):
            key = random_key()
            insert_result = cache.insert(dataset_key=key,
                                         data=large_csv_frame,
                                         content_type=CONTENT_TYPE_CSV,
                                         data_types={'index': 'string'},
                                         stand_in_columns=[('extra_insert', '42')])
            assert insert_result.status == InsertResult.STATUS_SUCCESS

            query_result = cache.query(dataset_key=key,
                                       q={'limit': 200},
                                       filter_engine=None,
                                       stand_in_columns=[('extra_query', '24')],
                                       accept_type=CONTENT_TYPE_JSON)
            assert query_result.status == QueryResult.STATUS_SUCCESS

        stats = cache.statistics()
        assert set(stats) == {'cache_size',
                              'cache_size',
                              'dataset_count',
                              'durations_until_eviction',
                              'hit_count',
                              'query_durations',
                              'query_request_durations',
                              'size_evict_count',
                              'statistics_buffer_size',
                              'statistics_duration',
                              'store_count',
                              'store_durations',
                              'store_request_durations',
                              'store_row_counts'}

        assert stats['cache_size'] >= 90000
        assert stats['dataset_count'] >= 10
        assert len(stats['store_durations']) == 100
        assert len(stats['durations_until_eviction']) >= 80
