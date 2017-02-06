import json
import time
import gc

from qcache.cache_common import QueryResult, InsertResult, UTF8JSONDecoder
from qcache.constants import CONTENT_TYPE_CSV
from qcache.dataset_cache import DatasetCache
from qcache.qframe import MalformedQueryException, QFrame
from qcache.statistics import Statistics


class InProcessCache(object):
    def __init__(self, statistics_buffer_size, max_cache_size, max_age, default_filter_engine):
        self.stats = Statistics(buffer_size=statistics_buffer_size)
        self.dataset_cache = DatasetCache(max_size=max_cache_size, max_age=max_age)
        self.query_count = 0
        self.default_filter_engine = default_filter_engine

    def _post_query_processing(self):
        if self.query_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact query performance negatively.
            gc.collect()

        self.query_count += 1

    def query(self, dataset_key, q, filter_engine, stand_in_columns, accept_type):
        t0 = time.time()
        filter_engine = filter_engine or self.default_filter_engine
        if dataset_key not in self.dataset_cache:
            self.stats.inc('miss_count')
            return QueryResult(status=QueryResult.STATUS_NOT_FOUND)

        if self.dataset_cache.evict_if_too_old(dataset_key):
            self.stats.inc('miss_count')
            self.stats.inc('age_evict_count')
            return QueryResult(status=QueryResult.STATUS_NOT_FOUND)

        qf = self.dataset_cache[dataset_key]
        try:
            result_frame = qf.query(q, filter_engine=filter_engine, stand_in_columns=stand_in_columns)
        except MalformedQueryException as e:
            return QueryResult(status=QueryResult.STATUS_MALFORMED_QUERY, data=str(e))

        self.stats.inc('hit_count')
        result = QueryResult(status=QueryResult.STATUS_SUCCESS,
                             data=result_frame.to_csv() if accept_type == CONTENT_TYPE_CSV else result_frame.to_json(),
                             content_type=accept_type,
                             unsliced_length=result_frame.unsliced_df_len,
                             query_stats={})

        duration = time.time() - t0
        self.stats.append('query_durations', duration)

        # TODO: Remove this one, it was not very interesting
        self.stats.append('query_request_durations', duration+0.000001)
        return result

    def insert(self, dataset_key, data, content_type, data_types, stand_in_columns):
        # TODO: Move unpacking to in here to be able to kill the buffer once used?
        t0 = time.time()
        if dataset_key in self.dataset_cache:
            self.stats.inc('replace_count')
            del self.dataset_cache[dataset_key]

        if content_type == CONTENT_TYPE_CSV:
            durations_until_eviction = self.dataset_cache.ensure_free(len(data))
            qf = QFrame.from_csv(data, column_types=data_types, stand_in_columns=stand_in_columns)
        else:
            # This is a waste of CPU cycles, first the JSON decoder decodes all strings
            # from UTF-8 then we immediately encode them back into UTF-8. Couldn't
            # find an easy solution to this though.
            durations_until_eviction = self.dataset_cache.ensure_free(len(data) / 2)
            data = json.loads(data, cls=UTF8JSONDecoder)
            qf = QFrame.from_dicts(data, stand_in_columns=stand_in_columns)

        self.dataset_cache[dataset_key] = qf
        self.stats.inc('size_evict_count', count=len(durations_until_eviction))
        self.stats.inc('store_count')
        self.stats.append('store_row_counts', len(qf))
        self.stats.extend('durations_until_eviction', durations_until_eviction)

        duration = time.time() - t0
        self.stats.append('store_durations', duration)
        # TODO: Remove this one, it was not very interesting
        self.stats.append('store_request_durations', duration+0.000001)

        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def delete(self, dataset_key):
        if dataset_key in self.dataset_cache:
            del self.dataset_cache[dataset_key]

    def statistics(self):
        stats = self.stats.snapshot()
        stats['dataset_count'] = len(self.dataset_cache)
        stats['cache_size'] = self.dataset_cache.size
        return stats

    def stop(self):
        # Part of the interface but nothing to do in this implementation
        pass
