from qcache.qframe.constants import FILTER_ENGINE_NUMEXPR

from qcache.sharded_cache import ShardedCache


def test_insert_query_delete():
    cache = ShardedCache(statistics_buffer_size=100,
                         max_cache_size=100000,
                         max_age=100,
                         default_filter_engine=FILTER_ENGINE_NUMEXPR,
                         cache_count=3)

    cache.stop()
