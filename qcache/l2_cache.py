from qcache.cache_common import InsertResult, DeleteResult
from qcache.dataset_cache import DatasetCache
from qcache.statistics import Statistics


class GetResult(object):
    STATUS_NOT_FOUND = "not_found"
    STATUS_SUCCESS = "success"

    def __init__(self, status, data=None):
        self.status = status
        self.data = data


class DataWrapper(object):
    __slots__ = ('data',)

    def __init__(self, data):
        self.data = data

    def byte_size(self):
        return len(self.data)


class L2CacheHandle(object):
    def __init__(self, statistics_buffer_size, max_age, max_size):
        self.dataset_cache = DatasetCache(max_age=max_age, max_size=max_size)
        self.stats = Statistics(buffer_size=statistics_buffer_size)

    def insert(self, dataset_key, data):
        if self.dataset_cache.max_size <= 0:
            # TODO Temporary fix
            return InsertResult(status=InsertResult.STATUS_SUCCESS)

        if dataset_key in self.dataset_cache:
            self.stats.inc('replace_count')
            del self.dataset_cache[dataset_key]

        wrapped_data = DataWrapper(data)
        durations_until_eviction = self.dataset_cache.ensure_free(wrapped_data.byte_size())
        self.dataset_cache[dataset_key] = wrapped_data
        self.stats.inc('size_evict_count', count=len(durations_until_eviction))
        self.stats.inc('store_count')
        self.stats.extend('durations_until_eviction', durations_until_eviction)
        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def get(self, dataset_key):
        if dataset_key not in self.dataset_cache:
            self.stats.inc('miss_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        if self.dataset_cache.evict_if_too_old(dataset_key):
            self.stats.inc('miss_count')
            self.stats.inc('age_evict_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        return GetResult(status=GetResult.STATUS_SUCCESS, data=self.dataset_cache[dataset_key].data)

    def delete(self, dataset_key):
        self.dataset_cache.delete(dataset_key)
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self):
        stats = self.stats.snapshot()
        stats['dataset_count'] = len(self.dataset_cache)
        stats['cache_size'] = self.dataset_cache.size
        return stats

    def status(self):
        return "OK"

    def reset(self):
        self.dataset_cache.reset()
        self.stats.reset()


def create_l2_cache(statistics_buffer_size, max_age, size):
    return L2CacheHandle(statistics_buffer_size, max_age, size)
