import datetime
import time

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

    @property
    def size(self):
        # 100 bytes is just a very rough estimate of the object overhead of this instance
        return 100 + self._dataset.memory_usage(index=True).sum()


class DatasetCache(object):
    def __init__(self, max_size):
        self.max_size = max_size
        self._cache_dict = {}

    @property
    def size(self):
        return sum(df.size for df in self._cache_dict.values())

    def __contains__(self, item):
        return item in self._cache_dict

    def __getitem__(self, item):
        return self._cache_dict[item].dataset

    def __setitem__(self, key, df):
        self._cache_dict[key] = CacheItem(df)

    def __delitem__(self, key):
        del self._cache_dict[key]

    def ensure_free(self, byte_count):
        if byte_count > self.max_size:
            raise Exception('Impossible to allocate')

        current_size = self.size
        free_size = self.max_size - current_size
        if free_size >= byte_count:
            return

        requirement = byte_count - free_size

        # This is not very efficient but good enough for now
        lru_datasets = sorted(self._cache_dict.items(), key=lambda item: item[1].last_access_time)
        for key, dataset in lru_datasets:
            requirement -= dataset.size
            del self._cache_dict[key]
            if requirement <= 0:
                return
