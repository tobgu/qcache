from time import time


# TODO: Rename this file and classes to "Map"s instead to avoid confusion with
#       the new higher level caches?

class CacheItem(object):
    def __init__(self, dataset):
        self.creation_time = time()
        self.last_access_time = self.creation_time
        self._dataset = dataset
        self.access_count = 0

    @property
    def dataset(self):
        self.last_access_time = time()
        self.access_count += 1
        return self._dataset

    @property
    def size(self):
        # 100 bytes is just a very rough estimate of the object overhead of this instance
        size = 100 + self._dataset.byte_size()
        return size


class DatasetCache(object):
    def __init__(self, max_size, max_age):
        self.max_size = max_size
        self.max_age = max_age
        self.reset()

    def has_expired(self, item):
        return self.max_age and time() > item.creation_time + self.max_age

    def evict_if_too_old(self, key):
        if self.has_expired(self._cache_dict[key]):
            del self[key]
            return True

        return False

    def reset(self):
        self._cache_dict = {}
        self.size = 0.0

    def __contains__(self, key):
        return key in self._cache_dict

    def __getitem__(self, item):
        return self._cache_dict[item].dataset

    def __setitem__(self, key, qframe):
        current_size = 0.0
        if key in self._cache_dict:
            current_size = self._cache_dict[key].size

        new_item = CacheItem(qframe)
        self.size += new_item.size - current_size
        self._cache_dict[key] = new_item

    def __delitem__(self, key):
        self.size -= self._cache_dict[key].size
        del self._cache_dict[key]

    def __len__(self):
        return len(self._cache_dict)

    def delete(self, key):
        if key in self:
            del self[key]

    def ensure_free(self, byte_count):
        """
        :return: A list of durations in seconds that the dataset spent in the cache before
                 being evicted.
        """
        if byte_count > self.max_size:
            raise Exception('Impossible to allocate')

        if self.max_size - self.size >= byte_count:
            return []

        # This is not very efficient but good enough for now
        lru_datasets = sorted(self._cache_dict.items(), key=lambda item: item[1].last_access_time)
        now = time()
        durations_until_eviction = []
        for key, _ in lru_datasets:
            durations_until_eviction.append(now - self._cache_dict[key].creation_time)
            del self[key]
            if self.max_size - self.size >= byte_count:
                break

        return durations_until_eviction
