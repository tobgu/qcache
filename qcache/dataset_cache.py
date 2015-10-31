from datetime import datetime, timedelta

class CacheItem(object):
    def __init__(self, qframe):
        self.creation_time = datetime.utcnow()
        self.last_access_time = self.creation_time
        self._qframe = qframe
        self.access_count = 0

    @property
    def dataset(self):
        self.last_access_time = datetime.utcnow()
        self.access_count += 1
        return self._qframe

    @property
    def size(self):
        # 100 bytes is just a very rough estimate of the object overhead of this instance
        return 100 + self._qframe.byte_size()


class DatasetCache(object):
    def __init__(self, max_size, max_age):
        self.max_size = max_size
        self.max_age = timedelta(seconds=max_age)
        self._cache_dict = {}

    @property
    def size(self):
        return sum(item.size for item in self._cache_dict.values())

    def has_expired(self, item):
        return self.max_age and datetime.utcnow() > item.creation_time + self.max_age

    def evict_if_too_old(self, key):
        if self.has_expired(self._cache_dict[key]):
            del self[key]
            return True

        return False

    def __contains__(self, key):
        return key in self._cache_dict

    def __getitem__(self, item):
        return self._cache_dict[item].dataset

    def __setitem__(self, key, qframe):
        self._cache_dict[key] = CacheItem(qframe)

    def __delitem__(self, key):
        del self._cache_dict[key]

    def ensure_free(self, byte_count):
        """
        :return: The number of evicted datasets
        """
        if byte_count > self.max_size:
            raise Exception('Impossible to allocate')

        current_size = self.size
        free_size = self.max_size - current_size
        if free_size >= byte_count:
            return 0

        requirement = byte_count - free_size

        # This is not very efficient but good enough for now
        lru_datasets = sorted(self._cache_dict.items(), key=lambda item: item[1].last_access_time)
        count = 0
        for key, dataset in lru_datasets:
            requirement -= dataset.size
            del self._cache_dict[key]
            count += 1
            if requirement <= 0:
                return count
