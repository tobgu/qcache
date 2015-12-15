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
        self.size = 0.0

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
        current_size = 0.0
        if key in self._cache_dict:
            current_size = self._cache_dict[key].size

        new_item = CacheItem(qframe)
        self.size += new_item.size - current_size
        self._cache_dict[key] = new_item

    def __delitem__(self, key):
        self.size -= self._cache_dict[key].size
        del self._cache_dict[key]

    def ensure_free(self, byte_count):
        """
        :return: The number of evicted datasets
        """
        if byte_count > self.max_size:
            raise Exception('Impossible to allocate')

        if self.max_size - self.size >= byte_count:
            return 0

        # This is not very efficient but good enough for now
        lru_datasets = sorted(self._cache_dict.items(), key=lambda item: item[1].last_access_time)
        count = 0
        for key, _ in lru_datasets:
            del self[key]
            count += 1
            if self.max_size - self.size >= byte_count:
                break

        return count
