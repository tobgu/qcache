import traceback
from multiprocessing import Process

import zmq

from qcache.cache.cache_common import InsertResult, DeleteResult
from qcache.cache.dataset_cache import DatasetCache
from qcache.cache.ipc import ProcessHandle, STOP_COMMAND, receive_serialized_objects, serialize_object, \
    deserialize_object, send_serialized_objects, send_objects
from qcache.cache.statistics import Statistics


class L2CacheException(Exception):
    pass


class GetResult(object):
    STATUS_NOT_FOUND = "not_found"
    STATUS_SUCCESS = "success"

    def __init__(self, status):
        self.status = status
        self.data = None


class DataWrapper(object):
    __slots__ = ('data',)

    def __init__(self, data):
        self.data = data

    def byte_size(self):
        return len(self.data)


class L2Cache(object):
    def __init__(self, statistics_buffer_size, max_age, max_size):
        self.dataset_cache = DatasetCache(max_age=max_age, max_size=max_size)
        self.stats = Statistics(buffer_size=statistics_buffer_size)

    def insert(self, dataset_key, data):
        if dataset_key in self.dataset_cache:
            self.stats.inc('l2_replace_count')
            del self.dataset_cache[dataset_key]

        wrapped_data = DataWrapper(data)
        durations_until_eviction = self.dataset_cache.ensure_free(wrapped_data.byte_size())
        self.dataset_cache[dataset_key] = wrapped_data
        self.stats.inc('l2_size_evict_count', count=len(durations_until_eviction))
        self.stats.inc('l2_store_count')
        self.stats.extend('l2_durations_until_eviction', durations_until_eviction)
        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def get(self, dataset_key):
        if dataset_key not in self.dataset_cache:
            self.stats.inc('l2_miss_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        if self.dataset_cache.evict_if_too_old(dataset_key):
            self.stats.inc('l2_miss_count')
            self.stats.inc('l2_age_evict_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        self.stats.inc('l2_hit_count')
        return GetResult(status=GetResult.STATUS_SUCCESS), self.dataset_cache[dataset_key].data

    def delete(self, dataset_key):
        self.dataset_cache.delete(dataset_key)
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self):
        stats = self.stats.snapshot()
        stats['l2_dataset_count'] = len(self.dataset_cache)
        stats['l2_cache_size'] = self.dataset_cache.size
        return stats

    def status(self):
        return "OK"

    def reset(self):
        self.dataset_cache.reset()
        self.stats.reset()
        return None


class NopL2CacheHandle(object):
    """
    L2 cache implementation with NOPs for all operations.

    Used when L2 caching is not activated.
    """
    def insert(self, dataset_key, data):
        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def get(self, dataset_key):
        return GetResult(status=GetResult.STATUS_NOT_FOUND)

    def delete(self, dataset_key):
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self):
        return {'l2_cache_size': 0, 'l2_dataset_count': 0}

    def status(self):
        return "OK"

    def reset(self):
        pass

    def stop(self):
        pass


class DatasetCommand(object):
    def __init__(self, dataset_key):
        self.dataset_key = dataset_key


class InsertCommand(DatasetCommand):
    def execute(self, l2cache, data):
        return l2cache.insert(self.dataset_key, data)


class GetCommand(DatasetCommand):
    def execute(self, l2cache, _):
        return l2cache.get(self.dataset_key)


class DeleteCommand(DatasetCommand):
    def execute(self, l2cache, _):
        return l2cache.delete(self.dataset_key)


class StatisticsCommand(object):
    def execute(self, l2cache, _):
        return l2cache.statistics()


class StatusCommand(object):
    def execute(self, l2cache, _):
        return l2cache.status()


class ResetCommnad(object):
    def execute(self, l2cache, _):
        return l2cache.reset()


class L2CacheHandle(object):
    def __init__(self, process_handle):
        self.process_handle = process_handle

    def insert(self, dataset_key, data):
        # Get is the only method that actually sends two objects from the cache,
        # the insert command and the actual data.
        serialized_insert = serialize_object(InsertCommand(dataset_key))
        self.process_handle.send_serialized_objects(serialized_insert, data)
        result, _ = self.process_handle.receive_objects()
        return result

    def get(self, dataset_key):
        # Get is the only method that actually receives two objects from the cache,
        # the result object and the actual data.
        self.process_handle.send_objects(GetCommand(dataset_key))
        serialized_objects = self.process_handle.receive_serialized_objects()
        result = deserialize_object(serialized_objects[0])
        result.data = serialized_objects[1]
        return result

    def run_command(self, command):
        self.process_handle.send_objects(command)
        return self.process_handle.receive_objects()[0]

    def delete(self, dataset_key):
        return self.run_command(DeleteCommand(dataset_key))

    def statistics(self):
        return self.run_command(StatisticsCommand())

    def status(self):
        return self.run_command(StatusCommand())

    def reset(self):
        return self.run_command(StatisticsCommand())

    def stop(self):
        return self.process_handle.stop()


def l2_cache_process(ipc_address, statistics_buffer_size, max_cache_size, max_age):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(ipc_address)
    l2_cache = L2Cache(statistics_buffer_size=statistics_buffer_size,
                       max_size=max_cache_size,
                       max_age=max_age)
    while True:
        try:
            objects = receive_serialized_objects(socket)
            command = deserialize_object(objects[0])
            input_data = None
            if len(objects) == 2:
                input_data = objects[1]

            if command == STOP_COMMAND:
                return

            response = command.execute(l2_cache, input_data)
            result, output_data = response if isinstance(response, tuple) else (response, serialize_object(None))
            send_serialized_objects(socket, serialize_object(result), output_data)
        except Exception as e:
            send_objects(socket, L2CacheException(e))
            traceback.print_exc()


def create_l2_cache(statistics_buffer_size, max_age, max_size):
    if max_size <= 0:
        return NopL2CacheHandle()

    ipc_address = 'ipc:///tmp/qcache_ipc_l2_cache'
    p = Process(name='qcache_l2_cache',
                target=l2_cache_process,
                args=(ipc_address, statistics_buffer_size, max_size, max_age))
    p.start()
    return L2CacheHandle(ProcessHandle(p, ipc_address))
