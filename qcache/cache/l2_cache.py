"""
'Layer 2 cache' used to store data in a more memory efficient way. Data in the
L2 cache cannot be queried directly but must be moved into the primary cache.

Unlike the primary cache the layer 2 cache stores opaque bytes objects that
can be inserted, accessed and deleted by key.
"""
import traceback
from multiprocessing import Process

import time

import gc
import zmq
from setproctitle import setproctitle

from abc import ABCMeta, abstractmethod

from qcache.cache.cache_common import InsertResult, DeleteResult, Result
from qcache.cache.dataset_cache import DatasetMap
from qcache.cache.ipc import ProcessHandle, STOP_COMMAND, receive_serialized_objects, serialize_object, \
    deserialize_object, send_serialized_objects, send_objects, STATUS_OK
from qcache.cache.statistics import Statistics


class DataWrapper(object):
    """
    Thin wrapper around an object that supports the "len()" to make it compatible
    with the DatasetMap.
    """
    __slots__ = ('data',)

    def __init__(self, data):
        self.data = data

    def byte_size(self):
        return len(self.data)


class L2Cache(object):
    """
    Layer 2 cache server process logic.
    """
    def __init__(self, statistics_buffer_size, max_age, max_size):
        self.dataset_map = DatasetMap(max_age=max_age, max_size=max_size)
        self.stats = Statistics(buffer_size=statistics_buffer_size)
        self.insert_count = 0

    def insert(self, dataset_key, data):
        if dataset_key in self.dataset_map:
            self.stats.inc('l2_replace_count')
            del self.dataset_map[dataset_key]

        wrapped_data = DataWrapper(data)
        durations_until_eviction = self.dataset_map.ensure_free(wrapped_data.byte_size())
        self.dataset_map[dataset_key] = wrapped_data
        self.stats.inc('l2_size_evict_count', count=len(durations_until_eviction))
        self.stats.inc('l2_store_count')
        self.stats.extend('l2_durations_until_eviction', durations_until_eviction)
        self.insert_count += 1
        if self.insert_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact performance negatively.
            gc.collect()

        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def get(self, dataset_key):
        if dataset_key not in self.dataset_map:
            self.stats.inc('l2_miss_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        if self.dataset_map.evict_if_too_old(dataset_key):
            self.stats.inc('l2_miss_count')
            self.stats.inc('l2_age_evict_count')
            return GetResult(status=GetResult.STATUS_NOT_FOUND)

        self.stats.inc('l2_hit_count')
        return GetResult(status=GetResult.STATUS_SUCCESS), self.dataset_map[dataset_key].data

    def delete(self, dataset_key):
        self.dataset_map.delete(dataset_key)
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self):
        stats = self.stats.snapshot()
        stats['l2_dataset_count'] = len(self.dataset_map)
        stats['l2_cache_size'] = self.dataset_map.size
        return stats

    def status(self):
        return STATUS_OK

    def reset(self):
        self.dataset_map.reset()
        self.stats.reset()
        return None


class AbstractL2CacheHandle(metaclass=ABCMeta):
    @abstractmethod
    def insert(self, dataset_key, data):
        pass

    @abstractmethod
    def get(self, dataset_key):
        pass

    @abstractmethod
    def delete(self, dataset_key):
        pass

    @abstractmethod
    def statistics(self):
        pass

    @abstractmethod
    def status(self):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def stop(self):
        pass


class NopL2CacheHandle(AbstractL2CacheHandle):
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
        return STATUS_OK

    def reset(self):
        pass

    def stop(self):
        pass


class L2CacheHandle(AbstractL2CacheHandle):
    """
    Client process API for communication with the L2 server process.
    """
    def __init__(self, process_handle):
        self.process_handle = process_handle

    def insert(self, dataset_key, data):
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
        if not self.process_handle.is_alive():
            return "L2 cache process dead"

        return self.run_command(StatusCommand())

    def reset(self):
        return self.run_command(StatisticsCommand())

    def stop(self):
        return self.process_handle.stop()


def l2_cache_process(ipc_address, statistics_buffer_size, max_cache_size, max_age):
    """
    Function executing the Layer 2 cache server.
    """
    setproctitle('qcache_l2_cache')
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(ipc_address)
    l2_cache = L2Cache(statistics_buffer_size=statistics_buffer_size,
                       max_size=max_cache_size,
                       max_age=max_age)
    while True:
        try:
            objects = receive_serialized_objects(socket)
            t0 = time.time()
            command = deserialize_object(objects[0])
            input_data = None
            if len(objects) == 2:
                input_data = objects[1]

            if command == STOP_COMMAND:
                return

            response = command.execute(l2_cache, input_data)
            result, output_data = response if isinstance(response, tuple) else (response, serialize_object(None))
            if isinstance(result, Result):
                result.stats['l2_execution_duration'] = time.time() - t0
            send_serialized_objects(socket, serialize_object(result), output_data)
        except Exception as e:
            send_objects(socket, e)
            traceback.print_exc()


def create_l2_cache(statistics_buffer_size: int, max_age: int, max_size: int) -> AbstractL2CacheHandle:
    """
    Create a layer 2 cache. Start server process and return a client side API
    object for interaction with the server side cache.
    """
    if max_size <= 0:
        return NopL2CacheHandle()

    ipc_address = 'ipc:///tmp/qcache_ipc_l2_cache'
    p = Process(name='qcache_l2_cache',
                target=l2_cache_process,
                args=(ipc_address, statistics_buffer_size, max_size, max_age))
    p.start()
    return L2CacheHandle(ProcessHandle(p, ipc_address))


# ##################################################################
# ### Commands sent from L2 client process to the server process ###
# ##################################################################

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


# #####################################################################
# ### Results returned from L2 server process to the client process ###
# #####################################################################

class GetResult(Result):
    STATUS_NOT_FOUND = "not_found"
    STATUS_SUCCESS = "success"

    def __init__(self, status):
        self.status = status
        self.data = None
        super().__init__()
