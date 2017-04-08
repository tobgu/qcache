"""
QFrame cache. Stores frames in memory where they can be queried.

The cache consists of several parts:
* One or more cache shards that hold QFrames and executes queries on them. Each shard
  executes in a separate process.
* A client side API that handles data serialization, routing of commands to cache shards
  depending  on the dataset key.
* Optionally a "layer 2 cache" that holds compressed and hence more space efficient representations
  of the datasets. This allows the cache to hold a larger number of datasets in memory at the cost
  of access time since the data must be moved from the L2 cache to the primary cache before being
  queried.
"""
from typing import List
from typing import Optional, Tuple

import collections
import time

import gc
import traceback
from multiprocessing import Process

import zmq
from setproctitle import setproctitle
from qcache.cache.ipc import receive_object, ProcessHandle, STOP_COMMAND, send_object, STATUS_OK
from qcache.cache.cache_ring import NodeRing
from qcache.cache.l2_cache import create_l2_cache, GetResult
from qcache.cache.cache_common import QueryResult, InsertResult, DeleteResult, Result
from qcache.cache.dataset_map import DatasetMap
from qcache.cache.statistics import Statistics

from qcache.qframe.constants import FILTER_ENGINE_NUMEXPR
from qcache.constants import CONTENT_TYPE_CSV, CONTENT_TYPE_JSON
from qcache.qframe import MalformedQueryException, QFrame, StandInColumns


# ###############################################################
# ### Commands sent from client to cache shard server process ###
# ###############################################################


class QueryCommand:
    def __init__(self, dataset_key: str, q: dict, filter_engine: str, stand_in_columns: StandInColumns) -> None:
        self.dataset_key = dataset_key
        self.q = q
        self.filter_engine = filter_engine
        self.stand_in_columns = stand_in_columns

    def execute(self, cache_shard):
        return cache_shard.query(dataset_key=self.dataset_key,
                                 q=self.q,
                                 filter_engine=self.filter_engine,
                                 stand_in_columns=self.stand_in_columns)


class InsertCommand:
    def __init__(self, dataset_key: str, qf: QFrame) -> None:
        self.dataset_key = dataset_key
        self.qf = qf

    def execute(self, cache_shard: 'CacheShard'):
        return cache_shard.insert(dataset_key=self.dataset_key, qf=self.qf)


class DeleteCommand:
    def __init__(self, dataset_key: str) -> None:
        self.dataset_key = dataset_key

    def execute(self, cache_shard: 'CacheShard'):
        return cache_shard.delete(self.dataset_key)


class StatsCommand:
    def execute(self, cache_shard: 'CacheShard'):
        return cache_shard.statistics()


class ResetCommand:
    def execute(self, cache_shard: 'CacheShard'):
        return cache_shard.reset()


class StatusCommand:
    def execute(self, cache_shard: 'CacheShard'):
        return cache_shard.status()


# ############
# ## Cache ###
# ############


class CacheShard:
    """
    Server side cache shard.
    """
    def __init__(self, statistics_buffer_size: int, max_cache_size: int, max_age: int) -> None:
        self.stats = Statistics(buffer_size=statistics_buffer_size)
        self.dataset_map = DatasetMap(max_size=max_cache_size, max_age=max_age)
        self.query_count = 0

    def query(self, dataset_key: str, q: dict, filter_engine: str, stand_in_columns: StandInColumns) -> QueryResult:
        t0 = time.time()
        if dataset_key not in self.dataset_map:
            self.stats.inc('miss_count')
            return QueryResult(status=QueryResult.STATUS_NOT_FOUND)

        if self.dataset_map.evict_if_too_old(dataset_key):
            self.stats.inc('miss_count')
            self.stats.inc('age_evict_count')
            return QueryResult(status=QueryResult.STATUS_NOT_FOUND)

        qf = self.dataset_map[dataset_key]
        try:
            result_frame = qf.query(q, filter_engine=filter_engine, stand_in_columns=stand_in_columns)
        except MalformedQueryException as e:
            return QueryResult(status=QueryResult.STATUS_MALFORMED_QUERY, data=str(e))

        self.stats.inc('hit_count')
        result = QueryResult(status=QueryResult.STATUS_SUCCESS,
                             data=result_frame,
                             content_type="pickled",
                             unsliced_length=result_frame.unsliced_df_len,
                             query_stats={})

        duration = time.time() - t0
        self.stats.append('query_durations', duration)

        # TODO: Remove this one, it was not very interesting only left for backwards compatibility
        self.stats.append('query_request_durations', duration+0.000001)
        return result

    def insert(self, dataset_key: str, qf: QFrame) -> InsertResult:
        t0 = time.time()
        if dataset_key in self.dataset_map:
            self.stats.inc('replace_count')
            del self.dataset_map[dataset_key]

        durations_until_eviction = self.dataset_map.ensure_free(qf.byte_size())
        self.dataset_map[dataset_key] = qf
        self.stats.inc('size_evict_count', count=len(durations_until_eviction))
        self.stats.inc('store_count')
        self.stats.append('store_row_counts', len(qf))
        self.stats.extend('durations_until_eviction', durations_until_eviction)

        duration = time.time() - t0
        self.stats.append('store_durations', duration)
        # TODO: Remove this one, it was not very interesting only left for backwards compatibility
        self.stats.append('store_request_durations', duration+0.000001)

        return InsertResult(status=InsertResult.STATUS_SUCCESS)

    def delete(self, dataset_key: str) -> DeleteResult:
        self.dataset_map.delete(dataset_key)
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self) -> dict:
        stats = self.stats.snapshot()
        stats['dataset_count'] = len(self.dataset_map)
        stats['cache_size'] = self.dataset_map.size
        return stats

    def status(self) -> str:
        return STATUS_OK

    def reset(self):
        self.dataset_map.reset()
        self.stats.reset()


# TODO: Add mypy extension  -> NoReturn?
def shard_process(name: str, ipc_address: str, statistics_buffer_size: int, max_cache_size: int, max_age: int):
    """
    Function executing a cache shard server.
    """
    setproctitle(name)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(ipc_address)
    worker = CacheShard(statistics_buffer_size=statistics_buffer_size,
                        max_cache_size=max_cache_size,
                        max_age=max_age)
    while True:
        try:
            command, t0 = receive_object(socket)
            if command == STOP_COMMAND:
                return

            result = command.execute(worker)
            if isinstance(result, Result):
                result.stats['shard_execution_duration'] = time.time() - t0
            send_object(socket, result)
        except Exception as e:
            send_object(socket, e)
            traceback.print_exc()


def spawn_shards(count: int, statistics_buffer_size: int, max_cache_size: int, max_age: int):
    """
    Start requested number of cache shard servers and return client side handles to
    the servers.
    """
    shards = []
    for i in range(count):
        ipc_address = 'ipc:///tmp/qcache_ipc_{}'.format(i)
        name = 'qcache_shard_{}'.format(i)
        p = Process(target=shard_process,
                    args=(name, ipc_address, statistics_buffer_size, max_cache_size, max_age))
        p.start()
        shards.append(ProcessHandle(process=p, ipc_address=ipc_address))

    return shards


class ShardedCache:
    """
    Cache client side API.
    """
    def __init__(self,
                 statistics_buffer_size: int=1000,
                 max_cache_size: int=1000000000,
                 max_age: int=0,
                 default_filter_engine: str=FILTER_ENGINE_NUMEXPR,
                 shard_count: int=1,
                 l2_cache_size: int=0) -> None:
        self.query_count = 0
        self.default_filter_engine = default_filter_engine
        self.max_age = max_age
        self.cache_ring = NodeRing(range(shard_count))
        self.cache_shards = spawn_shards(shard_count,
                                         statistics_buffer_size,
                                         int(max_cache_size / shard_count),
                                         max_age)
        self.l2_cache = create_l2_cache(statistics_buffer_size, max_age, l2_cache_size)

    def _post_query_processing(self):
        if self.query_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact query performance negatively.
            gc.collect()

        self.query_count += 1

    def _shard_for_dataset(self, dataset_key: str):
        shard_id = self.cache_ring.get_node(dataset_key)
        return self.cache_shards[shard_id]

    def _run_command(self, command):
        shard = self._shard_for_dataset(command.dataset_key)
        input_data = shard.send_object(command)
        result, _ = shard.receive_object()
        return result, input_data

    def _run_command_on_all_shards(self, command):
        results = []
        for shard in self.cache_shards:
            shard.send_object(command)
            result, _ = shard.receive_object()
            results.append(result)

        return results

    def _do_query(self, command: QueryCommand, accept_type: str):
        result, _ = self._run_command(command)
        if result.status == QueryResult.STATUS_SUCCESS:
            result.data = result.data.to_json() if accept_type == CONTENT_TYPE_JSON else result.data.to_csv()
            result.content_type = accept_type
        return result

    def query(self, dataset_key: str, q: dict, filter_engine: str, stand_in_columns: Optional[List[Tuple[str, ...]]], accept_type: str):
        filter_engine = filter_engine or self.default_filter_engine
        command = QueryCommand(dataset_key=dataset_key,
                               q=q,
                               filter_engine=filter_engine,
                               stand_in_columns=stand_in_columns)

        result = self._do_query(command, accept_type)
        if result.status == QueryResult.STATUS_NOT_FOUND:
            get_result = self.l2_cache.get(dataset_key)
            if get_result.status == GetResult.STATUS_SUCCESS:
                shard = self._shard_for_dataset(dataset_key)
                shard.send_serialized_object(get_result.data)
                result, _ = shard.receive_object()
                query_result = self._do_query(command, accept_type)
                query_result.stats.update(get_result.stats)
                return query_result

        return result

    def insert(self, dataset_key: str, data: bytes, content_type: str, data_types: dict, stand_in_columns: StandInColumns):
        if content_type == CONTENT_TYPE_CSV:
            qf = QFrame.from_csv(data, column_types=data_types, stand_in_columns=stand_in_columns)
        else:
            qf = QFrame.from_json(data, column_types=data_types, stand_in_columns=stand_in_columns)

        # Pre-calculate and cache the byte size here, to avoid having to do it in the cache.
        qf.byte_size()

        shard = self._shard_for_dataset(dataset_key)
        input_data = shard.send_object(InsertCommand(dataset_key=dataset_key, qf=qf))
        l2_result = self.l2_cache.insert(dataset_key, input_data)
        result, _ = shard.receive_object()
        result.stats.update(l2_result.stats)
        return result

    def delete(self, dataset_key: str):
        l2_result = self.l2_cache.delete(dataset_key)
        result, _ = self._run_command(DeleteCommand(dataset_key=dataset_key))
        result.stats.update(l2_result.stats)
        return result

    def statistics(self) -> dict:
        stats = self.l2_cache.statistics()
        results = self._run_command_on_all_shards(StatsCommand())

        # Merge statistics from the different shards
        for result in results:
            for stat, value in result.items():
                if stat in stats:
                    if isinstance(value, collections.Iterable):
                        stats[stat].extend(value)
                    elif stat not in {'statistics_duration', 'statistics_buffer_size'}:
                        stats[stat] += value
                else:
                    stats[stat] = value

        return stats

    def stop(self):
        # Currently only used for testing
        self.l2_cache.stop()
        for shard in self.cache_shards:
            shard.stop()

        zmq.Context.instance(io_threads=2).destroy()

    def reset(self):
        # Currently only used for testing
        self._run_command_on_all_shards(ResetCommand())
        self.l2_cache.reset()

    def status(self) -> str:
        l2_status = self.l2_cache.status()
        if l2_status != STATUS_OK:
            return l2_status

        for shard in self.cache_shards:
            if not shard.is_alive():
                return "Cache shard process dead"

        for s in self._run_command_on_all_shards(StatusCommand()):
            if s != STATUS_OK:
                return s

        return STATUS_OK
