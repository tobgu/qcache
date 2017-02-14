import collections
import json
import time

import gc
import traceback
from multiprocessing import Process
import pickle


import blosc
import zmq

from qcache.cache_ring import NodeRing
from qcache.constants import CONTENT_TYPE_CSV, CONTENT_TYPE_JSON
from qcache.dataset_cache import DatasetCache
from qcache.qframe import MalformedQueryException, QFrame
from qcache.statistics import Statistics
from qcache.cache_common import QueryResult, InsertResult, DeleteResult

STOP_COMMAND = "stop"


class QueryCommand(object):
    def __init__(self, dataset_key, q, filter_engine, stand_in_columns):
        self.dataset_key = dataset_key
        self.q = q
        self.filter_engine = filter_engine
        self.stand_in_columns = stand_in_columns

    def execute(self, worker):
        return worker.query(dataset_key=self.dataset_key,
                            q=self.q,
                            filter_engine=self.filter_engine,
                            stand_in_columns=self.stand_in_columns)


class InsertCommand(object):
    def __init__(self, dataset_key, qf):
        self.dataset_key = dataset_key
        self.qf = qf

    def execute(self, worker):
        return worker.insert(dataset_key=self.dataset_key, qf=self.qf)


class DeleteCommand(object):
    def __init__(self, dataset_key):
        self.dataset_key = dataset_key

    def execute(self, worker):
        return worker.delete(self.dataset_key)


class StatsCommand(object):
    def execute(self, worker):
        return worker.statistics()


class CacheShard(object):
    def __init__(self, statistics_buffer_size, max_cache_size, max_age):
        self.stats = Statistics(buffer_size=statistics_buffer_size)
        self.dataset_cache = DatasetCache(max_size=max_cache_size, max_age=max_age)
        self.query_count = 0

    def query(self, dataset_key, q, filter_engine, stand_in_columns):
        t0 = time.time()
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
                             data=result_frame,
                             content_type="pickled",
                             unsliced_length=result_frame.unsliced_df_len,
                             query_stats={})

        duration = time.time() - t0
        self.stats.append('query_durations', duration)

        # TODO: Remove this one, it was not very interesting
        self.stats.append('query_request_durations', duration+0.000001)
        return result

    def insert(self, dataset_key, qf):
        t0 = time.time()
        if dataset_key in self.dataset_cache:
            self.stats.inc('replace_count')
            del self.dataset_cache[dataset_key]

        durations_until_eviction = self.dataset_cache.ensure_free(qf.byte_size())
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
        self.dataset_cache.delete(dataset_key)
        return DeleteResult(status=DeleteResult.STATUS_SUCCESS)

    def statistics(self):
        stats = self.stats.snapshot()
        stats['dataset_count'] = len(self.dataset_cache)
        stats['cache_size'] = self.dataset_cache.size
        return stats


def shard_process(ipc_address, statistics_buffer_size, max_cache_size, max_age):
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
            send_object(socket, result)
        except Exception as e:
            traceback.print_exc(e)
            # TODO: Formalize this...
            send_object(socket, "error")


class ShardHandle(object):
    def __init__(self, process, socket):
        self.process = process
        self.socket = socket

    def stop(self):
        send_object(self.socket, STOP_COMMAND)
        self.process.join()


def spawn_shards(zmq_context, count, statistics_buffer_size, max_cache_size, max_age):
    shards = []
    for i in range(count):
        ipc_address = 'ipc:///tmp/qcache_ipc_{}'.format(i)
        p = Process(target=shard_process, args=(ipc_address, statistics_buffer_size, max_cache_size, max_age))
        p.start()
        socket = zmq_context.socket(zmq.REQ)
        socket.connect(ipc_address)
        shards.append(ShardHandle(process=p, socket=socket))

    return shards


def send_object(socket, obj):
    serialized_object = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    compressed = blosc.compress(serialized_object, typesize=1, cname='lz4')
    socket.send(compressed)


def receive_object(socket):
    msg = socket.recv(copy=False)
    t0 = time.time()
    serialized_command = blosc.decompress(msg.buffer)
    obj = pickle.loads(serialized_command)
    return obj, t0


class ShardedCache(object):
    def __init__(self, statistics_buffer_size, max_cache_size, max_age, default_filter_engine, cache_count):
        self.query_count = 0
        self.default_filter_engine = default_filter_engine
        self.max_age = max_age
        self.cache_ring = NodeRing(range(cache_count))
        self.zmq_context = zmq.Context()
        self.cache_shards = spawn_shards(self.zmq_context,
                                         cache_count,
                                         statistics_buffer_size,
                                         max_cache_size/cache_count,
                                         max_age)

    def _post_query_processing(self):
        if self.query_count % 10 == 0:
            # Run a collect every now and then. It reduces the process memory consumption
            # considerably but always doing it will impact query performance negatively.
            gc.collect()

        self.query_count += 1

    def run_command(self, command):
        shard_id = self.cache_ring.get_node(command.dataset_key)
        shard = self.cache_shards[shard_id]
        send_object(shard.socket, command)
        result, _ = receive_object(shard.socket)
        return result

    def run_command_on_all_shards(self, command):
        results = []
        for shard in self.cache_shards:
            send_object(shard.socket, command)
            result, _ = receive_object(shard.socket)
            results.append(result)

        return results

    def query(self, dataset_key, q, filter_engine, stand_in_columns, accept_type):
        filter_engine = filter_engine or self.default_filter_engine
        command = QueryCommand(dataset_key=dataset_key,
                               q=q,
                               filter_engine=filter_engine,
                               stand_in_columns=stand_in_columns)

        result = self.run_command(command)
        if result.status == QueryResult.STATUS_SUCCESS:
            result.data = result.data.to_json() if accept_type == CONTENT_TYPE_JSON else result.data.to_csv()
            result.content_type = accept_type

        return result

    def insert(self, dataset_key, data, content_type, data_types, stand_in_columns):
        if content_type == CONTENT_TYPE_CSV:
            qf = QFrame.from_csv(data, column_types=data_types, stand_in_columns=stand_in_columns)
        else:
            data = json.loads(data)
            qf = QFrame.from_dicts(data, stand_in_columns=stand_in_columns)

        command = InsertCommand(dataset_key=dataset_key, qf=qf)
        return self.run_command(command)

    def delete(self, dataset_key):
        return self.run_command(DeleteCommand(dataset_key=dataset_key))

    def statistics(self):
        stats = {}
        results = self.run_command_on_all_shards(StatsCommand())

        # Merge statistics from the different shards
        for result in results:
            for stat, value in result.items():
                if stat in stats:
                    if isinstance(value, collections.Iterable):
                        stats[stat].extend(value)
                    else:
                        stats[stat] += value
                else:
                    stats[stat] = value

        return stats

    def stop(self):
        # Currently only used for testing
        for shard in self.cache_shards:
            shard.stop()

        self.zmq_context.destroy()

