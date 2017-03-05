import json
import random
from contextlib import contextmanager
from subprocess import Popen

import psutil
import signal
import pytest
import requests
import time


def kill_child_processes(parent_pid):
    parent = psutil.Process(parent_pid)
    children = parent.children(recursive=True)
    parent.send_signal(signal.SIGKILL)
    for process in children:
        process.send_signal(signal.SIGKILL)


def kill_random_child_process(parent_pid, name_contains):
    parent = psutil.Process(parent_pid)
    children = [c for c in parent.children(recursive=True) if name_contains in c.name()]
    random.shuffle(children)
    children[0].send_signal(signal.SIGKILL)


def await_qcache():
    t0 = time.time()
    while time.time() < t0 + 10.0:
        try:
            if requests.get('http://localhost:8888/qcache/status').status_code == 200:
                return
        except:
            pass

    raise Exception('Error waiting for qcache to come up')


@contextmanager
def qcache(api_workers=1, cache_shards=1, l2_cache_size=0):
    proc = Popen(["python",
                  "qcache/__init__.py",
                  "--api-workers={}".format(api_workers),
                  "--cache-shards={}".format(cache_shards),
                  "--l2-cache-size={}".format(l2_cache_size)])
    await_qcache()
    yield proc
    kill_child_processes(proc.pid)
    proc.wait()


@pytest.mark.parametrize("api_workers, cache_shards", [
    (1, 1),
    (1, 3),
    (3, 1),
    (3, 3)])
def test_e2e_statistics(api_workers, cache_shards):
    with qcache(api_workers=api_workers, cache_shards=cache_shards):
        for _ in range(4):
            response = requests.get('http://localhost:8888/qcache/statistics')
            assert response.status_code == 200
            assert response.json()


def get_query_stats(response):
    stats = response.headers.get('X-QCache-stats', '')
    return dict(kv.strip().split('=') for kv in stats.split(','))


def test_e2e_dataset_key_match():
    # Push many datasets into cache and verify retrieving the dataset
    # returns the correct data. This is done to verify internal routing
    # to the different shards.

    with qcache(api_workers=3, cache_shards=3, l2_cache_size=1000000):
        t0 = time.time()
        dataset_count = 100
        shard_duration = 0
        request_duration = 0
        for i in range(dataset_count):
            response = requests.post('http://localhost:8888/qcache/dataset/{}'.format(i),
                                     data=json.dumps([{str(i): i}]),
                                     headers={'Content-Type': 'application/json'})
            assert response.status_code == 201

            stats = get_query_stats(response)
            shard_duration += float(stats['shard_execution_duration'])
            request_duration += float(stats['request_duration'])

        print("Insert of {} datasets took: {} s, shard_duration: {}, request_duration: {}".format(
            dataset_count, time.time() - t0, shard_duration, request_duration))

        t0 = time.time()
        shard_duration = 0
        request_duration = 0
        for i in range(dataset_count):
            response = requests.get('http://localhost:8888/qcache/dataset/{}'.format(i),
                                    headers={'Accept': 'application/json'})
            assert response.status_code == 200
            assert response.json() == [{str(i): i}]

            stats = get_query_stats(response)
            shard_duration += float(stats['shard_execution_duration'])
            request_duration += float(stats['request_duration'])

        print("Querying {} datasets took: {} s, shard_time: {}, request_duration: {}".format(
            dataset_count, time.time() - t0, shard_duration, request_duration))


def test_e2e_kill_random_api_workers():
    # Test killing API worker processes and verify that new ones are started
    # and can communicate with the cache shards.
    with qcache(api_workers=3, cache_shards=3) as proc:
        for i in range(100):
            response = requests.post('http://localhost:8888/qcache/dataset/{}'.format(i),
                                     data=json.dumps([{str(i): i}]),
                                     headers={'Content-Type': 'application/json'})
            assert response.status_code == 201

            if i % 10 == 0:
                kill_random_child_process(proc.pid, name_contains='python')

            response = requests.get('http://localhost:8888/qcache/dataset/{}'.format(i),
                                    headers={'Accept': 'application/json'})
            assert response.status_code == 200


def test_e2e_kill_cache_shard():
    # Calling the status API should timeout when this happens
    with qcache(api_workers=3, cache_shards=3) as proc:
        response = requests.get('http://localhost:8888/qcache/status', timeout=2.0)
        assert response.status_code == 200

        kill_random_child_process(proc.pid, name_contains='shard')
        response = requests.get('http://localhost:8888/qcache/status', timeout=2.0)
        assert response.status_code == 500


def test_e2e_kill_l2_cache():
    # Calling the status API should timeout when this happens
    with qcache(api_workers=3, cache_shards=3, l2_cache_size=100000) as proc:
        response = requests.get('http://localhost:8888/qcache/status', timeout=2.0)
        assert response.status_code == 200

        kill_random_child_process(proc.pid, name_contains='l2')
        response = requests.get('http://localhost:8888/qcache/status', timeout=2.0)
        assert response.status_code == 500

# TODO: Long running memory consumption test
