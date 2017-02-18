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
def qcache(api_workers=1, cache_shards=1):
    proc = Popen(["python",
                  "qcache/__init__.py",
                  "--api-workers={}".format(api_workers),
                  "--cache-shards={}".format(cache_shards)])
    await_qcache()
    yield
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
