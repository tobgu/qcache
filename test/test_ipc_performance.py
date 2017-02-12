import time
from contextlib import contextmanager
from multiprocessing import Process

import blosc
import lz4
import pytest
import zmq
from qcache.qframe import QFrame


def to_from_csv(msg):
    qf = QFrame.from_csv(msg)
    new_qf = qf.query({'limit': 1000})
    return new_qf.to_csv().encode('utf-8')


def subprocess(ipc_file_name):
    context = zmq.Context.instance()

    sock = context.socket(zmq.REP)
    sock.bind('ipc://' + ipc_file_name)

    while True:
        msg = sock.recv(copy=False)
        if msg.buffer == b"stop":
            return

        csv_frame = blosc.decompress(msg.buffer)
        result = to_from_csv(csv_frame)
        z_frame = blosc.compress(result, typesize=1, cname='lz4')
        sock.send(z_frame, copy=False)


@pytest.fixture
def large_csv_frame():
    d = 1000000 * [{'aaa': 123456789, 'bbb': 'abcdefghijklmnopqrvwxyz', 'ccc': 1.23456789}]
    return QFrame.from_dicts(d).to_csv().encode('utf-8')


@contextmanager
def timeit(name):
    t0 = time.time()
    yield
    print('{name} duration: {duration} s'.format(name=name, duration=time.time()-t0))


@pytest.mark.benchmark
def test_zmq_ipc_performance_with_df(large_csv_frame):
    p = Process(target=subprocess, args=('/tmp/qcache_ipc_1',))
    p.start()

    context = zmq.Context.instance()
    sock = context.socket(zmq.REQ)
    sock.connect('ipc:///tmp/qcache_ipc_1')
    time.sleep(0.5)
    with timeit('blosc'):
        z_frame = blosc.compress(large_csv_frame, typesize=1, cname='lz4')
    assert large_csv_frame == blosc.decompress(z_frame)

    with timeit('lz4'):
        z_frame2 = lz4.dumps(large_csv_frame)

    with timeit('zmq'):
        sock.send(z_frame, copy=False)
        result = blosc.decompress(sock.recv(copy=False).buffer)

    with timeit('zmq2'):
        sock.send(z_frame, copy=False)
        result2 = blosc.decompress(sock.recv(copy=False).buffer)

    sock.send(b"stop")
    p.join()

    with timeit('fn'):
        result3 = to_from_csv(large_csv_frame)

    assert result == result2
    assert result == result3
    context.destroy()


def echoer(address, copy):
    context = zmq.Context.instance()

    sock = context.socket(zmq.REP)
    sock.bind(address)

    while True:
        msg = sock.recv(copy=copy)
        if copy:
            if msg == b"stop":
                context.destroy()
                return
            response = msg
        else:
            if msg.bytes == b"stop":
                context.destroy()
                return

            response = msg.bytes

        sock.send(response, copy=copy)


@pytest.mark.benchmark
@pytest.mark.parametrize('copy', [True, False])
def test_zmq_raw_ipc_performance(copy):
    address = 'ipc:///tmp/qcache_ipc_1'
    p = Process(target=echoer, args=(address, copy))
    p.start()

    context = zmq.Context.instance()
    sock = context.socket(zmq.REQ)
    sock.connect(address)
    time.sleep(0.5)

    for size in (1, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000):
        data = size * b"X"
        with timeit('zmq copy={}, size={}'.format(copy, size)):
            sock.send(data, copy=copy)
            resp = sock.recv(copy=copy)

    sock.send(b"stop")
    p.join()
    context.destroy()


###########
# Results #
###########

# test/test_ipc_performance.py::test_zmq_ipc_performance_with_df blosc duration: 0.00459003448486 s
# lz4 duration: 0.00556683540344 s
# zmq duration: 0.715618848801 s
# zmq2 duration: 0.681484937668 s
# fn duration: 0.670202970505 s

# test/test_ipc_performance.py::test_zmq_raw_ipc_performance[True] zmq copy=True, size=1 duration: 0.000946998596191 s
# zmq copy=True, size=100 duration: 0.000172853469849 s
# zmq copy=True, size=1000 duration: 0.000140905380249 s
# zmq copy=True, size=10000 duration: 0.000241041183472 s
# zmq copy=True, size=100000 duration: 0.000426054000854 s
# zmq copy=True, size=1000000 duration: 0.00445914268494 s
# zmq copy=True, size=10000000 duration: 0.0513310432434 s
# zmq copy=True, size=100000000 duration: 0.363895893097 s

# test/test_ipc_performance.py::test_zmq_raw_ipc_performance[False] zmq copy=False, size=1 duration: 0.00168204307556 s
# zmq copy=False, size=100 duration: 0.00039005279541 s
# zmq copy=False, size=1000 duration: 0.000436782836914 s
# zmq copy=False, size=10000 duration: 0.000378131866455 s
# zmq copy=False, size=100000 duration: 0.000405073165894 s
# zmq copy=False, size=1000000 duration: 0.00276207923889 s
# zmq copy=False, size=10000000 duration: 0.016685962677 s
# zmq copy=False, size=100000000 duration: 0.200177192688 s


