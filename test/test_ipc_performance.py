import json
import pickle
import time
from contextlib import contextmanager
from multiprocessing import Process

import blosc
import lz4
import pandas
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
    return QFrame.from_json(json.dumps(d)).to_csv().encode('utf-8')


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


@pytest.mark.benchmark
def test_serialization_performance():
    import pyarrow as pa
    from pyarrow.feather import (read_feather, write_feather)

    d = 1000000 * [{'aaa': 123456789, 'bbb': 'abcdefghijklmnopqrvwxyz', 'ccc': 1.23456789}]
    qf = QFrame.from_json(json.dumps(d))
    test_count = 3

    print("\nPickle")
    for x in range(test_count):
        print("----------------")
        with timeit(f'pickle {x}'):
            pickled_object = pickle.dumps(qf.df, protocol=pickle.HIGHEST_PROTOCOL)

        with timeit('compress pickle'):
            z_frame = blosc.compress(pickled_object, typesize=1, cname='lz4')

        print("Pickle size", len(pickled_object), len(z_frame))

        with timeit('uncompress pickle'):
            uncomp = blosc.decompress(z_frame)

        with timeit(f'unpickle {x}'):
            unpickled_object = pickle.loads(pickled_object)

        qf2 = QFrame(unpickled_object)
        print("Pickle qf byte size", qf.byte_size(), qf2.byte_size())

    import io
    print("\nFeather")
    for x in range(test_count):
        print("-----------------")
        b = io.BytesIO()
        with timeit(f'feather {x}'):
            write_feather(qf.df, b)

        b.seek(0)
        with timeit(f'unfeather {x}'):
            unfeathered_object = read_feather(b)
        qf2 = QFrame(unfeathered_object)

        print("qf byte size", qf.byte_size(), qf2.byte_size())

        with timeit('compress feather'):
            z_frame = blosc.compress(b.getvalue(), typesize=1, cname='lz4')

        print("Feather size", len(b.getvalue()), len(z_frame))

        with timeit('uncompress feather'):
            uncomp = blosc.decompress(z_frame)

    import pyarrow.parquet as pq

    print("\nParquet")
    for x in range(test_count):
        print("-----------------")
        b = io.BytesIO()
        with timeit(f'parquet {x}'):
            table = pa.Table.from_pandas(qf.df)
            pq.write_table(table, b)

        b.seek(0)
        with timeit(f'unparquet {x}'):
            unparqueted_object = pq.read_pandas(b).to_pandas()

        qf2 = QFrame(unparqueted_object)
        print("qf byte size", qf.byte_size(), qf2.byte_size())

        with timeit('compress parquet'):
            z_frame = blosc.compress(b.getvalue(), typesize=1, cname='lz4')

        print("Parquet size", len(b.getvalue()), len(z_frame))

        with timeit('uncompress parquet'):
            uncomp = blosc.decompress(z_frame)

#        print(unarrowed_object)

###########
# Results #
###########

# test/test_ipc_performance.py::test_zmq_ipc_performance_with_df
# blosc duration: 0.00459003448486 s
# lz4 duration: 0.00556683540344 s
# zmq duration: 0.715618848801 s
# zmq2 duration: 0.681484937668 s
# fn duration: 0.670202970505 s

# test/test_ipc_performance.py::test_zmq_raw_ipc_performance[True]
# zmq copy=True, size=1 duration: 0.000946998596191 s
# zmq copy=True, size=100 duration: 0.000172853469849 s
# zmq copy=True, size=1000 duration: 0.000140905380249 s
# zmq copy=True, size=10000 duration: 0.000241041183472 s
# zmq copy=True, size=100000 duration: 0.000426054000854 s
# zmq copy=True, size=1000000 duration: 0.00445914268494 s
# zmq copy=True, size=10000000 duration: 0.0513310432434 s
# zmq copy=True, size=100000000 duration: 0.363895893097 s

# test/test_ipc_performance.py::test_zmq_raw_ipc_performance[False]
# zmq copy=False, size=1 duration: 0.00168204307556 s
# zmq copy=False, size=100 duration: 0.00039005279541 s
# zmq copy=False, size=1000 duration: 0.000436782836914 s
# zmq copy=False, size=10000 duration: 0.000378131866455 s
# zmq copy=False, size=100000 duration: 0.000405073165894 s
# zmq copy=False, size=1000000 duration: 0.00276207923889 s
# zmq copy=False, size=10000000 duration: 0.016685962677 s
# zmq copy=False, size=100000000 duration: 0.200177192688 s

# test/test_ipc_performance.py::test_serialization_performance
# Pickle
# ----------------
# pickle 0 duration: 0.40921902656555176 s
# compress pickle duration: 0.025865793228149414 s
# Pickle size 50006563 4180251
# uncompress pickle duration: 0.030571460723876953 s
# unpickle 0 duration: 0.19137215614318848 s
# Pickle qf byte size 104000000 104000000
# ----------------
# pickle 1 duration: 0.5409905910491943 s
# compress pickle duration: 0.02100515365600586 s
# Pickle size 50006563 4180251
# uncompress pickle duration: 0.02867746353149414 s
# unpickle 1 duration: 0.16989493370056152 s
# Pickle qf byte size 104000000 104000000
# ----------------
# pickle 2 duration: 0.4594745635986328 s
# compress pickle duration: 0.021045684814453125 s
# Pickle size 50006563 4180251
# uncompress pickle duration: 0.029650449752807617 s
# unpickle 2 duration: 0.18954753875732422 s
# Pickle qf byte size 104000000 104000000
#
# Feather
# -----------------
# feather 0 duration: 0.19074225425720215 s
# unfeather 0 duration: 0.09412074089050293 s
# qf byte size 104000000 96000080
# compress feather duration: 0.00874948501586914 s
# Feather size 43000320 4161968
# uncompress feather duration: 0.04875063896179199 s
# -----------------
# feather 1 duration: 0.20453500747680664 s
# unfeather 1 duration: 0.09333467483520508 s
# qf byte size 104000000 96000080
# compress feather duration: 0.008579254150390625 s
# Feather size 43000320 4161968
# uncompress feather duration: 0.017756223678588867 s
# -----------------
# feather 2 duration: 0.20637273788452148 s
# unfeather 2 duration: 0.0884242057800293 s
# qf byte size 104000000 96000080
# compress feather duration: 0.007230997085571289 s
# Feather size 43000320 4161968
# uncompress feather duration: 0.020099163055419922 s
#
# Parquet
# -----------------
# parquet 0 duration: 0.2807800769805908 s
# unparquet 0 duration: 0.1965043544769287 s
# qf byte size 104000000 104000000
# compress parquet duration: 0.001428842544555664 s
# Parquet size 4282979 346487
# uncompress parquet duration: 0.0013706684112548828 s
# -----------------
# parquet 1 duration: 0.2639808654785156 s
# unparquet 1 duration: 0.18688130378723145 s
# qf byte size 104000000 104000000
# compress parquet duration: 0.0009987354278564453 s
# Parquet size 4282979 346487
# uncompress parquet duration: 0.0010502338409423828 s
# -----------------
# parquet 2 duration: 0.2606379985809326 s
# unparquet 2 duration: 0.22973346710205078 s
# qf byte size 104000000 104000000
# compress parquet duration: 0.0007367134094238281 s
# Parquet size 4282979 346487
# uncompress parquet duration: 0.0007739067077636719 s
