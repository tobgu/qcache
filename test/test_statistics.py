from qcache.statistics import Statistics


def test_ring_buffer_size():
    s = Statistics(buffer_size=3)
    s.append('foo', 1)
    s.append('foo', 2)
    s.append('foo', 3)

    assert list(s.stats['foo']) == [1, 2, 3]

    s.append('foo', 4)
    assert list(s.stats['foo']) == [2, 3, 4]
