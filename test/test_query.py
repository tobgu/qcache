from StringIO import StringIO
import pandas
import pytest
from qcache.query import query, MalformedQueryException


######################### Filtering ##########################

@pytest.fixture
def basic_frame():
    data = """
foo,bar,baz,qux
bbb,1.25,5,qqq
aaa,3.25,7,qqq
ccc,,9,www"""

    return pandas.read_csv(StringIO(data))


def assert_rows(frame, rows):
    assert len(frame) == len(rows)

    for ix, row in enumerate(rows):
        assert frame.iloc[ix]['foo'] == row


@pytest.mark.parametrize("operation, column, value, expected", [
    ("<",  'bar', 2, 'bbb'),
    (">",  'bar', 2, 'aaa'),
    (">",  'foo', "'bbb'", 'ccc'),
    ("<=", 'baz', 6, 'bbb'),
    ("<=", 'baz', 5, 'bbb'),
    (">=", 'foo', "'bbc'", 'ccc'),
    (">=", 'foo', "'ccc'", 'ccc'),
    ("==", 'foo', "'ccc'", 'ccc'),
    ("!=", 'qux', "'qqq'", 'ccc'),
])
def test_filter_operations(basic_frame, operation, column, value, expected):
    frame = query(basic_frame, {'where': [operation, column, value]})
    assert_rows(frame, [expected])


def test_negation(basic_frame):
    frame = query(basic_frame, {'where': ["!", ["==", "qux", "'qqq'"]]})
    assert_rows(frame, ['ccc'])


def test_and(basic_frame):
    frame = query(basic_frame, {'where': ["&", ["==", "qux", "'qqq'"], [">", "baz", 6]]})
    assert_rows(frame, ['aaa'])


def test_or(basic_frame):
    frame = query(basic_frame, {'where': ["|", ["==", "baz", 5], ["==", "baz", 7]]})
    assert_rows(frame, ['bbb', 'aaa'])


def test_col_in_list(basic_frame):
    frame = query(basic_frame, {'where': ["in", "baz", [5, 8, -2]]})
    assert_rows(frame, ['bbb'])


def test_null_value(basic_frame):
    frame = query(basic_frame, {'where': ["isnull", "bar"]})
    assert_rows(frame, ['ccc'])


@pytest.mark.skipif(True, reason='This should work I think, but it does not... Perhaps a bug in Pandas.')
def test_string_in_col(basic_frame):
    frame = query(basic_frame, {'where': ["in", "'bb'", "foo"]})
    assert_rows(frame, ['bbb'])

# UndefinedVariableError, happens when row is referred that does not exist
# Error cases

############### Projections #######################


def test_select_subset(basic_frame):
    frame = query(basic_frame, {'select': ['foo', 'baz']})
    assert list(frame.columns) == ['foo', 'baz']


def test_select_subset_invalid_column(basic_frame):
    with pytest.raises(MalformedQueryException):
        query(basic_frame, {'select': ['foof', 'baz']})


################ Aggregation #####################

#TODO: More tests and error handling

def test_basic_sum_aggregation(basic_frame):
    expected = pandas.read_csv(StringIO("""
qux,baz
qqq,12
www,9"""))

    frame = query(basic_frame, {
        'select': ['qux', ['sum', 'baz']],
        'group_by': ['qux']})

    assert frame.to_csv() == expected.to_csv()


def test_basic_count_aggregation(basic_frame):
    expected = pandas.read_csv(StringIO("""
qux,baz
qqq,2
www,1"""))

    frame = query(basic_frame, {
        'select': ['qux', ['count', 'baz']],
        'group_by': ['qux']})

    assert frame.to_csv() == expected.to_csv()


def test_count_without_aggregation(basic_frame):
    expected = pandas.read_csv(StringIO("""
count
3"""))

    frame = query(basic_frame, {'select': [['count']]})
    assert frame.to_csv() == expected.to_csv()


def test_max_without_aggregation(basic_frame):
    expected = pandas.read_csv(StringIO("""
max
9"""))

    frame = query(basic_frame, {'select': [['max', 'baz']]})
    assert frame.to_csv() == expected.to_csv()

############### Ordering ################

def test_single_column_ascending_ordering(basic_frame):
    frame = query(basic_frame, {'order_by': ['foo']})
    assert_rows(frame, ['aaa', 'bbb', 'ccc'])


def test_single_column_decending_ordering(basic_frame):
    frame = query(basic_frame, {'order_by': ['-foo']})
    assert_rows(frame, ['ccc', 'bbb', 'aaa'])


def test_sort_on_unknown_column(basic_frame):
    with pytest.raises(MalformedQueryException):
        query(basic_frame, {'order_by': ['foof']})


############## Slicing ##################

def test_offset_and_limit(basic_frame):
    frame = query(basic_frame, {"offset": 1, "limit": 1})
    assert_rows(frame, ['aaa'])

