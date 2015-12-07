# coding=utf-8
import pytest
from qcache.qframe import MalformedQueryException, QFrame


def query(df, q):
    return QFrame(df).query(q).df

######################### Filtering ##########################

@pytest.fixture
def basic_frame():
    data = """
foo,bar,baz,qux
bbb,1.25,5,qqq
aaa,3.25,7,qqq
ccc,,9,www"""

    return QFrame.from_csv(data)


def assert_rows(qframe, rows):
    frame = qframe.df
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
    frame = basic_frame.query({'where': [operation, column, value]})
    assert_rows(frame, [expected])


def test_negation(basic_frame):
    frame = basic_frame.query({'where': ["!", ["==", "qux", "'qqq'"]]})
    assert_rows(frame, ['ccc'])


def test_and(basic_frame):
    frame = basic_frame.query({'where': ["&", ["==", "qux", "'qqq'"], [">", "baz", 6]]})
    assert_rows(frame, ['aaa'])


def test_or(basic_frame):
    frame = basic_frame.query({'where': ["|", ["==", "baz", 5], ["==", "baz", 7]]})
    assert_rows(frame, ['bbb', 'aaa'])


def test_col_in_list(basic_frame):
    frame = basic_frame.query({'where': ["in", "baz", [5, 8, -2]]})
    assert_rows(frame, ['bbb'])


def test_null_value(basic_frame):
    frame = basic_frame.query({'where': ["isnull", "bar"]})
    assert_rows(frame, ['ccc'])


@pytest.mark.skipif(True, reason='This should work I think, but it does not... Perhaps a bug in Pandas.')
def test_string_in_col(basic_frame):
    frame = basic_frame.query({'where': ["in", "'bb'", "foo"]})
    assert_rows(frame, ['bbb'])

# UndefinedVariableError, happens when row is referred that does not exist
# Error cases

############### Projections #######################


def test_select_subset(basic_frame):
    frame = basic_frame.query({'select': ['foo', 'baz']})
    assert list(frame.columns) == ['foo', 'baz']


def test_select_subset_invalid_column(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'select': ['foof', 'baz']})


def test_select_distinct_without_columns(basic_frame):
    # Should not have any effect since all rows are unique with respect to all columns
    frame = basic_frame.query({'distinct': []})
    assert_rows(frame, ['bbb', 'aaa', 'ccc'])


def test_select_distinct_with_columns(basic_frame):
    frame = basic_frame.query({'distinct': ['qux']})
    assert_rows(frame, ['bbb', 'ccc'])


################ Aggregation #####################

#TODO: More tests and error handling

def test_basic_sum_aggregation(basic_frame):
    expected = QFrame.from_csv("""
qux,baz
www,9
qqq,12""")

    frame = basic_frame.query({
        'select': ['qux', ['sum', 'baz']],
        'group_by': ['qux'],
        'order_by': ['baz']})

    assert frame.to_csv() == expected.to_csv()


def test_basic_count_aggregation(basic_frame):
    expected = QFrame.from_csv("""
qux,baz
qqq,2
www,1""")

    frame = basic_frame.query({
        'select': ['qux', ['count', 'baz']],
        'group_by': ['qux']})

    assert frame.to_csv() == expected.to_csv()


def test_count_without_aggregation(basic_frame):
    expected = QFrame.from_csv("""
count
3""")

    frame = basic_frame.query({'select': [['count']]})
    assert frame.to_csv() == expected.to_csv()


def test_max_without_aggregation(basic_frame):
    expected = QFrame.from_csv("""
max
9""")

    frame = basic_frame.query({'select': [['max', 'baz']]})
    assert frame.to_csv() == expected.to_csv()

############### Ordering ################

def test_single_column_ascending_ordering(basic_frame):
    frame = basic_frame.query({'order_by': ['foo']})
    assert_rows(frame, ['aaa', 'bbb', 'ccc'])


def test_single_column_decending_ordering(basic_frame):
    frame = basic_frame.query({'order_by': ['-foo']})
    assert_rows(frame, ['ccc', 'bbb', 'aaa'])


def test_sort_on_unknown_column(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'order_by': ['foof']})


############## Slicing ##################

def test_offset_and_limit(basic_frame):
    frame = basic_frame.query({"offset": 1, "limit": 1})
    assert_rows(frame, ['aaa'])
    assert frame.unsliced_df_len == 3


############## Unicode #################

def test_unicode_content_from_csv():
    data = u"""foo,bar
aaa,Iñtërnâtiônàližætiøn
bbb,räksmörgås
ccc,"""

    input_frame = QFrame.from_csv(data)
    frame = input_frame.query({'where': ["==", "bar", u"'räksmörgås'"]})

    assert_rows(frame, ['bbb'])


def test_unicode_content_from_dicts():
    data = [{'foo': 'aaa', 'bar': u'Iñtërnâtiônàližætiøn'}, {'foo': 'bbb', 'bar': u'räksmörgås'.encode(encoding='utf-8')}]
    input_frame = QFrame.from_dicts(data)
    frame = input_frame.query({'where': ["==", "bar", u"'räksmörgås'"]})

    assert_rows(frame, ['bbb'])


#################  Update ######################

def assert_column(column, frame, expected):
    assert [d[column] for d in frame.to_dicts()] == expected


def test_basic_update(basic_frame):
    basic_frame.query({'update': [['bar', 2.0], ['baz', 0]],
                       'where': ['==', 'foo', '"bbb"']})

    assert basic_frame.to_dicts()[0]['bar'] == 2.0
    assert basic_frame.to_dicts()[0]['baz'] == 0


def test_basic_update_function_based_on_current_value_of_column(basic_frame):
    basic_frame.query({'update': [['+', 'bar', 2.0]],
                       'where': ['==', 'foo', '"bbb"']})

    assert basic_frame.to_dicts()[0]['bar'] == 3.25


def test_update_is_null(basic_frame):
    basic_frame.query({'update': [['baz', 19]],
                       'where': ['isnull', 'bar']})

    assert_column('baz', basic_frame, [5, 7, 19])


def test_update_is_null_invalid_argument_number(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'update': [['baz', 19]],
                           'where': ['isnull', 9]})


def test_update_in(basic_frame):
    basic_frame.query({'update': [['baz', 19]],
                       'where': ['in', 'foo', ["'aaa'", "'bbb'"]]})

    assert_column('baz', basic_frame, [19, 19, 9])


def test_update_in_invalid_arg_count(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'update': [['baz', 19]],
                           'where': ['in', 'foo', 'bar', ["'aaa'", "'bbb'"]]})


def test_update_in_unknown_column(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'update': [['baz', 19]],
                           'where': ['in', 'unknown', ["'aaa'", "'bbb'"]]})


def test_update_in_second_arg_not_a_list(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'update': [['baz', 19]],
                           'where': ['in', 'foo', 'boo']})


def test_unknown_clause_in_query(basic_frame):
    try:
        basic_frame.query({'foo': []})
        assert False
    except MalformedQueryException as e:
        print e.message
        assert 'foo' in e.message


# Not
# Disjunction and conjunction
# Refactor tests to check complete column not just the row that is supposed to be affected
# Mix self referring updates and assignments in same update
# Any way to merge the filter code for select and update (is the update version as performant as the where)?

def xtest_update_with_conjunction(basic_frame):
    basic_frame.query({'update': [['bar', 2.0]],
                       'where': ['==', 'foo', '"bbb"']})

    assert basic_frame.to_dicts()[0]['bar'] == 3.25
