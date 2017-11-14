# coding=utf-8
import json
from contextlib import contextmanager
import pytest
import time

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


def assert_rows(qframe, rows, column='foo'):
    frame = qframe.df
    assert len(frame) == len(rows)

    for ix, row in enumerate(rows):
        assert frame.iloc[ix][column] == row


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


def test_and_with_only_one_clause(basic_frame):
    frame = basic_frame.query({'where': ["&", ["==", "foo", "'aaa'"]]})
    assert_rows(frame, ['aaa'])

    frame = basic_frame.query({'where': ["&", ["==", "foo", "'abc'"]]})
    assert_rows(frame, [])


def test_or(basic_frame):
    frame = basic_frame.query({'where': ["|", ["==", "baz", 5], ["==", "baz", 7]]})
    assert_rows(frame, ['bbb', 'aaa'])


def test_or_with_only_one_clause(basic_frame):
    frame = basic_frame.query({'where': ["|", ["==", "foo", "'aaa'"]]})
    assert_rows(frame, ['aaa'])

    frame = basic_frame.query({'where': ["|", ["==", "foo", "'abc'"]]})
    assert_rows(frame, [])


def test_col_in_list(basic_frame):
    frame = basic_frame.query({'where': ["in", "baz", [5, 8, -2]]})
    assert_rows(frame, ['bbb'])


def test_null_value(basic_frame):
    frame = basic_frame.query({'where': ["isnull", "bar"]})
    assert_rows(frame, ['ccc'])


@pytest.mark.skipif(True, reason='This should work I think, but it does not...')
def test_string_in_col(basic_frame):
    frame = basic_frame.query({'where': ["contains", "foo", "'bb'"]})
    assert_rows(frame, ['bbb'])


def test_unknown_column_name(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': ["==", "unknown", 3]})


def test_invalid_column_name(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': ["==", "<foo:3>", 3]})


def test_empty_filter_returns_same_frame(basic_frame):
    assert basic_frame.query({'where': []}).df.equals(basic_frame.df)


def test_empty_filter_clause_not_allowed(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': ["|", []]})


@pytest.mark.parametrize("operation", ["!", "isnull"])
def test_single_argument_operators_require_single_argument(basic_frame, operation):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': [operation, 'foo', 'bar']})


@pytest.mark.parametrize("operation", ["<", ">", ">", "<=", "<=", ">=", ">=", "==", "!=", "in"])
def test_double_argument_operators_require_single_argument(basic_frame, operation):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': [operation, 'foo']})

    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': [operation, 'foo', 'bar', 'baz']})


@pytest.mark.parametrize("operation", ["&", "|"])
def test_and_or_requires_at_least_one_argument(basic_frame, operation):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'where': [operation]})


@pytest.fixture
def bitwise_frame():
    data = """foo,bar,baz
    1,1.5,abc
    2,1.5,def
    3,1.5,ghi
    4,1.5,ijk
    5,1.5,lmn"""

    return QFrame.from_csv(data)


@pytest.mark.parametrize("filter, expected_rows", [
    (1,  [1, 3, 5]),
    (2,  [2, 3]),
    (3,  [3]),
    (4,  [4, 5]),
    (5,  [5]),
    (6,  []),
])
def test_bitwise_all_bits_with_constant(filter, expected_rows, bitwise_frame):
    result = bitwise_frame.query({'where': ["all_bits", "foo", filter]})
    assert_rows(result, expected_rows)


@pytest.mark.parametrize("filter, expected_rows", [
    (1,  [1, 3, 5]),
    (2,  [2, 3]),
    (3,  [1, 2, 3, 5]),
    (4,  [4, 5]),
    (5,  [1, 3, 4, 5]),
    (6,  [2, 3, 4, 5]),
    (8,  []),
])
def test_bitwise_any_bits_with_constant(filter, expected_rows, bitwise_frame):
    result = bitwise_frame.query({'where': ["any_bits", "foo", filter]})
    assert_rows(result, expected_rows)


def test_bitwise_invalid_arg(bitwise_frame):
    with pytest.raises(MalformedQueryException):
        bitwise_frame.query({'where': ["any_bits", "foo", 1.3]})


def test_bitwise_invalid_column_type(bitwise_frame):
    with pytest.raises(MalformedQueryException):
        bitwise_frame.query({'where': ["any_bits", "baz", 1]})


def test_bitwise_column_missing(bitwise_frame):
    with pytest.raises(MalformedQueryException):
        bitwise_frame.query({'where': ["any_bits", "dont_exist", 1]})


def test_bitwise_invalid_filter_length(bitwise_frame):
    with pytest.raises(MalformedQueryException):
        bitwise_frame.query({'where': ["any_bits", "foo", 1, 2]})


@pytest.fixture
def string_frame():
    data = """foo,bar
    1,abcd
    2,defg
    3,ghij
    4,gxyj"""

    return QFrame.from_csv(data)


@pytest.mark.parametrize("operator, filter, expected_rows", [
    ("like", "'a%'",   [1]),
    ("like", "'%g'",   [2]),
    ("like", "'%d%'",  [1, 2]),
    ("like", "'%cc%'", []),
    ("like", "''",     []),
    ("like", "'%'",    [1, 2, 3, 4]),
    ("like", "'%%'",   [1, 2, 3, 4]),
    ("like", "'%D%'",  []),
    ("ilike", "'%D%'",  [1, 2]),
    ("like", "'%g[a-z]{2}j%'",  [3, 4]),
    ("like", "'%g[a-z]{3}j%'",  []),
    ("like", "'g[a-z]{2}j'",  [3, 4]),
    ("like", "'g[a-z]{2}'",  []),
    ("like", "'g[a-z]{2}%'",  [3, 4]),
    ("like", "'g[a-z]{3}'",  [3, 4]),
])
def test_like(operator, filter, expected_rows, string_frame):
    result = string_frame.query({'where': [operator, "bar", filter]})
    assert_rows(result, expected_rows)


def test_like_missing_quotes_on_argument(string_frame):
    with pytest.raises(MalformedQueryException):
        string_frame.query({'where': ['like', "bar", "%abc%"]})


def test_like_invalid_argument_type(string_frame):
    with pytest.raises(MalformedQueryException):
        string_frame.query({'where': ['like', "bar", 12]})


def test_like_invalid_column_type(string_frame):
    with pytest.raises(MalformedQueryException):
        string_frame.query({'where': ['like', "foo", "'%a%'"]})


############### Sub select ##################


@pytest.mark.parametrize("data", [
    """foo,bar
    1,1
    2,1
    3,2""",   # Numbers
    """foo,bar
    1,aa
    2,aa
    3,bb""",  # Strings
    """foo,bar
    1,
    2,
    3,bb""",  # null/None
])
def test_sub_select(data):
    frame = QFrame.from_csv(data)

    result = frame.query({'where': ['in', 'bar', {'where': ['==', 'foo', 2]}]})

    assert_rows(result, [1, 2])


def test_sub_select_in_column_missing_in_sub_select():
    frame = QFrame.from_csv("""foo,bar
    1,aa""")

    with pytest.raises(MalformedQueryException):
        frame.query({'where': ['in', 'bar', {'select': ['foo'],
                                             'where': ['==', 'foo', 2]}]})


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

# TODO: More tests and error handling

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


def test_unknown_aggregation_function(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({
            'select': ['qux', ['foo_bar', 'baz']],
            'group_by': ['qux']})


def test_missing_aggregation_function(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({
            'select': ['qux'],
            'group_by': ['qux']})


def test_count_without_aggregation(basic_frame):
    expected = QFrame.from_csv("""
count
3""")

    frame = basic_frame.query({'select': [['count']]})
    assert frame.to_csv() == expected.to_csv()


def test_max_without_aggregation(basic_frame):
    expected = QFrame.from_csv("""
baz
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
    data = [{'foo': 'aaa', 'bar': u'Iñtërnâtiônàližætiøn'},
            {'foo': 'bbb', 'bar': u'räksmörgås'.encode(encoding='utf-8')}]
    input_frame = QFrame.from_dicts(data)
    frame = input_frame.query({'where': ["==", "bar", u"'räksmörgås'"]})

    assert_rows(frame, ['bbb'])


@pytest.fixture
def calculation_frame():
    data = """
foo,bar
1,10
1,11
2,20
3,30
3,33"""

    return QFrame.from_csv(data)


def test_column_aliasing(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", "foo"]]})

    assert frame.to_dicts() == [
        {"baz": 1},
        {"baz": 1},
        {"baz": 2},
        {"baz": 3},
        {"baz": 3}
    ]


def test_constant_int_aliasing(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", 55]],
                                     "limit": 2})

    assert frame.to_dicts() == [
        {"baz": 55},
        {"baz": 55},
    ]


def test_constant_string_aliasing(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", "'qux'"]],
                                     "limit": 2})

    assert frame.to_dicts() == [
        {"baz": "qux"},
        {"baz": "qux"},
    ]


def test_alias_as_sum_of_two_other_columns(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", ["+", "bar", "foo"]]],
                                     "limit": 2})

    assert frame.to_dicts() == [
        {"baz": 11},
        {"baz": 12},
    ]


def test_alias_as_nested_expression(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", ["+", ["*", "bar", 2], "foo"]]],
                                     "limit": 2})

    assert frame.to_dicts() == [
        {"baz": 21},
        {"baz": 23},
    ]


def test_alias_with_single_argument_function(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", ["sqrt", ["+", 3, "foo"]]]],
                                     "limit": 1})

    assert frame.to_dicts() == [{"baz": 2}]


@pytest.fixture
def frame_with_zero():
    data = """
foo,bar
1,0
1,11"""

    return QFrame.from_csv(data)


def test_alias_with_division_by_zero(frame_with_zero):
    frame = frame_with_zero.query({"select": [["=", "baz", ["/", "foo", "bar"]]],
                                   "limit": 1})

    assert frame.to_dicts() == [{"baz": float("inf")}]


def test_invalid_alias_target_string_with_invalid_character(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "ba/r", 1]]})


def test_invalid_alias_target_non_string(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", 23, 1]]})


def test_aliasing_does_not_overwrite_original_qframe(calculation_frame):
    frame = calculation_frame.query({"select": [["=", "baz", "foo"]]})
    assert list(frame.columns.values) == ['baz']
    assert 'baz' not in list(calculation_frame.df.columns.values)


def test_cannot_mix_aliasing_and_aggregation_expressions(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "bar", 1], ["max", "foo"]],
                                 "group_by": ["bar"]})


def test_aliasing_with_wrong_number_of_parameters_in_function(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "baz", ["+", "bar", "foo", "foo"]]]})


def test_aliasing_with_unknown_function(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "baz", ["?", "bar", "foo"]]]})


def test_aliasing_with_unknown_function_2(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "baz", ["zin", "bar"]]]})


def test_aliasing_with_invalid_arity(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["=", "baz", ["zin", "bar"], "foobar"]]})


def test_multiple_aggregation_functions_without_group_by(calculation_frame):
    frame = calculation_frame.query({"select": [["max", "bar"], ["min", "foo"]]})
    assert frame.to_dicts() == [{"bar": 33, "foo": 1}]


def test_cannot_mix_aggregation_functions_and_columns_without_group_by(calculation_frame):
    with pytest.raises(MalformedQueryException):
        calculation_frame.query({"select": [["max", "bar"], "foo"]})


################# Sub queries ###################


@pytest.fixture
def subselect_frame():
    data = """
foo,bar
1,10
1,15
5,50"""

    return QFrame.from_csv(data)


def test_alias_aggregation_from_sub_select(subselect_frame):
    frame = subselect_frame.query({"select": [["=", "foo_pct",
                                               ["*", 100, ["/", "foo", "bar"]]]],
                                   "from":
                                       {"select": ["foo", ["sum", "bar"]],
                                        "group_by": ["foo"]}})

    assert frame.to_dicts() == [
        {"foo_pct": 4.0},
        {"foo_pct": 10.0}
    ]


################ Enums ########################

@pytest.fixture
def enum_data():
    return """
foo,bar
ccc,10
ccc,11
ccc,12
ccc,13
ccc,14
ccc,15
ccc,16
bbb,20
aaa,25"""


@pytest.fixture
def enum_frame(enum_data):
    return QFrame.from_csv(enum_data, column_types={'foo': 'category'})


def test_enum_basic_sorting(enum_frame):
    assert enum_frame.query({'order_by': ['foo']}).to_dicts() == [
        {'foo': 'aaa', 'bar': 25},
        {'foo': 'bbb', 'bar': 20},
        {'foo': 'ccc', 'bar': 10},
        {'foo': 'ccc', 'bar': 11},
        {'foo': 'ccc', 'bar': 12},
        {'foo': 'ccc', 'bar': 13},
        {'foo': 'ccc', 'bar': 14},
        {'foo': 'ccc', 'bar': 15},
        {'foo': 'ccc', 'bar': 16},
    ]


def test_enum_filter_by_equality(enum_frame):
    assert enum_frame.query({'where': ['==', 'foo', '"bbb"']}).to_dicts() == [
        {'foo': 'bbb', 'bar': 20},
    ]


def test_enum_filter_by_order_comparison_not_possible(enum_frame):
    with pytest.raises(MalformedQueryException):
        enum_frame.query({'where': ['<', 'foo', '"bbb"']})


def test_enum_size(enum_frame, enum_data):
    # Space savings should be possible using categorials
    # when multiple rows containing the same value exists.
    frame = QFrame.from_csv(enum_data)
    assert enum_frame.byte_size() < frame.byte_size()


def test_enum_from_dicts(enum_frame):
    cat_frame = QFrame.from_dicts(enum_frame.to_dicts(), column_types={'foo': 'category'})
    frame = QFrame.from_dicts(enum_frame.to_dicts())

    assert cat_frame.byte_size() < frame.byte_size()


############# NaN ###############


def test_like_ignores_nan_values():
    f = QFrame.from_csv("""
    foo,bar
    aaa,xyz
    bbb,""")

    assert f.query({'where': ['ilike', 'bar', '"ccc"']}).to_dicts() == []


def test_only_empty_string_is_nan():
    f = QFrame.from_csv("""
    foo,bar
    aaa,N/A
    aaa,n/a
    aaa,NA
    aaa,na
    aaa,nan
    aaa,NaN
    aaa,-NaN
    aaa,null
    aaa,NULL
    bbb,""")

    assert json.loads(f.query({'select': ['bar']}).to_json()) == [
        {"bar": "N/A"},
        {"bar": "n/a"},
        {"bar": "NA"},
        {"bar": "na"},
        {"bar": "nan"},
        {"bar": "NaN"},
        {"bar": "-NaN"},
        {"bar": "null"},
        {"bar": "NULL"},
        {"bar": None},
    ]


################# Update ######################


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


def test_unknown_update_function(basic_frame):
    with pytest.raises(MalformedQueryException):
        basic_frame.query({'update': [['_', 'bar', 2.0]],
                           'where': ['==', 'foo', '"bbb"']})


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


################### Performance ####################


@pytest.fixture
def large_frame():
    d = 1000000 * [{'aaa': 123456789, 'bbb': 'abcdefghijklmnopqrvwxyz', 'ccc': 1.23456789}]
    return QFrame.from_dicts(d)


@contextmanager
def timeit(name):
    t0 = time.time()
    yield
    print('\n{name} duration: {duration} s'.format(name=name, duration=time.time()-t0))


@pytest.mark.benchmark
def test_large_frame_csv(large_frame):
    with timeit('to_csv'):
        csv_string = large_frame.to_csv()

    with timeit('from_csv'):
        QFrame.from_csv(csv_string)

    # Results:
    # to_csv duration: 2.43983101845 s
    # from_csv duration: 0.532874107361 s


@pytest.mark.benchmark
def test_large_frame_json(large_frame):
    with timeit('to_json'):
        large_frame.to_json()

    # with timeit('from_json'):
    #    QFrame.from_json(json_string)

    # to_json duration: 0.792788982391 s
    # from_json duration: 3.07192707062 s, This implementation no longer exists


@pytest.mark.benchmark
@pytest.mark.skipif(True, reason="No implementation")
def test_large_frame_msgpack(large_frame):
    # NOTE: This implementation does not exist but once did as an experiment
    #       This test is left as reference and reminder
    with timeit('to_msgpack'):
        msgpack_string = large_frame.to_msgpack()

    with timeit('from_msgpack'):
        QFrame.from_msgpack(msgpack_string)

    # These numbers explain why there is no msgpack implementation
    # to_msgpack duration: 7.02977800369 s
    # from_msgpack duration: 1.52387404442 s

    # It's not because msgpack is slow (it's fast), it's because the
    # code has to first create a list of python dicts and then serialize
    # that using msgpack rather than serializing the dataframe to msgpack
    # directly.

# Not
# Disjunction and conjunction
# Refactor tests to check complete column not just the row that is supposed to be affected
# Mix self referring updates and assignments in same update
# Any way to merge the filter code for select and update (is the update version as performant as the where)?


def xtest_update_with_conjunction(basic_frame):
    basic_frame.query({'update': [['bar', 2.0]],
                       'where': ['==', 'foo', '"bbb"']})

    assert basic_frame.to_dicts()[0]['bar'] == 3.25
