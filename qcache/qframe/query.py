from __future__ import unicode_literals
import re

from pandas import DataFrame
from pandas.core.computation.ops import UndefinedVariableError
from pandas.core.groupby import DataFrameGroupBy
from qcache.qframe.pandas_filter import pandas_filter
from qcache.qframe.common import assert_list, assert_integer, raise_malformed, MalformedQueryException


CLAUSE_WHERE = 'where'
CLAUSE_GROUP_BY = 'group_by'
CLAUSE_DISTINCT = 'distinct'
CLAUSE_SELECT = 'select'
CLAUSE_ORDER_BY = 'order_by'
CLAUSE_OFFSET = 'offset'
CLAUSE_LIMIT = 'limit'
CLAUSE_FROM = 'from'
QUERY_CLAUSES = {CLAUSE_WHERE, CLAUSE_GROUP_BY, CLAUSE_DISTINCT, CLAUSE_SELECT,
                 CLAUSE_ORDER_BY, CLAUSE_OFFSET, CLAUSE_LIMIT, CLAUSE_FROM}


def _group_by(dataframe, group_by_q):
    if not group_by_q:
        return dataframe

    assert_list('group_by', group_by_q)

    try:
        return dataframe.groupby(group_by_q, as_index=False)
    except KeyError:
        raise_malformed('Group by column not in table', group_by_q)


def is_aggregate_function(expr):
    return type(expr) is list and len(expr) == 2


def is_alias_assignment(expr):
    """
    Examples:
    ['=', 'column_name', 1]                                       Constant assignment
    ['=', 'column_name', 'other_column']                          Basic aliasing
    ['=', 'column_name', ['sin', 'column_name']]
    ['=', 'column_name', ['+', 'column_name', 'other_column']]    Complex calculations
    """
    return type(expr) is list and len(expr) == 3 and expr[0] == '='


def _aggregate(dataframe_group_by, project_q, aggregate_fns):
    if not aggregate_fns:
        raise_malformed("Aggregate function required when group_by is specified", project_q)

    try:
        return dataframe_group_by.agg(aggregate_fns)
    except AttributeError as e:
        functions = [fn_name for fn_name in aggregate_fns.values() if fn_name in str(e)]
        raise_malformed("Unknown aggregation function '{fn}'".format(fn=functions[0]), project_q)


def _aggregate_without_group_by(dataframe, project_q, aggregate_fns):
    if len(aggregate_fns) != len(project_q):
        raise_malformed('Cannot mix aggregation functions and columns without group_by clause', project_q)

    results = {}
    for column_name, fn_name in aggregate_fns.items():
        # Intricate, apply the selected function to the selected column
        temp_dataframe = dataframe[[column_name]]
        fn = getattr(temp_dataframe, fn_name, None)
        if not fn or not callable(fn):
            raise_malformed('Unknown aggregation function', project_q)

        results[column_name] = [fn(axis=0)[0]]

    # The result must be a data frame
    return DataFrame.from_dict(results)

ALIAS_STRING = "^([A-Za-z0-9_-]+)$"
ALIAS_RE = re.compile(ALIAS_STRING)


def _build_eval_expression(expr):
    if type(expr) is list:
        if len(expr) == 3:
            arg1 = _build_eval_expression(expr[1])
            arg2 = _build_eval_expression(expr[2])
            op = expr[0]
            return "({arg1} {op} {arg2})".format(arg1=arg1, op=op, arg2=arg2)

        if len(expr) == 2:
            arg1 = _build_eval_expression(expr[1])
            op = expr[0]
            return "{op}({arg1})".format(op=op, arg1=arg1)

        raise_malformed('Invalid number of arguments', expr)

    return expr


def _alias(dataframe, expressions):
    result_frame = dataframe
    for expression in expressions:
        destination, source = expression[1], expression[2]
        if not isinstance(destination, basestring):
            raise_malformed('Invalid alias, must be a string', expression)

        if not re.match(ALIAS_RE, destination):
            raise_malformed('Invalid alias, must match {alias}'.format(alias=ALIAS_STRING), expression)

        eval_expr = _build_eval_expression(source)
        try:
            result_frame = result_frame.eval('{destination} = {expr}'.format(destination=destination, expr=eval_expr), inplace=False)
        except (SyntaxError, ValueError):
            raise_malformed('Unknown function in alias', source)

    return result_frame


def classify_expressions(project_q):
    aggregate_functions = {}
    alias_expressions = []
    for expression in project_q:
        if is_aggregate_function(expression):
            aggregate_functions[expression[1]] = expression[0]
        elif is_alias_assignment(expression):
            alias_expressions.append(expression)
        elif type(expression) is list:
            raise_malformed('Invalid expression in select', expression)

    return aggregate_functions, alias_expressions


def _project(dataframe, project_q):
    if not project_q:
        return dataframe

    assert_list('project', project_q)

    if project_q == [['count']]:
        # Special case for count only, ~equal to SQL count(*)
        return DataFrame.from_dict({'count': [len(dataframe)]})

    aggregate_fns, alias_expressions = classify_expressions(project_q)

    if aggregate_fns and alias_expressions:
        raise_malformed("Cannot mix aliasing and aggregation functions", project_q)

    if isinstance(dataframe, DataFrameGroupBy):
        dataframe = _aggregate(dataframe, project_q, aggregate_fns)
    elif aggregate_fns:
        return _aggregate_without_group_by(dataframe, project_q, aggregate_fns)
    elif alias_expressions:
        dataframe = _alias(dataframe, alias_expressions)
    else:
        # Nothing to do here
        pass

    columns = [e if type(e) is not list else e[1] for e in project_q]

    try:
        return dataframe[columns]
    except KeyError:
        missing_columns = set(columns) - set(dataframe.columns.values)
        raise_malformed("Selected columns not in table", list(missing_columns))


def _order_by(dataframe, order_q):
    if not order_q:
        return dataframe

    assert_list('order_by', order_q)
    if not all(isinstance(c, basestring) for c in order_q):
        raise_malformed("Invalid order by format", order_q)

    columns = [e[1:] if e.startswith('-') else e for e in order_q]
    ascending = [not e.startswith('-') for e in order_q]

    try:
        return dataframe.sort_values(by=columns, ascending=ascending)
    except KeyError:
        raise_malformed("Order by column not in table", columns)


def _do_slice(dataframe, offset, limit):
    if offset:
        assert_integer('offset', offset)
        dataframe = dataframe[offset:]

    if limit:
        assert_integer('limit', limit)
        dataframe = dataframe[:limit]

    return dataframe


def _distinct(dataframe, columns):
    if columns is None:
        return dataframe

    args = {}
    if columns:
        args['subset'] = columns

    return dataframe.drop_duplicates(**args)


def query(dataframe, q):
    if not isinstance(q, dict):
        raise MalformedQueryException('Query must be a dictionary, not "{q}"'.format(q=q))

    key_set = set(q.keys())
    if not key_set.issubset(QUERY_CLAUSES):
        raise MalformedQueryException('Unknown query clauses: {keys}'.format(
            keys=', '.join(key_set.difference(QUERY_CLAUSES))))

    try:
        if CLAUSE_FROM in q:
            dataframe, _ = query(dataframe, q[CLAUSE_FROM])

        filtered_df = pandas_filter(dataframe, q.get('where'))
        grouped_df = _group_by(filtered_df, q.get('group_by'))
        distinct_df = _distinct(grouped_df, q.get('distinct'))
        projected_df = _project(distinct_df, q.get('select'))
        ordered_df = _order_by(projected_df, q.get('order_by'))
        sliced_df = _do_slice(ordered_df, q.get('offset'), q.get('limit'))
        return sliced_df, len(ordered_df)
    except UndefinedVariableError as e:
        raise MalformedQueryException(e.message)
