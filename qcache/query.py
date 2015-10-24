from __future__ import unicode_literals
import json
from pandas import DataFrame
from pandas.computation.ops import UndefinedVariableError
from pandas.core.groupby import DataFrameGroupBy


class MalformedQueryException(Exception):
    pass


def raise_malformed(message, q):
    raise MalformedQueryException(message + ': {q}'.format(q=q))


def assert_integer(name, i):
    if not isinstance(i, (int, long)):
        raise_malformed('Invalid type for {name}'.format(name=name), i)


def assert_list(name, l):
    if not isinstance(l, list):
        raise_malformed('Invalid format for {name}'.format(name=name), l)


def build_filter(q):
    if type(q) is not list:
        return unicode(q)

    if not q:
        raise_malformed("Empty expression not allowed", q)

    op = q[0]
    if op == "!":
        if len(q) != 2:
            raise_malformed("! is a single arity operator, invalid number of arguments", q)

        result = "not " + build_filter(q[1])
    elif op == "isnull":
        if len(q) != 2:
            raise_malformed("! is a single arity operator, invalid number of arguments", q)

        # Slightly hacky but the only way I've come up with so far.
        result = "({arg} != {arg})".format(arg=q[1])
    elif op in ('==', '!=', '<', '<=', '>', '>='):
        if len(q) != 3:
            raise_malformed("Invalid number of arguments", q)

        _, arg1, arg2 = q
        result = build_filter(arg1) + " " + op + " " + build_filter(arg2)
    elif op in ('&', '|'):
        if len(q) < 3:
            raise_malformed("Invalid number of arguments", q)

        result = ' {op} '.format(op=op).join(build_filter(x) for x in q[1:])
    elif op == 'in':
        if len(q) != 3:
            raise_malformed("Invalid number of arguments", q)

        _, arg1, arg2 = q
        result = '{arg1} in {arg2}'.format(arg1=arg1, arg2=arg2)
    else:
        raise_malformed("Unknown operator", q)

    return "({result})".format(result=result)


def do_filter(dataframe, filter_q):
    if filter_q:
        assert_list('where', filter_q)
        filter_str = build_filter(filter_q)
        return dataframe.query(filter_str)

    return dataframe


def group_by(dataframe, group_by_q):
    if not group_by_q:
        return dataframe

    assert_list('where', group_by_q)

    try:
        return dataframe.groupby(group_by_q, as_index=False)
    except KeyError:
        raise_malformed('Group by column not in table', group_by_q)


def project(dataframe, project_q):
    if not project_q:
        return dataframe

    assert_list('project', project_q)

    if project_q == [['count']]:
        # Special case for count only, ~equal to SQL count(*)
        return DataFrame.from_dict({'count': [len(dataframe)]})

    aggregate_fns = {e[1]: e[0] for e in project_q if type(e) is list}
    if aggregate_fns:
        if not isinstance(dataframe, DataFrameGroupBy):
            if len(aggregate_fns) > 1:
                raise_malformed("Multiple aggregation functions without group by", project_q)

            # Intricate, apply the selected function to the selected column
            arg, fn_name = next(iter(aggregate_fns.items()))
            dataframe = dataframe[[arg]]

            fn = getattr(dataframe, fn_name, None)
            if not fn or not callable(fn):
                raise_malformed('Unknown function', project_q)

            result = fn(axis=0)[0]

            # The response must be a data frame
            return DataFrame.from_dict({fn_name: [result]})

        dataframe = dataframe.agg(aggregate_fns)

    columns = [e if type(e) is not list else e[1] for e in project_q]
    try:
        return dataframe[columns]
    except KeyError:
        raise_malformed("Selected column not in table", columns)


def order_by(dataframe, order_q):
    if not order_q:
        return dataframe

    assert_list('order by', order_q)
    if not all(isinstance(c, basestring) for c in order_q):
        raise_malformed("Invalid order by format", order_q)

    columns = [e[1:] if e.startswith('-') else e for e in order_q]
    ascending = [not e.startswith('-') for e in order_q]

    try:
        return dataframe.sort(columns, ascending=ascending)
    except KeyError:
        raise_malformed("Order by column not in table", columns)


def do_slice(dataframe, offset, limit):
    if offset:
        assert_integer('offset', offset)
        dataframe = dataframe[offset:]

    if limit:
        assert_integer('limit', limit)
        dataframe = dataframe[:limit]

    return dataframe


def distinct(dataframe, columns):
    if columns is None:
        return dataframe

    args = {}
    if columns:
        args['subset'] = columns

    return dataframe.drop_duplicates(**args)


def query(dataframe, q_json):
    try:
        q = json.loads(q_json)
    except ValueError:
        raise MalformedQueryException('Could not load JSON: {json}'.format(json=json))

    if not isinstance(q, dict):
        raise MalformedQueryException('Query must be a dictionary, not "{q}"'.format(q=q))

    try:
        filtered_df = do_filter(dataframe, q.get('where'))
        grouped_df = group_by(filtered_df, q.get('group_by'))
        distinct_df = distinct(grouped_df, q.get('distinct'))
        projected_df = project(distinct_df, q.get('select'))
        ordered_df = order_by(projected_df, q.get('order_by'))
        sliced_df = do_slice(ordered_df, q.get('offset'), q.get('limit'))
        return sliced_df
    except UndefinedVariableError as e:
        raise MalformedQueryException(e.message)
