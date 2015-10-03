from pandas import DataFrame
from pandas.core.groupby import DataFrameGroupBy


class MalformedQueryException(Exception):
    pass


def raise_malformed(message, q):
    raise MalformedQueryException(message + ': {q}'.format(q=q))


def build_filter(q):
    if type(q) is not list:
        return str(q)

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
        filter_str = build_filter(filter_q)
        return dataframe.query(filter_str)

    return dataframe


def group_by(dataframe, group_by_q):
    if not group_by_q:
        return dataframe

    return dataframe.groupby(group_by_q, as_index=False)


def project(dataframe, project_q):
    if not project_q:
        return dataframe

    if project_q == [['count']]:
        # Special case for count only, ~equal to SQL count(*)
        return DataFrame.from_dict({'count': [len(dataframe)]})

    aggregate_fns = {e[1]: e[0] for e in project_q if type(e) is list}
    if aggregate_fns:
        if not isinstance(dataframe, DataFrameGroupBy):
            if len(aggregate_fns) > 1:
                raise_malformed("Multiple aggregation functions without group by", project_q)

            # Intricate, apply the selected function to the selected column
            arg, fn = aggregate_fns.items()[0]
            dataframe = dataframe[[arg]]
            return DataFrame.from_dict({fn: [getattr(dataframe, fn)(axis=0)[0]]})

        dataframe = dataframe.agg(aggregate_fns)

    columns = [e if type(e) is not list else e[1] for e in project_q]
    try:
        return dataframe[columns]
    except KeyError:
        raise_malformed("Selected column not in table", columns)


def order_by(dataframe, order_q):
    if not order_q:
        return dataframe

    columns = [e[1:] if e.startswith('-') else e for e in order_q]
    ascending = [not e.startswith('-') for e in order_q]

    try:
        return dataframe.sort(columns, ascending=ascending)
    except KeyError:
        raise_malformed("Order by column not in table", columns)


def do_slice(dataframe, offset, limit):
    if offset:
        dataframe = dataframe[offset:]

    if limit:
        dataframe = dataframe[:limit]

    return dataframe


def query(dataframe, q):
    filtered_df = do_filter(dataframe, q.get('where'))
    grouped_df = group_by(filtered_df, q.get('group_by'))
    ordered_df = order_by(grouped_df, q.get('order_by'))
    sliced_df = do_slice(ordered_df, q.get('offset'), q.get('limit'))
    projected_df = project(sliced_df, q.get('select'))
    return projected_df
