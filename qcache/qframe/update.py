from qcache.qframe.common import assert_len, raise_malformed, is_quoted, unquote
from qcache.qframe.constants import COMPARISON_OPERATORS


def _prepare_arg(df, arg):
    if isinstance(arg, basestring):
        if is_quoted(arg):
            return unquote(arg)

        return getattr(df, arg)

    return arg


def _build_update_filter(df, update_q):
    if type(update_q) is not list:
        raise_malformed("Expressions must be lists", update_q)

    if not update_q:
        raise_malformed("Empty expression not allowed", update_q)

    operator = update_q[0]
    if operator == "isnull":
        assert_len(update_q, 2, 'Invalid length of isnull query')
        try:
            return getattr(_prepare_arg(df, update_q[1]), 'isnull')()
        except AttributeError:
            raise_malformed("Unknown column for 'isnull'", update_q)

    if operator == "in":
        if len(update_q) != 3:
            raise_malformed("Invalid length of 'in' query", update_q)

        _, column, values = update_q
        if column not in df:
            raise_malformed("First argument to 'in' must be a column present in frame", update_q)

        if not isinstance(values, (list, tuple)):
            raise_malformed("Second argument to 'in' must be a list", update_q)

        return getattr(df, column).isin([_prepare_arg(df, val) for val in values])

    if operator in COMPARISON_OPERATORS:
        arg1 = _prepare_arg(df, update_q[1])
        arg2 = _prepare_arg(df, update_q[2])
        return COMPARISON_OPERATORS[operator](arg1, arg2)

    raise_malformed("Unknown operator '{operator}'".format(operator=operator), update_q)


def _build_update_values(df, updates):
    columns, values = zip(*updates)
    return columns, [_prepare_arg(df, val) for val in values]


def classify_updates(q):
    # Updates can be either simple assignments or self referring updates (e. column += 1).
    # The former can be applied all at once while pandas only supports updates of one column
    # at the time for the latter. All updates are performed in the order they are declared
    # in the query.
    simple_run = []
    for update in q['update']:
        if not isinstance(update, (list, tuple)):
            raise_malformed("Invalid update clause", update)

        if len(update) == 2:
            simple_run.append(update)
        else:
            if simple_run:
                yield ('simple', simple_run)
                simple_run = []
            yield ('self-referring', update)

    if simple_run:
        yield ('simple', simple_run)


def apply_operation(df, update_filter, column, op, value):
    # This is repetitive and ugly but the only way I've found to do in place updates
    if op == '+':
        df.ix[update_filter, column] += value
    elif op == '-':
        df.ix[update_filter, column] -= value
    elif op == '*':
        df.ix[update_filter, column] *= value
    elif op == '/':
        df.ix[update_filter, column] /= value
    elif op == '<<':
        df.ix[update_filter, column] <<= value
    elif op == '>>':
        df.ix[update_filter, column] >>= value
    elif op == '&':
        df.ix[update_filter, column] &= value
    elif op == '|':
        df.ix[update_filter, column] |= value
    elif op == '^':
        df.ix[update_filter, column] ^= value
    elif op == '%':
        df.ix[update_filter, column] %= value
    elif op == '**':
        df.ix[update_filter, column] **= value
    else:
        raise_malformed('Invalid update operator', (op, value, column))


def update_frame(df, q):
    update_filter = _build_update_filter(df, q['where'])
    for update_type, updates in classify_updates(q):
        if update_type == 'simple':
            columns, values = _build_update_values(df, updates)
            df.ix[update_filter, columns] = values
        else:
            op, column, value = updates
            apply_operation(df, update_filter, column, op, value)
