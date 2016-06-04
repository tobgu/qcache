from __future__ import unicode_literals
import re
import operator
from StringIO import StringIO
from pandas import DataFrame, pandas
from pandas.computation.ops import UndefinedVariableError
from pandas.core.groupby import DataFrameGroupBy


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


class Env(object):
    pass


class Filter(object):
    def __init__(self):
        self.env_counter = 0
        self.env = Env()

    def filter(self, dataframe, filter_q):
        if filter_q:
            assert_list('where', filter_q)
            filter_str = self._build_filter(filter_q)
            try:
                # The filter string may contain references to variables in env.
                # That's why it is defined here.
                env = self.env  # noqa
                return dataframe.query(filter_str)
            except SyntaxError:
                raise_malformed('Syntax error in where clause', filter_q)

        return dataframe

    def _build_filter(self, q):
        if type(q) is not list:
            return unicode(q)

        if not q:
            raise_malformed("Empty expression not allowed", q)

        op = q[0]
        if op == "!":
            if len(q) != 2:
                raise_malformed("! is a single arity operator, invalid number of arguments", q)

            result = "not " + self._build_filter(q[1])
        elif op == "isnull":
            if len(q) != 2:
                raise_malformed("isnull is a single arity operator, invalid number of arguments", q)

            # Slightly hacky but the only way I've come up with so far.
            result = "({arg} != {arg})".format(arg=q[1])
        elif op in ('==', '!=', '<', '<=', '>', '>='):
            if len(q) != 3:
                raise_malformed("Invalid number of arguments", q)

            _, arg1, arg2 = q
            result = self._build_filter(arg1) + " " + op + " " + self._build_filter(arg2)
        elif op in ('&', '|'):
            if len(q) < 2:
                raise_malformed("Invalid number of arguments", q)
            elif len(q) == 2:
                # Conjunctions and disjunctions with only one clause are OK
                result = self._build_filter(q[1])
            else:
                result = ' {op} '.format(op=op).join(self._build_filter(x) for x in q[1:])
        elif op == 'in':
            if len(q) != 3:
                raise_malformed("Invalid number of arguments", q)

            _, arg1, arg2 = q
            var_name = self._insert_in_env(arg2)
            result = '{arg1} in @env.{var_name}'.format(arg1=arg1, var_name=var_name)
        else:
            raise_malformed("Unknown operator", q)

        return "({result})".format(result=result)

    def _insert_in_env(self, variable):
        var_name = 'var_{count}'.format(count=self.env_counter)
        setattr(self.env, var_name, variable)
        self.env_counter += 1
        return var_name


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


def _query(dataframe, q):
    if not isinstance(q, dict):
        raise MalformedQueryException('Query must be a dictionary, not "{q}"'.format(q=q))

    key_set = set(q.keys())
    if not key_set.issubset(QUERY_CLAUSES):
        raise MalformedQueryException('Unknown query clauses: {keys}'.format(
            keys=', '.join(key_set.difference(QUERY_CLAUSES))))

    try:
        if CLAUSE_FROM in q:
            dataframe, _ = _query(dataframe, q[CLAUSE_FROM])

        filter = Filter()
        filtered_df = filter.filter(dataframe, q.get('where'))
        grouped_df = _group_by(filtered_df, q.get('group_by'))
        distinct_df = _distinct(grouped_df, q.get('distinct'))
        projected_df = _project(distinct_df, q.get('select'))
        ordered_df = _order_by(projected_df, q.get('order_by'))
        sliced_df = _do_slice(ordered_df, q.get('offset'), q.get('limit'))
        return sliced_df, len(ordered_df)
    except UndefinedVariableError as e:
        raise MalformedQueryException(e.message)


def quoted(string):
    l = len(string)
    return (l >= 2) and \
           ((string[0] == "'" and string[l - 1] == "'") or
            (string[0] == '"' and string[l - 1] == '"'))


def unquote(s):
    return s[1:len(s) - 1]


def _prepare_arg(df, arg):
    if isinstance(arg, basestring):
        if quoted(arg):
            return unquote(arg)

        return getattr(df, arg)

    return arg

COMPARISON_OPERATORS = {'==': operator.eq,
                        '!=': operator.ne,
                        '<': operator.lt,
                        '<=': operator.le,
                        '>': operator.gt,
                        '>=': operator.ge}


def _build_update_filter(df, update_q):
    if type(update_q) is not list:
        raise_malformed("Expressions must be lists", update_q)

    if not update_q:
        raise_malformed("Empty expression not allowed", update_q)

    operator = update_q[0]
    if operator == "isnull":
        if len(update_q) != 2:
            raise_malformed('Invalid length of isnull query', update_q)
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

    if operator in COMPARISON_OPERATORS.keys():
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


def _update(df, q):
    update_filter = _build_update_filter(df, q['where'])
    for update_type, updates in classify_updates(q):
        if update_type == 'simple':
            columns, values = _build_update_values(df, updates)
            df.ix[update_filter, columns] = values
        else:
            op, column, value = updates
            apply_operation(df, update_filter, column, op, value)


class QFrame(object):
    """
    Thin wrapper around a Pandas dataframe.
    """
    __slots__ = ('df', 'unsliced_df_len')

    def __init__(self, pandas_df, unsliced_df_len=None):
        self.unsliced_df_len = len(pandas_df) if unsliced_df_len is None else unsliced_df_len
        self.df = pandas_df

    @staticmethod
    def from_csv(csv_string, column_types=None):
        return QFrame(pandas.read_csv(StringIO(csv_string), dtype=column_types))

    @staticmethod
    def from_dicts(d):
        f = QFrame(DataFrame.from_records(d))
        return f

    def query(self, q):
        if 'update' in q:
            # In place operation, should it be?
            _update(self.df, q)
            return None

        new_df, unsliced_df_len = _query(self.df, q)
        return QFrame(new_df, unsliced_df_len=unsliced_df_len)

    def to_csv(self):
        return self.df.to_csv(index=False)

    def to_json(self):
        return self.df.to_json(orient='records')

    def to_dicts(self):
        return self.df.to_dict(orient='records')

    @property
    def columns(self):
        return self.df.columns

    def __len__(self):
        return len(self.df)

    def byte_size(self):
        # Estimate of the number of bytes consumed by this QFrame
        return self.df.memory_usage(index=True).sum()
