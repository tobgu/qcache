from __future__ import unicode_literals

from qcache.qframe.common import raise_malformed, assert_list, assert_len, prepare_in_clause
from qcache.qframe.constants import JOINING_OPERATORS, COMPARISON_OPERATORS, FILTER_ENGINE_NUMEXPR


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
            except ValueError:
                raise_malformed('Invalid type in comparison in where clause', filter_q)

        return dataframe

    def _build_filter(self, q):
        result = None
        if type(q) is not list:
            return unicode(q)

        if not q:
            raise_malformed("Empty expression not allowed", q)

        op = q[0]
        if op == "!":
            assert_len(q, 2, "! is a single arity operator, invalid number of arguments")
            result = "not " + self._build_filter(q[1])
        elif op == "isnull":
            assert_len(q, 2, "isnull is a single arity operator, invalid number of arguments")

            # Slightly hacky but the only way I've come up with so far.
            result = "({arg} != {arg})".format(arg=q[1])
        elif op in COMPARISON_OPERATORS:
            assert_len(q, 3)
            _, arg1, arg2 = q
            result = self._build_filter(arg1) + " " + op + " " + self._build_filter(arg2)
        elif op in JOINING_OPERATORS:
            if len(q) < 2:
                raise_malformed("Invalid number of arguments", q)
            elif len(q) == 2:
                # Conjunctions and disjunctions with only one clause are OK
                result = self._build_filter(q[1])
            else:
                result = ' {op} '.format(op=op).join(self._build_filter(x) for x in q[1:])
        elif op == 'in':
            col_name, args = prepare_in_clause(q, FILTER_ENGINE_NUMEXPR)
            var_name = self._insert_in_env(args)
            result = '{col_name} in @env.{var_name}'.format(col_name=col_name, var_name=var_name)
        else:
            raise_malformed("Unknown operator", q)

        return "({result})".format(result=result)

    def _insert_in_env(self, variable):
        var_name = 'var_{count}'.format(count=self.env_counter)
        setattr(self.env, var_name, variable)
        self.env_counter += 1
        return var_name


def numexpr_filter(dataframe, q):
    return Filter().filter(dataframe, q)
