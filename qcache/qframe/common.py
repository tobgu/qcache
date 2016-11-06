from __future__ import unicode_literals

import numpy

from qcache.qframe.context import get_current_qframe


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


def assert_len(q, expected, error_message="Invalid number of arguments"):
    if len(q) != expected:
        raise_malformed(error_message, q)


def is_quoted(string):
    l = len(string)
    return (l >= 2) and \
           ((string[0] == "'" and string[-1] == "'") or
            (string[0] == '"' and string[-1] == '"'))


def unquote(s):
    if s.startswith("'") or s.startswith('"'):
        s = s[1:]

    if s.endswith("'") or s.endswith('"'):
        s = s[:-1]

    return s


def prepare_in_clause(q, filter_engine):
    """
    The arguments to an in expression may be either a list of values or
    a sub query which is then executed to produce a list of values.
    """
    assert_len(q, 3)
    _, col_name, args = q

    if isinstance(args, dict):
        # Sub query, circular dependency on query by nature so need to keep the import local
        from qcache.qframe import query
        current_qframe = get_current_qframe()
        sub_df, _ = query(current_qframe.df, args, filter_engine=filter_engine)
        try:
            args = sub_df[col_name].values
        except KeyError:
            raise_malformed('Unknown column "{}"'.format(col_name), q)

    if not isinstance(args, (list, numpy.ndarray)):
        raise_malformed("Second argument must be a list", q)

    return col_name, args
