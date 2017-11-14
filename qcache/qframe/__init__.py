from __future__ import unicode_literals

from StringIO import StringIO

import numpy
from pandas import DataFrame, pandas

from qcache.qframe.common import unquote, MalformedQueryException
from qcache.qframe.context import set_current_qframe
from qcache.qframe.query import query
from qcache.qframe.update import update_frame


def _get_dtype(obj):
    try:
        try:
            int(obj)
            return numpy.int64
        except ValueError:
            float(obj)
            return numpy.float64
    except ValueError:
        return numpy.object


def _add_stand_in_columns(df, stand_in_columns):
    if not stand_in_columns:
        return df

    for column_name, stand_in_value in stand_in_columns:
        if column_name not in df:
            if stand_in_value in df:
                df.loc[:, column_name] = df[stand_in_value]
            else:
                dtype = _get_dtype(stand_in_value)
                stand_in_value = unquote(stand_in_value)
                arr = numpy.full(len(df), stand_in_value, dtype=dtype)
                df.loc[:, column_name] = pandas.Series(arr, index=df.index)


class QFrame(object):
    """
    Thin wrapper around a Pandas dataframe.
    """
    __slots__ = ('df', 'unsliced_df_len')

    def __init__(self, pandas_df, unsliced_df_len=None):
        self.unsliced_df_len = len(pandas_df) if unsliced_df_len is None else unsliced_df_len
        self.df = pandas_df

    @staticmethod
    def from_csv(csv_string, column_types=None, stand_in_columns=None):
        df = pandas.read_csv(StringIO(csv_string), dtype=column_types, na_values=[''], keep_default_na=False)
        _add_stand_in_columns(df, stand_in_columns)
        return QFrame(df)

    @staticmethod
    def from_dicts(d, column_types=None, stand_in_columns=None):
        df = DataFrame.from_records(d)

        # Setting columns to categorials is slightly awkward from dicts
        # than from CSV...
        if column_types:
            for name, type in column_types.items():
                if type == 'category':
                    df[name] = df[name].astype("category")

        _add_stand_in_columns(df, stand_in_columns=stand_in_columns)
        return QFrame(df)

    def query(self, q, stand_in_columns=None):
        _add_stand_in_columns(self.df, stand_in_columns)
        set_current_qframe(self)
        if 'update' in q:
            # In place operation, should it be?
            update_frame(self.df, q)
            return None

        new_df, unsliced_df_len = query(self.df, q)
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
        return self.df.memory_usage(index=True, deep=True).sum()
