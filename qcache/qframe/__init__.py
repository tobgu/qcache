import io

import numpy
from pandas import DataFrame, pandas

from qcache.qframe.common import unquote, MalformedQueryException
from qcache.qframe.context import set_current_qframe
from qcache.qframe.query import query
from qcache.qframe.update import update_frame
from qcache.qframe.constants import FILTER_ENGINE_NUMEXPR


def convert_if_number(obj):
    for fn in int, float:
        try:
            return fn(obj)
        except ValueError:
            pass

    return obj


def _add_stand_in_columns(df, stand_in_columns):
    if not stand_in_columns:
        return df

    for column_name, stand_in_value in stand_in_columns:
        if column_name not in df:
            if stand_in_value in df:
                df.loc[:, column_name] = df[stand_in_value]
            else:
                stand_in_value = convert_if_number(stand_in_value)
                stand_in_value = unquote(stand_in_value)
                arr = numpy.full(len(df), stand_in_value)
                df.loc[:, column_name] = pandas.Series(arr, index=df.index)


class QFrame(object):
    """
    Thin wrapper around a Pandas dataframe.
    """
    __slots__ = ('df', 'unsliced_df_len', '_size')

    def __init__(self, pandas_df, unsliced_df_len=None):
        self.unsliced_df_len = len(pandas_df) if unsliced_df_len is None else unsliced_df_len
        self.df = pandas_df
        self._size = None

    @staticmethod
    def from_csv(csv_string, column_types=None, stand_in_columns=None):
        df = pandas.read_csv(io.BytesIO(csv_string), dtype=column_types)
        _add_stand_in_columns(df, stand_in_columns)
        return QFrame(df)

    @staticmethod
    def from_dicts(d, column_types=None, stand_in_columns=None):
        df = DataFrame.from_records(d)

        # Setting columns to categorials is slightly more awkward from dicts
        # than from CSV...
        if column_types:
            for name, type in column_types.items():
                if type == 'category':
                    df[name] = df[name].astype("category")

        _add_stand_in_columns(df, stand_in_columns=stand_in_columns)
        return QFrame(df)

    def query(self, q, filter_engine=None, stand_in_columns=None):
        _add_stand_in_columns(self.df, stand_in_columns)
        set_current_qframe(self)
        if 'update' in q:
            # In place operation, should it be?
            update_frame(self.df, q)
            return None

        new_df, unsliced_df_len = query(self.df, q, filter_engine=filter_engine)
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

        # There is something fishy with the memory usage and to_json in Python 3,
        # see https://github.com/pandas-dev/pandas/issues/15344
        if self._size is None:
            self._size = self.df.memory_usage(index=True, deep=True).sum()

        return self._size
