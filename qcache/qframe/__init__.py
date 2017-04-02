from typing import Sized, Optional, List, Tuple, Any

import io

import numpy
from pandas import DataFrame, pandas

from qcache.qframe.common import unquote, MalformedQueryException
from qcache.qframe.context import set_current_qframe
from qcache.qframe.query import query
from qcache.qframe.update import update_frame
from qcache.qframe.constants import FILTER_ENGINE_NUMEXPR


def convert_if_number(obj: Any) -> Any:
    for fn in int, float:
        try:
            return fn(obj)
        except ValueError:
            pass

    return obj

StandInColumns = Optional[List[Tuple[str, ...]]]


def _add_stand_in_columns(df: DataFrame, stand_in_columns: StandInColumns):
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


class QFrame(Sized):
    """
    Thin wrapper around a Pandas dataframe.
    """
    __slots__ = ('df', 'unsliced_df_len', '_byte_size')

    def __init__(self, pandas_df: DataFrame, unsliced_df_len: Optional[int]=None) -> None:
        self.unsliced_df_len = len(pandas_df) if unsliced_df_len is None else unsliced_df_len
        self.df = pandas_df
        self._byte_size: Optional[int] = None

    @staticmethod
    def from_csv(csv_data: bytes, column_types: dict=None, stand_in_columns: StandInColumns=None) -> 'QFrame':
        df = pandas.read_csv(io.BytesIO(csv_data), dtype=column_types)
        _add_stand_in_columns(df, stand_in_columns)
        return QFrame(df)

    @staticmethod
    def from_json(json_data: bytes, column_types: dict=None, stand_in_columns: StandInColumns=None) -> 'QFrame':
        df = pandas.read_json(json_data, orient='records', dtype=column_types)

        # Setting columns to categorials is slightly more awkward from dicts
        # than from CSV...
        if column_types:
            for name, type in column_types.items():
                if type == 'category':
                    df[name] = df[name].astype("category")

        _add_stand_in_columns(df, stand_in_columns=stand_in_columns)
        return QFrame(df)

    def query(self, q: dict, filter_engine: Optional[str]=None, stand_in_columns: StandInColumns=None) -> Optional['QFrame']:
        _add_stand_in_columns(self.df, stand_in_columns)
        set_current_qframe(self)
        if 'update' in q:
            # In place operation, should it be?
            update_frame(self.df, q)
            return None

        new_df, unsliced_df_len = query(self.df, q, filter_engine=filter_engine)
        return QFrame(new_df, unsliced_df_len=unsliced_df_len)

    def to_csv(self) -> bytes:
        return self.df.to_csv(index=False)

    def to_json(self) -> bytes:
        return self.df.to_json(orient='records')

    def to_dicts(self) -> List[dict]:
        return self.df.to_dict(orient='records')

    @property
    def columns(self) -> List:
        return self.df.columns

    def __len__(self) -> int:
        return len(self.df)

    def byte_size(self) -> int:
        # Estimate of the number of bytes consumed by this QFrame, this is a
        # fairly heavy operation for large frames so we cache the result.
        if self._byte_size is None:
            self._byte_size = self.df.memory_usage(index=True, deep=True).sum()

        return self._byte_size
