import polars as pl
from polars import DataFrame
from typing import Any, Callable

__all__ = [
    'ItemT',
    'MapT',
    'NameT',
    'ColumnT',
    'RecordT',
    'RowT',
    'RecordEntryT',
    'RowEntryT',
    'NamesT',
    'ColumnsT',
    'RecordsT',
    'RowsT',
    'RecordEntriesT',
    'RowEntriesT',
    'RowMappingT',
    'RecordMappingT',
    'NameColumnMapT',
    'TableOfT',
    'item',
    'get_item',
    'record',
    'get_record',
    'row',
    'get_row',
    'record_entry',
    'get_record_entry',
    'row_entry',
    'get_row_entry',
    'column',
    'keys',
    'records',
    'rows',
    'columns',
    'record_entries',
    'row_entries',
    'keyed_record_entries',
    'keyed_row_entries',
    'map',
    'record_map',
    'row_map',
    'column_map',
    'keyed_record_map',
    'keyed_row_map',
    'table_records',
    'table_rows',
    'table_columns',
    'table_record_entries',
    'table_row_entries',
    'table_keyed_record_entries',
    'table_keyed_row_entries',
    'table_map',
    'table_record_map',
    'table_row_map',
    'table_keyed_record_map',
    'table_keyed_row_map',
]

# Default types (when no pydantic validation is desired)
type ItemT = Any  # Single cell value
type MapT = dict[ItemT, ItemT]

type NameT = str  # Column name
type ColumnT = list[ItemT]  # Values in a column
type RecordT = dict[NameT, ItemT]  # A row as a dict
type RowT = tuple[ItemT, ...]  # A row as a tuple
type RecordEntryT = tuple[ItemT, RecordT]
type RowEntryT = tuple[ItemT, RowT]

type NamesT = tuple[NameT, ...]  # All column names
type ColumnsT = tuple[ColumnT, ...]  # All values in all columns
type RecordsT = list[RecordT]  # All rows as dicts
type RowsT = list[RowT]  # All rows as tuples
type RecordEntriesT = list[RecordEntryT]
type RowEntriesT = list[RowEntryT]
type RowMappingT = dict[ItemT, RowT]
type RecordMappingT = dict[ItemT, RecordT]
type NameColumnMapT = dict[NameT, ColumnT]

type TableOfT[T] = tuple[NamesT, T]


def item(df: DataFrame, /) -> ItemT:
    _raise_if_bad_df_dimensions(item, df, min_h=1, max_h=1, min_w=1, max_w=1)
    return df.item()


def get_item[D](df: DataFrame, default: D, /) -> ItemT | D:
    _raise_if_bad_df_dimensions(get_item, df, max_h=1, min_w=1, max_w=1)
    return item(df) if df.height > 0 else default


def record(df: DataFrame, /) -> RecordT:
    _raise_if_bad_df_dimensions(record, df, min_h=1, max_h=1)
    return df.rows(named=True)[0]


def get_record[D](df: DataFrame, default: D, /) -> RecordT | D:
    _raise_if_bad_df_dimensions(get_record, df, max_h=1)
    return record(df) if df.height > 0 else default


def row(df: DataFrame, /) -> RowT:
    _raise_if_bad_df_dimensions(row, df, min_h=1, max_h=1)
    return df.rows()[0]


def get_row[D](df: DataFrame, default: D, /) -> RowT | D:
    _raise_if_bad_df_dimensions(get_row, df, max_h=1)
    return row(df) if df.height > 0 else default


def record_entry(df: DataFrame, /) -> RecordEntryT:
    _raise_if_bad_df_dimensions(record_entry, df, min_w=2, min_h=1, max_h=1)
    rec = df.rows(named=True)[0]
    key = rec.pop(df.columns[0])
    return key, rec


def get_record_entry[D](df: DataFrame, default: D, /) -> RecordEntryT | D:
    _raise_if_bad_df_dimensions(get_record_entry, df, min_w=2, max_h=1)
    return record_entry(df) if df.height > 0 else default


def row_entry(df: DataFrame, /) -> RowEntryT:
    _raise_if_bad_df_dimensions(row_entry, df, min_w=2, min_h=1, max_h=1)
    key, *rest_row = df.rows()[0]
    return key, tuple(rest_row)


def get_row_entry[D](df: DataFrame, default: D, /) -> RowEntryT | D:
    _raise_if_bad_df_dimensions(get_row_entry, df, min_w=2, max_h=1)
    return row_entry(df) if df.height > 0 else default


def column(df: DataFrame, /) -> ColumnT:
    _raise_if_bad_df_dimensions(column, df, min_w=1, max_w=1)
    return df.to_series(0).to_list()


def keys(df: DataFrame, /) -> ColumnT:
    _raise_if_bad_df_dimensions(keys, df, min_w=1, max_w=1)
    series = df.to_series(0)
    _raise_if_duplicates(df.height, series.n_unique(), keys, df.columns[0])
    return series.to_list()


def records(df: DataFrame, /) -> RecordsT:
    return df.rows(named=True)


def rows(df: DataFrame, /) -> RowsT:
    return df.rows()


def columns(df: DataFrame, /) -> ColumnsT:
    return tuple(c.to_list() for c in df)


def record_entries(df: DataFrame, /) -> RecordEntriesT:
    _raise_if_bad_df_dimensions(record_entries, df, min_w=2)
    values = df.drop(df.columns[0]).rows(named=True)
    return list(zip(df.to_series(0).to_list(), values))


def row_entries(df: DataFrame, /) -> RowEntriesT:
    _raise_if_bad_df_dimensions(row_entries, df, min_w=2)
    values = df.drop(df.columns[0]).rows()
    return list(zip(df.to_series(0).to_list(), values))


def keyed_record_entries(df: DataFrame, /) -> RecordEntriesT:
    _raise_if_bad_df_dimensions(keyed_record_entries, df, min_w=1)
    return list(zip(df.to_series(0).to_list(), df.rows(named=True)))


def keyed_row_entries(df: DataFrame, /) -> RowEntriesT:
    _raise_if_bad_df_dimensions(keyed_row_entries, df, min_w=1)
    return list(zip(df.to_series(0).to_list(), df.rows()))


def map(df: DataFrame, /) -> MapT:
    _raise_if_bad_df_dimensions(map, df, max_w=2, min_w=2)
    result = dict(df.rows())
    _raise_if_duplicates(df.height, len(result), map, df.columns[0])
    return result


def record_map(df: DataFrame, /) -> RecordMappingT:
    _raise_if_bad_df_dimensions(record_map, df, min_w=2)
    values = df.drop(df.columns[0]).rows(named=True)
    result = dict(zip(df.to_series(0).to_list(), values))
    _raise_if_duplicates(df.height, len(result), record_map, df.columns[0])
    return result


def row_map(df: DataFrame, /) -> RowMappingT:
    _raise_if_bad_df_dimensions(row_map, df, min_w=2)
    values = df.drop(df.columns[0]).rows()
    result = dict(zip(df.to_series(0).to_list(), values))
    _raise_if_duplicates(df.height, len(result), record_map, df.columns[0])
    return result


def column_map(df: DataFrame, /) -> NameColumnMapT:
    return {s.name: s.to_list() for s in df}


def keyed_record_map(df: DataFrame, /) -> RecordMappingT:
    _raise_if_bad_df_dimensions(keyed_record_map, df, min_w=1)
    result = dict(zip(df.to_series(0).to_list(), df.rows(named=True)))
    _raise_if_duplicates(df.height, len(result), keyed_record_map, df.columns[0])
    return result


def keyed_row_map(df: DataFrame, /) -> RowMappingT:
    _raise_if_bad_df_dimensions(keyed_row_map, df, min_w=1)
    result = dict(zip(df.to_series(0).to_list(), df.rows()))
    _raise_if_duplicates(df.height, len(result), keyed_record_map, df.columns[0])
    return result


# fmt: off
def table_records(df: DataFrame):               return tuple(df.columns), records(df)
def table_rows(df: DataFrame):                  return tuple(df.columns), rows(df)
def table_columns(df: DataFrame):               return tuple(df.columns), columns(df)
def table_record_entries(df: DataFrame):        return tuple(df.columns), record_entries(df)
def table_row_entries(df: DataFrame):           return tuple(df.columns), row_entries(df)
def table_keyed_record_entries(df: DataFrame):  return tuple(df.columns), keyed_record_entries(df)
def table_keyed_row_entries(df: DataFrame):     return tuple(df.columns), keyed_row_entries(df)
def table_map(df: DataFrame):                   return tuple(df.columns), map(df)
def table_record_map(df: DataFrame):            return tuple(df.columns), record_map(df)
def table_row_map(df: DataFrame):               return tuple(df.columns), row_map(df)
def table_keyed_record_map(df: DataFrame):      return tuple(df.columns), keyed_record_map(df)
def table_keyed_row_map(df: DataFrame):         return tuple(df.columns), keyed_row_map(df)
# fmt: on


def _raise_if_bad_df_dimensions(
    cls: type[Any] | Callable[..., Any],
    df: pl.DataFrame,
    *,
    min_h: int | None = None,
    min_w: int | None = None,
    max_h: int | None = None,
    max_w: int | None = None,
) -> None:
    """
    Used before Pydantic validation, to catch user errors due to
    wrongly-shaped queries.
    """
    if max_h is not None and df.height > max_h:
        raise ValueError(f'`{cls.__name__}` got {df.height} rows.')
    if max_w is not None and df.width > max_w:
        raise ValueError(f'`{cls.__name__}` got {df.width} columns.')
    if min_h is not None and df.height < min_h:
        raise ValueError(f'`{cls.__name__}` got {df.height} rows.')
    if min_w is not None and df.width < min_w:
        raise ValueError(f'`{cls.__name__}` got {df.width} columns.')


def _raise_if_duplicates(
    len_df: int, len_result: int, cls: Callable[..., Any], colname: str
):
    if len_df != len_result:
        raise ValueError(f'`{cls.__name__}` got duplicates in column, {colname}.')
