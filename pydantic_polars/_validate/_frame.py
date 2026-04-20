"""
DataFrame/LazyFrame validators for various data shape interpretations.
"""

from typing import Any, Callable
import polars as pl
from polars import DataFrame
from ._base_validator import BaseValidator

__all__ = [
    'Columns',
    'Records',
    'Rows',
    'Map',
    'RecordMap',
    'RowMap',
    'KeyedRecords',
    'KeyedRows',
    'ColumnMap',
    'ColumnEntries',
    'TableRecords',
    'TableRows',
    'TableColumns',
    'Column',
    'Keys',
    'ColumnEntry',
    'Record',
    'GetRecord',
    'Row',
    'GetRow',
    'Item',
    'GetItem',
]

# Default types (when no pydantic validation is desired)
type ItemT = Any  # Single cell value

type NameT = str  # Column name
type ColumnT = list[ItemT]  # Values in a column
type RecordT = dict[NameT, ItemT]  # A row as a dict
type RowT = tuple[ItemT, ...]  # A row as a tuple
type ColumnEntryT = tuple[NameT, ColumnT]  # A column name and its values

type NamesT = tuple[NameT, ...]  # All column names
type ColumnsT = tuple[ColumnT, ...]  # All values in all columns
type RowsT = list[RowT]  # All rows as tuples
type RecordsT = list[RecordT]  # All rows as dicts
type ColumnEntriesT = tuple[ColumnEntryT, ...]


# Row-wise, many


class Columns[T: Any = ColumnsT](BaseValidator[T]):
    """
    Validate a tuple of columns.
    - `T` : The tuple of column value lists.
    - `(['Joe', 'Bob'], [23, 45])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ColumnsT:
        return tuple(c.to_list() for c in df)


class Records[T: Any = RecordsT](BaseValidator[T]):
    """
    Validate a list of row dicts.
    - `T` : The list of row dicts
    - `[{ name: 'Joe', age: 23 }, { name: 'Bob', age: 45 }]`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RecordsT:
        return df.rows(named=True)


class Rows[T: Any = RowsT](BaseValidator[T]):
    """
    Validate a list of row tuples.
    - `T` : The list of row tuples.
    - `[('Joe', 23), ('Bob', 45)]`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RowsT:
        return df.rows()


# Row-wise, unique


class Map[T: Any = dict[ItemT, ItemT]](BaseValidator[T]):
    """
    Validate the dict that maps keys from the first column to values in the second.
    - Query must produce exactly 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to second-column-values.
    - `{ 'Joe': 23, 'Bob': 45 }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[ItemT, ItemT]:
        _raise_if_bad_query_structure(cls, df, max_width=2, min_width=2)
        result = dict(df.rows())
        _raise_if_duplicates(df.height, len(result), cls, df.columns[0])
        return result


class RecordMap[T: Any = dict[ItemT, RecordT]](BaseValidator[T]):
    """
    Validate the dict that maps keys from the first column to dict records
    made from the remaining columns.
    - Query must produce at least 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to dict records.
    - `{ 'Joe': {'age': 23}, 'Bob': {'age': 45} }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[ItemT, RecordT]:
        _raise_if_bad_query_structure(cls, df, min_width=2)
        values = df.select(*df.columns[1:]).rows(named=True)
        result = dict(zip(df.to_series(0).to_list(), values))
        _raise_if_duplicates(df.height, len(result), cls, df.columns[0])
        return result


class RowMap[T: Any = dict[ItemT, RowT]](BaseValidator[T]):
    """
    Validate the dict that maps keys from the first column to row tuples
    made from the remaining columns.
    - Query must produce at least 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to row tuples.
    - `{ 'Joe': (23,), 'Bob': (45,) }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[ItemT, RowT]:
        _raise_if_bad_query_structure(cls, df, min_width=2)
        values = df.select(*df.columns[1:]).rows()
        result = dict(zip(df.to_series(0).to_list(), values))
        _raise_if_duplicates(df.height, len(result), cls, df.columns[0])
        return result


class KeyedRecords[T: Any = dict[ItemT, RecordT]](BaseValidator[T]):
    """
    Validate the dict that maps keys from the first column to full dict
    records with all columns, i.e. the same values as those from `records`.
    - Query must produce at least 1 column. It must be unique.
    - `T` : A dict that maps column-0 keys to whole record dicts.
    - `{ 'Joe': {'name': 'Joe', 'age': 23}, 'Bob': {'name': 'Bob', 'age': 45} }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[ItemT, RecordT]:
        _raise_if_bad_query_structure(cls, df, min_width=1)
        result = dict(zip(df.to_series(0).to_list(), df.rows(named=True)))
        _raise_if_duplicates(df.height, len(result), cls, df.columns[0])
        return result


class KeyedRows[T: Any = dict[ItemT, RowT]](BaseValidator[T]):
    """
    Validate the dict that maps keys from the first column to full row
    tuples with all columns, i.e. the same values as those from `rows`.
    - Query must produce at least 1 column. It must be unique.
    - `T` : A dict that maps column-0 keys to whole row tuples.
    - `{ 'Joe': ('Joe', 23), 'Bob': ('Bob', 45) }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[ItemT, RowT]:
        _raise_if_bad_query_structure(cls, df, min_width=1)
        result = dict(zip(df.to_series(0).to_list(), df.rows()))
        _raise_if_duplicates(df.height, len(result), cls, df.columns[0])
        return result


# Column names paired with data


class ColumnEntries[T: Any = ColumnEntriesT](BaseValidator[T]):
    """
    Validate a tuple of (name, column) pairs.
    - `T` : The tuple of (column name, value list) pairs.
    - `(('name', ['Joe', 'Bob']), ('age', [23, 45]))`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ColumnEntriesT:
        return tuple((s.name, s.to_list()) for s in df)


class ColumnMap[T: Any = dict[NameT, ColumnT]](BaseValidator[T]):
    """
    Validate a dict of name -> column.
    - `T` : The dict with column name keys and value-list values.
    - `{ name: ['Joe', 'Bob'], age: [23, 45] }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[NameT, ColumnT]:
        return {s.name: s.to_list() for s in df}


# Column names separated from data


class TableRecords[T: Any = tuple[NamesT, RecordsT]](BaseValidator[T]):
    """
    Validate the pair, (names, records).
    - `T` : The tuple, (tuple-of-names, list-of-row-dicts).
    - `(('name', 'age'), [{ name: 'Joe', age: 23 }, { name: 'Bob', age: 45 }])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[NamesT, RecordsT]:
        return (tuple(df.columns), df.rows(named=True))


class TableRows[T: Any = tuple[NamesT, RowsT]](BaseValidator[T]):
    """
    Validate the pair, (names, rows).
    - `T` : The tuple, (tuple-of-names, list-of-row-tuples).
    - `(('name', 'age'), [('Joe', 23), ('Bob', 45)])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[NamesT, RowsT]:
        return (tuple(df.columns), df.rows())


class TableColumns[T: Any = tuple[NamesT, ColumnsT]](BaseValidator[T]):
    """
    Validate the pair, (names, columns).
    - `T` : The tuple, (tuple-of-names, tuple-of-values-lists).
    - `(('name', 'age'), (['Joe', 'Bob'], [23, 45]))`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[NamesT, ColumnsT]:
        return (tuple(df.columns), tuple(s.to_list() for s in df))


# Single (horizontal)


class Column[T: Any = ColumnT](BaseValidator[T]):
    """
    Validate the list of values in a single column.
    - Query must produce exactly one column.
    - `T` : The list of values.
    - `['Joe', 'Bob']`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ColumnT:
        _raise_if_bad_query_structure(cls, df, min_width=1, max_width=1)
        return df.to_series(0).to_list()


class Keys[T: Any = ColumnT](BaseValidator[T]):
    """
    Validate the list of values in a single unique column (preserves order).
    - Query must produce exactly one column. That column must be unique.
    - `T` : The list of values.
    - `['Joe', 'Bob']`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ColumnT:
        _raise_if_bad_query_structure(cls, df, min_width=1, max_width=1)
        series = df.to_series(0)
        _raise_if_duplicates(df.height, series.n_unique(), cls, df.columns[0])
        return series.to_list()


class ColumnEntry[T: Any = ColumnEntryT](BaseValidator[T]):
    """
    For a query that produces one column, validate (column-name, values-list).
    - `T` : The tuple of (column-name, list-of-values).
    - `('name', ['Joe', 'Bob'])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ColumnEntryT:
        _raise_if_bad_query_structure(cls, df, min_width=1, max_width=1)
        return (df.columns[0], df.to_series(0).to_list())


# Single (vertical)


class Record[T: Any = RecordT](BaseValidator[T]):
    """
    For a query that produces exactly one row, validate the row dict.
    - `T` : The row dict.
    - `{ name: 'Joe', age: 23 }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RecordT:
        _raise_if_bad_query_structure(cls, df, min_height=1, max_height=1)
        return df.rows(named=True)[0]


class GetRecord[T: Any = RecordT](BaseValidator[T | None]):
    """Same as `record`, but returns None if no row found."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RecordT | None:
        _raise_if_bad_query_structure(cls, df, max_height=1)
        records = df.rows(named=True)
        return None if len(records) == 0 else records[0]


class Row[T: Any = RowT](BaseValidator[T]):
    """
    For a query that produces exactly one row, validate the row tuple.
    - `T` : The row tuple.
    - `('Joe', 23)`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RowT:
        _raise_if_bad_query_structure(cls, df, min_height=1, max_height=1)
        return df.rows()[0]


class GetRow[T: Any = RowT](BaseValidator[T | None]):
    """Same as `row`, but returns None if no row found."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> RowT | None:
        _raise_if_bad_query_structure(cls, df, max_height=1)
        rows = df.rows()
        return None if len(rows) == 0 else rows[0]


class Item[T: Any = ItemT](BaseValidator[T]):
    """
    For a query that produces exactly one value, validate the value.
    - `T` : The single value.
    - `'Joe'`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ItemT:
        _raise_if_bad_query_structure(
            cls, df, min_height=1, max_height=1, min_width=1, max_width=1
        )
        return df.item()


class GetItem[T: Any = ItemT](BaseValidator[T | None]):
    """Same as `item`, but returns None if no value found."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> ItemT | None:
        _raise_if_bad_query_structure(cls, df, max_height=1, min_width=1, max_width=1)
        return None if df.height == 0 else df.item()


def _raise_if_bad_query_structure(
    cls: type[Any] | Callable[..., Any],
    df: pl.DataFrame,
    *,
    min_height: int | None = None,
    min_width: int | None = None,
    max_height: int | None = None,
    max_width: int | None = None,
) -> None:
    """
    Used before Pydantic validation, to catch user errors due to
    wrongly-shaped queries.
    """
    if max_height is not None and df.height > max_height:
        raise ValueError(f'`{cls.__name__}` got {df.height} rows.')
    if max_width is not None and df.width > max_width:
        raise ValueError(f'`{cls.__name__}` got {df.width} columns.')
    if min_height is not None and df.height < min_height:
        raise ValueError(f'`{cls.__name__}` got {df.height} rows.')
    if min_width is not None and df.width < min_width:
        raise ValueError(f'`{cls.__name__}` got {df.width} columns.')


def _raise_if_duplicates(
    len_df: int, len_result: int, cls: type[BaseValidator[Any]], colname: str
):
    if len_df != len_result:
        raise ValueError(f'`{cls.__name__}` got duplicates in column, {colname}.')
