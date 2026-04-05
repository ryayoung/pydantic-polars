"""
DataFrame/LazyFrame validators for various data shape interpretations.
"""

from typing import Any
import polars as pl
from polars import DataFrame
from pydantic_core import PydanticUndefined
from ._base_validator import BaseValidator

__all__ = [
    'records',
    'rows',
    'columns',
    'column_map',
    'column_entries',
    'table_records',
    'table_rows',
    'table_columns',
    'column',
    'column_entry',
    'map',
    'record',
    'get_record',
    'row',
    'get_row',
    'item',
    'get_item',
]

# Remove type information (hinted Any) to keep pyright happy.
_UNDEFINED: Any = PydanticUndefined

# Default types (when no pydantic validation is desired)
type _Item = Any  # Single cell value

type _Name = str  # Column name
type _Column = list[_Item]  # Values in a column
type _Record = dict[_Name, _Item]  # A row as a dict
type _Row = tuple[_Item, ...]  # A row as a tuple
type _ColumnEntry = tuple[_Name, _Column]  # A column name and its values

type _Names = tuple[_Name, ...]  # All column names
type _Columns = tuple[_Column, ...]  # All values in all columns
type _Rows = list[_Row]  # All rows as tuples
type _Records = list[_Record]  # All rows as dicts
type _ColumnEntries = tuple[
    _ColumnEntry, ...
]  # All column entries as (name, values) pairs


# Unconstrained | Row-wise | All data


class records[T: Any = _Records](BaseValidator[T]):
    """
    Validate a list of row dicts.
    - `T` : The list of row dicts
    - `[{ name: 'Joe', age: 23 }, { name: 'Bob', age: 45 }]`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Records:
        return df.rows(named=True)


class rows[T: Any = _Rows](BaseValidator[T]):
    """
    Validate a list of row tuples.
    - `T` : The list of row tuples.
    - `[('Joe', 23), ('Bob', 45)]`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Rows:
        return df.rows()


class columns[T: Any = _Columns](BaseValidator[T]):
    """
    Validate a tuple of columns.
    - `T` : The tuple of column value lists.
    - `(['Joe', 'Bob'], [23, 45])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Columns:
        return tuple(c.to_list() for c in df)


# Column names paired with data


class column_entries[T: Any = _ColumnEntries](BaseValidator[T]):
    """
    Validate a tuple of (name, column) pairs.
    - `T` : The tuple of (column name, value list) pairs.
    - `(('name', ['Joe', 'Bob']), ('age', [23, 45]))`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _ColumnEntries:
        return tuple((s.name, s.to_list()) for s in df)


class column_map[T: Any = dict[_Name, _Column]](BaseValidator[T]):
    """
    Validate a dict of name -> column.
    - `T` : The dict with column name keys and value-list values.
    - `{ name: ['Joe', 'Bob'], age: [23, 45] }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[_Name, _Column]:
        return {s.name: s.to_list() for s in df}


# Column names separated from data


class table_records[T: Any = tuple[_Names, _Records]](BaseValidator[T]):
    """
    Validate the pair, (names, records).
    - `T` : The tuple, (tuple-of-names, list-of-row-dicts).
    - `(('name', 'age'), [{ name: 'Joe', age: 23 }, { name: 'Bob', age: 45 }])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[_Names, _Records]:
        return (tuple(df.columns), df.rows(named=True))


class table_rows[T: Any = tuple[_Names, _Rows]](BaseValidator[T]):
    """
    Validate the pair, (names, rows).
    - `T` : The tuple, (tuple-of-names, list-of-row-tuples).
    - `(('name', 'age'), [('Joe', 23), ('Bob', 45)])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[_Names, _Rows]:
        return (tuple(df.columns), df.rows())


class table_columns[T: Any = tuple[_Names, _Columns]](BaseValidator[T]):
    """
    Validate the pair, (names, columns).
    - `T` : The tuple, (tuple-of-names, tuple-of-values-lists).
    - `(('name', 'age'), (['Joe', 'Bob'], [23, 45]))`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> tuple[_Names, _Columns]:
        return (tuple(df.columns), tuple(s.to_list() for s in df))


# Single (horizontal)


class map[T: Any = dict[_Item, _Item]](BaseValidator[T]):
    """
    For a query that produces exactly two columns, validate the dict that maps
    keys from the first column to values in the second column.
    - `T` : The dict that maps first-column-values to second-column-values.
    - `{ 'Joe': 23, 'Bob': 45 }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> dict[_Item, _Item]:
        _raise_if_bad_query_structure(cls, df, max_width=2, min_width=2)
        return dict(df.rows())


class column[T: Any = _Column](BaseValidator[T]):
    """
    For a query that produces exactly one column, validate the list of values.
    - `T` : The list of values.
    - `['Joe', 'Bob']`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Column:
        _raise_if_bad_query_structure(cls, df, min_width=1, max_width=1)
        return df[df.columns[0]].to_list()


class column_entry[T: Any = _ColumnEntry](BaseValidator[T]):
    """
    For a query that produces one column, validate (column-name, values-list).
    - `T` : The tuple of (column-name, list-of-values).
    - `('name', ['Joe', 'Bob'])`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _ColumnEntry:
        _raise_if_bad_query_structure(cls, df, min_width=1, max_width=1)
        return (df.columns[0], df[df.columns[0]].to_list())


# Single (vertical)


class record[T: Any = _Record](BaseValidator[T]):
    """
    For a query that produces exactly one row, validate the row dict.
    - `T` : The row dict.
    - `{ name: 'Joe', age: 23 }`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Record:
        _raise_if_bad_query_structure(cls, df, min_height=1, max_height=1)
        return df.rows(named=True)[0]


class get_record[T: Any = _Record](BaseValidator[T | None]):
    """Same as `record`, but returns None if no row found."""

    root: T | None = None

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Record | Any:
        _raise_if_bad_query_structure(cls, df, max_height=1)
        records = df.rows(named=True)
        return _UNDEFINED if len(records) == 0 else records[0]


class row[T: Any = _Row](BaseValidator[T]):
    """
    For a query that produces exactly one row, validate the row tuple.
    - `T` : The row tuple.
    - `('Joe', 23)`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Row:
        _raise_if_bad_query_structure(cls, df, min_height=1, max_height=1)
        return df.rows()[0]


class get_row[T: Any = _Row](BaseValidator[T | None]):
    """Same as `row`, but returns None if no row found."""

    root: T | None = None

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Row | Any:
        _raise_if_bad_query_structure(cls, df, max_height=1)
        rows = df.rows()
        return _UNDEFINED if len(rows) == 0 else rows[0]


class item[T: Any = _Item](BaseValidator[T]):
    """
    For a query that produces exactly one value, validate the value.
    - `T` : The single value.
    - `'Joe'`
    """

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Item:
        _raise_if_bad_query_structure(
            cls, df, min_height=1, max_height=1, min_width=1, max_width=1
        )
        return df.item()


class get_item[T: Any = _Item](BaseValidator[T | None]):
    """Same as `item`, but returns None if no value found."""

    root: T | None = None

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> _Item:
        _raise_if_bad_query_structure(cls, df, max_height=1, min_width=1, max_width=1)
        return _UNDEFINED if df.height == 0 else df.item()


def _raise_if_bad_query_structure(
    cls: type[Any],
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
