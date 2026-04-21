"""
DataFrame/LazyFrame validators for various data shape interpretations.
"""

from typing import TYPE_CHECKING, Any, Callable
import functools
from polars import DataFrame
from ._base_shape import BaseBatchableShape, BaseShape
from . import _shape as shp

__all__ = [
    'Item',
    'Record',
    'Row',
    'RecordEntry',
    'RowEntry',
    'GetItem',
    'GetRecord',
    'GetRow',
    'GetRecordEntry',
    'GetRowEntry',
    'Column',
    'Keys',
    'Records',
    'Rows',
    'Columns',
    'RecordEntries',
    'RowEntries',
    'KeyedRecordEntries',
    'KeyedRowEntries',
    'Map',
    'RecordMap',
    'RowMap',
    'ColumnMap',
    'KeyedRecordMap',
    'KeyedRowMap',
    'TableRecords',
    'TableRows',
    'TableColumns',
    'TableRecordEntries',
    'TableRowEntries',
    'TableKeyedRecordEntries',
    'TableKeyedRowEntries',
    'TableMap',
    'TableRecordMap',
    'TableRowMap',
    'TableKeyedRecordMap',
    'TableKeyedRowMap',
]


def _make_df_to_py[T](func: Callable[[DataFrame], T]):
    """
    Helper to take a shape implementation function and wrap it so that it's a
    classmethod that ignores `cls`. Can only be used if shape takes only a DataFrame.
    """

    @functools.wraps(func)
    def _dataframe_to_python(_: Any, df: DataFrame, /) -> T:
        return func(df)

    if TYPE_CHECKING:
        return _dataframe_to_python
    return classmethod(_dataframe_to_python)


class Item[T: Any = shp.ItemT](BaseShape[T]):
    """
    One value. Query must produce exactly one row, and one column.
    - `'Joe'`
    """

    _dataframe_to_python = _make_df_to_py(shp.item)


class Record[T: Any = shp.RecordT](BaseShape[T]):
    """
    One row dict. Query must produce exactly one row.
    - `{ name: 'Joe', age: 23 }`
    """

    _dataframe_to_python = _make_df_to_py(shp.record)


class Row[T: Any = shp.RowT](BaseShape[T]):
    """
    One row tuple. Query must produce exactly one row.
    - `('Joe', 23)`
    """

    _dataframe_to_python = _make_df_to_py(shp.row)


class RecordEntry[T: Any = shp.RecordEntryT](BaseShape[T]):
    """
    One (key, partial-record) pair, where the key is from column-0
    and the record dict is made from the remaining columns.
    - `('Joe', { age: 23 })`
    """

    _dataframe_to_python = _make_df_to_py(shp.record_entry)


class RowEntry[T: Any = shp.RowEntryT](BaseShape[T]):
    """
    One (key, partial-row) pair, where the key is from column-0
    and the row tuple is made from the remaining columns.
    - `('Joe', (23,))`
    """

    _dataframe_to_python = _make_df_to_py(shp.row_entry)


class GetItem[T: Any = shp.ItemT | None](BaseShape[T]):
    """Like `item`, but returns None if 0 rows came back."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> shp.ItemT | None:
        return shp.get_item(df, None)


class GetRecord[T: Any = shp.RecordT | None](BaseShape[T]):
    """Like `record`, but returns None if 0 rows came back."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> shp.RecordT | None:
        return shp.get_record(df, None)


class GetRow[T: Any = shp.RowT | None](BaseShape[T]):
    """Like `row`, but returns None if 0 rows came back."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> shp.RowT | None:
        return shp.get_row(df, None)


class GetRecordEntry[T: Any = shp.RecordEntryT | None](BaseShape[T]):
    """Like `record_entry`, but returns None if 0 rows came back."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> shp.RecordEntryT | None:
        return shp.get_record_entry(df, None)


class GetRowEntry[T: Any = shp.RowEntryT | None](BaseShape[T]):
    """Like `row_entry`, but returns None if 0 rows came back."""

    @classmethod
    def _dataframe_to_python(cls, df: DataFrame) -> shp.RowEntryT | None:
        return shp.get_row_entry(df, None)


class Column[T: Any = shp.ColumnT](BaseBatchableShape[T]):
    """
    The list of values in a single column.
    - Query must produce exactly one column.
    - `T` : The list of values.
    - `['Joe', 'Bob']`
    """

    _dataframe_to_python = _make_df_to_py(shp.column)


class Keys[T: Any = shp.ColumnT](BaseShape[T]):
    """
    Like `column`, but the column must be unique (no duplicate values).
    """

    _dataframe_to_python = _make_df_to_py(shp.keys)


class Records[T: Any = shp.RecordsT](BaseBatchableShape[T]):
    """
    A list of row dicts.
    - `T` : The list of row dicts
    - `[{ name: 'Joe', age: 23 }, { name: 'Bob', age: 45 }]`
    """

    _dataframe_to_python = _make_df_to_py(shp.records)


class Rows[T: Any = shp.RowsT](BaseBatchableShape[T]):
    """
    A list of row tuples.
    - `T` : The list of row tuples.
    - `[('Joe', 23), ('Bob', 45)]`
    """

    _dataframe_to_python = _make_df_to_py(shp.rows)


class Columns[T: Any = shp.ColumnsT](BaseBatchableShape[T]):
    """
    A tuple of columns.
    - `T` : The tuple of column value lists.
    - `(['Joe', 'Bob'], [23, 45])`
    """

    _dataframe_to_python = _make_df_to_py(shp.columns)


class RecordEntries[T: Any = shp.RecordEntriesT](BaseBatchableShape[T]):
    """
    The list of (key, partial-record) pairs, with keys from
    the first column and dict records made from the remaining columns.
    - Query must produce at least 2 columns.
    - `T` : The list of (key, partial-record) pairs.
    - `[ ('Joe', {'age': 23}), ('Bob', {'age': 45}) ]`
    """

    _dataframe_to_python = _make_df_to_py(shp.record_entries)


class RowEntries[T: Any = shp.RowEntriesT](BaseBatchableShape[T]):
    """
    The list of (key, partial-row) pairs, with keys from
    the first column and tuple rows made from the remaining columns.
    - Query must produce at least 2 columns.
    - `T` : The list of (key, partial-row) pairs.
    - `[ ('Joe', (23,)), ('Bob', (45,)) ]`
    """

    _dataframe_to_python = _make_df_to_py(shp.row_entries)


class KeyedRecordEntries[T: Any = shp.RecordEntriesT](BaseBatchableShape[T]):
    """
    The list of (key, record) pairs, with keys from the first
    column, and dict records made from all columns (including the first).
    - Query must produce at least 1 column.
    - `T` : The list of (key, record) pairs.
    - `[ ('Joe', {'name': 'Joe', 'age': 23}), ('Bob', {'name': 'Bob', 'age': 45}) ]`
    """

    _dataframe_to_python = _make_df_to_py(shp.keyed_record_entries)


class KeyedRowEntries[T: Any = shp.RowEntriesT](BaseBatchableShape[T]):
    """
    The list of (key, row) pairs, with keys from the first
    column, and full rows made from all columns (including the first).
    - Query must produce at least 1 column.
    - `T` : The list of (key, row) pairs.
    - `{ ('Joe', ('Joe', 23)), ('Bob', ('Bob', 45)) }`
    """

    _dataframe_to_python = _make_df_to_py(shp.keyed_row_entries)


# Row-wise, unique


class Map[T: Any = shp.MapT](BaseShape[T]):
    """
    A tall dict that maps keys from the first column to values in the second.
    - Query must produce exactly 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to second-column-values.
    - `{ 'Joe': 23, 'Bob': 45 }`
    """

    _dataframe_to_python = _make_df_to_py(shp.map)


class RecordMap[T: Any = shp.RecordMappingT](BaseShape[T]):
    """
    A tall dict that maps keys from the first column to dict records
    made from the remaining columns.
    - Query must produce at least 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to dict records.
    - `{ 'Joe': {'age': 23}, 'Bob': {'age': 45} }`
    """

    _dataframe_to_python = _make_df_to_py(shp.record_map)


class RowMap[T: Any = shp.RowMappingT](BaseShape[T]):
    """
    A tall dict that maps keys from the first column to row tuples
    made from the remaining columns.
    - Query must produce at least 2 columns. The first must be unique.
    - `T` : The dict that maps first-column-values to row tuples.
    - `{ 'Joe': (23,), 'Bob': (45,) }`
    """

    _dataframe_to_python = _make_df_to_py(shp.row_map)


class ColumnMap[T: Any = shp.NameColumnMapT](BaseBatchableShape[T]):
    """
    A dict of column-name -> column.
    - `T` : The dict with column name keys and value-list values.
    - `{ name: ['Joe', 'Bob'], age: [23, 45] }`
    """

    _dataframe_to_python = _make_df_to_py(shp.column_map)


class KeyedRecordMap[T: Any = shp.RecordMappingT](BaseShape[T]):
    """
    A tall dict that maps keys from the first column to full dict
    records with all columns, i.e. the same values as those from `records`.
    - Query must produce at least 1 column. It must be unique.
    - `T` : A dict that maps column-0 keys to whole record dicts.
    - `{ 'Joe': {'name': 'Joe', 'age': 23}, 'Bob': {'name': 'Bob', 'age': 45} }`
    """

    _dataframe_to_python = _make_df_to_py(shp.keyed_record_map)


class KeyedRowMap[T: Any = shp.RowMappingT](BaseShape[T]):
    """
    A tall dict that maps keys from the first column to full row
    tuples with all columns, i.e. the same values as those from `rows`.
    - Query must produce at least 1 column. It must be unique.
    - `T` : A dict that maps column-0 keys to whole row tuples.
    - `{ 'Joe': ('Joe', 23), 'Bob': ('Bob', 45) }`
    """

    _dataframe_to_python = _make_df_to_py(shp.keyed_row_map)


# Column names separated from data


type _Tbl[T] = shp.TableOfT[T]


class TableRecords[T: Any = _Tbl[shp.RecordsT]](BaseBatchableShape[T]):
    """
    Like `records`, but includes a column-names tuple. Produces `(names, records)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_records)


class TableRows[T: Any = _Tbl[shp.RowsT]](BaseBatchableShape[T]):
    """
    Like `rows`, but includes a column-names tuple. Produces `(names, rows)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_rows)


class TableColumns[T: Any = _Tbl[shp.ColumnsT]](BaseBatchableShape[T]):
    """
    Like `columns`, but includes a column-names tuple. Produces`(names, columns)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_columns)


class TableRecordEntries[T: Any = _Tbl[shp.RecordEntriesT]](BaseBatchableShape[T]):
    """
    Like `record_entries`, but includes a column-names tuple. Produces `(names, record_entries)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_record_entries)


class TableRowEntries[T: Any = _Tbl[shp.RowEntriesT]](BaseBatchableShape[T]):
    """
    Like `row_entries`, but includes a column-names tuple. Produces `(names, row_entries)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_row_entries)


class TableKeyedRecordEntries[T: Any = _Tbl[shp.RecordEntriesT]](BaseBatchableShape[T]):
    """
    Like `keyed_record_entries`, but includes a column-names tuple.
    Produces `(names, keyed_record_entries)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_keyed_record_entries)


class TableKeyedRowEntries[T: Any = _Tbl[shp.RowEntriesT]](BaseBatchableShape[T]):
    """
    Like `keyed_row_entries`, but includes a column-names tuple.
    Produces `(names, keyed_row_entries)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_keyed_row_entries)


class TableMap[T: Any = _Tbl[shp.MapT]](BaseShape[T]):
    """
    Like `map`, but includes a column-names tuple. Produces `(names, map)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_map)


class TableRecordMap[T: Any = _Tbl[shp.RecordMappingT]](BaseShape[T]):
    """
    Like `record_map`, but includes a column-names tuple. Produces `(names, record_map)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_record_map)


class TableRowMap[T: Any = _Tbl[shp.RowMappingT]](BaseShape[T]):
    """
    Like `row_map`, but includes a column-names tuple. Produces `(names, row_map)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_row_map)


class TableKeyedRecordMap[T: Any = _Tbl[shp.RecordMappingT]](BaseShape[T]):
    """
    Like `keyed_record_map`, but includes a column-names tuple.
    Produces `(names, keyed_record_map)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_keyed_record_map)


class TableKeyedRowMap[T: Any = _Tbl[shp.RowMappingT]](BaseShape[T]):
    """
    Like `keyed_row_map`, but includes a column-names tuple.
    Produces `(names, keyed_row_map)`.
    """

    _dataframe_to_python = _make_df_to_py(shp.table_keyed_row_map)


# SET SHAPE IDENTITY
# ------------------
for cls in list(globals().values()):
    if (
        isinstance(cls, type)
        and cls.__module__ == __name__
        and issubclass(cls, BaseShape)
        and cls is not BaseShape
    ):
        cls._original_shape_cls = cls  # pyright: ignore[reportPrivateUsage]
