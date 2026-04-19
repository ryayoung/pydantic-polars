# Pydantic, for Polars

[![PyPI](https://img.shields.io/pypi/v/pydantic-polars)](https://pypi.org/project/pydantic-polars/)
[![Tests](https://github.com/ryayoung/pydantic-polars/actions/workflows/tests.yml/badge.svg)](https://github.com/ryayoung/pydantic-polars/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/ryayoung/pydantic-polars/branch/main/graph/badge.svg)](https://codecov.io/gh/ryayoung/pydantic-polars)
[![License](https://img.shields.io/github/license/ryayoung/pydantic-polars)](https://github.com/ryayoung/pydantic-polars/blob/main/LICENSE)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/pydantic-polars.svg)](https://pypi.python.org/pypi/pydantic-polars/)
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Pyright](https://img.shields.io/badge/type%20checker-pyright-blue)](https://github.com/microsoft/pyright)


Type-safe, maintainable interfaces between Polars and Python objects.

```
uv add pydantic-polars
```

# pydantic_polars.validate

**Go from Polars query -> Python objects**

Learn the API by example:

```python
from pydantic_polars import validate as plv

# Equivalent to `lf.collect().rows(named=True)`
users = plv.records.collect(lf)  # -> list[dict[str, Any]]

# I have a model now. Parse + validate a list of them.
users = plv.records[list[User]].collect(lf)  # -> list[User]

# Can there be an api around the list so I can model_dump?
users = plv.records[list[User]].collect_model(lf)  # -> pydantic.RootModel[list[User]]
users_json = users.model_dump_json()

# My query produces, at most, 1 user. But 0 rows may come back.
user = plv.get_record[User].collect(lf.filter(name='Mo').head(1))  # -> User | None

# My query produces *exactly* 1 user. It cannot produce 0 or 2.
user = plv.record[User].collect(lf.head(1))  # -> User

# Tuples instead of objects? Also...can we do async?
users = await plv.rows[list[UserNamedTuple]].collect_async(lf)  # -> list[UserNamedTupleRow]

# Need one huge {name: age} mapping. My query returns exactly 2 columns.
name_age_map = plv.map[dict[str, int]].collect(lf.select(c.name, c.age))

# Everyone's names, please
users_names = plv.column[list[str]].collect(lf.select(c.name))  # -> list[str]

# Age of oldest person?
oldest_age = plv.item[int | None].collect(lf.select(c.age.max()))  # -> int | None

# Can we parallelize those in Rust, on other threads?
users_names, oldest_age = await plv.collect_all_async(
    plv.column[list[str]].defer(lf.select(c.name)),
    plv.item[int | None].defer(lf.select(c.age.max())),
)  # -> (list[str], int | None)

# Only need his age, but 0 rows may come back. Safely get int or None.
age = plv.get_item[int].collect(
    lf.filter(c.name == 'jeff').select(c.age).head(1)
)  # -> int | None
```

## 1. Pick a Shape

A *shape* is a fixed, non-configurable representation of a dataframe as plain Python objects.

`records` means, *Produce a list of row dicts*. It translates to `df.rows(named=True)`.

`records[T]` means, *Produce `T` by passing a list of row dicts as input to Pydantic validation*.

```python
plv.<shape>.collect(lf)     # Returns Default T for <shape>
plv.<shape>[T].collect(lf)  # Returns T
```

- **Scalar**
  - `item`: One value.
- **Row-oriented**
  - `record`: One row as a dict. `records`: List of many.
  - `row`: One row as a tuple. `rows`: List of many.
  - `map`: The rows of 2 columns, as one {col0: col1} dict.
  - `keyed_records`: Rows as one {col0: record} dict.
  - `keyed_rows`: Rows as one {col0: row} dict.
  - `record_map`: Rows of 2+ columns, as one {col0: {**rest_record}} dict.
  - `row_map`: Rows of 2+ columns, as one {col0: (*rest_row)} dict.
- **Column-oriented**
  - `column`: One column as a list of values. `columns`: Tuple of many.
  - `keys`: One unique column as a list of values.
  - `column_entry`: One (name, column). `column_entries`: Tuple of many.
  - `column_map`: Many columns, as one {name: column} dict.
- **With table header**
  - `table_records`: (names, records)
  - `table_rows`: (names, rows)
  - `table_columns`: (names, columns)


| Shape            | Default `T`                  | Returns     | Input query must produce |
|------------------|------------------------------|-------------|--------------------------|
| `item`           | `Any`                        | `T`         | height == 1, width == 1  |
| `column`         | `list[item]`                 | `T`         | width == 1               |
| `keys`           | `list[item]`                 | `T`         | width == 1, col0 UNIQUE  |
| `row`            | `tuple[item, ...]`           | `T`         | height == 1              |
| `record`         | `dict[name, item]`           | `T`         | height == 1              |
| `column_entry`   | `tuple[name, column]`        | `T`         | width == 1               |
| `records`        | `list[record]`               | `T`         |                          |
| `rows`           | `list[row]`                  | `T`         |                          |
| `columns`        | `tuple[column, ...]`         | `T`         |                          |
| `keyed_records`  | `dict[item, record]`         | `T`         | width >= 1, col0 UNIQUE  |
| `keyed_rows`     | `dict[item, row]`            | `T`         | width >= 1, col0 UNIQUE  |
| `map`            | `dict[item, item]`           | `T`         | width == 2, col0 UNIQUE  |
| `record_map`     | `dict[item, partial_record]` | `T`         | width >= 2, col0 UNIQUE  |
| `row_map`        | `dict[item, partial_row]`    | `T`         | width >= 2, col0 UNIQUE  |
| `column_entries` | `tuple[column_entry, ...]`   | `T`         |                          |
| `column_map`     | `dict[name, column]`         | `T`         |                          |
| `table_columns`  | `tuple[names, columns]`      | `T`         |                          |
| `table_rows`     | `tuple[names, rows]`         | `T`         |                          |
| `table_records`  | `tuple[names, records]`      | `T`         |                          |
| `get_item`       | `item`                       | `T or None` | height <= 1, width == 1  |
| `get_row`        | `row`                        | `T or None` | height <= 1              |
| `get_record`     | `record`                     | `T or None` | height <= 1              |


## 2. Call a method to create `T`

All shapes have the same methods.

```python
# Single query
result = shape.collect(lf)
result = await shape.collect_async(lf)
result = shape.validate(df)  # DataFrame equivalent

# Parallel queries
result1, result2 = plv.collect_all(shape.defer(lf1), shape.defer(lf2))
result1, result2 = await plv.collect_all_async(shape.defer(lf1), shape.defer(lf2))
```
