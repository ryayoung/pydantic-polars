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

---

Each name after `plv.` is a **shape**.

```python
plv.<shape>.collect(lf)     # Returns primitive T for <shape>
plv.<shape>[T].collect(lf)  # Returns T  (yes, T can be any type form)
```

> Example: `plv.records` -> `list[dict[Any]]` whereas `plv.records[T]` -> `T` by passing `list[dict[Any]]` through Pydantic.

A shape is a **fixed contract**. A structural guarantee.

Part of the contract is that **all values in the dataframe are returned** (materializing data you don't need is a **bug**).

Example: `map` makes a tall dict from 2 columns, but only if column 0 was unique. (if `len(result) == input_df.height`)

A shape has **only one meaning**. It can't be configured to change the structure.

Example: `item` doesn't just grab a value, it asserts the dataframe has *exactly 1 value*. If it may have 0, that's a different shape: `get_item`.


### Step 1. Pick a Shape

- **Scalar**
  - `item`: One value. `get_item`: 0 or one.
- **Row-oriented**
  - `record`: One row as a dict. `get_record`: 0 or one. `records`: List of any.
  - `row`: One row as a tuple. `get_row`: 0 or one. `rows`: List of any.
  - `keyed_records`, `keyed_rows`: One tall dict with column-0 keys to **full-record/row** values.
  - `map`: One tall dict from 2 columns: `{col0: col1}`
  - `record_map`, `row_map`: Like `keyed_*`, but column-0 isn't included in values.
- **Column-oriented**
  - `column`: One column as a list (use `keys` if unique). `columns`: Tuple of any.
  - `column_entry`: One (name, column). `column_entries`: Tuple of any.
  - `column_map`: Many columns, as one {name: column} dict.
- **With table header**
  - `table_records`: (names, records)
  - `table_rows`: (names, rows)
  - `table_columns`: (names, columns)


### Step 2 (optional). Set a custom `T` for Pydantic to validate into

Examples:

```python
plv.column[list[float]]
plv.column[tuple[float, ...]]
plv.column[list[float] | list[Decimal]]
plv.column[list[float | None]]
plv.column[MyCustomArrayType[float | None]]
```

> [!TIP]
> Skipping this step (e.g. `plv.column.collect(lf)`) means skipping Pydantic validation. For `column`, this means you get `lf.collect().to_series().to_list()` directly.

### Step 3. Call a method to create `T`

All shapes have the same methods. These ones return `T`:

```python
# Single query
result = shape.collect(lf)
result = await shape.collect_async(lf)
result = shape.validate(df)  # DataFrame equivalent
# Parallel queries
result1, result2 = plv.collect_all(shape.defer(lf1), shape.defer(lf2))
result1, result2 = await plv.collect_all_async(shape.defer(lf1), shape.defer(lf2))
```

`*_model` variants of each method also exist, to return `T` wrapped in `pydantic.RootModel[T]`. For example, `.collect(lf)` returns `T`, whereas `.collect_model(lf)` returns `RootModel[T]`.

**All** methods are statically type-safe, so your type-checker/IDE will understand the resulting variable. Even with `foo, bar = plv.collect_all(...)`, the exact type of `bar` will be understood.

## Shapes

| Shape            | Default `T`                  | Input df **must** have  |
|------------------|------------------------------|-------------------------|
| `item`           | `Any`                        | height == 1, width == 1 |
| `get_item`       | `Any`                        | height <= 1, width == 1 |
| `column`         | `list[item]`                 | width == 1              |
| `keys`           | `list[item]`                 | width == 1, col0 UNIQUE |
| `row`            | `tuple[item, ...]`           | height == 1             |
| `get_row`        | `tuple[item, ...]`           | height <= 1             |
| `record`         | `dict[name, item]`           | height == 1             |
| `get_record`     | `dict[name, item]`           | height <= 1             |
| `column_entry`   | `tuple[name, column]`        | width == 1              |
| `records`        | `list[record]`               |                         |
| `rows`           | `list[row]`                  |                         |
| `columns`        | `tuple[column, ...]`         |                         |
| `keyed_records`  | `dict[item, record]`         | width >= 1, col0 UNIQUE |
| `keyed_rows`     | `dict[item, row]`            | width >= 1, col0 UNIQUE |
| `map`            | `dict[item, item]`           | width == 2, col0 UNIQUE |
| `record_map`     | `dict[item, partial_record]` | width >= 2, col0 UNIQUE |
| `row_map`        | `dict[item, partial_row]`    | width >= 2, col0 UNIQUE |
| `column_entries` | `tuple[column_entry, ...]`   |                         |
| `column_map`     | `dict[name, column]`         |                         |
| `table_columns`  | `tuple[names, columns]`      |                         |
| `table_rows`     | `tuple[names, rows]`         |                         |
| `table_records`  | `tuple[names, records]`      |                         |
