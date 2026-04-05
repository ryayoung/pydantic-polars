# Pydantic, for Polars

Type-safe, maintainable interfaces between Polars and Python objects.

# pydantic_polars.validate`

**Go from Polars query -> Python objects**

Learn the API by example:

```python
from pydantic_polars import validate as plv

# Equivalent to `lf.collect().rows(named=True)`
users = plv.records.collect(lf)  # -> list[dict[str, Any]]

# I have a model now. Parse + validate.
users = plv.records[list[UserModel]].collect(lf)

# My polars query always produces exactly 1 user.
user = plv.record[UserModel].collect(lf.limit(1))

# At most, 1 user. But 0 rows might come back. Get User or None.
user = plv.get_record[UserModel].collect(lf.filter(c.name == 'jeff').limit(1))

# NamedTuple instead of dataclass? Also, can we do async?
users = await plv.rows[list[UserNamedTupleRow]].collect_async(lf)

# Need one huge mapping of all name -> age
name_age_map = plv.map[dict[str, int | None]].collect(lf.select(c.name, c.age))

# Everyone's names, please
users_names = plv.column[list[str]].collect(lf.select(c.name))

# Age of oldest person?
oldest_age = plv.item[int | None].collect(lf.select(c.age.max()))

# Can we parallelize those in Rust, on other threads?
users_names, oldest_age = await plv.collect_all_async(
    plv.column[list[str]].defer(lf.select(c.name)),
    plv.item[int | None].defer(lf.select(c.age.max())),
)

# Only need his age, but 0 rows may come back. Safely get int or None.
age = plv.get_item[int].collect(
    lf.filter(c.name == 'jeff').select(c.age).limit(1)
)
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
  - `map`: The rows of 2 columns, as one {col1: col2} dict.
- **Column-oriented**
  - `column`: One column as a list of values. `columns`: Tuple of many.
  - `column_entry`: One (name, column). `column_entries`: Tuple of many.
  - `column_map`: Many columns, as one {name: column} dict.
- **With table header**
  - `table_records`: (names, records)
  - `table_rows`: (names, rows)
  - `table_columns`: (names, columns)


| Shape            | Default `T`                | Returns     | Input query must produce |
|------------------|----------------------------|-------------|--------------------------|
| `item`           | `Any`                      | `T`         | height == 1, width == 1  |
| `column`         | `list[item]`               | `T`         | width == 1               |
| `row`            | `tuple[item, ...]`         | `T`         | height == 1              |
| `record`         | `dict[name, item]`         | `T`         | height == 1              |
| `column_entry`   | `tuple[name, column]`      | `T`         | width == 1               |
| `map`            | `dict[item, item]`         | `T`         | width == 2               |
| `records`        | `list[record]`             | `T`         |                          |
| `rows`           | `list[row]`                | `T`         |                          |
| `columns`        | `tuple[column, ...]`       | `T`         |                          |
| `column_entries` | `tuple[column_entry, ...]` | `T`         |                          |
| `column_map`     | `dict[name, column]`       | `T`         |                          |
| `table_columns`  | `tuple[names, columns]`    | `T`         |                          |
| `table_rows`     | `tuple[names, rows]`       | `T`         |                          |
| `table_records`  | `tuple[names, records]`    | `T`         |                          |
| `get_item`       | `item`                     | `T or None` | height <= 1, width == 1  |
| `get_row`        | `row`                      | `T or None` | height <= 1              |
| `get_record`     | `record`                   | `T or None` | height <= 1              |


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
