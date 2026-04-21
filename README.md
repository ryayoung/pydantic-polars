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

Provides an exhaustive set of distinct *shape* contracts.

Each is a structural guarantee, to which you can attach a type-form for parsing/validation.

```python
from pydantic_polars import validate as plv

# Equivalent to `lf.collect().rows(named=True)`
users = plv.records.collect(lf)  # -> list[dict[str, Any]]

# "I have a model now. Parse + validate a list of them."
users = plv.records[list[User]].collect(lf)  # -> list[User]
# Can I have model methods on that list, so I can model_dump() it later?
users = plv.records[list[User]].collect_model(lf)  # -> pydantic.RootModel[list[User]]

# My query produces *exactly* 1 user. It cannot produce 0.
user = plv.record[User].collect(lf.head(1))  # -> User
# Correction: Zero rows is possible
user = plv.get_record[User | None].collect(lf.head(1))  # -> User | None

# Tuples instead of objects? Also...can we do async?
users = await plv.rows[list[UserTuple]].collect_async(lf)  # -> list[UserNamedTupleRow]

# Ok, that query was too big to fit into memory. Let's stream-compute + batch-collect
for users in plv.rows[list[UserTuple]].collect_batches(lf):  # -> Iterator[list[UserTuple]]
    write_somewhere(users)

# I need one huge {name: age} mapping. My query returns exactly 2 columns.
name_age_map = plv.map[dict[str, int]].collect(lf.select(c.name, c.age))  # -> dict[str, int]

# Everyone's names?
users_names = plv.column[list[str]].collect(lf.select(c.name))  # -> list[str]
# Age of oldest person?
oldest_age = plv.item[int | None].collect(lf.select(c.age.max()))  # -> int | None
# Can we parallelize those and await them, without confusing the type-checker?
users_names, oldest_age = await plv.collect_all_async(
    plv.column[list[str]].defer(lf.select(c.name)),
    plv.item[int | None].defer(lf.select(c.age.max())),
)  # -> (list[str], int | None)
```

---

Each name after `plv.` is a **shape**.

```python
plv.<shape>.collect(lf)     # Returns primitive T for <shape>
plv.<shape>[T].collect(lf)  # Returns T  (yes, T can be any type form)
```

A shape is a **fixed contract**. Part of the contract is that **all values in the dataframe are returned** (materializing data you don't need is a bug).

Example: `map` makes a dict from 2 columns, but only if column 0 was unique. (if `len(result) == input_df.height`)


### Step 1. Pick a Shape

- **Scalar**
  - `item`: One value.
- **Row-oriented**
  - `record`, `records`: Row(s) as dict(s). `row`, `rows`: tuple(s) instead of dict(s).
  - `(record|row)_entry`, `(record|row)_entries`: Row(s) as '(col0, other_cols)' pair(s).
    - `keyed_(record|row)_entries`: As '(col0, all_cols)' pairs.
- **Uniquely-keyed rows**
  - `map`: Rows as one tall dict from 2 columns: {unique_col0: col1}.
  - `(record|row)_map`: Rows as one tall dict: {unique_col0: other_cols}.
    - `keyed_(record|row)_map`: As {unique_col0: all_cols}.
- **Column-oriented**
  - `column`: One column as a list (use `keys` if unique). `columns`: Tuple of any.
  - `column_map`: One {name: column} dict of any columns.
- **With table header**
  - `table_<shape>`: (names, shapeT). For example, `table_records`: (names, records)


### Step 2 (optional). Set a custom `T` for Pydantic to validate into

Examples:

```python
plv.column[list[float]]
plv.column[tuple[float, ...]]
plv.column[list[float] | list[Decimal] | list[int]]
plv.column[MyCustomArrayType[float | None]]
```

> [!TIP]
> Skipping this step (e.g. `plv.column.collect(lf)`) means skipping Pydantic validation. For `column`, this means you get `lf.collect().to_series().to_list()` directly.


### Step 3. Call a method to create `T`

All shapes have the same interface. These produce `T`:

```python
# Single query
result = shape.collect(lf)
result = await shape.collect_async(lf)
result = shape.validate(df)  # DataFrame equivalent to collect

# Parallel queries
result1, result2 = plv.collect_all(shape.defer(lf1), shape.defer(lf2))
result1, result2 = await plv.collect_all_async(shape.defer(lf1), shape.defer(lf2))

# Streaming-compute, with batch materialization
for result_batch in shape.collect_batches(lf, chunk_size=1_000_000):
    ...  # Each batch is still T
```

`*_model` variants of each method exist, to return `T` wrapped in `pydantic.RootModel[T]`.


## Shapes

| Shape                        | Default `T`                     | Input df **must** have  |
|------------------------------|---------------------------------|-------------------------|
| `item`                       | `Any`                           | height == 1, width == 1 |
| `get_item`                   | `Any or None`                   | height <= 1, width == 1 |
| `map`                        | `dict[item, item]`              | width == 2, col0 UNIQUE |
| `table_map`                  | `(names, map)`                  | width == 2, col0 UNIQUE |
| `column`                     | `list[item]`                    | width == 1              |
| `keys`                       | `list[item]`                    | width == 1, col0 UNIQUE |
| `columns`                    | `(column, ...)`                 |                         |
| `column_map`                 | `dict[name, column]`            |                         |
| `table_columns`              | `(names, columns)`              |                         |
| `record`                     | `dict[name, item]`              | height == 1             |
| `get_record`                 | `dict[name, item] or None`      | height <= 1             |
| `record_entry`               | `(item, rest_record)`           | height == 1, width >= 2 |
| `get_record_entry`           | `(item, rest_record) or None`   | height <= 1, width >= 2 |
| `records`                    | `list[record]`                  |                         |
| `record_map`                 | `dict[item, rest_record]`       | width >= 2, col0 UNIQUE |
| `keyed_record_map`           | `dict[item, record]`            | width >= 1, col0 UNIQUE |
| `record_entries`             | `list[record_entry]`            | width >= 2              |
| `keyed_record_entries`       | `list[(item, record)]`          | width >= 1              |
| `table_records`              | `(names, records)`              |                         |
| `table_record_map`           | `(names, record_map)`           | width >= 2, col0 UNIQUE |
| `table_keyed_record_map`     | `(names, keyed_record_map)`     | width >= 1, col0 UNIQUE |
| `table_record_entries`       | `(names, record_entries)`       | width >= 2              |
| `table_keyed_record_entries` | `(names, keyed_record_entries)` | width >= 1              |
| `row`                        | `(item, ...)`                   | height == 1             |
| `get_row`                    | `(item, ...) or None`           | height <= 1             |
| `row_entry`                  | `(item, rest_row)`              | height == 1, width >= 2 |
| `get_row_entry`              | `(item, rest_row) or None`      | height <= 1, width >= 2 |
| `rows`                       | `list[row]`                     |                         |
| `row_map`                    | `dict[item, rest_row]`          | width >= 2, col0 UNIQUE |
| `keyed_row_map`              | `dict[item, row]`               | width >= 1, col0 UNIQUE |
| `row_entries`                | `list[row_entry]`               | width >= 2              |
| `keyed_row_entries`          | `list[(item, row)]`             | width >= 1              |
| `table_rows`                 | `(names, rows)`                 |                         |
| `table_row_map`              | `(names, row_map)`              | width >= 2, col0 UNIQUE |
| `table_keyed_row_map`        | `(names, keyed_row_map)`        | width >= 1, col0 UNIQUE |
| `table_row_entries`          | `(names, row_entries)`          | width >= 2              |
| `table_keyed_row_entries`    | `(names, keyed_row_entries)`    | width >= 1              |


<table>
<tr>
<td colspan="2">
<code>item</code>, <code>get_item</code>
</td>
</tr>
<tr>
<td>

```
┌──────┐
│ name │
╞══════╡
│ Joy  │
└──────┘
```

</td>
<td>

```js
 
 
 
'Joy'
 
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┐
│ id ┆ name │
╞════╪══════╡
│ A  ┆ Joy  │
│ B  ┆ Ben  │
│ C  ┆ Jin  │
└────┴──────┘
```

</td>
<td>

```js
 
 
{
  'A': 'Joy',
  'B': 'Ben',
  'C': 'Jin',
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┐
│ id ┆ name │
╞════╪══════╡
│ A  ┆ Joy  │
│ B  ┆ Ben  │
│ C  ┆ Jin  │
└────┴──────┘
```

</td>
<td>

```py
(
  ('id', 'name'),
  {
    'A': 'Joy',
    'B': 'Ben',
    'C': 'Jin',
  },
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>column</code>, <code>keys</code>
</td>
</tr>
<tr>
<td>

```
┌──────┐
│ name │
╞══════╡
│ Joy  │
│ Ben  │
│ Jin  │
└──────┘
```

</td>
<td>

```js
 
 
[
  'Joy',
  'Ben',
  'Jin',
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>columns</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
(
  ['A', 'B', 'C'],
  ['Joy', 'Ben', 'Jin'],
  [59, 25, 40],
)
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>column_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
{
  id:   ['A', 'B', 'C'],
  name: ['Joy', 'Ben', 'Jin'],
  age:  [59, 25, 40],
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_columns</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  (
    ['A', 'B', 'C'],
    ['Joy', 'Ben', 'Jin'],
    [59, 25, 40],
  ),
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>record</code>, <code>get_record</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 

 
{ id: 'A', name: 'Joy', age: 59 }
 
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>record_entry</code>, <code>get_record_entry</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 

 
('A', { name: 'Joy', age: 59 })
 
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>records</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
[
  { id: 'A', name: 'Joy', age: 59 },
  { id: 'B', name: 'Ben', age: 25 },
  { id: 'C', name: 'Jin', age: 40 },
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>record_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
{
  'A': { name: 'Joy', age: 59 },
  'B': { name: 'Ben', age: 25 },
  'C': { name: 'Jin', age: 40 },
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>keyed_record_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
{
  'A': { id: 'A', name: 'Joy', age: 59 },
  'B': { id: 'B', name: 'Ben', age: 25 },
  'C': { id: 'C', name: 'Jin', age: 40 },
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>record_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
[
  ('A', { name: 'Joy', age: 59 }),
  ('B', { name: 'Ben', age: 25 }),
  ('C', { name: 'Jin', age: 40 }),
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>keyed_record_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
 
 
[
  ('A', { id: 'A', name: 'Joy', age: 59 }),
  ('B', { id: 'B', name: 'Ben', age: 25 }),
  ('C', { id: 'C', name: 'Jin', age: 40 }),
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_records</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
(
  ('id', 'name', 'age'),
  [
    { id: 'A', name: 'Joy', age: 59},
    { id: 'B', name: 'Ben', age: 25},
    { id: 'C', name: 'Jin', age: 40},
  ],
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_record_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
(
  ('id', 'name', 'age'),
  {
    'A': { name: 'Joy', age: 59 },
    'B': { name: 'Ben', age: 25 },
    'C': { name: 'Jin', age: 40 },
  },
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_keyed_record_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
(
  ('id', 'name', 'age'),
  {
    'A': { id: 'A', name: 'Joy', age: 59 },
    'B': { id: 'B', name: 'Ben', age: 25 },
    'C': { id: 'C', name: 'Jin', age: 40 },
  },
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_record_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
(
  ('id', 'name', 'age'),
  [
    ('A', { name: 'Joy', age: 59 }),
    ('B', { name: 'Ben', age: 25 }),
    ('C', { name: 'Jin', age: 40 }),
  ],
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_keyed_record_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```js
(
  ('id', 'name', 'age'),
  [
    ('A', { id: 'A', name: 'Joy', age: 59 }),
    ('B', { id: 'B', name: 'Ben', age: 25 }),
    ('C', { id: 'C', name: 'Jin', age: 40 }),
  ],
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>row</code>, <code>get_row</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
 
('A', 'Joy', 59)
 
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>row_entry</code>, <code>get_row_entry</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
 
('A', ('Joy', 59))
 
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>rows</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
[
  ('A', 'Joy', 59),
  ('B', 'Ben', 25),
  ('C', 'Jin', 40),
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>row_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
{
  'A': ('Joy', 59),
  'B': ('Ben', 25),
  'C': ('Jin', 40),
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>keyed_row_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
{
  'A': ('A', 'Joy', 59),
  'B': ('B', 'Ben', 25),
  'C': ('C', 'Jin', 40),
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>row_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
[
  ('A', ('Joy', 59)),
  ('B', ('Ben', 25)),
  ('C', ('Jin', 40)),
]
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>keyed_row_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
 
 
{
  ('A', ('A', 'Joy', 59)),
  ('B', ('B', 'Ben', 25)),
  ('C', ('C', 'Jin', 40)),
}
 
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_rows</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  [
    ('A', 'Joy', 59),
    ('B', 'Ben', 25),
    ('C', 'Jin', 40),
  ],
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_row_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  {
    'A': ('Joy', 59),
    'B': ('Ben', 25),
    'C': ('Jin', 40),
  },
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_keyed_row_map</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  {
    'A': ('A', 'Joy', 59),
    'B': ('A', 'Ben', 25),
    'C': ('A', 'Jin', 40),
  },
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_row_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  [
    ('A', ('Joy', 59)),
    ('B', ('Ben', 25)),
    ('C', ('Jin', 40)),
  ],
)
```

</td>
</tr>

<tr>
<td colspan="2">
<code>table_keyed_row_entries</code>
</td>
</tr>
<tr>
<td>

```
┌────┬──────┬─────┐
│ id ┆ name ┆ age │
╞════╪══════╪═════╡
│ A  ┆ Joy  ┆ 59  │
│ B  ┆ Ben  ┆ 25  │
│ C  ┆ Jin  ┆ 40  │
└────┴──────┴─────┘
```

</td>
<td>

```py
(
  ('id', 'name', 'age'),
  [
    ('A', ('A', 'Joy', 59)),
    ('B', ('A', 'Ben', 25)),
    ('C', ('A', 'Jin', 40)),
  ],
)
```

</td>
</tr>

</table>
