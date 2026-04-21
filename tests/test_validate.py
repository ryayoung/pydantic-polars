# pyright: reportPrivateUsage=false

import asyncio
from typing import Any, NamedTuple

# --
import polars as pl
import pytest
from pydantic import BaseModel, RootModel
from pydantic_core import ValidationError
from polars import col as c

# --
from pydantic_polars import validate as plv
from pydantic_polars._validate import _shape
from pydantic_polars._validate._base_shape import (
    BaseBatchableShape,
    BaseShape,
    DeferredValidation,
)


class User(BaseModel):
    name: str
    age: int | None


class UserRow(NamedTuple):
    name: str
    age: int | None


def sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            'name': ['Joe', 'Bob'],
            'age': [23, None],
            'active': [True, False],
        }
    )


def sample_lf() -> pl.LazyFrame:
    return sample_df().lazy()


def empty_df() -> pl.DataFrame:
    return pl.DataFrame()


@pytest.mark.parametrize(
    ('validator', 'df', 'expected'),
    [
        (
            plv.item,
            sample_df().select(c.age.max()),
            23,
        ),
        (
            plv.get_item,
            sample_df().filter(c.name == 'Missing').select(c.age),
            None,
        ),
        (
            plv.record,
            sample_df().head(1),
            {'name': 'Joe', 'age': 23, 'active': True},
        ),
        (
            plv.get_record,
            sample_df().head(0),
            None,
        ),
        (
            plv.row,
            sample_df().head(1),
            ('Joe', 23, True),
        ),
        (
            plv.get_row,
            sample_df().head(0),
            None,
        ),
        (
            plv.record_entry,
            sample_df().head(1),
            ('Joe', {'age': 23, 'active': True}),
        ),
        (
            plv.get_record_entry,
            sample_df().head(0),
            None,
        ),
        (
            plv.row_entry,
            sample_df().head(1),
            ('Joe', (23, True)),
        ),
        (
            plv.get_row_entry,
            sample_df().head(0),
            None,
        ),
        (
            plv.column,
            sample_df().select(c.name),
            ['Joe', 'Bob'],
        ),
        (
            plv.keys,
            sample_df().select(c.name),
            ['Joe', 'Bob'],
        ),
        (
            plv.records,
            sample_df(),
            [
                {'name': 'Joe', 'age': 23, 'active': True},
                {'name': 'Bob', 'age': None, 'active': False},
            ],
        ),
        (
            plv.rows,
            sample_df(),
            [('Joe', 23, True), ('Bob', None, False)],
        ),
        (
            plv.columns,
            sample_df(),
            (['Joe', 'Bob'], [23, None], [True, False]),
        ),
        (
            plv.record_entries,
            sample_df(),
            [
                ('Joe', {'age': 23, 'active': True}),
                ('Bob', {'age': None, 'active': False}),
            ],
        ),
        (
            plv.row_entries,
            sample_df(),
            [('Joe', (23, True)), ('Bob', (None, False))],
        ),
        (
            plv.keyed_record_entries,
            sample_df(),
            [
                ('Joe', {'name': 'Joe', 'age': 23, 'active': True}),
                ('Bob', {'name': 'Bob', 'age': None, 'active': False}),
            ],
        ),
        (
            plv.keyed_row_entries,
            sample_df(),
            [('Joe', ('Joe', 23, True)), ('Bob', ('Bob', None, False))],
        ),
        (
            plv.map,
            sample_df().select(c.name, c.age),
            {'Joe': 23, 'Bob': None},
        ),
        (
            plv.record_map,
            sample_df(),
            {
                'Joe': {'age': 23, 'active': True},
                'Bob': {'age': None, 'active': False},
            },
        ),
        (
            plv.row_map,
            sample_df(),
            {'Joe': (23, True), 'Bob': (None, False)},
        ),
        (
            plv.column_map,
            sample_df(),
            {
                'name': ['Joe', 'Bob'],
                'age': [23, None],
                'active': [True, False],
            },
        ),
        (
            plv.keyed_record_map,
            sample_df(),
            {
                'Joe': {'name': 'Joe', 'age': 23, 'active': True},
                'Bob': {'name': 'Bob', 'age': None, 'active': False},
            },
        ),
        (
            plv.keyed_row_map,
            sample_df(),
            {'Joe': ('Joe', 23, True), 'Bob': ('Bob', None, False)},
        ),
        (
            plv.table_records,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [
                    {'name': 'Joe', 'age': 23, 'active': True},
                    {'name': 'Bob', 'age': None, 'active': False},
                ],
            ),
        ),
        (
            plv.table_rows,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [('Joe', 23, True), ('Bob', None, False)],
            ),
        ),
        (
            plv.table_columns,
            sample_df(),
            (
                ('name', 'age', 'active'),
                (['Joe', 'Bob'], [23, None], [True, False]),
            ),
        ),
        (
            plv.table_record_entries,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [
                    ('Joe', {'age': 23, 'active': True}),
                    ('Bob', {'age': None, 'active': False}),
                ],
            ),
        ),
        (
            plv.table_row_entries,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [('Joe', (23, True)), ('Bob', (None, False))],
            ),
        ),
        (
            plv.table_keyed_record_entries,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [
                    ('Joe', {'name': 'Joe', 'age': 23, 'active': True}),
                    ('Bob', {'name': 'Bob', 'age': None, 'active': False}),
                ],
            ),
        ),
        (
            plv.table_keyed_row_entries,
            sample_df(),
            (
                ('name', 'age', 'active'),
                [('Joe', ('Joe', 23, True)), ('Bob', ('Bob', None, False))],
            ),
        ),
        (
            plv.table_map,
            sample_df().select(c.name, c.age),
            (('name', 'age'), {'Joe': 23, 'Bob': None}),
        ),
        (
            plv.table_record_map,
            sample_df(),
            (
                ('name', 'age', 'active'),
                {
                    'Joe': {'age': 23, 'active': True},
                    'Bob': {'age': None, 'active': False},
                },
            ),
        ),
        (
            plv.table_row_map,
            sample_df(),
            (
                ('name', 'age', 'active'),
                {'Joe': (23, True), 'Bob': (None, False)},
            ),
        ),
        (
            plv.table_keyed_record_map,
            sample_df(),
            (
                ('name', 'age', 'active'),
                {
                    'Joe': {'name': 'Joe', 'age': 23, 'active': True},
                    'Bob': {'name': 'Bob', 'age': None, 'active': False},
                },
            ),
        ),
        (
            plv.table_keyed_row_map,
            sample_df(),
            (
                ('name', 'age', 'active'),
                {'Joe': ('Joe', 23, True), 'Bob': ('Bob', None, False)},
            ),
        ),
    ],
)
def test_default_shape_validators_return_expected_python_objects(
    validator: type[BaseShape[object]],
    df: pl.DataFrame,
    expected: object,
) -> None:
    assert validator.validate(df) == expected


def test_generic_validators_apply_pydantic_validation() -> None:
    df = sample_df()
    user_df = pl.DataFrame(
        {
            'id': [1, 2],
            'name': ['Joe', 'Bob'],
            'age': [23, None],
        }
    )

    assert plv.records[list[User]].validate(df) == [
        User(name='Joe', age=23),
        User(name='Bob', age=None),
    ]
    assert plv.record[User].validate(df.head(1).select(c.name, c.age)) == User(
        name='Joe', age=23
    )
    assert plv.record_entry[tuple[int, User]].validate(user_df.head(1)) == (
        1,
        User(name='Joe', age=23),
    )
    assert plv.record_entries[list[tuple[int, User]]].validate(user_df) == [
        (1, User(name='Joe', age=23)),
        (2, User(name='Bob', age=None)),
    ]
    assert plv.rows[list[UserRow]].validate(df.select(c.name, c.age)) == [
        UserRow('Joe', 23),
        UserRow('Bob', None),
    ]
    assert plv.row_entry[tuple[int, UserRow]].validate(user_df.head(1)) == (
        1,
        UserRow('Joe', 23),
    )
    assert plv.row_entries[list[tuple[int, UserRow]]].validate(user_df) == [
        (1, UserRow('Joe', 23)),
        (2, UserRow('Bob', None)),
    ]
    assert plv.column[tuple[str, ...]].validate(df.select(c.name)) == ('Joe', 'Bob')
    assert plv.item[int].validate(df.select(c.age.max())) == 23
    assert plv.map[dict[str, int | None]].validate(df.select(c.name, c.age)) == {
        'Joe': 23,
        'Bob': None,
    }
    assert plv.keyed_record_map[dict[str, User]].validate(df.select(c.name, c.age)) == {
        'Joe': User(name='Joe', age=23),
        'Bob': User(name='Bob', age=None),
    }
    assert plv.keyed_row_map[dict[str, UserRow]].validate(df.select(c.name, c.age)) == {
        'Joe': UserRow('Joe', 23),
        'Bob': UserRow('Bob', None),
    }
    assert plv.table_record_map[tuple[tuple[str, ...], dict[int, User]]].validate(
        user_df
    ) == (
        ('id', 'name', 'age'),
        {
            1: User(name='Joe', age=23),
            2: User(name='Bob', age=None),
        },
    )
    assert plv.keys[tuple[str, ...]].validate(df.select(c.name)) == ('Joe', 'Bob')


def test_keys_preserves_input_order() -> None:
    df = pl.DataFrame({'x': [3, 1, 2]})

    assert plv.keys.validate(df) == [3, 1, 2]


def test_keys_allows_one_null() -> None:
    df = pl.DataFrame({'x': [None, 1, 2, 3]})

    assert plv.keys.validate(df) == [None, 1, 2, 3]


def test_validate_model_and_collect_model_return_root_model() -> None:
    df = sample_df().select(c.name)
    lf = df.lazy()

    validated = plv.column[tuple[str, ...]].validate_model(df)
    collected = plv.column[tuple[str, ...]].collect_model(lf)

    assert isinstance(validated, RootModel)
    assert isinstance(collected, RootModel)
    assert validated.root == ('Joe', 'Bob')
    assert collected.root == ('Joe', 'Bob')
    assert validated.model_dump() == ('Joe', 'Bob')


def test_collect_and_defer_validate_lazy_frames() -> None:
    lf = sample_lf()

    assert plv.item[int].collect(lf.select(c.age.max())) == 23

    deferred = plv.column[list[str]].defer(lf.select(c.name))

    assert isinstance(deferred, DeferredValidation)
    assert deferred.validator is plv.column[list[str]]
    assert deferred.lf.collect().to_dict(as_series=False) == {'name': ['Joe', 'Bob']}


def test_collect_async_and_collect_model_async_validate_lazy_frames() -> None:
    async def run() -> tuple[list[str], RootModel[User]]:
        lf = sample_lf()
        return (
            await plv.column[list[str]].collect_async(lf.select(c.name)),
            await plv.record[User].collect_model_async(
                lf.select(c.name, c.age).head(1)
            ),
        )

    values, model = asyncio.run(run())

    assert values == ['Joe', 'Bob']
    assert isinstance(model, RootModel)
    assert model.root == User(name='Joe', age=23)


def test_batchable_shapes_collect_batches() -> None:
    lf = pl.DataFrame(
        {
            'id': [1, 2, 3, 4, 5],
            'name': ['Joe', 'Bob', 'Mo', 'Ana', 'Kim'],
            'age': [23, None, 44, 31, 19],
        }
    ).lazy()

    assert list(plv.records.collect_batches(lf, chunk_size=2)) == [
        [
            {'id': 1, 'name': 'Joe', 'age': 23},
            {'id': 2, 'name': 'Bob', 'age': None},
        ],
        [
            {'id': 3, 'name': 'Mo', 'age': 44},
            {'id': 4, 'name': 'Ana', 'age': 31},
        ],
        [{'id': 5, 'name': 'Kim', 'age': 19}],
    ]
    assert list(plv.column.collect_batches(lf.select(c.name), chunk_size=2)) == [
        ['Joe', 'Bob'],
        ['Mo', 'Ana'],
        ['Kim'],
    ]
    assert list(plv.record_entries.collect_batches(lf, chunk_size=2)) == [
        [
            (1, {'name': 'Joe', 'age': 23}),
            (2, {'name': 'Bob', 'age': None}),
        ],
        [
            (3, {'name': 'Mo', 'age': 44}),
            (4, {'name': 'Ana', 'age': 31}),
        ],
        [(5, {'name': 'Kim', 'age': 19})],
    ]
    assert list(plv.table_rows.collect_batches(lf, chunk_size=2)) == [
        (('id', 'name', 'age'), [(1, 'Joe', 23), (2, 'Bob', None)]),
        (('id', 'name', 'age'), [(3, 'Mo', 44), (4, 'Ana', 31)]),
        (('id', 'name', 'age'), [(5, 'Kim', 19)]),
    ]


def test_batchable_shapes_collect_model_batches() -> None:
    lf = pl.DataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Joe', 'Bob', 'Mo'],
            'age': [23, None, 44],
        }
    ).lazy()

    batches = list(
        plv.record_entries[list[tuple[int, User]]].collect_model_batches(
            lf, chunk_size=2
        )
    )

    assert all(isinstance(batch, RootModel) for batch in batches)
    assert [batch.root for batch in batches] == [
        [
            (1, User(name='Joe', age=23)),
            (2, User(name='Bob', age=None)),
        ],
        [(3, User(name='Mo', age=44))],
    ]


def test_only_batch_safe_shapes_support_collect_batches() -> None:
    batchable = [
        plv.column,
        plv.records,
        plv.rows,
        plv.columns,
        plv.record_entries,
        plv.row_entries,
        plv.keyed_record_entries,
        plv.keyed_row_entries,
        plv.column_map,
        plv.table_records,
        plv.table_rows,
        plv.table_columns,
        plv.table_record_entries,
        plv.table_row_entries,
        plv.table_keyed_record_entries,
        plv.table_keyed_row_entries,
    ]
    non_batchable = [
        plv.item,
        plv.get_item,
        plv.record,
        plv.get_record,
        plv.row,
        plv.get_row,
        plv.record_entry,
        plv.get_record_entry,
        plv.row_entry,
        plv.get_row_entry,
        plv.keys,
        plv.map,
        plv.record_map,
        plv.row_map,
        plv.keyed_record_map,
        plv.keyed_row_map,
        plv.table_map,
        plv.table_record_map,
        plv.table_row_map,
        plv.table_keyed_record_map,
        plv.table_keyed_row_map,
    ]

    assert all(issubclass(shape, BaseBatchableShape) for shape in batchable)
    assert all(hasattr(shape, 'collect_batches') for shape in batchable)
    assert all(hasattr(shape, 'collect_model_batches') for shape in batchable)
    assert all(not hasattr(shape, 'collect_batches') for shape in non_batchable)
    assert all(not hasattr(shape, 'collect_model_batches') for shape in non_batchable)


def test_collect_all_variants_validate_multiple_lazy_frames() -> None:
    lf = sample_lf()

    values = plv.collect_all(
        plv.column[list[str]].defer(lf.select(c.name)),
        plv.item[int].defer(lf.select(c.age.max())),
    )
    models = plv.collect_all_models(
        plv.column[list[str]].defer(lf.select(c.name)),
        plv.record[User].defer(lf.select(c.name, c.age).head(1)),
    )

    assert values == (['Joe', 'Bob'], 23)
    assert isinstance(models[0], RootModel)
    assert isinstance(models[1], RootModel)
    assert models[0].root == ['Joe', 'Bob']
    assert models[1].root == User(name='Joe', age=23)


def test_collect_all_async_variants_validate_multiple_lazy_frames() -> None:
    async def run() -> tuple[
        tuple[list[str], int],
        tuple[RootModel[list[str]], RootModel[User]],
    ]:
        lf = sample_lf()
        return (
            await plv.collect_all_async(
                plv.column[list[str]].defer(lf.select(c.name)),
                plv.item[int].defer(lf.select(c.age.max())),
            ),
            await plv.collect_all_models_async(
                plv.column[list[str]].defer(lf.select(c.name)),
                plv.record[User].defer(lf.select(c.name, c.age).head(1)),
            ),
        )

    values, models = asyncio.run(run())

    assert values == (['Joe', 'Bob'], 23)
    assert isinstance(models[0], RootModel)
    assert isinstance(models[1], RootModel)
    assert models[0].root == ['Joe', 'Bob']
    assert models[1].root == User(name='Joe', age=23)


@pytest.mark.parametrize(
    ('validator', 'expected'),
    [
        (plv.records, []),
        (plv.rows, []),
        (plv.columns, ()),
        (plv.column_map, {}),
        (plv.table_records, ((), [])),
        (plv.table_rows, ((), [])),
        (plv.table_columns, ((), ())),
    ],
)
def test_unconstrained_shapes_allow_empty_dataframes(
    validator: type[BaseShape[object]], expected: object
) -> None:
    assert validator.validate(empty_df()) == expected


@pytest.mark.parametrize(
    ('validator', 'df', 'message'),
    [
        (plv.column, empty_df(), 'got 0 columns.'),
        (plv.keys, empty_df(), 'got 0 columns.'),
        (plv.map, sample_df().select(c.name), 'got 1 columns.'),
        (plv.map, sample_df(), 'got 3 columns.'),
        (
            plv.record_map,
            sample_df().select(c.name),
            'got 1 columns.',
        ),
        (plv.row_map, sample_df().select(c.name), 'got 1 columns.'),
        (plv.record_entry, sample_df().select(c.name).head(1), 'got 1 columns.'),
        (plv.row_entry, sample_df().select(c.name).head(1), 'got 1 columns.'),
        (plv.record_entry, sample_df(), 'got 2 rows.'),
        (plv.row_entry, sample_df(), 'got 2 rows.'),
        (plv.record_entries, sample_df().select(c.name), 'got 1 columns.'),
        (plv.row_entries, sample_df().select(c.name), 'got 1 columns.'),
        (plv.keyed_record_map, empty_df(), 'got 0 columns.'),
        (plv.keyed_row_map, empty_df(), 'got 0 columns.'),
        (plv.keyed_record_entries, empty_df(), 'got 0 columns.'),
        (plv.keyed_row_entries, empty_df(), 'got 0 columns.'),
        (plv.table_record_entries, sample_df().select(c.name), 'got 1 columns.'),
        (plv.table_row_entries, sample_df().select(c.name), 'got 1 columns.'),
        (plv.table_map, sample_df().select(c.name), 'got 1 columns.'),
        (plv.table_record_map, sample_df().select(c.name), 'got 1 columns.'),
        (plv.table_row_map, sample_df().select(c.name), 'got 1 columns.'),
        (plv.table_keyed_record_map, empty_df(), 'got 0 columns.'),
        (plv.table_keyed_row_map, empty_df(), 'got 0 columns.'),
        (plv.table_keyed_record_entries, empty_df(), 'got 0 columns.'),
        (plv.table_keyed_row_entries, empty_df(), 'got 0 columns.'),
        (plv.column, sample_df().select(c.name, c.age), 'got 2 columns.'),
        (plv.keys, sample_df().select(c.name, c.age), 'got 2 columns.'),
        (plv.record, sample_df().head(0), 'got 0 rows.'),
        (plv.record, sample_df(), 'got 2 rows.'),
        (plv.row, sample_df().head(0), 'got 0 rows.'),
        (plv.row, sample_df(), 'got 2 rows.'),
        (plv.item, empty_df(), r'got 0 (rows|columns)\.'),
        (
            plv.item,
            sample_df().filter(c.name == 'Missing').select(c.age),
            'got 0 rows.',
        ),
        (
            plv.item,
            sample_df().head(1).select(c.name, c.age),
            'got 2 columns.',
        ),
    ],
)
def test_shape_validators_raise_clear_errors_for_bad_query_structure(
    validator: type[BaseShape[object]],
    df: pl.DataFrame,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator.validate(df)


@pytest.mark.parametrize(
    ('validator', 'df', 'message'),
    [
        (
            plv.map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.record_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.row_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.keyed_record_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.keyed_row_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.table_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.table_record_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.table_row_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.table_keyed_record_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.table_keyed_row_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            'got duplicates in column, name.',
        ),
        (
            plv.keys,
            pl.DataFrame({'name': ['Joe', 'Joe']}),
            'got duplicates in column, name.',
        ),
        (
            plv.keys,
            pl.DataFrame({'name': [None, None, 'Joe']}),
            'got duplicates in column, name.',
        ),
    ],
)
def test_unique_shapes_raise_clear_errors_for_duplicate_keys(
    validator: type[BaseShape[object]],
    df: pl.DataFrame,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator.validate(df)


def test_entry_shapes_preserve_duplicate_keys() -> None:
    df = pl.DataFrame(
        {
            'team': ['red', 'red', 'blue'],
            'name': ['Joe', 'Bob', 'Mo'],
            'score': [10, 12, 9],
        }
    )

    assert plv.record_entries.validate(df) == [
        ('red', {'name': 'Joe', 'score': 10}),
        ('red', {'name': 'Bob', 'score': 12}),
        ('blue', {'name': 'Mo', 'score': 9}),
    ]
    assert plv.row_entries.validate(df) == [
        ('red', ('Joe', 10)),
        ('red', ('Bob', 12)),
        ('blue', ('Mo', 9)),
    ]
    assert plv.keyed_record_entries.validate(df) == [
        ('red', {'team': 'red', 'name': 'Joe', 'score': 10}),
        ('red', {'team': 'red', 'name': 'Bob', 'score': 12}),
        ('blue', {'team': 'blue', 'name': 'Mo', 'score': 9}),
    ]
    assert plv.keyed_row_entries.validate(df) == [
        ('red', ('red', 'Joe', 10)),
        ('red', ('red', 'Bob', 12)),
        ('blue', ('blue', 'Mo', 9)),
    ]
    assert plv.table_record_entries.validate(df) == (
        ('team', 'name', 'score'),
        [
            ('red', {'name': 'Joe', 'score': 10}),
            ('red', {'name': 'Bob', 'score': 12}),
            ('blue', {'name': 'Mo', 'score': 9}),
        ],
    )


@pytest.mark.parametrize(
    ('validator', 'df'),
    [
        (plv.get_record, sample_df().head(0)),
        (plv.get_row, sample_df().head(0)),
        (plv.get_record_entry, sample_df().head(0)),
        (plv.get_row_entry, sample_df().head(0)),
        (plv.get_item, sample_df().filter(c.name == 'Missing').select(c.age)),
    ],
)
def test_optional_single_value_validators_allow_missing_but_respect_constraints(
    validator: type[BaseShape[object]],
    df: pl.DataFrame,
) -> None:
    assert validator.validate(df) is None


@pytest.mark.parametrize(
    ('validator', 'df', 'message'),
    [
        (plv.get_record, sample_df(), 'got 2 rows.'),
        (plv.get_row, sample_df(), 'got 2 rows.'),
        (plv.get_record_entry, sample_df(), 'got 2 rows.'),
        (plv.get_row_entry, sample_df(), 'got 2 rows.'),
        (plv.get_record_entry, sample_df().select(c.name).head(0), 'got 1 columns.'),
        (plv.get_row_entry, sample_df().select(c.name).head(0), 'got 1 columns.'),
        (plv.get_item, empty_df(), 'got 0 columns.'),
        (
            plv.get_item,
            sample_df().head(1).select(c.name, c.age),
            'got 2 columns.',
        ),
        (
            plv.get_item,
            sample_df().select(c.age),
            'got 2 rows.',
        ),
    ],
)
def test_optional_single_value_validators_still_enforce_shape_constraints(
    validator: type[BaseShape[object]],
    df: pl.DataFrame,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator.validate(df)


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    [
        ({'max_h': 1}, '`FakeValidator` got 2 rows.'),
        ({'max_w': 1}, '`FakeValidator` got 2 columns.'),
        ({'min_h': 3}, '`FakeValidator` got 2 rows.'),
        ({'min_w': 3}, '`FakeValidator` got 2 columns.'),
    ],
)
def test_raise_if_bad_query_structure_covers_min_and_max_bounds(
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _shape._raise_if_bad_df_dimensions(
            type('FakeValidator', (), {}),
            pl.DataFrame({'a': [1, 2], 'b': [3, 4]}),
            **kwargs,
        )


def test_base_validator_requires_dataframe_conversion_override() -> None:
    class MissingImplementation(BaseShape[int]):
        pass

    MissingImplementation._original_shape_cls = MissingImplementation

    with pytest.raises(NotImplementedError):
        MissingImplementation.validate(pl.DataFrame({'value': [1]}))


def test_unspecialized_validator_skips_pydantic_validation() -> None:
    class fake[T: Any = int](BaseShape[T]):
        @classmethod
        def _dataframe_to_python(cls, _: pl.DataFrame, /) -> Any:
            return 'not an int'

    fake._original_shape_cls = fake
    df = pl.DataFrame({'value': [1]})

    assert fake.validate(df) == 'not an int'
    with pytest.raises(ValidationError):
        fake[int].validate(df)


def test_validate_module_exports_expected_public_api() -> None:
    assert plv.__all__ == [
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
        'collect_all',
        'collect_all_async',
        'collect_all_models',
        'collect_all_models_async',
    ]
