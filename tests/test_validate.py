# pyright: reportPrivateUsage=false

import asyncio
from typing import NamedTuple

import polars as pl
import pytest
from pydantic import BaseModel, RootModel
from polars import col as c

from pydantic_polars import validate as plv
from pydantic_polars._validate import _frame
from pydantic_polars._validate._base_validator import BaseValidator, DeferredValidation


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
            plv.column_entries,
            sample_df(),
            (
                ('name', ['Joe', 'Bob']),
                ('age', [23, None]),
                ('active', [True, False]),
            ),
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
            plv.keyed_records,
            sample_df(),
            {
                'Joe': {'name': 'Joe', 'age': 23, 'active': True},
                'Bob': {'name': 'Bob', 'age': None, 'active': False},
            },
        ),
        (
            plv.keyed_rows,
            sample_df(),
            {'Joe': ('Joe', 23, True), 'Bob': ('Bob', None, False)},
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
            plv.column_entry,
            sample_df().select(c.name),
            ('name', ['Joe', 'Bob']),
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
            plv.item,
            sample_df().select(c.age.max()),
            23,
        ),
        (
            plv.get_item,
            sample_df().filter(c.name == 'Missing').select(c.age),
            None,
        ),
    ],
)
def test_default_shape_validators_return_expected_python_objects(
    validator: type[BaseValidator[object]],
    df: pl.DataFrame,
    expected: object,
) -> None:
    assert validator.validate(df) == expected


def test_generic_validators_apply_pydantic_validation() -> None:
    df = sample_df()

    assert plv.records[list[User]].validate(df) == [
        User(name='Joe', age=23),
        User(name='Bob', age=None),
    ]
    assert plv.record[User].validate(df.head(1).select(c.name, c.age)) == User(
        name='Joe', age=23
    )
    assert plv.rows[list[UserRow]].validate(df.select(c.name, c.age)) == [
        UserRow('Joe', 23),
        UserRow('Bob', None),
    ]
    assert plv.column[tuple[str, ...]].validate(df.select(c.name)) == ('Joe', 'Bob')
    assert plv.item[int].validate(df.select(c.age.max())) == 23
    assert plv.map[dict[str, int | None]].validate(df.select(c.name, c.age)) == {
        'Joe': 23,
        'Bob': None,
    }
    assert plv.keyed_records[dict[str, User]].validate(df.select(c.name, c.age)) == {
        'Joe': User(name='Joe', age=23),
        'Bob': User(name='Bob', age=None),
    }
    assert plv.keyed_rows[dict[str, UserRow]].validate(df.select(c.name, c.age)) == {
        'Joe': UserRow('Joe', 23),
        'Bob': UserRow('Bob', None),
    }
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
        (plv.column_entries, ()),
        (plv.column_map, {}),
        (plv.table_records, ((), [])),
        (plv.table_rows, ((), [])),
        (plv.table_columns, ((), ())),
    ],
)
def test_unconstrained_shapes_allow_empty_dataframes(
    validator: type[BaseValidator[object]], expected: object
) -> None:
    assert validator.validate(empty_df()) == expected


@pytest.mark.parametrize(
    ('validator', 'df', 'message'),
    [
        (plv.column, empty_df(), '`column` got 0 columns.'),
        (plv.keys, empty_df(), '`keys` got 0 columns.'),
        (plv.column_entry, empty_df(), '`column_entry` got 0 columns.'),
        (plv.map, sample_df().select(c.name), '`map` got 1 columns.'),
        (plv.map, sample_df(), '`map` got 3 columns.'),
        (
            plv.record_map,
            sample_df().select(c.name),
            '`record_map` got 1 columns.',
        ),
        (plv.row_map, sample_df().select(c.name), '`row_map` got 1 columns.'),
        (plv.keyed_records, empty_df(), '`keyed_records` got 0 columns.'),
        (plv.keyed_rows, empty_df(), '`keyed_rows` got 0 columns.'),
        (plv.column, sample_df().select(c.name, c.age), '`column` got 2 columns.'),
        (plv.keys, sample_df().select(c.name, c.age), '`keys` got 2 columns.'),
        (plv.column_entry, sample_df(), '`column_entry` got 3 columns.'),
        (plv.record, sample_df().head(0), '`record` got 0 rows.'),
        (plv.record, sample_df(), '`record` got 2 rows.'),
        (plv.row, sample_df().head(0), '`row` got 0 rows.'),
        (plv.row, sample_df(), '`row` got 2 rows.'),
        (plv.item, empty_df(), r'`item` got 0 (rows|columns)\.'),
        (
            plv.item,
            sample_df().filter(c.name == 'Missing').select(c.age),
            '`item` got 0 rows.',
        ),
        (
            plv.item,
            sample_df().head(1).select(c.name, c.age),
            '`item` got 2 columns.',
        ),
    ],
)
def test_shape_validators_raise_clear_errors_for_bad_query_structure(
    validator: type[BaseValidator[object]],
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
            '`map` got duplicates in column, name.',
        ),
        (
            plv.record_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            '`record_map` got duplicates in column, name.',
        ),
        (
            plv.row_map,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            '`row_map` got duplicates in column, name.',
        ),
        (
            plv.keyed_records,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            '`keyed_records` got duplicates in column, name.',
        ),
        (
            plv.keyed_rows,
            pl.DataFrame({'name': ['Joe', 'Joe'], 'age': [23, 24]}),
            '`keyed_rows` got duplicates in column, name.',
        ),
        (
            plv.keys,
            pl.DataFrame({'name': ['Joe', 'Joe']}),
            '`keys` got duplicates in column, name.',
        ),
        (
            plv.keys,
            pl.DataFrame({'name': [None, None, 'Joe']}),
            '`keys` got duplicates in column, name.',
        ),
    ],
)
def test_unique_shapes_raise_clear_errors_for_duplicate_keys(
    validator: type[BaseValidator[object]],
    df: pl.DataFrame,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator.validate(df)


@pytest.mark.parametrize(
    ('validator', 'df'),
    [
        (plv.get_record, sample_df().head(0)),
        (plv.get_row, sample_df().head(0)),
        (plv.get_item, sample_df().filter(c.name == 'Missing').select(c.age)),
    ],
)
def test_optional_single_value_validators_allow_missing_but_respect_constraints(
    validator: type[BaseValidator[object]],
    df: pl.DataFrame,
) -> None:
    assert validator.validate(df) is None


@pytest.mark.parametrize(
    ('validator', 'df', 'message'),
    [
        (plv.get_record, sample_df(), '`get_record` got 2 rows.'),
        (plv.get_row, sample_df(), '`get_row` got 2 rows.'),
        (plv.get_item, empty_df(), '`get_item` got 0 columns.'),
        (
            plv.get_item,
            sample_df().head(1).select(c.name, c.age),
            '`get_item` got 2 columns.',
        ),
        (
            plv.get_item,
            sample_df().select(c.age),
            '`get_item` got 2 rows.',
        ),
    ],
)
def test_optional_single_value_validators_still_enforce_shape_constraints(
    validator: type[BaseValidator[object]],
    df: pl.DataFrame,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validator.validate(df)


@pytest.mark.parametrize(
    ('kwargs', 'message'),
    [
        ({'max_height': 1}, '`FakeValidator` got 2 rows.'),
        ({'max_width': 1}, '`FakeValidator` got 2 columns.'),
        ({'min_height': 3}, '`FakeValidator` got 2 rows.'),
        ({'min_width': 3}, '`FakeValidator` got 2 columns.'),
    ],
)
def test_raise_if_bad_query_structure_covers_min_and_max_bounds(
    kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _frame._raise_if_bad_query_structure(
            type('FakeValidator', (), {}),
            pl.DataFrame({'a': [1, 2], 'b': [3, 4]}),
            **kwargs,
        )


def test_base_validator_requires_dataframe_conversion_override() -> None:
    class MissingImplementation(BaseValidator[int]):
        pass

    with pytest.raises(NotImplementedError):
        MissingImplementation.validate(pl.DataFrame({'value': [1]}))


def test_validate_module_exports_expected_public_api() -> None:
    assert plv.__all__ == [
        'records',
        'rows',
        'columns',
        'record_map',
        'row_map',
        'keyed_records',
        'keyed_rows',
        'column_map',
        'column_entries',
        'table_records',
        'table_rows',
        'table_columns',
        'column',
        'keys',
        'column_entry',
        'map',
        'record',
        'get_record',
        'row',
        'get_row',
        'item',
        'get_item',
        'collect_all',
        'collect_all_async',
        'collect_all_models',
        'collect_all_models_async',
    ]
