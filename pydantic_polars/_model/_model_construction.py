"""
Internal utilities for constructing the Polars BaseModel
"""

from typing import Any, NoReturn
import polars as pl
from pydantic.fields import FieldInfo
from dataclasses import dataclass
import polars as pl
from polars.datatypes import is_polars_dtype
from ._metadata import (
    ConstraintMetadata,
    ENUM,
    CAST,
    NOT_NULL,
    UNIQUE,
    PRIMARY_KEY,
    CHECK,
    DEFAULT,
    PIPE,
    FillNullMetadata,
    PipeMetadata,
)

type Frame = pl.DataFrame | pl.LazyFrame


@dataclass(frozen=True, slots=True)
class BuiltPolarsModel:
    """
    Internal structure that stores the final representation of
    the Polars model built from a BasePolarsModel's fields.
    """

    constraints: tuple[pl.Expr, ...]
    transforms: tuple[pl.Expr, ...]


_DEBUG_CONFIG = pl.Config(
    tbl_hide_column_data_types=True,
    tbl_hide_dataframe_shape=True,
    tbl_hide_dtype_separator=True,
    tbl_cols=50,
    tbl_rows=50,
    tbl_formatting='UTF8_HORIZONTAL_ONLY',
    # This config will be used as a context manager.
    apply_on_context_enter=True,
)
"""Internally used for outputs when an exception is raised."""


def raise_pretty_constraint_error_report(
    fr: Frame, constraints: tuple[pl.Expr, ...]
) -> NoReturn:
    """
    Raises a pretty error report for the given failed constraints.
    """
    lf_valid = fr.lazy().select(*constraints)
    df = lf_valid.collect(engine='streaming').transpose(include_header=True)
    df.columns = ['column', 'result']
    c = pl.col
    df = df.select(
        Column=c.column.str.split('___').list.first(),
        Check=c.column.str.split('___').list.last(),
        Result=c.result.replace_strict({True: '✅ PASS', False: '❌ FAIL'}),
    )
    with _DEBUG_CONFIG:
        raise ValueError(f'Failed one or more constraints\n{df}')


def build_polars_model(
    fields: dict[str, FieldInfo],
) -> tuple[pl.Schema, BuiltPolarsModel]:
    """
    Build a Polars model from Pydantic field definitions
    """
    schema = pl.Schema(
        {name: _get_field_pl_dtype(name, field) for name, field in fields.items()}
    )

    # Catch common mistakes
    for name, field in fields.items():
        for m in field.metadata:
            if m is CHECK or m is DEFAULT or m is PIPE:
                raise ValueError(
                    f'Field {name} has invalid metadata {m}. '
                    f'Did you mean to call {m.__name__}(...)?'
                )

    # For each column, using `col(name)` as the root, build a
    # single expression that chains all transformations in the order
    # they were declared. The expression needs to be finished with
    # an alias() that points back to the original column name,
    # so there are no issues.
    transforms: list[pl.Expr] = []

    for name, field in fields.items():
        entrypoint_expr = pl.col(name)
        expr = entrypoint_expr

        for meta in field.metadata:
            if meta is CAST:
                expr = expr.cast(schema[name])
            elif isinstance(meta, FillNullMetadata):
                expr = expr.fill_null(meta.expr)
            elif isinstance(meta, PipeMetadata):
                expr = meta.func(expr)
            else:
                continue

        if expr is not entrypoint_expr:
            transforms.append(expr.alias(name))

    # Unlike transforms, constraints are flat - a single list of all the
    # constraints applied.
    constraints: list[pl.Expr] = []
    primary_key_names = [
        name
        for name, field in fields.items()
        if any(m is PRIMARY_KEY for m in field.metadata)
    ]
    if primary_key_names:
        if len(primary_key_names) == 1:
            col_expr = pl.col(primary_key_names[0])
        else:
            col_expr = pl.struct(*[pl.col(c) for c in primary_key_names])

        # Nulls are first-class in polars, and can be joined on (if nulls_equal=True).
        # So we will only enforce a uniqueness constraint for primary keys.
        column_name = f'PRIMARY KEY ({", ".join(primary_key_names)})'
        constr_name = 'UNIQUE'
        constraints.append(
            col_expr.is_unique().all().alias(f'{column_name}___{constr_name}')
        )

    for name, field in fields.items():
        custom_ex_counter = 0

        for meta in field.metadata:
            if meta is PRIMARY_KEY:
                continue

            elif meta is NOT_NULL:
                con_expr = pl.col(name).is_not_null()
                con_name = 'NOT NULL'

            elif meta is UNIQUE:
                if name in primary_key_names:
                    raise ValueError(f'{name} is PRIMARY KEY. Already UNIQUE.')
                con_expr = pl.col(name).is_unique()
                con_name = 'UNIQUE'

            elif isinstance(meta, (ConstraintMetadata, pl.Expr)):
                if isinstance(meta, pl.Expr):
                    con_expr = meta
                else:
                    con_expr = meta.func(pl.col(name))

                expr_str = str(con_expr).strip().replace('"', '').strip('[]')
                if len(expr_str) <= 40:
                    con_name = expr_str

                elif (
                    isinstance(meta, ConstraintMetadata)
                    and '<lambda' not in meta.func.__name__
                ):
                    con_name = f'CHECK({meta.func.__name__})'

                else:
                    con_name = f'EXPR {custom_ex_counter}'
                custom_ex_counter += 1
            else:
                continue

            constraints.append(
                # We wrap every constraint in cast->bool|null, agg-all, null->False,
                # to ensure it's always a single boolean result. This sounds confusing
                # at first, but...think about it. It's the most reasonable way to handle it.
                #   1. This wrapper has no effect on constraints that already evaluate
                #      to a single boolean. So it's "safe" in that sense.
                #   2. It "automatically" handles aggregating expressions that haven't
                #      been aggregated yet. Given `c.foo > c.bar`, the only reasonable
                #      interpretation is they want an "all" constraint. No ambiguity there.
                #   3. If an expression is malformed in some other way, like producing
                #      a non-boolean type, we want to fail loudly. But safely casting
                #      to bool|null here and filling nulls with False ensures the failure
                #      happens in the report, not during query execution.
                con_expr.cast(bool, strict=False)
                .all(ignore_nulls=False)
                .fill_null(False)
                .alias(f'{name}___{con_name}')
            )

    model = BuiltPolarsModel(
        constraints=tuple(constraints),
        transforms=tuple(transforms),
    )
    return schema, model


from datetime import date, time, datetime, timedelta
from decimal import Decimal as PyDecimal

_valid_schema_python_types = (
    str,
    bool,
    int,
    float,
    date,
    time,
    datetime,
    timedelta,
    bytes,
    PyDecimal,
)


def _is_schema_value_type(obj: Any):
    """
    Determine if an object is a valid Polars schema-compatible type, including
    Polars-specific dtypes and supported Python types.
    """
    if is_polars_dtype(obj):
        return True
    if isinstance(obj, type):
        for t in _valid_schema_python_types:
            if obj is t:
                return True
    return False


def _get_dtype_from_nested_model_tp(field_name: str, obj: Any) -> pl.Struct | None:
    """
    Creates the Polars `Struct` dtype from a nested `BasePolars` type annotation.
    """
    from ._basemodel import BasePolars

    if isinstance(obj, type) and issubclass(obj, BasePolars):
        raise RuntimeError(
            f'Nested BasePolarsModel was almost fully implemented...but then I '
            f'realized last-minute that no...it was not. Not even close. '
            "Totally forgot about the child model's constraints and tranforms. "
            'We need to parse deep, and only support a subset of features. Most '
            'likely will disallow custom constraints and PIPEs within '
            'child models. But I think CAST, UNIQUE, NOT_NULL, and DEFAULT are '
            'totally fair game. Can someone please figure this out? Thanks.'
        )
        if obj.pl_enums_deferred:
            raise ValueError(
                f'Field {field_name} references BasePolarsModel type {obj.__name__} '
                f'which has unresolved EnumDeferred fields. Please resolve them '
                f"before declaring this model. We haven't yet figured out how to "
                f'notify parent models when a child model resolves its enums. '
                f'So this class would otherwise have a stale schema when you '
                f"resolved {obj.__name__}'s enums later."
            )
        return pl.Struct(obj.pl_schema)
    return None


def _get_field_pl_dtype(name: str, field: FieldInfo) -> Any:
    """
    Gets the Polars data type (dtype) of a field based on metadata and annotations.
    """
    metadata = field.metadata
    dtypes = [m for m in metadata if _is_schema_value_type(m)]
    nested_dtypes = [_get_dtype_from_nested_model_tp(name, m) for m in metadata]
    dtypes.extend([t for t in nested_dtypes if t is not None])

    if dtypes:
        if len(dtypes) > 1:
            raise ValueError(f'Field {name} cant have more than one Polars dtype')
        if any(m is ENUM for m in metadata):
            raise ValueError(
                f'Field {name} cant have both a Polars dtype and be EnumDeferred'
            )
        return dtypes[0]

    enums_deferred = [m for m in metadata if m is ENUM]
    if enums_deferred:
        if len(enums_deferred) > 1:
            raise ValueError(
                f'Field {name} cant have more than one EnumDeferred marker'
            )
        return str

    annotation = field.annotation
    if _is_schema_value_type(annotation):
        return annotation

    dtype = _get_dtype_from_nested_model_tp(name, annotation)
    if dtype is not None:
        return dtype

    raise ValueError(
        f"Couldn't locate a Polars dtype in field {name}'s metadata, "
        f'and unable to use its annotation, {annotation}, either. '
        f'Expected either: 1.) A Polars dtype, or 2.) Any of the following Python '
        f'builtins: {", ".join([t.__name__ for t in _valid_schema_python_types])}. '
        f'To prevent this error in the future, consider always defining '
        f'a valid Polars dtype in the field Annotation metadata, so your '
        f"Python type hint won't affect this Polars model initialization step."
    )
