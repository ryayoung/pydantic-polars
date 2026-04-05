from typing import Any, ClassVar, Mapping, cast
from pydantic import BaseModel
import polars as pl
from ._model_construction import (
    raise_pretty_constraint_error_report,
    build_polars_model,
    BuiltPolarsModel,
)
from ._metadata import ENUM

__all__ = ['BasePolars']

type Frame = pl.DataFrame | pl.LazyFrame


class BasePolars(BaseModel):
    """
    A Pydantic BaseModel that behaves just like a normal BaseModel, while also serving
    as a place for you to declare Polars types, constraints, checks, primary keys,
    and defaults for filling nulls, and use as an API to efficiently apply them.

    Pydantic behavior and Polars behavior are separate. There are no inferred/implicit
    assumptions. Configure this model in the same ways you would configure any Pydantic
    model, without affecting how Polars frames are handled. And vice versa.

    ## Polars-specific API

    Class-level utilities efficiently apply declared Polars constraints/transforms
    in a single pass that maximizes parallelism/SIMD on the CPU, and is fully compatible
    with larger-than-memory datasets. Start typing `.pl_` on this class and use
    auto-complete to browse the API.

    ## Polars-specific Declarations

    The Polars API is configured with metadata passed to `typing.Annotated`.

    ```python
    from typing import Annotated
    import polars as pl

    class MyModel(BasePolars):
        # Use UInt8 instead of Int64, in Polars schema.
        my_id: Annotated[int, pl.UInt8]
        # Pass `str` to Polars schema when generating it.
        code: str

    MyModel.pl_schema()  # pl.Schema({'my_id': UInt8, 'code': String})
    ```

    With `typing.Annotated`, you can attach as many things as you like, in any order.
    Pydantic will pick up Pydantic stuff. BasePolars will pick up the Polars stuff.

    ```python
    from pydantic import Field, BeforeValidator
    from polars import UInt8, col as c

    class MyModel(BasePolars):
        # 'PRIMARY_KEY' is like 'UNIQUE', but when declared on multiple columns
        # creates a composite `struct(col1, col2, ...).is_unique().all()`
        # constraint.
        my_id: Annotated[int, pl.UInt8, PRIMARY_KEY]

        # Let's declare a mix of Polars stuff and Pydantic stuff.
        code: Annotated[
            str,                           # type hint seen by Pydantic. No Polars
                                           # dtype given, so this is passed to Schema()
            UNIQUE,                        # `.is_unique().all()` check
            NOT_NULL,                      # `.is_not_null().all()` check
            CAST,                          # `.cast(MyModel.pl_schema['code'])`
            PIPE(lambda e: e.replace('', None)),  # Custom transform
            DEFAULT(0),                    # `.fill_null(lit(0))` transform
            (c.my_id.cast(str) != c.code).all(), # Make sure code != my_id
            c.code.is_not_null().all(),    # Redundant, but valid check.
            {'message': 'hello world'},    # some bullshit that won't do anything
            Field(default=0),              # Pydantic: Field() with default
            BeforeValidator(lambda v: v.upper()),  # Pydantic: make uppercase
        ]
    ```

    The 2 polars Exprs in the above code were shorthand for `CHECK(...)` constraints.
    To make a dynamic, reusable check that doesn't depend on column names, pass
    a callback that takes an expr pointing to whatever column is being validated.

    So, instead of hardcoding `(c.code != '').all()`, you could...

    ```python
    check_non_empty_string = CHECK(lambda expr: (expr != '').all())

    class MyModel(BasePolars):
        code: Annotated[str, check_non_empty_string]
    ```
    """

    pl_schema: ClassVar[pl.Schema]
    """Polars Schema for this model."""

    pl_columns: ClassVar[tuple[str, ...]]
    """Names of all columns in this model's Polars schema."""

    pl_enums_deferred: ClassVar[tuple[str, ...]]
    """Names of fields marked EnumDeferred that have not yet been resolved."""

    @classmethod
    def pl_match[T: Frame](cls, fr: T, /) -> T:
        """
        Fully match the given frame to this model. This includes transformations
        and constraints. Constraint-enforcement requires a small collect() to verify
        that all checks passed.

        1. Transformations are applied: e.g. CAST, DEFAULT, PIPE, etc.
           - A given field's transforms are chained in the order they were defined,
             into a single Expr that's aliased back to the field name.
           - We add a step to the query plan that applies the chained transformations
             for all columns in parallel.
        2. `.match_to_schema()` is called to validate the schema, and arrange columns.
        3. Constraints are enforced by collecting a single boolean with the horizontal
           truthiness of all checks in parallel.

        To apply transformations without collecting constraints, use `pl_transform()`.
        To collect constraints without modifying the data, use `pl_collect_constraints()`.
        """
        fr = cls.pl_transform(fr)
        return cls.pl_collect_constraints(fr)

    @classmethod
    def pl_transform[T: Frame](cls, fr: T, /) -> T:
        """
        Apply transformations and match schema, but don't collect constraints.
        This method is lazy and doesn't collect any results.

        1. Transformations are applied: e.g. CAST, DEFAULT, PIPE, etc.
           - A given field's transforms are chained in the order they were defined,
             into a single Expr that's aliased back to the field name.
           - We add a step to the query plan that applies the chained transformations
             for all columns in parallel.

        To check constraints, use `pl_collect_constraints()`.
        To both transform and check constraints, use `pl_match()`.
        """
        cls._validate_no_deferred_enums()
        result = fr.select(cls.pl_columns).with_columns(*cls._pl_model.transforms)
        return cast(T, result)

    @classmethod
    def pl_collect_constraints[T: Frame](cls, fr: T, /) -> T:
        """
        Check this model's constraints against the given frame, and match to schema.
        This requires collecting a single boolean value, to check if all constraints passed.

        We aggregate the constraints horizontally in parallel, to get a single
        boolean that will be True if every expression was truthy.
        """
        cls._validate_no_deferred_enums()
        fr = cast(T, fr.match_to_schema(cls.pl_schema))

        if not (constraints := cls._pl_model.constraints):
            return fr
        try:
            lf_scalar = fr.lazy().select(pl.all_horizontal(*constraints))
            all_valid: bool = lf_scalar.collect(engine='streaming').item()
        except Exception as e:
            raise ValueError(f'Error while querying constraint expr: {e}\n') from e

        assert isinstance(all_valid, bool), f'Should be impossible. {all_valid}'
        if all_valid:
            return fr
        raise_pretty_constraint_error_report(fr, constraints)

    @classmethod
    def pl_resolve_enums(
        cls, mapping: Mapping[str, pl.Enum] | None = None, /, **kwargs: pl.Enum
    ) -> None:
        """
        Resolve all `EnumDeferred` fields by providing a mapping of field names
        to `pl.Enum` instances.
        The given mapping may only specify fields which are already deferred.
        It must specify all of them. No more, no less.
        """
        if not cls.pl_enums_deferred:
            raise ValueError('There are no EnumDeferred fields to resolve.')

        mapping = {**(mapping or {}), **kwargs}

        if set(mapping.keys()) != set(cls.pl_enums_deferred):
            raise ValueError(
                f'Must provide Enums for exactly the fields which are EnumDeferred. '
                f'Expected {cls.pl_enums_deferred}, got {list(mapping.keys())}.'
            )

        # Modify Pydantic model_fields.metadata list in-place to swap the
        # EnumDeferred markers with the actual Enums.
        # Then rebuild the Polars model.

        for field_name in cls.pl_enums_deferred:
            field = cls.model_fields[field_name]
            for i, meta in enumerate(field.metadata):
                if meta is ENUM:
                    field.metadata[i] = mapping[field_name]

        cls.pl_rebuild()

    @classmethod
    def pl_resolve_enums_from_frame[T: Frame](cls, fr: T, /) -> T:
        """
        Convenient wrapper around `.pl_resolve_enums()` that creates
        the enums automatically, from a given frame.

        The frame's columns must include at least all of the deferred enum
        fields, and those columns must be string-like.
        """

        if not cls.pl_enums_deferred:
            return fr

        schema = fr.lazy().collect_schema()
        found_types: dict[str, pl.DataType] = {}
        for name in cls.pl_enums_deferred:
            if name not in schema:
                raise ValueError(
                    f'Column {name} not found in given frame. '
                    f'Cannot resolve EnumDeferred fields.'
                )
            found_types[name] = schema[name]

        if all(isinstance(v, pl.Enum) for v in found_types.values()):
            cls.pl_resolve_enums(cast(Mapping[str, pl.Enum], found_types))
            return fr

        # For each deferred column, get the unique non-null values, sorted.
        # To run all columns in a single pass, the trick is to finish with
        # an .implode() so the result is a single row where each value is a list.
        # Without implode(), this wouldn't be possible because each column
        # has a different number of uniques.
        fr_enums = fr.lazy().select(
            pl.col(*cls.pl_enums_deferred).unique().drop_nulls().sort().implode()
        )
        df_enums = fr_enums.collect(engine='streaming')
        mapping: dict[str, list[str]] = df_enums.row(0, named=True)
        enum_mapping: dict[str, pl.Enum] = {k: pl.Enum(v) for k, v in mapping.items()}
        cls.pl_resolve_enums(enum_mapping)
        return fr

    @classmethod
    def pl_rebuild(cls) -> None:
        """
        Rebuild the internal Polars model from the current model fields.

        This runs once during class creation, but can be called again
        if the model fields have been modified dynamically.
        """
        cls.pl_schema, cls._pl_model = build_polars_model(cls.model_fields)
        cls.pl_columns = tuple(cls.pl_schema.names())
        cls.pl_enums_deferred = tuple(
            name
            for name, field in cls.model_fields.items()
            if any(m is ENUM for m in field.metadata)
        )

    @classmethod
    def _validate_no_deferred_enums(cls):
        """Raises if there are any unresolved EnumDeferred fields."""
        if cls.pl_enums_deferred:
            raise ValueError(
                f'Fields {cls.pl_enums_deferred} are marked EnumDeferred, '
                'but have not yet been resolved. Please resolve them by calling '
                '.pl_resolve_enums()'
            )

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        # Runs on class creation.
        super().__pydantic_init_subclass__(**kwargs)
        cls.pl_rebuild()

    _pl_model: ClassVar[BuiltPolarsModel]
