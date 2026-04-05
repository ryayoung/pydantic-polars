"""
Metadata you can attach to fields in a BasePolarsModel
"""
from typing import Callable, Any
from dataclasses import dataclass
import polars as pl
from polars import Expr
from typing_extensions import Sentinel

__all__ = [
    'NOT_NULL',
    'UNIQUE',
    'PRIMARY_KEY',
    'CHECK',
    'CAST',
    'ENUM',
    'DEFAULT',
    'PIPE',
]

type Pipe[T] = Callable[[T], T]


NOT_NULL = Sentinel('NOT_NULL')
"""
Enforce a not-null constraint on a column.
"""

UNIQUE = Sentinel('UNIQUE')
"""
Enforce that all values in a column are unique.
"""

PRIMARY_KEY = Sentinel('PRIMARY_KEY')
"""
Declare a primary key.
Using this on multiple columns will create a composite primary key.

PRIMARY_KEY behaves just like UNIQUE, except that when it's present
in multiple columns, the uniqueness constraint is applied to the
concatenation of those columns, instead of each one individually.

PRIMARY_KEY does NOT enforce a not-null constraint. Nulls are
first-class in Polars, and you can join on them. The UNIQUE
constraint takes this into account, and will ensure that if
nulls are present, there's only one of them.
"""


@dataclass(frozen=True, slots=True)
class ConstraintMetadata:
    """
    Container for a custom constraint (e.g. CHECK(...))
    that was given to `typing.Annotated`, and placed into a field's
    FieldInfo.metadata list by Pydantic during model creation.
    """

    func: Pipe[Expr]


def CHECK(chk: Pipe[Expr]) -> ConstraintMetadata:
    """
    Takes a callback to declare a custom constraint. If a
    callback is give, it will be called with an expression pointing to
    whatever column is being validated.

    The check:
    - Should evaluate "truthy" to pass.
    - May evaluate to multiple columns.
    """

    if isinstance(chk, pl.Expr):
        return ConstraintMetadata(lambda _: chk)
    return ConstraintMetadata(chk)


CAST = Sentinel('CAST')
"""
Declare that a column should be cast to its schema dtype.

`CAST` is a **transformation**, just like `DEFAULT()` and `PIPE()`.
It will be applied in the order it appears in the field's metadata,
with respect to other transformations. All transformations are treated
the same, and chained together.
"""

ENUM = Sentinel('ENUM')
"""
Mark a field whose Polars type will be assigned to an Enum at
some later time after class construction,
using `BasePolarsModel.pl_resolve_enums()`.
"""


@dataclass(frozen=True, slots=True)
class FillNullMetadata:
    """
    Container for a fill-nulls transformation (e.g. DEFAULT(...))
    that was given to `typing.Annotated`, and placed into a field's
    FieldInfo.metadata list by Pydantic during model creation.
    """

    expr: pl.Expr


def DEFAULT(val: pl.Expr | Any) -> FillNullMetadata:
    """
    Declare a default expression to use as the replacement for null values.

    If a non-Expr is given, it will be wrapped in `pl.lit()`.

    `DEFAULT()` is a **transformation**, not a constraint. It will be
    applied in the order it appears in the field's metadata, with respect
    to other transformations (e.g. CAST, PIPE(), etc.)
    """
    return FillNullMetadata(val if isinstance(val, pl.Expr) else pl.lit(val))


@dataclass(frozen=True, slots=True)
class PipeMetadata:
    """
    Container for a custom pipe transformation (e.g. PIPE(...))
    that was given to `typing.Annotated`, and placed into a field's
    FieldInfo.metadata list by Pydantic during model creation.
    """

    func: Pipe[Expr]


def PIPE(func: Pipe[Expr]) -> PipeMetadata:
    """
    Takes a callback to declare a custom transformation.

    Multiple can be applied to the same field.

    Transformations are applied in the order they appear in the field's
    metadata, with respect to other transformations (e.g. CAST, DEFAULT, etc.)
    All transformations are treated the same, and chained together.
    """

    return PipeMetadata(func)
