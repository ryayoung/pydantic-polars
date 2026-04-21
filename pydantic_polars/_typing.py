from typing import Literal, TypedDict, Unpack, TYPE_CHECKING
from polars import (
    DataFrame,
    LazyFrame,
    Series,
    Expr,
    QueryOptFlags,
    GPUEngine,
    DataType,
)
from polars.datatypes import DataTypeClass

__all__ = [
    'PolarsDataType',
    'PolarsType',
    'ConcatMethod',
    'EngineType',
    'CollectKwargs',
    'CollectAllKwargs',
    'CollectAsyncKwargs',
    'CollectAllAsyncKwargs',
    'CollectBatchesKwargs',
]

# #######################################################################################
# COPIED internal type aliases from polars that aren't publicly exported.
#   - Most are from `polars._typing`.
# #######################################################################################

type PolarsDataType = DataType | DataTypeClass
type PolarsType = DataFrame | LazyFrame | Series | Expr
POLARS_TYPES = (DataFrame, LazyFrame, Series, Expr)

type ConcatMethod = Literal[
    'vertical',
    'vertical_relaxed',
    'diagonal',
    'diagonal_relaxed',
    'horizontal',
    'align',
    'align_full',
    'align_inner',
    'align_left',
    'align_right',
]
type EngineTypeSlug = Literal['auto', 'in-memory', 'streaming', 'gpu']
type EngineType = EngineTypeSlug | GPUEngine


class CollectKwargs(TypedDict, total=False):
    """
    `LazyFrame.collect()` parameters.

    Excludes params that affect the return type (e.g. `background: bool`).
    You'll need to specify those manually and write the overload signatures yourself.
    """

    type_coercion: bool
    predicate_pushdown: bool
    projection_pushdown: bool
    simplify_expression: bool
    slice_pushdown: bool
    comm_subplan_elim: bool
    comm_subexpr_elim: bool
    cluster_with_columns: bool
    collapse_joins: bool
    no_optimization: bool
    engine: EngineType
    optimizations: QueryOptFlags


CollectAllKwargs = CollectKwargs
"""`pl.collect_all()` parameters."""


class CollectAsyncKwargs(TypedDict, total=False):
    """
    `LazyFrame.collect_async()` parameters.

    Excludes params that affect the return type (e.g. `gevent: bool`).
    You'll need to specify those manually and write the overload signatures yourself.
    """

    engine: EngineType
    optimizations: QueryOptFlags


CollectAllAsyncKwargs = CollectAsyncKwargs
"""`pl.collect_all_async()` parameters."""


class CollectBatchesKwargs(TypedDict, total=False):
    """
    `LazyFrame.collect_batches()` parameters.
    """

    chunk_size: int | None
    maintain_order: bool
    lazy: bool
    engine: EngineType
    optimizations: QueryOptFlags


# STATIC TEST (raise pyright errors if above param types are incorrect)
if TYPE_CHECKING:
    from typing import Unpack
    from polars import collect_all, collect_all_async

    def _(**kwargs: Unpack[CollectKwargs]):
        LazyFrame().collect(**kwargs)

    def _(**kwargs: Unpack[CollectAllKwargs]):
        collect_all([LazyFrame()], **kwargs)

    def _(**kwargs: Unpack[CollectAsyncKwargs]):
        LazyFrame().collect_async(**kwargs)

    def _(**kwargs: Unpack[CollectAllAsyncKwargs]):
        collect_all_async([LazyFrame()], **kwargs)

    def _(**kwargs: Unpack[CollectBatchesKwargs]):
        LazyFrame().collect_batches(**kwargs)
