from typing import Any, TYPE_CHECKING, overload, Unpack
from pydantic import RootModel
import polars as pl
from .._typing import CollectAllKwargs, CollectAllAsyncKwargs
from ._base_validator import DeferredValidation

__all__ = [
    'collect_all',
    'collect_all_async',
    'collect_all_models',
    'collect_all_models_async',
]

_Defer = DeferredValidation

if TYPE_CHECKING:
    # Since python typing doesn't yet support variadic type variables, we
    # manually overload up to 6 args then provide a permissive same-type
    # fallback. This is exactly how `asyncio.gather` is typed.
    @overload
    @staticmethod
    def collect_all[T1](
        a1: _Defer[T1], /, **kwargs: Unpack[CollectAllKwargs]
    ) -> tuple[T1]: ...
    @overload
    @staticmethod
    def collect_all[T1, T2](
        a1: _Defer[T1],
        a2: _Defer[T2],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[T1, T2]: ...
    @overload
    @staticmethod
    def collect_all[T1, T2, T3](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[T1, T2, T3]: ...
    @overload
    @staticmethod
    def collect_all[T1, T2, T3, T4](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[T1, T2, T3, T4]: ...
    @overload
    @staticmethod
    def collect_all[T1, T2, T3, T4, T5](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[T1, T2, T3, T4, T5]: ...
    @overload
    @staticmethod
    def collect_all[T1, T2, T3, T4, T5, T6](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        a6: _Defer[T6],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[T1, T2, T3, T4, T5, T6]: ...
    @overload  # permissive same-type fallback (must be last overload)
    @staticmethod
    def collect_all[T](
        *args: _Defer[T], **kwargs: Unpack[CollectAllKwargs]
    ) -> tuple[T, ...]: ...


@staticmethod
def collect_all[T](
    *args: _Defer[T], **kwargs: Unpack[CollectAllKwargs]
) -> tuple[Any, ...]:
    dfs = pl.collect_all([a.lf for a in args], **kwargs)
    return tuple(v.validator.validate(df) for v, df in zip(args, dfs, strict=True))


if TYPE_CHECKING:
    # Since python typing doesn't yet support variadic type variables, we
    # manually overload up to 6 args then provide a permissive same-type
    # fallback. This is exactly how `asyncio.gather` is typed.
    @overload
    @staticmethod
    async def collect_all_async[T1](
        a1: _Defer[T1], /, **kwargs: Unpack[CollectAllAsyncKwargs]
    ) -> tuple[T1]: ...
    @overload
    @staticmethod
    async def collect_all_async[T1, T2](
        a1: _Defer[T1],
        a2: _Defer[T2],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[T1, T2]: ...
    @overload
    @staticmethod
    async def collect_all_async[T1, T2, T3](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[T1, T2, T3]: ...
    @overload
    @staticmethod
    async def collect_all_async[T1, T2, T3, T4](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[T1, T2, T3, T4]: ...
    @overload
    @staticmethod
    async def collect_all_async[T1, T2, T3, T4, T5](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[T1, T2, T3, T4, T5]: ...
    @overload
    @staticmethod
    async def collect_all_async[T1, T2, T3, T4, T5, T6](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        a6: _Defer[T6],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[T1, T2, T3, T4, T5, T6]: ...
    @overload  # permissive same-type fallback (must be last overload)
    @staticmethod
    async def collect_all_async[T](
        *args: _Defer[T], **kwargs: Unpack[CollectAllAsyncKwargs]
    ) -> tuple[T, ...]: ...


@staticmethod
async def collect_all_async[T](
    *args: _Defer[T], **kwargs: Unpack[CollectAllAsyncKwargs]
) -> tuple[Any, ...]:
    dfs = await pl.collect_all_async([a.lf for a in args], **kwargs)
    return tuple(v.validator.validate(df) for v, df in zip(args, dfs, strict=True))


if TYPE_CHECKING:
    # Since python typing doesn't yet support variadic type variables, we
    # manually overload up to 6 args then provide a permissive same-type
    # fallback. This is exactly how `asyncio.gather` is typed.
    @overload
    @staticmethod
    def collect_all_models[T1](
        a1: _Defer[T1], /, **kwargs: Unpack[CollectAllKwargs]
    ) -> tuple[RootModel[T1]]: ...
    @overload
    @staticmethod
    def collect_all_models[T1, T2](
        a1: _Defer[T1],
        a2: _Defer[T2],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2]]: ...
    @overload
    @staticmethod
    def collect_all_models[T1, T2, T3](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2], RootModel[T3]]: ...
    @overload
    @staticmethod
    def collect_all_models[T1, T2, T3, T4](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2], RootModel[T3], RootModel[T4]]: ...
    @overload
    @staticmethod
    def collect_all_models[T1, T2, T3, T4, T5](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[
        RootModel[T1], RootModel[T2], RootModel[T3], RootModel[T4], RootModel[T5]
    ]: ...
    @overload
    @staticmethod
    def collect_all_models[T1, T2, T3, T4, T5, T6](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        a6: _Defer[T6],
        /,
        **kwargs: Unpack[CollectAllKwargs],
    ) -> tuple[
        RootModel[T1],
        RootModel[T2],
        RootModel[T3],
        RootModel[T4],
        RootModel[T5],
        RootModel[T6],
    ]: ...
    @overload  # permissive same-type fallback (must be last overload)
    @staticmethod
    def collect_all_models[T](
        *args: _Defer[T], **kwargs: Unpack[CollectAllKwargs]
    ) -> tuple[RootModel[T], ...]: ...


@staticmethod
def collect_all_models[T](
    *args: _Defer[T], **kwargs: Unpack[CollectAllKwargs]
) -> tuple[Any, ...]:
    dfs = pl.collect_all([a.lf for a in args], **kwargs)
    return tuple(
        v.validator.validate_model(df) for v, df in zip(args, dfs, strict=True)
    )


if TYPE_CHECKING:
    # Since python typing doesn't yet support variadic type variables, we
    # manually overload up to 6 args then provide a permissive same-type
    # fallback. This is exactly how `asyncio.gather` is typed.
    @overload
    @staticmethod
    async def collect_all_models_async[T1](
        a1: _Defer[T1], /, **kwargs: Unpack[CollectAllAsyncKwargs]
    ) -> tuple[RootModel[T1]]: ...
    @overload
    @staticmethod
    async def collect_all_models_async[T1, T2](
        a1: _Defer[T1],
        a2: _Defer[T2],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2]]: ...
    @overload
    @staticmethod
    async def collect_all_models_async[T1, T2, T3](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2], RootModel[T3]]: ...
    @overload
    @staticmethod
    async def collect_all_models_async[T1, T2, T3, T4](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[RootModel[T1], RootModel[T2], RootModel[T3], RootModel[T4]]: ...
    @overload
    @staticmethod
    async def collect_all_models_async[T1, T2, T3, T4, T5](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[
        RootModel[T1], RootModel[T2], RootModel[T3], RootModel[T4], RootModel[T5]
    ]: ...
    @overload
    @staticmethod
    async def collect_all_models_async[T1, T2, T3, T4, T5, T6](
        a1: _Defer[T1],
        a2: _Defer[T2],
        a3: _Defer[T3],
        a4: _Defer[T4],
        a5: _Defer[T5],
        a6: _Defer[T6],
        /,
        **kwargs: Unpack[CollectAllAsyncKwargs],
    ) -> tuple[
        RootModel[T1],
        RootModel[T2],
        RootModel[T3],
        RootModel[T4],
        RootModel[T5],
        RootModel[T6],
    ]: ...
    @overload  # permissive same-type fallback (must be last overload)
    @staticmethod
    async def collect_all_models_async[T](
        *args: _Defer[T], **kwargs: Unpack[CollectAllAsyncKwargs]
    ) -> tuple[RootModel[T], ...]: ...


@staticmethod
async def collect_all_models_async[T](
    *args: _Defer[T], **kwargs: Unpack[CollectAllAsyncKwargs]
) -> tuple[Any, ...]:
    dfs = await pl.collect_all_async([a.lf for a in args], **kwargs)
    return tuple(
        v.validator.validate_model(df) for v, df in zip(args, dfs, strict=True)
    )
