from dataclasses import dataclass
from typing import Any, TYPE_CHECKING, Unpack
from pydantic import RootModel
from pydantic._internal._generics import get_args  # Needed for inspecting models
from polars import DataFrame, LazyFrame
from .._typing import CollectKwargs, CollectAsyncKwargs

__all__ = ['BaseValidator', 'DeferredValidation']

# Keep it a secret to Pyright that this is a subclass of RootModel.
# I'm not yet sure whether this should be the long-term API solution, or if we'll want
# a custom base of our own.
# What Pydantic does under the hood with RootModel(BaseModel) is very powerful, and
# particularly useful for our use case. For example, it has internal caching that makes
# specialization extremely fast.
# So using RootModel but hiding the API is probably the best move until we come
# up with something better.
if TYPE_CHECKING:

    class _TotallyNotARootModel[T]:
        @classmethod
        def root_model(cls) -> type[RootModel[T]]: ...

else:

    class _TotallyNotARootModel[T](RootModel[T]):
        @classmethod
        def root_model(cls):
            return cls


class BaseValidator[T](_TotallyNotARootModel[T]):
    """
    Base class with validate(), collect(), and collect_async() methods for validating
    a Polars DataFrame/LazyFrame to python objects of type T.

    Pydantic validation is only performed if a generic type argument is given for T.
    """

    # DataFrame

    @classmethod
    def validate(cls, df: DataFrame, /) -> T:
        """Validate a DataFrame to type `T`"""
        return cls.validate_model(df).root

    @classmethod
    def validate_model(cls, df: DataFrame, /) -> RootModel[T]:
        """Validate a DataFrame to a `RootModel[T]`, which wraps `T`
        while offering Pydantic model methods for serialization, etc.
        """
        model_cls = cls.root_model()
        if get_args(model_cls):
            return model_cls.model_validate(cls._dataframe_to_python(df))
        return model_cls.model_construct(cls._dataframe_to_python(df))

    # LazyFrame

    @classmethod
    def collect(cls, lf: LazyFrame, **kwargs: Unpack[CollectKwargs]) -> T:
        """Validate a LazyFrame, after collecting in the current thread."""
        return cls.validate(lf.collect(**kwargs))

    @classmethod
    def collect_model(
        cls, lf: LazyFrame, **kwargs: Unpack[CollectKwargs]
    ) -> RootModel[T]:
        """Validate a LazyFrame, after collecting in the current thread.
        Returns a `RootModel[T]` which wraps `T` while offering Pydantic model
        methods for serialization, etc.
        """
        return cls.validate_model(lf.collect(**kwargs))

    # LazyFrame async

    @classmethod
    async def collect_async(
        cls, lf: LazyFrame, **kwargs: Unpack[CollectAsyncKwargs]
    ) -> T:
        """Validate a LazyFrame, after collecting asynchronously in another thread."""
        return cls.validate(await lf.collect_async(**kwargs))

    @classmethod
    async def collect_model_async(
        cls, lf: LazyFrame, **kwargs: Unpack[CollectAsyncKwargs]
    ) -> RootModel[T]:
        """Validate a LazyFrame, after collecting asynchronously in another thread.
        Returns `RootModel[T]` results, which wraps `T` while offering Pydantic model
        methods for serialization, etc.
        """
        return cls.validate_model(await lf.collect_async(**kwargs))

    # LazyFrame batch collection

    @classmethod
    def defer(cls, lf: LazyFrame, /) -> DeferredValidation[T]:
        """
        Get an object that can be passed to validate.collect_all() or
        validate.collect_all_async() for parallel query execution.
        """
        return DeferredValidation(validator=cls, lf=lf)

    # Helpers

    @classmethod
    def _dataframe_to_python(cls, _: DataFrame, /) -> Any:
        """Subclasses should implement this."""
        raise NotImplementedError()


@dataclass(slots=True, frozen=True)
class DeferredValidation[T]:
    """
    Short-lived container for a LazyFrame and a validator to run.
    Returned by `validator.defer()`.
    Used by `collect_all()` and `collect_all_async()` for batch collection.
    """

    validator: type[BaseValidator[T]]
    lf: LazyFrame
