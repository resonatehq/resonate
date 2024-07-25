# This is a copy from library https://github.com/rustedpy/result.
from __future__ import annotations

import sys
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generic,
    NoReturn,
    TypeVar,
    Union,
)

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias


if TYPE_CHECKING:
    from collections.abc import Iterator


T_co = TypeVar("T_co", covariant=True)  # Success type
E_co = TypeVar("E_co", covariant=True)  # Error type


class Ok(Generic[T_co]):
    """
    A value that indicates success and which stores arbitrary data for the return value.
    """

    __match_args__ = ("ok_value",)
    __slots__ = ("_value",)

    def __iter__(self) -> Iterator[T_co]:
        yield self._value

    def __init__(self, value: T_co) -> None:
        self._value = value

    def __repr__(self) -> str:
        return f"Ok({self._value!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Ok) and self._value == other._value

    def __ne__(self, other: object) -> bool:
        return not (self == other)

    def __hash__(self) -> int:
        return hash((True, self._value))

    def ok(self) -> T_co:
        """
        Return the value.
        """
        return self._value

    def err(self) -> None:
        """
        Return `None`.
        """
        return

    def unwrap(self) -> T_co:
        """
        Return the value.
        """
        return self._value


class DoExceptionError(Exception):
    """
    This is used to signal to `do()` that the result is an `Err`,
    which short-circuits the generator and returns that Err.
    Using this exception for control flow in `do()` allows us
    to simulate `and_then()` in the Err case: namely, we don't call `op`,
    we just return `self` (the Err).
    """

    def __init__(self, err: Err[E_co]) -> None:
        self.err = err


class Err(Generic[E_co]):
    """
    A value that signifies failure and which stores arbitrary data for the error.
    """

    __match_args__ = ("err_value",)
    __slots__ = ("_value",)

    def __iter__(self) -> Iterator[NoReturn]:
        def _iter() -> Iterator[NoReturn]:
            raise DoExceptionError(self)

        return _iter()

    def __init__(self, value: E_co) -> None:
        self._value = value

    def __repr__(self) -> str:
        return f"Err({self._value!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Err) and self._value == other._value

    def __ne__(self, other: object) -> bool:
        return not (self == other)

    def __hash__(self) -> int:
        return hash((False, self._value))

    def ok(self) -> None:
        """
        Return `None`.
        """
        return

    def err(self) -> E_co:
        """
        Return the error.
        """
        return self._value

    def unwrap(self) -> NoReturn:
        """
        Raises an `UnwrapError`.
        """
        exc = UnwrapError(
            self,
            f"Called `Result.unwrap()` on an `Err` value: {self._value!r}",
        )
        if isinstance(self._value, BaseException):
            raise exc from self._value
        raise exc


# define Result as a generic type alias for use
# in type annotations
"""
A simple `Result` type inspired by Rust.
Not all methods (https://doc.rust-lang.org/std/result/enum.Result.html)
have been implemented, only the ones that make sense in the Python context.
"""
Result: TypeAlias = Union[Ok[T_co], Err[E_co]]

"""
A type to use in `isinstance` checks.
This is purely for convenience sake, as you could also just write `isinstance(res, (Ok, Err))`
"""  # noqa: E501
OkErr: Final = (Ok, Err)


class UnwrapError(Exception):
    """
    Exception raised from ``.unwrap_<...>`` and ``.expect_<...>`` calls.

    The original ``Result`` can be accessed via the ``.result`` attribute, but
    this is not intended for regular use, as type information is lost:
    ``UnwrapError`` doesn't know about both ``T_co`` and ``E_co``, since it's raised
    from ``Ok()`` or ``Err()`` which only knows about either ``T_co`` or ``E_co``,
    not both.
    """

    _result: Result[object, object]

    def __init__(self, result: Result[object, object], message: str) -> None:
        self._result = result
        super().__init__(message)

    @property
    def result(self) -> Result[Any, Any]:
        """
        Returns the original result.
        """
        return self._result
