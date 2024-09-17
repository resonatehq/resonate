from __future__ import annotations

import random
from typing import TYPE_CHECKING, TypeVar, Union, final

from typing_extensions import ParamSpec, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

T = TypeVar("T")
P = ParamSpec("P")

FloatOrInt: TypeAlias = Union[float, int]


@final
class Random:
    """Random class implementation that allows to enforce a prefixable behaviour."""

    def __init__(self, seed: int, prefix: list[FloatOrInt] | None = None) -> None:
        self.seed = seed
        self._prefix: list[FloatOrInt] = []
        if prefix is not None:
            self._prefix.extend(prefix)
        self._count = 0
        self._random = random.Random(seed)  # noqa: RUF100, S311

    def _take_number(
        self, fn: Callable[P, FloatOrInt], *args: P.args, **kwargs: P.kwargs
    ) -> FloatOrInt:
        self._count += 1
        if self._count <= len(self._prefix):
            return self._prefix[self._count - 1]

        random_number = fn(*args, **kwargs)
        self._prefix.append(random_number)
        return random_number

    def randint(self, a: int, b: int) -> int:
        return int(self._take_number(self._random.randint, a=a, b=b))

    def choice(self, seq: Sequence[T]) -> T:
        return seq[self.randint(0, len(seq) - 1)]

    def uniform(self, a: float, b: float) -> float:
        return self._take_number(self._random.uniform, a=a, b=b)

    def random(self) -> float:
        return self._take_number(self._random.random)

    def gauss(self, mu: float, sigma: float) -> float:
        return self._take_number(self._random.gauss, mu=mu, sigma=sigma)

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> int:
        return int(
            self._take_number(self._random.randrange, start=start, stop=stop, step=step)
        )

    def triangular(
        self, low: float = 0, high: float = 1, mode: float | None = None
    ) -> float:
        return self._take_number(self._random.triangular, low=low, high=high, mode=mode)

    def export(self) -> list[float]:
        return self._prefix
