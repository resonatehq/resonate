from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator
    from concurrent.futures import Future


class Handle[T]:
    def __init__(self, id: str, f: Future[T]) -> None:
        self._id = id
        self._f = f

    @property
    def id(self) -> str:
        return self._id

    @property
    def future(self) -> Future[T]:
        return self._f

    def done(self) -> bool:
        return self._f.done()

    def result(self, timeout: float | None = None) -> T:
        return self._f.result(timeout)

    def __await__(self) -> Generator[None, None, T]:
        return asyncio.wrap_future(self._f).__await__()
