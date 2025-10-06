from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from concurrent.futures import Future


class Handle[T]:
    def __init__(self, id: str, f: Future[T], cb: Callable[[str, Future[T]], None]) -> None:
        self._id = id
        self._f = f
        self._should_subscribe = not self._f.done()
        self._cb = cb

    @property
    def id(self) -> str:
        return self._id

    @property
    def future(self) -> Future[T]:
        return self._f

    def done(self) -> bool:
        self._subscribe()
        return self._f.done()

    def result(self, timeout: float | None = None) -> T:
        self._subscribe()
        return self._f.result(timeout)

    def __await__(self) -> Generator[None, None, T]:
        self._subscribe()
        return asyncio.wrap_future(self._f).__await__()

    def _subscribe(self) -> None:
        if self._should_subscribe:
            self._cb(self._id, self._f)
            self._should_subscribe = False
