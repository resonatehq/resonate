from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from resonate.error import PlatformError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class Chain:
    """Serializes async work in acquisition order under concurrency.

    Backs the durable ops on :class:`~resonate.context.Context`
    (``run``/``rpc``/``sleep``/``promise``/``detached``). Each op acquires a
    :class:`Link` *synchronously* at call time -- so chain position matches
    call order regardless of when the background tasks happen to run -- and
    then runs its work through that link from a background task. A link runs
    only after its predecessor has completed, and a failed link poisons every
    successor: the chain fails as a unit.

    One chain lives per execution state (root and each child each own one), so
    sibling ops at the same level share an ordering while different levels are
    independent.
    """

    def __init__(self) -> None:
        # Tail of the chain: the ``done`` future of the most recently acquired
        # link, or ``None`` before the first. Advanced synchronously in
        # :meth:`link`.
        self._tail: asyncio.Future[None] | None = None

    def link(self) -> Link:
        """Acquire the next link, advancing the tail synchronously."""
        prev = self._tail
        done = asyncio.Future[None]()
        self._tail = done
        return Link(prev, done)


class Link:
    """One position in a :class:`CreationChain`.

    :attr:`done` resolves when this link's work completes (or fails with its
    exception). It is the gate the next link waits on, and is exposed so an
    external awaiter -- e.g. a future that reports the created promise id --
    observes the same outcome.
    """

    def __init__(
        self, prev: asyncio.Future[None] | None, done: asyncio.Future[None]
    ) -> None:
        self._prev = prev
        self.done = done

    async def run[T](self, work: Callable[[], Awaitable[T]]) -> T:
        """Wait for the predecessor, run ``work``, then release the successor.

        A predecessor that failed propagates its exception through
        ``await self._prev``; we re-raise it on this link too (re-installing it
        on :attr:`done`), so the whole chain fails as a unit rather than running
        work on top of an inconsistent prefix. The *same* upstream exception
        object travels down every successor link, so the failure surfaces
        identically no matter how far down it is observed.
        """
        try:
            if self._prev is not None:
                await self._prev
            result = await work()
            self.done.set_result(None)
        # ``PlatformError`` is a ``BaseException`` (not ``Exception``), so it is
        # listed explicitly: a platform failure must still settle :attr:`done`
        # or successors and id-awaiters deadlock. A bare ``BaseException`` is
        # deliberately avoided -- that would swallow task cancellation.
        except (Exception, PlatformError) as e:
            self.done.set_exception(e)
            self.done.exception()  # mark retrieved
            raise
        return result
