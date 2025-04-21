from __future__ import annotations

import threading
from collections.abc import Callable
from queue import Queue
from typing import Any

from resonate.models.result import Ko, Ok, Result

Fn = Callable[[], Any]
Cb = Callable[[Result], None]


class Processor:
    def __init__(self, workers: int = 5) -> None:
        self._sq = Queue[tuple[Fn, Cb] | None]()
        self._threads = [threading.Thread(target=self._run, args=(id,), daemon=True) for id in range(workers)]

    def start(self) -> None:
        for t in self._threads:
            if not t.is_alive():
                t.start()

    def stop(self) -> None:
        for t in self._threads:
            if t.is_alive():
                self._sq.put(None)

        for t in self._threads:
            if t.is_alive():
                t.join()

    def enqueue(self, fn: Fn, cb: Cb) -> None:
        self._sq.put((fn, cb))

    def _run(self, id: int) -> None:
        while sqe := self._sq.get():
            self._step(*sqe)

    def step(self) -> None:
        if sqe := self._sq.get_nowait():
            self._step(*sqe)

    def _step(self, fn: Fn, cb: Cb) -> None:
        try:
            cb(Ok(fn()))
        except Exception as e:
            cb(Ko(e))
