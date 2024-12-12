from __future__ import annotations

from threading import Event, Thread
from typing import Any

from resonate.processor.traits import CQE, SQE
from resonate.queue import Queue
from resonate.result import Err, Ok, Result


class Processor:
    def __init__(
        self,
        workers: int = 4,
    ) -> None:
        assert workers > 0, "workers must be greater than zero."

        self._workers = workers
        self._threads = set[Thread]()

        self._sq: Queue[SQE[Any]] = Queue()
        self._cq: Queue[CQE[Any]] = Queue()

    def start(self, event: Event) -> None:
        for _ in range(self._workers):
            t = Thread(target=self._run, args=(event,), daemon=True)
            self._threads.add(t)

            t.start()

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._sq.put_nowait(sqe)

    def dequeue(self) -> list[CQE[Any]]:
        return self._cq.dequeue_all()

    def _run(self, event: Event) -> None:
        while True:
            sqe = self._sq.get()
            result: Result[Any, Exception]

            try:
                result = Ok(sqe.thunk())
            except Exception as e:  # noqa: BLE001
                result = Err(e)

            # put on completion queue
            self._cq.put_nowait(CQE[Any](result=result, callback=sqe.callback))

            # raise event so scheduler knows there is something to process
            event.set()
