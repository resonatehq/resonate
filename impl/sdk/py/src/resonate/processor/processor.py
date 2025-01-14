from __future__ import annotations

from threading import Thread
from typing import TYPE_CHECKING, Any

from resonate import utils
from resonate.cmd_queue import CommandQ, Complete
from resonate.delay_queue import Queue
from resonate.processor.traits import IProcessor
from resonate.result import Err, Ok, Result

if TYPE_CHECKING:
    from resonate.processor.traits import SQE


class Processor(IProcessor):
    def __init__(
        self,
        workers: int = 4,
    ) -> None:
        assert workers > 0, "workers must be greater than zero."

        self._workers = workers
        self._threads = set[Thread]()

        self._sq: Queue[SQE[Any]] = Queue()

    def start(self, cmd_queue: CommandQ, pid: str) -> None:
        for _ in range(self._workers):
            t = Thread(target=self._run, args=(cmd_queue,), daemon=True)
            self._threads.add(t)

            t.start()

    def stop(self) -> None:
        raise NotImplementedError

    def enqueue(self, sqe: SQE[Any]) -> None:
        self._sq.put(sqe)

    @utils.exit_on_exception
    def _run(self, cmd_queue: CommandQ) -> None:
        while True:
            sqe = self._sq.get()
            result: Result[Any, Exception]

            try:
                result = Ok(sqe.thunk())
            except Exception as e:  # noqa: BLE001
                result = Err(e)

            # put on completion queue
            cmd_queue.put(Complete(sqe.id, result))
