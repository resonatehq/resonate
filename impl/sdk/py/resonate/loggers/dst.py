from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.models.clock import Clock


class DSTFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        time: float = getattr(record, "time", 0.0)
        return f"{time:09.0f}"


class DSTLogger:
    def __init__(self, cid: str, id: str, clock: Clock, level: int = logging.NOTSET) -> None:
        self.cid = cid
        self.id = id
        self.clock = clock

        self._logger = logging.getLogger(f"resonate:{cid}:{id}")
        self._logger.setLevel(level)
        self._logger.propagate = False

        if not self._logger.handlers:
            formatter = DSTFormatter("[%(asctime)s] [%(levelname)s] [%(cid)s] [%(id)s] %(message)s")
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.log(level, msg, *args, **{**kwargs, "extra": {"cid": self.cid, "id": self.id, "time": self.clock.time()}})

    def log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(level, msg, *args, **kwargs)

    def debug(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._log(logging.CRITICAL, msg, *args, **kwargs)
