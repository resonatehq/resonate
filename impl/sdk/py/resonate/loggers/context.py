from __future__ import annotations

import logging
from typing import Any


class ContextLogger:
    def __init__(self, cid: str, id: str, level: int | str = logging.NOTSET) -> None:
        self.cid = cid
        self.id = id

        self._logger = logging.getLogger(f"resonate:{cid}:{id}")
        self._logger.setLevel(level)
        self._logger.propagate = False

        if not self._logger.handlers:
            formatter = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [%(cid)s] [%(id)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def _log(self, level: int, msg: Any, *args: Any, **kwargs: Any) -> None:
        self._logger.log(level, msg, *args, **{**kwargs, "extra": {"cid": self.cid, "id": self.id}})

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
