from __future__ import annotations

import datetime
import json
from typing import Any


class ResonateError(Exception):
    def __init__(self, mesg: str, code: str, details: Any = None) -> None:
        super().__init__(mesg)
        self.mesg = mesg
        self.code = code
        try:
            self.details = json.dumps(details, indent=2) if details else None
        except Exception:
            self.details = details

    def __str__(self) -> str:
        return f"[{self.code}] {self.mesg}{'\n' + self.details if self.details else ''}"


class ResonateValidationError(ResonateError):
    def __init__(self, mesg: str) -> None:
        super().__init__(mesg, "10")


class ResonateShutdownError(ResonateError):
    def __init__(self, mesg: str) -> None:
        super().__init__(mesg, "20")


class ResonateStoreError(ResonateError):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        mesg = kwargs.pop("message", "Unknown store error")
        code = kwargs.pop("code", "0")
        details = kwargs.pop("details", [])
        super().__init__(mesg, f"30.{code}", details)


class ResonateCanceledError(ResonateError):
    def __init__(self, promise_id: str) -> None:
        super().__init__(f"Promise {promise_id} canceled", "40")
        self.promise_id = promise_id


class ResonateTimedoutError(ResonateError):
    def __init__(self, promise_id: str, timeout: float) -> None:
        formatted = datetime.datetime.fromtimestamp(timeout, tz=datetime.UTC).isoformat()
        super().__init__(f"Promise {promise_id} timedout at {formatted}", "41")
        self.promise_id = promise_id
        self.timeout = timeout
