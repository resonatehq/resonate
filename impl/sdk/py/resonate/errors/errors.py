from __future__ import annotations

import json
from typing import Any


class ResonateError(Exception):
    def __init__(self, mesg: str, code: float, details: Any = None) -> None:
        super().__init__(mesg)
        self.mesg = mesg
        self.code = code
        try:
            self.details = json.dumps(details, indent=2) if details else None
        except Exception:
            self.details = details

    def __str__(self) -> str:
        return f"[{self.code:09.5f}] {self.mesg}{'\n' + self.details if self.details else ''}"

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self.mesg, self.code, self.details))


# Error codes 100-199


class ResonateStoreError(ResonateError):
    def __init__(self, mesg: str, code: int, details: Any = None) -> None:
        super().__init__(mesg, float(f"{100}.{code}"), details)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self.mesg, self.code, self.details))


# Error codes 200-299


class ResonateCanceledError(ResonateError):
    def __init__(self, promise_id: str) -> None:
        super().__init__(f"Promise {promise_id} canceled", 200)
        self.promise_id = promise_id

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self.promise_id,))


class ResonateTimedoutError(ResonateError):
    def __init__(self, promise_id: str, timeout: float) -> None:
        super().__init__(f"Promise {promise_id} timedout at {timeout}", 201)
        self.promise_id = promise_id
        self.timeout = timeout

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self.promise_id, self.timeout))


# Error codes 300-399


class ResonateShutdownError(ResonateError):
    def __init__(self, mesg: str) -> None:
        super().__init__(mesg, 300)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self.mesg,))
