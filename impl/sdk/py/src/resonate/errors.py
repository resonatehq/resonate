from __future__ import annotations

from typing import Literal

from typing_extensions import TypeAlias

ResonateErrorCode: TypeAlias = Literal[
    "UNKNOWN",
    "FETCH",
    "CANCELED",
    "TIMEDOUT",
    "ABORT",
    "STORE",
    "STORE_UNAUTHORIZED",
    "STORE_PAYLOAD",
    "STORE_FORBIDDEN",
    "STORE_NOT_FOUND",
    "STORE_ALREADY_EXISTS",
    "STORE_INVALID_STATE",
    "STORE_ENCODER",
    "USER",
]


class ResonateError(Exception):
    def __init__(
        self,
        msg: str,
        code: ResonateErrorCode,
        cause: Exception | None = None,
        *,
        retriable: bool = False,
    ) -> None:
        super().__init__(msg)
        self.msg = msg
        self.code = code
        self.cause = cause
        self.retriable = retriable

    @classmethod
    def from_error(cls, e: Exception) -> ResonateError:
        return ResonateError("Unknown error", "UNKNOWN", e, retriable=True)
