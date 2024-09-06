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


def _error_to_code(string: ResonateErrorCode) -> int:
    error_code_map = {
        "UNKNOWN": 0,
        "FETCH": 1,
        "CANCELED": 10,
        "TIMEDOUT": 20,
        "ABORT": 30,
        "STORE": 40,
        "STORE_UNAUTHORIZED": 41,
        "STORE_PAYLOAD": 42,
        "STORE_FORBIDDEN": 43,
        "STORE_NOT_FOUND": 44,
        "STORE_ALREADY_EXISTS": 45,
        "STORE_INVALID_STATE": 46,
        "STORE_ENCODER": 47,
        "USER": 60,
    }

    return error_code_map[string]


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
