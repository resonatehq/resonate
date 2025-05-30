from __future__ import annotations

import json
from typing import Any


class JsonEncoder:
    def encode(self, obj: Any) -> str | None:
        if obj is None:
            return None

        return json.dumps(obj, default=_encode_exception)

    def decode(self, obj: str | None) -> Any:
        if obj is None:
            return None

        return json.loads(obj, object_hook=_decode_exception)


def _encode_exception(obj: Any) -> dict[str, Any]:
    if isinstance(obj, BaseException):
        return {"__error__": str(obj)}
    return {}  # ignore unencodable objects


def _decode_exception(obj: dict[str, Any]) -> Any:
    if "__error__" in obj:
        return Exception(obj["__error__"])
    return obj
