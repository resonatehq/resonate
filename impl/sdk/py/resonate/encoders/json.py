from __future__ import annotations

from typing import Any

import jsonpickle


class JsonEncoder:
    def encode(self, obj: Any) -> str | None:
        if obj is None:
            return None
        return jsonpickle.encode(obj)

    def decode(self, obj: str | None) -> Any:
        if obj is None:
            return None
        return jsonpickle.decode(obj)  # noqa: S301
