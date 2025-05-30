from __future__ import annotations

from typing import Any

import jsonpickle


class JsonPickleEncoder:
    def encode(self, obj: Any) -> str:
        data = jsonpickle.encode(obj, unpicklable=True)
        assert data
        return data

    def decode(self, obj: str | None) -> Any:
        return jsonpickle.decode(obj)  # noqa: S301
