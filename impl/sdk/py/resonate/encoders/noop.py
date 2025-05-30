from __future__ import annotations

from typing import Any


class NoopEncoder:
    def encode(self, obj: Any) -> Any:
        return None

    def decode(self, obj: Any) -> Any:
        return None
