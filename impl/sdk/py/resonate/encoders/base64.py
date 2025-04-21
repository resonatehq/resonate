from __future__ import annotations

import base64


class Base64Encoder:
    def encode(self, obj: str | None) -> str | None:
        return base64.b64encode(obj.encode()).decode() if obj is not None else None

    def decode(self, obj: str | None) -> str | None:
        return base64.b64decode(obj).decode() if obj is not None else None
