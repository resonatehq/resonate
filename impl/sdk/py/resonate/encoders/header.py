from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resonate.models.encoder import Encoder


class HeaderEncoder[T, U]:
    def __init__(self, key: str, encoder: Encoder[T, U]) -> None:
        self._key = key
        self._enc = encoder

    def encode(self, obj: T) -> dict[str, U] | None:
        return {self._key: self._enc.encode(obj)}

    def decode(self, obj: dict[str, U] | None) -> T | None:
        if obj and self._key in obj:
            return self._enc.decode(obj[self._key])

        return None
