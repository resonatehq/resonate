from __future__ import annotations

from typing import Protocol


class Context(Protocol):
    @property
    def info(self) -> Info: ...


class Info(Protocol):
    @property
    def attempt(self) -> int: ...
