from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from resonate.models.logger import Logger


class Context(Protocol):
    @property
    def id(self) -> str: ...
    @property
    def info(self) -> Info: ...
    @property
    def logger(self) -> Logger: ...

    def get_dependency(self, key: str, default: Any = None) -> Any: ...


class Info(Protocol):
    @property
    def attempt(self) -> int: ...
    @property
    def idempotency_key(self) -> str | None: ...
    @property
    def tags(self) -> dict[str, str] | None: ...
    @property
    def timeout(self) -> float: ...
    @property
    def version(self) -> int: ...
