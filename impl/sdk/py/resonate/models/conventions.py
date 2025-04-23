from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from resonate.options import Options


class Convention(Protocol):
    @property
    def opts(self) -> Options: ...
    def format(self) -> tuple[Any, dict[str, str], int, dict[str, str] | None]: ...
    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None: ...
