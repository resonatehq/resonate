from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from resonate.options import Options


class Convention(Protocol):
    @property
    def opts(self) -> Options: ...
    @property
    def data(self) -> Any: ...
    @property
    def tags(self) -> dict[str, str]: ...
    @property
    def timeout(self) -> int: ...
    @property
    def headers(self) -> dict[str, str] | None: ...
    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None: ...
