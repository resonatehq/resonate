from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class Local:
    id: str
    opts: Options = field(default_factory=Options, repr=False)

    @property
    def idempotency_key(self) -> str | None:
        return self.opts.idempotency_key(self.id) if callable(self.opts.idempotency_key) else self.opts.idempotency_key

    @property
    def headers(self) -> None:
        return None

    @property
    def data(self) -> Any:
        return None

    @property
    def timeout(self) -> float:
        return self.opts.timeout

    @property
    def tags(self) -> dict[str, str]:
        return {**self.opts.tags, "resonate:scope": "local"}

    def options(
        self,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Local:
        self.id = id or self.id

        # delibrately ignore target and version
        self.opts = self.opts.merge(id=id, idempotency_key=idempotency_key, tags=tags, timeout=timeout)
        return self
