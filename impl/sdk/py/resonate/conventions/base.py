from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from resonate.options import Options


@dataclass
class Base:
    data: Any
    headers: dict[str, str] | None
    opts: Options = field(default_factory=Options)

    @property
    def tags(self) -> dict[str, str]:
        return self.opts.tags

    @property
    def timeout(self) -> int:
        return self.opts.timeout

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        self.opts = self.opts.merge(timeout=timeout, tags=tags)
