from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class Remote:
    id: str
    r_id: str
    p_id: str
    name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    opts: Options = field(default_factory=Options, repr=False)

    @property
    def idempotency_key(self) -> str | None:
        return self.opts.get_idempotency_key(self.id)

    @property
    def data(self) -> dict[str, Any]:
        return {"func": self.name, "args": self.args, "kwargs": self.kwargs, "version": self.opts.version}

    @property
    def timeout(self) -> float:
        return self.opts.timeout

    @property
    def tags(self) -> dict[str, str]:
        return {**self.opts.tags, "resonate:root": self.r_id, "resonate:parent": self.p_id, "resonate:scope": "global", "resonate:invoke": self.opts.target}

    def options(
        self,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Remote:
        self.id = id or self.id
        self.opts = self.opts.merge(id=id, idempotency_key=idempotency_key, target=target, tags=tags, timeout=timeout, version=version)

        return self
