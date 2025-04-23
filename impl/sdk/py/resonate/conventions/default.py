from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from resonate.errors import ResonateValidationError
from resonate.options import Options

if TYPE_CHECKING:
    from resonate.registry import Registry


@dataclass
class Default:
    func: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    versions: set[int] | None
    registry: Registry
    opts: Options = field(default_factory=Options)

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

    @property
    def data(self) -> Any:
        if self.versions is not None:
            assert self.opts.version in self.versions
        assert self.opts.version > 0
        return {"func": self.func, "args": self.args, "kwargs": self.kwargs, "version": self.opts.version}

    @property
    def tags(self) -> dict[str, str]:
        return {**self.opts.tags, "resonate:invoke": self.opts.send_to}

    @property
    def timeout(self) -> int:
        return self.opts.timeout

    @property
    def headers(self) -> dict[str, str] | None:
        return None

    def options(self, send_to: str | None, tags: dict[str, str] | None, timeout: int | None, version: int | None) -> None:
        if version is not None and self.versions is not None and version not in self.versions:
            msg = f"version={version} not found."
            raise ResonateValidationError(msg)
        if timeout is not None:
            timeout = min(self._max_timeout, timeout)
        self.opts = self.opts.merge(send_to=send_to, timeout=timeout, version=version, tags=tags)
