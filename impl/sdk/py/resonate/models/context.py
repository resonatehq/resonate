from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self

from resonate.errors import ResonateValidationError
from resonate.models.options import Options
from resonate.models.promise import Promise

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.conventions import Convention
    from resonate.models.retry_policies import RetryPolicy


class Info(Protocol):
    @property
    def attempt(self) -> int: ...


class Context(Protocol):
    @property
    def info(self) -> Info: ...


type Yieldable = LFI | LFC | RFI | RFC | Promise


@dataclass
class LFX:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    versions: dict[int, Callable] | None = None

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        if version is not None and self.versions is not None:
            if version not in self.versions:
                msg = f"version={version} not found."
                raise ResonateValidationError(msg)
            self.func = self.versions[version]

        if timeout is not None:
            timeout = min(self._max_timeout, timeout)

        self.id = id or self.id
        self.opts = self.opts.merge(timeout=timeout, version=version, tags=tags, retry_policy=retry_policy, durable=durable)
        return self


@dataclass
class LFI(LFX):
    pass


@dataclass
class LFC(LFX):
    pass


@dataclass
class RFX:
    id: str
    convention: Convention

    def options(
        self,
        *,
        id: str | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        self.id = id or self.id
        self.convention.options(send_to=send_to, tags=tags, timeout=timeout, version=version)

        return self


@dataclass
class RFI(RFX):
    mode: Literal["attached", "detached"] = "attached"


@dataclass
class RFC(RFX):
    pass


@dataclass
class AWT:
    id: str
