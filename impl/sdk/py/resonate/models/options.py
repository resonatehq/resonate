from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypedDict

from resonate.errors import ResonateValidationError
from resonate.models.retry_policies import Never

if TYPE_CHECKING:
    from resonate.models.retry_policies import RetryPolicy


@dataclass(frozen=True)
class Options:
    durable: bool = True
    retry_policy: RetryPolicy = field(default_factory=Never)
    send_to: str = "poll://default"
    tags: dict[str, str] = field(default_factory=dict)
    timeout: int = sys.maxsize
    version: int = 0

    def __post_init__(self) -> None:
        if not (self.version >= 0):
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)
        if not (self.timeout >= 0):
            msg = "timeout must be greater or equal than 0"
            raise ResonateValidationError(msg)

    def merge(
        self,
        *,
        durable: bool | None = None,
        retry_policy: RetryPolicy | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Options:
        if version and not (version >= 0):
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)
        if timeout and not (timeout >= 0):
            msg = "timeout mut be greater or equal than 0"
            raise ResonateValidationError(msg)

        return Options(
            durable=durable if durable is not None else self.durable,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            send_to=send_to if send_to is not None else self.send_to,
            tags=tags if tags is not None else self.tags,
            timeout=timeout if timeout is not None else self.timeout,
            version=version if version is not None else self.version,
        )

    def to_dict(self) -> DictOptions:
        return DictOptions(
            retry_policy=self.retry_policy,
            send_to=self.send_to,
            tags=self.tags,
            timeout=self.timeout,
            version=self.version,
        )


class DictOptions(TypedDict):
    retry_policy: RetryPolicy
    send_to: str
    tags: dict[str, str]
    timeout: int
    version: int
