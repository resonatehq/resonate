from __future__ import annotations

import sys
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING

from resonate.errors import ResonateValidationError
from resonate.retry_policies import Exponential, Never

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.models.retry_policy import RetryPolicy


@dataclass(frozen=True)
class Options:
    durable: bool = True
    id: str | None = None
    idempotency_key: str | Callable[[str], str] | None = lambda id: id
    retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] = lambda f: Never() if isgeneratorfunction(f) else Exponential()
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
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
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
            id=id if id is not None else self.id,
            idempotency_key=idempotency_key if idempotency_key is not None else self.idempotency_key,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            send_to=send_to if send_to is not None else self.send_to,
            tags=tags if tags is not None else self.tags,
            timeout=timeout if timeout is not None else self.timeout,
            version=version if version is not None else self.version,
        )
