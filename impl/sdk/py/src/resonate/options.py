from __future__ import annotations

from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from resonate import retry_policy


@final
class Options:
    def __init__(
        self,
        *,
        id: str | None = None,
        durable: bool = True,
        send_to: str | None = None,
        retry_policy: retry_policy.RetryPolicy | None = None,
        version: int = 1,
    ) -> None:
        self.durable = durable
        self.id = id
        self.retry_policy = retry_policy
        self.send_to = send_to
        self.version = version
