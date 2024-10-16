from __future__ import annotations

from typing import final

from resonate.retry_policy import RetryPolicy, default_policy


@final
class Options:
    def __init__(
        self,
        *,
        durable: bool = True,
        promise_id: str | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.durable = durable
        self.promise_id = promise_id
        self.retry_policy = (
            retry_policy if retry_policy is not None else default_policy()
        )
        self.tags = tags or {}
