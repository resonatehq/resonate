from __future__ import annotations

from typing import final

from resonate.retry_policy import RetryPolicy, exponential


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
            retry_policy
            if retry_policy is not None
            else exponential(base_delay=1, factor=2, max_retries=5)
        )
        self.tags = tags or {}
