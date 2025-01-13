from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from resonate import retry_policy


@final
@dataclass
class Options:
    id: str | None = None
    durable: bool = True
    send_to: str | None = None
    retry_policy: retry_policy.RetryPolicy | None = None
    version: int = 1
    execute_here: bool = True
