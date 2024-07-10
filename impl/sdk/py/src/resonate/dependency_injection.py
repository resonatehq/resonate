from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Dependencies:
    deps: dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, obj: Any) -> None:  # noqa: ANN401
        self.deps[key] = obj

    def get(self, key: str) -> Any:  # noqa: ANN401
        return self.deps[key]
