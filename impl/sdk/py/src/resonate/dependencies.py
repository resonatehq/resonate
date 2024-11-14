from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, final


@final
@dataclass(frozen=True)
class Dependencies:
    deps: dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, obj: Any) -> None:  # noqa: ANN401
        assert not self._exists(key), f"There's already a dependency under name `{key}`"
        self.deps[key] = obj

    def get(self, key: str) -> Any:  # noqa: ANN401
        return self.deps[key]

    def _exists(self, key: str) -> bool:
        try:
            self.get(key)
        except KeyError:
            return False
        return True
