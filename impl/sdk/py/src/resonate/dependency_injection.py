from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, final


@final
@dataclass(frozen=True)
class Dependencies:
    deps: dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, obj: Any) -> None:  # noqa: ANN401
        assert not self.exists(key), f"There's already a dependency under name `{key}`"
        self.deps[key] = obj

    def get(self, key: str) -> Any:  # noqa: ANN401
        return self.deps[key]

    def remove(self, key: str) -> None:
        assert self.exists(key), f"There's no depedency under name `{key}`"
        self.deps.pop(key)

    def exists(self, key: str) -> bool:
        try:
            self.get(key)
        except KeyError:
            return False
        return True
