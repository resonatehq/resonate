from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar, final

T = TypeVar("T")


@final
@dataclass
class CmdBuffer(Generic[T]):
    max_length: int
    elements: list[list[T]] = field(init=False, default_factory=list)

    def append(self, e: T, /) -> None:
        if len(self.elements) == 0:
            self.elements.append([e])
        elif len(self.elements[-1]) < self.max_length:
            self.elements[-1].append(e)
        else:
            self.elements.append([e])

    def clear(self) -> None:
        self.elements.clear()
