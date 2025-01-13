from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar, final

T = TypeVar("T")


@final
@dataclass(frozen=True)
class Promise(Generic[T]):
    id: str
