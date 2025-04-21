from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

type Result[T] = Ok[T] | Ko


@dataclass
class Ok[T]:
    value: Final[T]


@dataclass
class Ko:
    value: Final[Any]
