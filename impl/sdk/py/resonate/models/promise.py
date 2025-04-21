from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Promise:
    id: str
    cid: str
