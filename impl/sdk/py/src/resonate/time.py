from __future__ import annotations

import time


def now() -> int:
    return time.time_ns()


def ns_to_secs(ns: float) -> float:
    return ns / 1e9


def secs_to_ns(secs: float) -> float:
    return secs * 1e9
