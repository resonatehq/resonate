from __future__ import annotations

import time


def now() -> int:
    return time.time_ns()
