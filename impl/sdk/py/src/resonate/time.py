from __future__ import annotations

import time


def now() -> int:
    "now in milliseconds"
    return int(time.time() * 1_000)
