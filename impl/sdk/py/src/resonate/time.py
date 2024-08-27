from __future__ import annotations

import time


def now() -> int:
    return int(time.time() * 1000)
