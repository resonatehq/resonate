from __future__ import annotations

import time


class WallClock:
    def time(self) -> int:
        return int(time.time() * 1000)
