from __future__ import annotations

import time

#: Protocol version string sent in all requests to the Resonate server.
PROTOCOL_VERSION = "2026-04-01"


def now_ms() -> int:
    """Return the current time in milliseconds since the UNIX epoch."""
    return time.time_ns() // 1_000_000
