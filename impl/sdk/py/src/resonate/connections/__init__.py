"""The transports the SDK ships with.

Each implements :class:`~resonate_base.connections.Network`,
:class:`~resonate_base.connections.Source`, or both. Connectors for other
substrates live in their own packages (``resonate-nats``) and depend only on
``resonate-base``.
"""

from __future__ import annotations

from resonate.connections.http import HttpConnection
from resonate.connections.local import LocalConnection
from resonate.connections.sse import SSEConnection

__all__ = [
    "HttpConnection",
    "LocalConnection",
    "SSEConnection",
]
