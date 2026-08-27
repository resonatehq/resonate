from __future__ import annotations

from resonate_base.connections import Network, Source
from resonate_base.error import ConnectorError, ResonateError

#: Protocol version string sent in the head of every request to the Resonate
#: server. It lives here rather than in the SDK because it describes the *wire*,
#: which a connector may need to stamp or assert on without the SDK installed.
PROTOCOL_VERSION = "2026-04-01"

#: Header key carrying the lineage origin a request routes by. It rides in the
#: ``headers`` handed to :meth:`~resonate_base.connections.Network.send`, so a
#: sharding substrate reads the value it partitions on without opening ``req``.
#: It lives here because it describes the *wire*, which the SDK stamps and any
#: connector reads -- the one name both halves of the seam must agree on.
ORIGIN_HEADER = "resonate:origin"

__all__ = [
    "ORIGIN_HEADER",
    "PROTOCOL_VERSION",
    "ConnectorError",
    "Network",
    "ResonateError",
    "Source",
]
