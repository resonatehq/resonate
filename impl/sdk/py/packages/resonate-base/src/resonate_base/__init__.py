"""The connector seam shared by the Resonate SDK and its connectors.

Everything needed to put Resonate on a new substrate, and nothing that
describes *executing durable functions* -- that lives in :mod:`resonate` (the
``resonate-sdk`` distribution), which depends on this package rather than the
other way round.

A connector implements :class:`~resonate_base.connections.Network` and/or
:class:`~resonate_base.connections.Source`, raises
:class:`~resonate_base.error.ConnectorError` when its substrate fails, and
routes by the promise id format in :mod:`resonate_base.ids`::

    from resonate_base import ConnectorError, Network, Source, origin_of


    class MyConnection:
        async def send(self, req: str) -> str:
            try:
                return await self._rpc(origin_of(req_id), req)
            except OSError as exc:
                raise ConnectorError(exc) from exc

:mod:`resonate_base.addresses` mints delivery addresses in the form the SDK's
own sources use; a connector addressing its substrate's namespace directly can
ignore it.
"""

from __future__ import annotations

from resonate_base.connections import Network, Source
from resonate_base.error import ConnectorError, InvalidIdError, ResonateError
from resonate_base.ids import origin_of

#: Protocol version string sent in the head of every request to the Resonate
#: server. It lives here rather than in the SDK because it describes the *wire*,
#: which a connector may need to stamp or assert on without the SDK installed.
PROTOCOL_VERSION = "2026-04-01"

__all__ = [
    "PROTOCOL_VERSION",
    "ConnectorError",
    "InvalidIdError",
    "Network",
    "ResonateError",
    "Source",
    "origin_of",
]
