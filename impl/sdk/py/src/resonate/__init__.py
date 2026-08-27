"""Distributed Async Await by Resonate HQ, Inc.

The connector seam this SDK is built on -- the ``Network``/``Source``
protocols, ``ConnectorError`` and the wire protocol version -- lives in
:mod:`resonate_base`, which connectors depend on without depending on the SDK.
The formats built *on top* of that seam stay here: :mod:`resonate.ids` (the
``origin:lineage`` promise id), and, in each connection, the addresses it
advertises -- ``poll://uni@group/pid`` in :mod:`resonate.connections.sse`, a
bare subject in ``resonate-nats``. There is no shared address format, because
the server hands an address back to whoever minted it.
"""

from __future__ import annotations
