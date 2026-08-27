"""Distributed Async Await by Resonate HQ, Inc.

The connector seam this SDK is built on -- the ``Network``/``Source``
protocols, the error vocabulary, the delivery address format, the promise id
format and the wire protocol version -- lives in :mod:`resonate_base`, which
connectors depend on without depending on the SDK.
"""

from __future__ import annotations
