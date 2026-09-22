"""Postgres connector for Resonate: the server is a database.

``PostgresConnection`` implements both connector protocols against a
`resonate-pg <https://github.com/resonatehq/resonate-pg>`_ schema -- requests
run as stored procedures, push messages are outbox rows drained on
LISTEN/NOTIFY. There is no Resonate server process.
"""

from __future__ import annotations

from resonate_pg.client import PsycopgSessions
from resonate_pg.connection import PostgresConnection

__all__ = ["PostgresConnection", "PsycopgSessions"]
