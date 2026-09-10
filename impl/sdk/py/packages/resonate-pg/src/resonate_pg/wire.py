"""The resonate-pg wire contract: addresses, NOTIFY channels, envelopes.

Three facts in this file must agree byte-for-byte with ``resonate.sql``: the
shape of a delivery address, the name of the per-address ``NOTIFY`` channel,
and the layout of the messages the outbox carries. They live here, pure and
IO-free, so agreement is a unit test on a string literal rather than an
integration test against a database.
"""

from __future__ import annotations

import hashlib
import json

__all__ = [
    "ANYCAST",
    "CHANNEL_PREFIX",
    "SCHEME",
    "UNICAST",
    "execute_message",
    "outbox_channel",
    "resolve_target",
    "unblock_message",
    "unicast",
]

#: URL scheme of the delivery addresses this connector advertises.
#: ponytail: ``resonate.promise_register_listener`` answers 400 to any address
#: that is not ``http://``, ``https://``, or ``poll://`` *with* userinfo -- and
#: the SDK registers listeners with :meth:`Source.unicast`. So the scheme and
#: the ``@`` are both load-bearing, not decoration.
SCHEME = "poll"

#: Userinfo marking an address that reaches exactly one process.
UNICAST = "uni"

#: Userinfo marking an address that reaches any one member of a group.
ANYCAST = "any"

#: Prefix of the per-address ``NOTIFY`` channel.
#: ponytail: must match ``resonate.outbox_channel``, which is
#: ``'resonate_q_' || md5(address)``.
CHANNEL_PREFIX = "resonate_q_"


def unicast(group: str, pid: str) -> str:
    """Mint the address that reaches this process alone.

    Total by design -- like every connector's address minting, it only
    formats. An address is opaque to the SDK and to the server alike, and a
    malformed one is declined at the point of use, which is observable.

    >>> unicast("workers", "7f3a")
    'poll://uni@workers/7f3a'
    """
    return f"{SCHEME}://{UNICAST}@{group}/{pid}"


def resolve_target(target: str) -> str:
    """Mint the address that reaches any one member of ``target``'s group.

    No pid, and that asymmetry with :func:`unicast` is not cosmetic:
    ``resonate.dequeue_execute`` matches ``address = p_target`` exactly against
    the ``resonate:target`` tag a peer stamped, so a worker's own anycast
    address has to be character-identical to what this function returns for
    its own group.

    >>> resolve_target("workers")
    'poll://any@workers'
    """
    return f"{SCHEME}://{ANYCAST}@{target}"


def outbox_channel(address: str) -> str:
    """Return the ``NOTIFY`` channel resonate-pg signals ``address`` on.

    Recomputed here rather than fetched with ``SELECT
    resonate.outbox_channel($1)`` so opening a listener costs one round trip
    instead of three, and so the formula is a pure function a test can pin
    against the SQL. md5 is the server's choice and is used as a name, not as
    a security primitive.

    The result is always ``resonate_q_`` followed by 32 hex characters, which
    is what makes it safe to interpolate into ``LISTEN``.
    """
    digest = hashlib.md5(address.encode("utf-8"), usedforsecurity=False).hexdigest()
    return f"{CHANNEL_PREFIX}{digest}"


def execute_message(task_id: str, version: int) -> str:
    """Render one dequeued ``execute`` row as the SDK's wire envelope.

    ponytail: mirrors ``resonate._outbox_http_body``, the same envelope the
    server's own HTTP push emits, so a worker cannot tell the two delivery
    paths apart.
    """
    return json.dumps(
        {
            "kind": "execute",
            "head": {},
            "data": {"task": {"id": task_id, "version": version}},
        }
    )


def unblock_message(promise_json: str) -> str:
    """Render one dequeued ``unblock`` row as the SDK's wire envelope.

    ``promise_json`` arrives as Postgres already serialized it (the query asks
    for ``promise::text``) and is spliced in verbatim. Parsing it into Python
    objects only to dump it again would buy nothing and could only lose
    fidelity -- large integers and float formatting, to name two.
    """
    return f'{{"kind":"unblock","head":{{}},"data":{{"promise":{promise_json}}}}}'
