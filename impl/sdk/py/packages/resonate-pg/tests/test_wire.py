"""The resonate-pg wire contract: addresses, NOTIFY channels, envelopes.

Every assertion here pins something that must agree byte-for-byte with
``resonate.sql``. They are pure-function tests on purpose: an address shape or
a channel name that drifts is a silent no-delivery bug in production, and the
cheapest place to catch it is here.
"""

from __future__ import annotations

import json
import re

import resonate_pg
from resonate_pg.wire import (
    execute_message,
    outbox_channel,
    resolve_target,
    unblock_message,
    unicast,
)


def test_the_package_exposes_its_public_surface() -> None:
    assert resonate_pg.__all__ == ["PostgresConnection", "PsycopgSessions"]


def test_unicast_names_one_process_inside_its_group() -> None:
    """``promise_register_listener`` rejects an address without ``@``."""
    assert unicast("workers", "7f3a") == "poll://uni@workers/7f3a"
    assert "@" in unicast("workers", "7f3a")


def test_resolve_target_names_a_group_and_carries_no_pid() -> None:
    """``dequeue_execute`` matches the address byte-for-byte.

    A pid here would make a worker's own anycast address differ from the
    ``resonate:target`` its peers stamp, and every anycast task would sit in
    the outbox undelivered.
    """
    assert resolve_target("workers") == "poll://any@workers"


def test_outbox_channel_matches_the_servers_formula() -> None:
    """ponytail: ``resonate.outbox_channel`` is ``'resonate_q_' || md5(addr)``."""
    assert outbox_channel("poll://any@workers") == (
        "resonate_q_9f09af58d876414736aceefc75c1dbfe"
    )
    assert outbox_channel("poll://uni@workers/7f3a") == (
        "resonate_q_69bd32df65aaa70b854b030ff2144a70"
    )


def test_outbox_channel_is_always_a_safe_bare_identifier() -> None:
    """The channel is interpolated into ``LISTEN``, so its shape is load-bearing."""
    weird = 'poll://uni@a group/"; DROP TABLE promises; --'
    assert re.fullmatch(r"resonate_q_[0-9a-f]{32}", outbox_channel(weird))


def test_execute_message_renders_the_sdks_wire_envelope() -> None:
    assert json.loads(execute_message("root:1", 3)) == {
        "kind": "execute",
        "head": {},
        "data": {"task": {"id": "root:1", "version": 3}},
    }


def test_unblock_message_splices_the_promise_verbatim() -> None:
    """Postgres already serialized the promise; re-encoding could only lose."""
    promise = '{"id":"root:1","state":"RESOLVED","value":{"headers":{},"data":"x"}}'
    assert json.loads(unblock_message(promise)) == {
        "kind": "unblock",
        "head": {},
        "data": {"promise": json.loads(promise)},
    }
    assert promise in unblock_message(promise)


def test_the_public_names_are_importable_from_the_package_root() -> None:
    """Users write ``from resonate_pg import PostgresConnection``, nothing deeper."""
    assert resonate_pg.PostgresConnection is not None
    assert resonate_pg.PsycopgSessions is not None
