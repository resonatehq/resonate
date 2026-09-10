"""The ``poll://`` address format, pinned.

These are :mod:`resonate.connections.sse`'s own addresses, not a format the SDK
imposes -- a connector names its destinations however its substrate does, and
minting is *total*: like every connector (see
:meth:`resonate_nats.NatsConnection.resolve_target`), these only format a
string. An address is opaque to the SDK and to the server alike; a malformed one
is the server's to decline, not the SDK's to refuse -- an incorrect group simply
mints an incorrect address. Refusing at mint time is especially wrong for
:func:`resolve_target`, which runs lazily inside a durable op: raising there is
caught by the workflow's own error boundary and permanently rejects the promise.
"""

from __future__ import annotations

import pytest

from resonate.connections.sse import ANYCAST, UNICAST, resolve_target, unicast


def test_unicast_names_a_concrete_process() -> None:
    assert unicast("workers", "7f3a") == "poll://uni@workers/7f3a"


def test_resolve_target_names_a_group_with_no_pid() -> None:
    """A resolver is handed a group whose members it does not know."""
    assert resolve_target("workers") == "poll://any@workers"


def test_the_two_forms_differ_only_in_kind() -> None:
    assert (
        unicast("workers", "7f3a").replace(UNICAST, ANYCAST)
        == "poll://any@workers/7f3a"
    )


@pytest.mark.parametrize(
    ("group", "expected"),
    [
        ("", "poll://uni@/pid"),
        ("has@sign", "poll://uni@has@sign/pid"),
        ("has/slash", "poll://uni@has/slash/pid"),
        ("has:colon", "poll://uni@has:colon/pid"),
        ("Workers", "poll://uni@Workers/pid"),
    ],
)
def test_unicast_formats_verbatim_without_refusing(group: str, expected: str) -> None:
    """Minting is total: a group that would confuse the server is passed through.

    The SDK does not front-run the server's parse -- an incorrect group mints an
    incorrect address, which the server declines to deliver to. Loud refusal
    here would break the connector-seam contract that addresses are opaque.
    """
    assert unicast(group, "pid") == expected


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("workers", "poll://any@workers"),
        ("Workers", "poll://any@Workers"),
        ("", "poll://any@"),
        ("has/slash", "poll://any@has/slash"),
    ],
)
def test_resolve_target_formats_verbatim_without_refusing(
    target: str, expected: str
) -> None:
    """Resolution is total -- doubly so, since it runs inside a durable op.

    Raising while a durable op builds its request would be caught by the
    workflow's own error boundary and permanently reject the promise. So an
    incorrect target just mints an incorrect address, recoverable by fixing it
    and re-dispatching.
    """
    assert resolve_target(target) == expected
