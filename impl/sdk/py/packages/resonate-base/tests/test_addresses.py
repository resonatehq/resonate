"""The delivery address format, pinned.

A malformed address does not raise anywhere: the server accepts it, stores it,
and then never delivers, so the task sits until its lease lapses and is
re-queued forever. That failure mode is invisible in a connector's own tests,
which is the whole reason the format is a module with a suite rather than an
f-string repeated in each connection.
"""

from __future__ import annotations

import pytest
from resonate_base import addresses
from resonate_base.error import InvalidIdError


def test_unicast_names_a_concrete_process() -> None:
    assert addresses.unicast("poll", "workers", "7f3a") == "poll://uni@workers/7f3a"


def test_anycast_names_the_group_but_still_carries_the_minting_pid() -> None:
    """The pid is not selective; it records who advertised the address."""
    assert addresses.anycast("poll", "workers", "7f3a") == "poll://any@workers/7f3a"


def test_resolve_target_names_a_group_with_no_pid() -> None:
    """A resolver is handed a group whose members it does not know."""
    assert addresses.resolve_target("poll", "workers") == "poll://any@workers"


def test_unicast_and_anycast_differ_only_in_kind() -> None:
    uni = addresses.unicast("local", "default", "pid")
    any_ = addresses.anycast("local", "default", "pid")
    assert uni != any_
    assert uni.replace(addresses.UNICAST, addresses.ANYCAST) == any_


@pytest.mark.parametrize(
    "group",
    ["", "has@sign", "has/slash", "has:colon", "has?query", "has#frag"],
)
def test_a_group_that_would_change_the_url_split_is_refused(group: str) -> None:
    with pytest.raises(InvalidIdError):
        addresses.unicast("poll", group, "pid")


def test_uppercase_is_refused_rather_than_folded() -> None:
    """The server lowercases the URL host, so an uppercase group cannot round trip.

    Folding it silently here would produce a valid-looking address pointing at
    a group nobody listens on -- the exact silent drop this module prevents.
    """
    with pytest.raises(InvalidIdError, match="lowercase"):
        addresses.unicast("poll", "Workers", "7f3a")

    with pytest.raises(InvalidIdError, match="lowercase"):
        addresses.resolve_target("poll", "Workers")


def test_an_empty_pid_is_refused() -> None:
    with pytest.raises(InvalidIdError):
        addresses.unicast("poll", "workers", "")
