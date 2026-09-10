"""Constructing a client, without a network and without the process environment.

Two changes make this file possible.

**``env`` is a parameter.** :func:`~resonate.resonate._resolve_env_url` used to
read :data:`os.environ` directly, so testing it meant ``monkeypatch.setenv`` --
process-wide mutation, hostile to parallel runs, and a live ``RESONATE_URL`` in
a developer's shell could silently redirect a "local" test at a real server.
It now takes a mapping, so it is a pure function called with a dict.

**Starting is separate from building.** ``__init__`` used to open connections
and spawn background tasks, so it required a running event loop and did IO. With
``autostart=False`` the wiring can be inspected with no loop at all.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, cast

import pytest
from resonate_testing import FakeNetwork, FakeSource

from resonate.connections import HttpConnection, LocalConnection, SSEConnection
from resonate.heartbeat import AsyncHeartbeat, NoopHeartbeat
from resonate.resonate import Resonate, _resolve_env_url
from resonate.testing import FakeClock, instant_sleeper, local_resonate
from resonate.types import Args

# ═══════════════════════════════════════════════════════════════
#  URL resolution -- a pure function over a mapping
# ═══════════════════════════════════════════════════════════════


def test_no_relevant_variables_resolves_to_nothing() -> None:
    assert _resolve_env_url({}) is None
    assert _resolve_env_url({"PATH": "/usr/bin", "HOME": "/root"}) is None


def test_resonate_url_wins_outright() -> None:
    env = {
        "RESONATE_URL": "http://explicit:9000",
        "RESONATE_HOST": "ignored",
        "RESONATE_PORT": "1234",
    }
    assert _resolve_env_url(env) == "http://explicit:9000"


def test_host_is_assembled_with_the_default_scheme_and_port() -> None:
    assert _resolve_env_url({"RESONATE_HOST": "server"}) == "http://server:8001"


def test_scheme_and_port_override_the_defaults() -> None:
    env = {
        "RESONATE_HOST": "server",
        "RESONATE_SCHEME": "https",
        "RESONATE_PORT": "443",
    }
    assert _resolve_env_url(env) == "https://server:443"


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_url_falls_through_to_the_host_form(blank: str) -> None:
    """Empty is not a URL -- an unset-but-exported variable must not win."""
    env = {"RESONATE_URL": blank.strip(), "RESONATE_HOST": "server"}
    assert _resolve_env_url(env) == "http://server:8001"


def test_a_blank_host_resolves_to_nothing() -> None:
    assert _resolve_env_url({"RESONATE_HOST": ""}) is None


def test_resolution_reads_only_its_argument() -> None:
    """No process state: the same mapping always yields the same answer."""
    env = {"RESONATE_HOST": "a"}
    assert _resolve_env_url(env) == _resolve_env_url(dict(env))


# ═══════════════════════════════════════════════════════════════
#  Construction without starting
# ═══════════════════════════════════════════════════════════════


def test_wiring_can_be_built_with_no_event_loop_running() -> None:
    """The headline: construction does no IO, so it needs no loop.

    Calling this outside ``asyncio.run`` would previously raise from
    ``asyncio.create_task``.
    """
    client = Resonate(autostart=False, env={})

    assert isinstance(client._network, LocalConnection)
    assert client._core is not None
    assert client._runtime.started is False
    assert client._runtime.bg_tasks == set()
    assert client._runtime.refresh_handle is None


def test_registration_works_before_starting() -> None:
    client = Resonate(autostart=False, env={})

    @client.register
    def greet(ctx: object, name: str) -> str:
        return f"hello {name}"

    assert client._registry.get("greet", 1) is not None


def test_a_url_selects_the_http_and_sse_pair_without_connecting() -> None:
    client = Resonate(url="http://server:8001/", autostart=False, env={})
    network = client._network

    assert isinstance(network, HttpConnection)
    assert isinstance(client._source, SSEConnection)
    # Trailing slash stripped, and nothing was dialled.
    assert network._url == "http://server:8001"


def test_env_supplies_the_url_when_none_is_passed() -> None:
    client = Resonate(autostart=False, env={"RESONATE_URL": "http://from-env:7000"})
    network = client._network
    assert isinstance(network, HttpConnection)
    assert network._url == "http://from-env:7000"


def test_an_explicit_url_beats_the_environment() -> None:
    client = Resonate(
        url="http://explicit:1",
        autostart=False,
        env={"RESONATE_URL": "http://from-env:2"},
    )
    network = client._network
    assert isinstance(network, HttpConnection)
    assert network._url == "http://explicit:1"


def test_an_empty_env_isolates_the_test_from_the_shell() -> None:
    """``env={}`` is what keeps a local-mode test local.

    Without it, a ``RESONATE_URL`` exported in a developer's shell turns every
    "local" test into an integration test against whatever is on that port.
    """
    client = Resonate(autostart=False, env={})
    assert isinstance(client._network, LocalConnection)


def test_a_token_is_read_from_the_injected_env() -> None:
    client = Resonate(
        url="http://s:1", autostart=False, env={"RESONATE_TOKEN": "secret"}
    )
    assert client._sender.auth == "secret"


def test_an_explicit_token_beats_the_environment() -> None:
    client = Resonate(
        url="http://s:1",
        token="explicit",  # noqa: S106
        autostart=False,
        env={"RESONATE_TOKEN": "from-env"},
    )
    assert client._sender.auth == "explicit"


def test_construction_validates_the_protocols_before_any_loop_exists() -> None:
    """The fail-fast guard runs at build time, not inside a background task."""
    with pytest.raises(TypeError, match="missing: send"):
        Resonate(network=cast("Any", FakeSource()), autostart=False, env={})


def test_heartbeat_selection_is_a_construction_time_decision() -> None:
    local = Resonate(autostart=False, env={})
    remote = Resonate(network=FakeNetwork(), autostart=False, env={})

    assert isinstance(local._heartbeat, NoopHeartbeat)
    assert isinstance(remote._heartbeat, AsyncHeartbeat)


# ═══════════════════════════════════════════════════════════════
#  Starting
# ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_start_opens_connections_and_spawns_the_refresh_loop() -> None:
    source = FakeSource()
    client = Resonate(
        network=FakeNetwork(),
        sources=[source],
        autostart=False,
        env={},
        sleeper=instant_sleeper,
    )
    assert not source.started

    client.start()
    await asyncio.sleep(0)

    assert source.started
    assert client._runtime.refresh_handle is not None
    await client.stop()


@pytest.mark.asyncio
async def test_start_is_idempotent() -> None:
    source = FakeSource()
    client = Resonate(
        network=FakeNetwork(),
        sources=[source],
        autostart=False,
        env={},
        sleeper=instant_sleeper,
    )

    client.start()
    first_refresh = client._runtime.refresh_handle
    client.start()
    await asyncio.sleep(0)

    # One recv registration, one refresh loop -- not two.
    assert len(source.callbacks) == 1
    assert client._runtime.refresh_handle is first_refresh
    await client.stop()


@pytest.mark.asyncio
async def test_start_returns_self_for_chaining() -> None:
    client = Resonate(autostart=False, env={}, sleeper=instant_sleeper)
    assert client.start() is client
    await client.stop()


@pytest.mark.asyncio
async def test_autostart_is_the_default_and_starts_immediately() -> None:
    client = Resonate(env={}, sleeper=instant_sleeper)
    try:
        assert client._runtime.started is True
        assert client._runtime.refresh_handle is not None
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_stopping_a_never_started_client_is_harmless() -> None:
    client = Resonate(autostart=False, env={})
    await client.stop()


# ═══════════════════════════════════════════════════════════════
#  Injected time reaches the wiring
# ═══════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_the_injected_clock_dates_top_level_promises() -> None:
    """A frozen clock makes ``timeoutAt`` an exact number instead of "about now".

    Previously this deadline came from the module-level ``now_ms``, so no test
    could assert it and every fixture used a far-future sentinel instead.
    """
    clock = FakeClock(start=1_000_000)
    client = local_resonate(clock=clock, autostart=False)

    req = client._build_root_promise_create_req(
        "wf-1", "greet", Args(), 1, timedelta(seconds=30), None
    )

    assert req.timeout_at == 1_000_000 + 30_000
    await client.stop()


@pytest.mark.asyncio
async def test_advancing_the_clock_moves_the_next_deadline() -> None:
    clock = FakeClock(start=1_000_000)
    client = local_resonate(clock=clock, autostart=False)

    first = client._build_root_promise_create_req(
        "a", "greet", Args(), 1, timedelta(seconds=10), None
    )
    clock.advance(seconds=5)
    second = client._build_root_promise_create_req(
        "b", "greet", Args(), 1, timedelta(seconds=10), None
    )

    assert second.timeout_at - first.timeout_at == 5_000
    await client.stop()


@pytest.mark.asyncio
async def test_the_refresh_interval_is_configurable() -> None:
    client = Resonate(
        autostart=False, env={}, sleeper=instant_sleeper, subscription_refresh_secs=0.5
    )
    assert client._refresh_secs == 0.5
    await client.stop()
