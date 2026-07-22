from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest import mock

import aiohttp
import msgspec
import pytest

from resonate.error import HttpError
from resonate.network import HttpNetwork, LocalNetwork
from resonate.network.http import DEFAULT_CONN_LIMIT
from resonate.network.local import I64_MAX

# -- helpers ------------------------------------------------------------------


async def send(net: LocalNetwork, req: Any) -> Any:
    """Encode ``req``, send it through ``net``, and decode the response."""
    resp = await net.send(msgspec.json.encode(req).decode("utf-8"))
    return msgspec.json.decode(resp)


def status(resp: Any) -> int:
    """Extract ``head.status`` from an envelope response."""
    head = resp.get("head") if isinstance(resp, dict) else None
    if isinstance(head, dict) and isinstance(head.get("status"), int):
        return head["status"]
    return 0


def data(resp: Any) -> Any:
    """Extract the ``data`` portion from an envelope response."""
    if isinstance(resp, dict) and "data" in resp:
        return resp["data"]
    return resp


# -- tests --------------------------------------------------------------------


def test_local_network_creates_and_gets_promise() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="test-pid", group="default")
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "id": "p1",
                "timeoutAt": I64_MAX,
                "param": {"data": "test"},
                "tags": {"resonate:scope": "global"},
            },
        }
        resp = await send(net, req)
        assert status(resp) in (200, 201)
        assert data(resp)["promise"]["id"] == "p1"
        assert data(resp)["promise"]["state"] == "pending"

        get_req = {
            "kind": "promise.get",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {"id": "p1"},
        }
        get_resp = await send(net, get_req)
        assert status(get_resp) == 200
        assert data(get_resp)["promise"]["id"] == "p1"

    asyncio.run(run())


def test_local_network_idempotent_promise_create() -> None:
    async def run() -> None:
        net = LocalNetwork()
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {"id": "p1", "timeoutAt": I64_MAX, "param": {}, "tags": {}},
        }
        r1 = await send(net, req)
        assert status(r1) in (200, 201)

        r2 = await send(net, req)
        assert status(r2) == 200
        assert data(r2)["promise"]["id"] == "p1"

    asyncio.run(run())


def test_local_network_task_create_and_fulfill() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")
        req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "p1",
                        "timeoutAt": I64_MAX,
                        "param": {"data": "test"},
                        "tags": {},
                    },
                },
            },
        }
        resp = await send(net, req)
        assert status(resp) in (200, 201)
        assert data(resp)["task"]["state"] == "acquired"
        assert data(resp)["promise"]["id"] == "p1"

        task_id = data(resp)["task"]["id"]
        fulfill = {
            "kind": "task.fulfill",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": task_id,
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": {"corrId": "c2a", "version": "2025-01-15"},
                    "data": {
                        "id": "p1",
                        "state": "resolved",
                        "value": {"data": "result"},
                    },
                },
            },
        }
        f_resp = await send(net, fulfill)
        assert status(f_resp) == 200

    asyncio.run(run())


def test_task_fence_rejects_wrong_version() -> None:
    """task.fence is gated on the task lease: a stale version is a 409 no-op."""

    async def run() -> None:
        net = LocalNetwork(pid="pid1")
        # Create + acquire a task at version 0.
        create = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {"id": "root", "timeoutAt": I64_MAX, "tags": {}},
                },
            },
        }
        resp = await send(net, create)
        version = data(resp)["task"]["version"]
        assert version == 0

        def fence(task_version: int, child_id: str) -> dict[str, Any]:
            return {
                "kind": "task.fence",
                "head": {"corrId": "f", "version": "2025-01-15"},
                "data": {
                    "id": "root",
                    "version": task_version,
                    "action": {
                        "kind": "promise.create",
                        "head": {"corrId": "fa", "version": "2025-01-15"},
                        "data": {"id": child_id, "timeoutAt": I64_MAX, "tags": {}},
                    },
                },
            }

        # Wrong version -> 409, and the child promise is NOT created.
        bad = await send(net, fence(version + 1, "child-bad"))
        assert status(bad) == 409
        assert "child-bad" not in net.state.promises

        # Unknown task -> 404.
        missing = await send(
            net,
            {
                "kind": "task.fence",
                "head": {"corrId": "f2", "version": "2025-01-15"},
                "data": {
                    "id": "nope",
                    "version": 0,
                    "action": {
                        "kind": "promise.create",
                        "head": {"corrId": "f2a", "version": "2025-01-15"},
                        "data": {"id": "child-x", "timeoutAt": I64_MAX, "tags": {}},
                    },
                },
            },
        )
        assert status(missing) == 404

        # Correct version -> 200, child promise created.
        ok = await send(net, fence(version, "child-ok"))
        assert status(ok) == 200
        assert data(ok)["action"]["data"]["promise"]["id"] == "child-ok"
        assert "child-ok" in net.state.promises

    asyncio.run(run())


def test_local_network_identity() -> None:
    net = LocalNetwork(pid="mypid", group="mygroup")
    assert net.pid() == "mypid"
    assert net.group() == "mygroup"
    assert net.unicast() == "local://uni@mygroup/mypid"
    assert net.anycast() == "local://any@mygroup/mypid"
    assert net.target_resolver("target") == "local://any@target"


def test_promise_create_with_target_creates_task_and_dispatches_execute() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")
        req = {
            "kind": "promise.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "id": "rpc-1",
                "timeoutAt": I64_MAX,
                "param": {"data": "test"},
                "tags": {
                    "resonate:target": "local://any@hello",
                    "resonate:scope": "global",
                },
            },
        }
        resp = await send(net, req)
        assert data(resp)["promise"]["state"] == "pending"

        # The task should exist in pending state.
        task = net.state.tasks["rpc-1"]
        assert task.state == "pending"
        assert task.id == "rpc-1"

    asyncio.run(run())


def test_task_suspend_registers_awaiters_and_suspends() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create a task (acquired).
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create a child promise (pending, represents an RPC dependency).
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": "child-1",
                "timeoutAt": I64_MAX,
                "tags": {"resonate:target": "local://any@hello"},
            },
        }
        await send(net, child_req)

        # Suspend the parent task waiting on child.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c3a", "version": "2025-01-15"},
                        "data": {"awaited": "child-1", "awaiter": "parent"},
                    }
                ],
            },
        }
        resp = await send(net, suspend_req)
        assert status(resp) == 200

        assert net.state.tasks["parent"].state == "suspended"
        assert "parent" in net.state.promises["child-1"].awaiters

    asyncio.run(run())


def test_settling_child_resumes_suspended_parent() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create parent task.
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create child promise.
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {
                "id": "child",
                "timeoutAt": I64_MAX,
                "tags": {"resonate:target": "local://any@hello"},
            },
        }
        await send(net, child_req)

        # Suspend parent on child.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c3a", "version": "2025-01-15"},
                        "data": {"awaited": "child", "awaiter": "parent"},
                    }
                ],
            },
        }
        await send(net, suspend_req)

        # Acquire child task then fulfill it.
        acquire_req = {
            "kind": "task.acquire",
            "head": {"corrId": "c4", "version": "2025-01-15"},
            "data": {"id": "child", "version": 0, "pid": "pid1", "ttl": 60000},
        }
        await send(net, acquire_req)

        fulfill_req = {
            "kind": "task.fulfill",
            "head": {"corrId": "c5", "version": "2025-01-15"},
            "data": {
                "id": "child",
                "version": 0,
                "action": {
                    "kind": "promise.settle",
                    "head": {"corrId": "c5a", "version": "2025-01-15"},
                    "data": {
                        "id": "child",
                        "state": "resolved",
                        "value": {"data": "hello"},
                    },
                },
            },
        }
        await send(net, fulfill_req)

        # Parent should be resumed (pending, version incremented).
        parent_task = net.state.tasks["parent"]
        assert parent_task.state == "pending"
        assert parent_task.version == 1

    asyncio.run(run())


def test_task_suspend_redirect_when_dependency_already_settled() -> None:
    async def run() -> None:
        net = LocalNetwork(pid="pid1")

        # Create parent task.
        create_req = {
            "kind": "task.create",
            "head": {"corrId": "c1", "version": "2025-01-15"},
            "data": {
                "pid": "pid1",
                "ttl": 60000,
                "action": {
                    "kind": "promise.create",
                    "head": {"corrId": "c1a", "version": "2025-01-15"},
                    "data": {
                        "id": "parent",
                        "timeoutAt": I64_MAX,
                        "tags": {"resonate:target": "local://any@wf"},
                    },
                },
            },
        }
        await send(net, create_req)

        # Create and immediately settle child promise.
        child_req = {
            "kind": "promise.create",
            "head": {"corrId": "c2", "version": "2025-01-15"},
            "data": {"id": "child", "timeoutAt": I64_MAX, "param": {}, "tags": {}},
        }
        await send(net, child_req)

        settle_req = {
            "kind": "promise.settle",
            "head": {"corrId": "c3", "version": "2025-01-15"},
            "data": {"id": "child", "state": "resolved", "value": {"data": "ok"}},
        }
        await send(net, settle_req)

        # Suspend parent on already-settled child -> should get redirect.
        suspend_req = {
            "kind": "task.suspend",
            "head": {"corrId": "c4", "version": "2025-01-15"},
            "data": {
                "id": "parent",
                "version": 0,
                "actions": [
                    {
                        "kind": "promise.register_callback",
                        "head": {"corrId": "c4a", "version": "2025-01-15"},
                        "data": {"awaited": "child", "awaiter": "parent"},
                    }
                ],
            },
        }
        resp = await send(net, suspend_req)
        assert status(resp) == 300

    asyncio.run(run())


def test_http_network_identity() -> None:
    net = HttpNetwork(
        "http://localhost:8001",
        pid="mypid",
        group="mygroup",
    )
    assert net.pid() == "mypid"
    assert net.group() == "mygroup"
    assert net.unicast() == "poll://uni@mygroup/mypid"
    assert net.anycast() == "poll://any@mygroup/mypid"


def test_http_network_match_returns_poll_anycast() -> None:
    net = HttpNetwork("http://localhost:8001")
    assert net.target_resolver("my-target") == "poll://any@my-target"


def test_http_network_strips_trailing_slash() -> None:
    net = HttpNetwork("http://localhost:8001/", pid="pid")
    assert net._url == "http://localhost:8001"


def test_http_network_default_group() -> None:
    net = HttpNetwork("http://localhost:8001", pid="pid1")
    assert net.group() == "default"
    assert net.unicast() == "poll://uni@default/pid1"


@pytest.mark.asyncio
async def test_http_session_connector_limit_above_aiohttp_default() -> None:
    """The shared session raises the connector cap above aiohttp's default 100.

    A saturated 100-connection pool is what lets the periodic heartbeat queue
    behind execution traffic until leases lapse; the higher cap (paired with a
    bounded execution concurrency) keeps a connection free for the heartbeat.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid")
    await net.start()
    try:
        session = net._ensure_session()
        assert net._conn_limit == DEFAULT_CONN_LIMIT
        assert DEFAULT_CONN_LIMIT > 100
        assert session.connector
        assert session.connector.limit == DEFAULT_CONN_LIMIT
    finally:
        await net.stop()


@pytest.mark.asyncio
async def test_http_session_connector_limit_override() -> None:
    net = HttpNetwork("http://localhost:8001", pid="pid", conn_limit=7)
    await net.start()
    try:
        conn = net._ensure_session().connector
        assert conn
        assert conn.limit == 7
    finally:
        await net.stop()


# ---------------------------------------------------------------------------
# Resilience: HttpNetwork.send must survive a server outage and recover.
# Mirrors the existing _sse_loop retry-with-backoff -- the request half had
# none, so any task.create / promise.create / task.fulfill / promise.settle
# in flight when the server died (or before it came up) would propagate
# HttpError and strand the awaiting handle. See resonate.network._http.send.
# ---------------------------------------------------------------------------


class _FlakySession:
    """Minimal ``aiohttp.ClientSession`` stand-in that fails N times then succeeds.

    The real session is built from ``_ensure_session`` and held on the network;
    monkey-patching that one factory keeps the rest of ``send`` exercised
    (header assembly, response decoding, the retry loop itself).
    """

    def __init__(self, fail_times: int, body: str = '{"ok": true}') -> None:
        self.fail_times = fail_times
        self.body = body
        self.attempts = 0

    def post(self, *_args: object, **_kwargs: object) -> _FlakySession._Ctx:
        self.attempts += 1
        if self.attempts <= self.fail_times:
            return _FlakySession._Ctx(error=aiohttp.ClientConnectionError("down"))
        return _FlakySession._Ctx(body=self.body)

    class _Ctx:
        def __init__(
            self, body: str | None = None, error: Exception | None = None
        ) -> None:
            self._body = body
            self._error = error

        async def __aenter__(self) -> _FlakySession._Resp:
            if self._error is not None:
                raise self._error
            assert self._body is not None
            return _FlakySession._Resp(self._body)

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _Resp:
        def __init__(self, body: str) -> None:
            self._body = body

        async def text(self) -> str:
            return self._body


@pytest.mark.asyncio
async def test_http_send_retries_through_connection_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``send`` must retry on ``aiohttp.ClientError`` and recover.

    Reproduces ``resonate dev`` not yet running when the client makes a
    request: without retry, the first ``task.create`` raises ``HttpError``,
    the bg task aborts, and ``handle.result()`` hangs forever.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    flaky = _FlakySession(fail_times=3, body='{"head":{"status":200},"data":{}}')
    monkeypatch.setattr(net, "_ensure_session", lambda: flaky)
    # Mark as ``started`` so the retry loop does not exit on ``not _running``.
    net._running = True
    # Collapse the backoff sleep so the test stays fast.
    monkeypatch.setattr(net, "_sleep_or_stop", lambda _s: asyncio.sleep(0))

    body = await net.send("{}")
    assert body == '{"head":{"status":200},"data":{}}'
    assert flaky.attempts == 4  # three failures + one success


@pytest.mark.asyncio
async def test_http_send_stops_retrying_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``stop`` must unblock a ``send`` parked in the retry backoff.

    Without ``_stop_event``, a request retrying through an outage would block
    the bounded join inside ``Resonate.stop`` (which only awaits bg tasks; it
    does not cancel them).
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    # Permanently failing session: only stop can break the loop.
    flaky = _FlakySession(fail_times=10_000)
    monkeypatch.setattr(net, "_ensure_session", lambda: flaky)
    net._running = True

    send_task = asyncio.create_task(net.send("{}"))
    # Let the retry enter its first real backoff sleep.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    await net.stop()  # signals _stop_event and flips _running

    with pytest.raises(HttpError):
        await asyncio.wait_for(send_task, timeout=2.0)


@pytest.mark.asyncio
async def test_http_send_after_stop_raises_http_error_not_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``send`` racing with ``stop`` must surface :class:`HttpError`.

    Reproduces the Ctrl+C shutdown race: ``Resonate.stop`` closes the
    aiohttp session by design *before* joining bg tasks, so an in-flight
    ``session.post`` raises a bare ``RuntimeError("Session is closed")``.
    Without the catch in :meth:`HttpNetwork.send`, that ``RuntimeError``
    propagates out of every untracked ``ctx.run`` ``bg()`` task and asyncio
    prints a ``Task exception was never retrieved`` traceback for each.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")

    class _ClosedSession:
        def post(self, *_args: object, **_kwargs: object) -> _ClosedSession._Ctx:
            return _ClosedSession._Ctx()

        async def close(self) -> None:
            pass

        class _Ctx:
            async def __aenter__(self) -> object:
                msg = "Session is closed"
                raise RuntimeError(msg)

            async def __aexit__(self, *_exc: object) -> None:
                return None

    monkeypatch.setattr(net, "_ensure_session", _ClosedSession)
    # send() must observe ``_running == False`` while the in-flight post
    # raises -- that is exactly the shutdown race we want to model.
    net._running = False

    with pytest.raises(HttpError):
        await net.send("{}")


@pytest.mark.asyncio
async def test_http_send_does_not_open_a_session_after_stop() -> None:
    """A post-stop ``send`` must not lazily open a fresh ``ClientSession``.

    Otherwise a retry loop racing with shutdown leaks a session that nobody
    will close (aiohttp's ``Unclosed client session`` warning at process
    exit).
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    await net.start()
    await net.stop()  # closes the session, sets _running=False

    assert net._session is None

    with pytest.raises(HttpError):
        await net.send("{}")

    # No session was created during the failed ``send``.
    assert net._session is None


@pytest.mark.asyncio
async def test_http_send_does_not_retry_server_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP responses (200/404/500/…) bypass the retry loop.

    Only ``aiohttp.ClientError`` -- the "server unreachable" signal -- is
    retried. A 404/500 body is a deliberate response: ``ServerError`` must
    propagate unchanged so callers see it on the first attempt.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    not_found = _FlakySession(
        fail_times=0,
        body='{"head":{"status":404},"data":{"error":"nope"}}',
    )
    monkeypatch.setattr(net, "_ensure_session", lambda: not_found)
    net._running = True
    monkeypatch.setattr(net, "_sleep_or_stop", mock.AsyncMock())

    body = await net.send("{}")
    assert '"status":404' in body
    assert not_found.attempts == 1  # one shot -- no retry on a real HTTP response


@pytest.mark.asyncio
async def test_http_send_before_start_does_not_raise_stopped_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``send`` must not raise "network has been stopped" before ``start`` fires.

    Reproduces the startup race in ``Resonate.__init__``: ``net.start()`` is
    scheduled via ``asyncio.create_task`` but the event loop has not yielded
    yet when user code calls an API (e.g. ``resonate.schedule()``). Before
    this fix, ``send``'s ``if not self._running`` guard fired immediately,
    raising a misleading ``HttpError("network has been stopped")`` even though
    ``stop()`` was never called. The fix adds a ``_stopped`` flag that is only
    set by ``stop()``, so "not yet started" and "explicitly stopped" are
    distinct states.

    This test patches ``_ensure_session`` to return a fake session that
    succeeds immediately, so the request goes through without a real server.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    ok_session = _FlakySession(fail_times=0, body='{"head":{"status":200},"data":{}}')
    monkeypatch.setattr(net, "_ensure_session", lambda: ok_session)

    # Simulate what Resonate.__init__ does: schedule start() as a background
    # task without yielding to the event loop.
    start_task = asyncio.create_task(net.start())
    assert not net._running, "start() has not run yet -- _running must still be False"
    assert not net._stopped, "_stopped must be False before stop() is ever called"

    # send() must NOT raise "network has been stopped" here.
    body = await net.send("{}")
    assert body == '{"head":{"status":200},"data":{}}'

    start_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await start_task


@pytest.mark.asyncio
async def test_http_send_after_stop_raises_even_if_never_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``send`` must raise ``HttpError`` after ``stop()``, even without a prior ``start()``.

    ``stop()`` sets ``_stopped = True``; the guard in ``send()`` keys on
    ``_stopped``, so an explicitly stopped network is always refused regardless
    of whether ``start()`` was ever called.
    """
    net = HttpNetwork("http://localhost:8001", pid="pid", group="g")
    ok_session = _FlakySession(fail_times=0, body='{"head":{"status":200},"data":{}}')
    monkeypatch.setattr(net, "_ensure_session", lambda: ok_session)

    # Stop without ever starting.
    await net.stop()
    assert net._stopped

    with pytest.raises(HttpError):
        await net.send("{}")
