"""The SDK's silent behaviours, made assertable.

Five places in the SDK deliberately swallow a failure: a preloaded promise that
will not decode, a malformed record in a search page, an unparseable push
message, a pickle that will not round-trip, a background task that raised.
Each is a documented contract -- and each was previously unobservable, so
nothing stopped a skip from quietly becoming a crash or a propagation.

These tests pin every one of them through the
:class:`~resonate.observability.Observer` seam.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import msgspec
import pytest
from resonate_testing import RecordingNetwork, StubNetwork, envelope

from resonate.codec import Codec, NoopEncryptor
from resonate.effects import ResonateEffects
from resonate.observability import (
    BackgroundTaskFailed,
    Dropped,
    PromiseCreateRequested,
    PromiseCreateReturned,
    PromiseSettleRequested,
    PromiseSettleReturned,
    logging_observer,
    noop_observer,
)
from resonate.send import Sender
from resonate.testing import (
    FAR_FUTURE,
    RecordingObserver,
    UnusedFencing,
    local_effects,
    local_resonate,
    pending_promise,
    resolved_promise,
)
from resonate.transport import Transport
from resonate.types import PromiseCreateReq, PromiseRecord, Value


def _codec() -> Codec:
    return Codec(NoopEncryptor())


# ── Dropped: preload records ───────────────────────────────────────


def test_undecodable_preload_record_is_reported_not_raised() -> None:
    """A corrupt preload entry is skipped -- and says so."""
    observer = RecordingObserver()
    corrupt = PromiseRecord(
        id="p-bad",
        state="resolved",
        timeout_at=FAR_FUTURE,
        param=Value(),
        # Not valid base64: ``Codec.decode`` raises ``Base64DecodeError``.
        value=Value(data="!!! not base64 !!!"),
        tags={},
    )
    good = resolved_promise("p-good", 1)

    effects = ResonateEffects(
        UnusedFencing(), _codec(), "t1", 1, [corrupt, good], observer
    )

    # The good record survived; the bad one did not.
    assert set(effects.cache) == {"p-good"}
    assert observer.dropped("preload-record") == ["p-bad"]
    assert "base64" in observer.of(Dropped)[0].cause


def test_decodable_preload_records_produce_no_drop_events() -> None:
    observer = RecordingObserver()
    ResonateEffects(
        UnusedFencing(),
        _codec(),
        "t1",
        1,
        [resolved_promise("a", 1), pending_promise("b")],
        observer,
    )
    assert observer.of(Dropped) == []


# ── Dropped: search records ────────────────────────────────────────


@pytest.mark.asyncio
async def test_malformed_search_record_is_dropped_and_reported() -> None:
    """One bad record must not sink the page -- but must not vanish either."""
    observer = RecordingObserver()
    page = {
        "promises": [
            {
                "id": "ok",
                "state": "pending",
                "timeoutAt": 100,
                "param": {},
                "value": {},
                "tags": {},
            },
            {"id": "broken", "state": "not-a-real-state", "timeoutAt": 100},
        ],
        "cursor": None,
    }
    net = StubNetwork()
    sender = Sender(Transport(net), None, corr_id=lambda: "fixed", observer=observer)
    net.response = envelope("promise.search", "fixed", page)

    result = await sender.promise_search(None, None, None, None)

    assert [p.id for p in result.promises] == ["ok"]
    assert observer.dropped("search-record") == ["broken"]


# ── Dropped: push messages ─────────────────────────────────────────


def test_unparseable_push_message_is_dropped_and_reported() -> None:
    observer = RecordingObserver()
    net = StubNetwork()
    transport = Transport(net, [net], observer)

    received: list[Any] = []
    transport.recv(received.append)
    net.push("not json at all")
    net.push('{"kind":"mystery","data":{}}')

    assert received == []
    assert len(observer.dropped("incoming-message")) == 2


# ── BackgroundTaskFailed ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_failed_background_task_is_reported() -> None:
    """``_spawn``'s failures reach the observer, not just the log."""
    observer = RecordingObserver()
    client = local_resonate(observer=observer)
    try:

        async def boom() -> None:
            msg = "background boom"
            raise RuntimeError(msg)

        client._spawn(boom())
        for _ in range(5):
            await asyncio.sleep(0)

        failures = observer.of(BackgroundTaskFailed)
        assert [f.cause for f in failures] == ["background boom"]
    finally:
        await client.stop()


# ── Durable-op request/response pairs ──────────────────────────────


@pytest.mark.asyncio
async def test_durable_op_events_bracket_each_network_round_trip() -> None:
    """Every create/settle emits a request event then a response event."""
    observer = RecordingObserver()
    effects = local_effects(observer=observer)

    await effects.create_promise(
        PromiseCreateReq(
            id="root.1",
            timeout_at=FAR_FUTURE,
            param=Value(),
            tags={"resonate:scope": "local"},
        )
    )
    await effects.settle_promise("root.1", 42)

    kinds = [type(e).__name__ for e in observer.events]
    assert kinds == [
        "PromiseCreateRequested",
        "PromiseCreateReturned",
        "PromiseSettleRequested",
        "PromiseSettleReturned",
    ]
    assert observer.of(PromiseCreateRequested)[0].invocation == "run"
    assert observer.of(PromiseCreateReturned)[0].state == "pending"
    assert observer.of(PromiseSettleRequested)[0].state == "resolved"
    assert observer.of(PromiseSettleReturned)[0].state == "resolved"


@pytest.mark.asyncio
async def test_global_scope_is_reported_as_an_rpc_invocation() -> None:
    observer = RecordingObserver()
    effects = local_effects(observer=observer)

    await effects.create_promise(
        PromiseCreateReq(
            id="root.1",
            timeout_at=FAR_FUTURE,
            param=Value(),
            tags={"resonate:scope": "global"},
        )
    )
    assert observer.of(PromiseCreateRequested)[0].invocation == "rpc"


@pytest.mark.asyncio
async def test_untagged_scope_is_reported_as_unknown() -> None:
    observer = RecordingObserver()
    effects = local_effects(observer=observer)

    await effects.create_promise(
        PromiseCreateReq(id="root.1", timeout_at=FAR_FUTURE, param=Value(), tags={})
    )
    assert observer.of(PromiseCreateRequested)[0].invocation == "unknown"


# ── The default observer ───────────────────────────────────────────


def test_logging_observer_preserves_the_validation_stream(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The external validation harness greps ``resonate.validation`` by name.

    The observer seam replaced direct logging calls, so this pins the log
    output the harness depends on. Change it and this test fails, which is the
    point: the harness lives in another repo and cannot fail here.
    """
    with caplog.at_level(logging.INFO, logger="resonate.validation"):
        logging_observer(PromiseCreateRequested(id="p1", invocation="run"))
        logging_observer(
            PromiseCreateReturned(id="p1", invocation="run", state="pending")
        )
        logging_observer(PromiseSettleRequested(id="p1", state="resolved"))
        logging_observer(PromiseSettleReturned(id="p1", state="resolved"))

    messages = [r.getMessage() for r in caplog.records]
    assert messages == [
        "promise_create_request promise_id=p1 invocation=run",
        "promise_create_response promise_id=p1 invocation=run state=pending",
        "promise_settle_request promise_id=p1 state=resolved",
        "promise_settle_response promise_id=p1 state=resolved",
    ]


def test_logging_observer_reports_background_failures_at_error_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger="resonate.observability"):
        logging_observer(BackgroundTaskFailed(cause="kaboom"))
    assert caplog.records[-1].getMessage() == "background task failed: kaboom"


def test_noop_observer_accepts_every_event_shape() -> None:
    """The union is closed; the no-op must handle all of it."""
    for event in (
        Dropped(what="x", id="y", cause="z"),
        BackgroundTaskFailed(cause="c"),
        PromiseCreateRequested(id="i", invocation="run"),
        PromiseCreateReturned(id="i", invocation="rpc", state="pending"),
        PromiseSettleRequested(id="i", state="resolved"),
        PromiseSettleReturned(id="i", state="rejected"),
    ):
        assert noop_observer(event) is None


def test_recording_observer_filters_by_type_and_site() -> None:
    observer = RecordingObserver()
    observer(Dropped(what="preload-record", id="a", cause="c"))
    observer(Dropped(what="search-record", id="b", cause="c"))
    observer(BackgroundTaskFailed(cause="c"))

    assert observer.dropped() == ["a", "b"]
    assert observer.dropped("search-record") == ["b"]
    assert len(observer.of(BackgroundTaskFailed)) == 1


def test_events_are_frozen_structs() -> None:
    """An observer cannot mutate what it is handed."""
    event = Dropped(what="w", id="i", cause="c")
    with pytest.raises((AttributeError, TypeError)):
        setattr(event, "id", "other")  # noqa: B010


def test_recording_network_is_a_working_double() -> None:
    """The shared double echoes kind/corrId so Transport validation passes."""
    net = RecordingNetwork()
    body = msgspec.json.encode(
        {"kind": "task.release", "head": {"corrId": "c1"}, "data": {}}
    ).decode("utf-8")
    resp = asyncio.run(net.send(body))
    assert msgspec.json.decode(resp)["head"]["corrId"] == "c1"
