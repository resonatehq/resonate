"""Parsing at the edge: one decode, then trusted values all the way down.

The response path used to hand ``Any`` from :meth:`Transport.send` to
:class:`Sender`, which then re-asked the same shape questions at every call
site -- ``isinstance(resp, dict)``, ``isinstance(head, dict)``,
``isinstance(status, int)``. Those guards were the whole of ``send.py``'s
coverage gap: unreachable except through a malformed server, so nobody wrote a
test, so they were dead weight *and* unverified.

Now the envelope is decoded once into :class:`~resonate.transport.Response`,
and the per-operation ``data`` shapes are decoded once each into typed pages.
This module pins that boundary: what it accepts, what it defaults, and what it
rejects.
"""

from __future__ import annotations

import pytest
from resonate_testing import StubNetwork, envelope

from resonate.error import DecodingError, ServerError
from resonate.send import Sender, default_corr_id
from resonate.testing import RecordingObserver
from resonate.transport import Response, ResponseHead, Transport
from resonate.types import PromiseCreateReq, Value

FAR = (1 << 62) - 1


def _sender(response: str, observer: RecordingObserver | None = None) -> Sender:
    net = StubNetwork(response)
    return Sender(
        Transport(net),
        None,
        corr_id=lambda: "fixed",
        observer=observer if observer is not None else RecordingObserver(),
    )


def _promise(id: str = "p1", state: str = "pending", **extra: object) -> dict:
    return {
        "id": id,
        "state": state,
        "timeoutAt": FAR,
        "param": {},
        "value": {},
        "tags": {},
        **extra,
    }


# ── The envelope ───────────────────────────────────────────────────


def test_response_defaults_every_field() -> None:
    """A bare ``{}`` is a valid envelope meaning "200, no echo, no data"."""
    resp = Response()
    assert resp.kind == ""
    assert resp.head == ResponseHead()
    assert resp.head.status == 200
    assert resp.head.corr_id == ""
    assert resp.data == {}


@pytest.mark.asyncio
async def test_status_is_read_off_a_typed_field() -> None:
    transport = Transport(StubNetwork(envelope("k", "c", {}, status=204)))
    resp = await transport.send("k", "c", "{}")
    assert resp.head.status == 204


@pytest.mark.asyncio
async def test_a_missing_status_defaults_to_200() -> None:
    transport = Transport(StubNetwork(envelope("k", "c", {})))
    assert (await transport.send("k", "c", "{}")).head.status == 200


@pytest.mark.asyncio
async def test_a_non_object_response_is_a_decoding_error() -> None:
    """``[]`` or ``null`` is not an envelope; say so rather than guessing."""
    for body in ("[]", "null", '"a string"', "42"):
        transport = Transport(StubNetwork(body))
        with pytest.raises(DecodingError):
            await transport.send("k", "c", "{}")


@pytest.mark.asyncio
async def test_a_non_integer_status_is_a_decoding_error() -> None:
    """Previously silently coerced to 200 by an ``isinstance`` guard."""
    transport = Transport(
        StubNetwork('{"kind":"k","head":{"corrId":"c","status":"oops"},"data":{}}')
    )
    with pytest.raises(DecodingError):
        await transport.send("k", "c", "{}")


# ── Status mapping ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_an_error_status_becomes_a_server_error_carrying_the_message() -> None:
    sender = _sender(envelope("promise.get", "fixed", {"error": "nope"}, status=404))
    with pytest.raises(ServerError) as exc:
        await sender.promise_get("p1")
    assert exc.value.code == 404
    assert exc.value.message == "nope"


@pytest.mark.asyncio
async def test_a_string_error_body_is_used_verbatim() -> None:
    sender = _sender(envelope("promise.get", "fixed", "plain text failure", status=500))
    with pytest.raises(ServerError, match="plain text failure"):
        await sender.promise_get("p1")


@pytest.mark.asyncio
async def test_an_unhelpful_error_body_falls_back_to_a_generic_message() -> None:
    sender = _sender(envelope("promise.get", "fixed", {"unexpected": 1}, status=502))
    with pytest.raises(ServerError, match=r"server error \(status 502\)"):
        await sender.promise_get("p1")


@pytest.mark.asyncio
async def test_409_is_a_conflict_outcome_not_an_error_for_task_create() -> None:
    """The one status that is a *value*, because the caller must act on it."""
    sender = _sender(envelope("task.create", "fixed", {}, status=409))
    outcome = await sender.task_create_or_conflict(
        "pid", 1000, PromiseCreateReq(id="p1", timeout_at=FAR, param=Value(), tags={})
    )
    assert outcome == "conflict"


@pytest.mark.asyncio
async def test_409_is_still_an_error_for_plain_task_create() -> None:
    sender = _sender(envelope("task.create", "fixed", {}, status=409))
    with pytest.raises(ServerError) as exc:
        await sender.task_create(
            "pid",
            1000,
            PromiseCreateReq(id="p1", timeout_at=FAR, param=Value(), tags={}),
        )
    assert exc.value.code == 409


@pytest.mark.asyncio
async def test_300_on_suspend_is_a_redirect_carrying_its_preload() -> None:
    sender = _sender(
        envelope("task.suspend", "fixed", {"preload": [_promise("dep")]}, status=300)
    )
    result = await sender.task_suspend("t1", 1, [])
    assert result != "suspended"
    assert [p.id for p in result.preload] == ["dep"]


# ── Per-operation data shapes ──────────────────────────────────────


@pytest.mark.asyncio
async def test_a_missing_promise_field_is_a_decoding_error() -> None:
    sender = _sender(envelope("promise.get", "fixed", {}))
    with pytest.raises(DecodingError, match="promise record"):
        await sender.promise_get("p1")


@pytest.mark.asyncio
async def test_a_missing_task_field_is_a_decoding_error() -> None:
    sender = _sender(envelope("task.acquire", "fixed", {"promise": _promise()}))
    with pytest.raises(DecodingError, match=r"task\.acquire"):
        await sender.task_acquire("t1", 1, "pid", 1000)


@pytest.mark.asyncio
async def test_a_fence_response_without_a_nested_promise_is_a_decoding_error() -> None:
    sender = _sender(envelope("task.fence", "fixed", {"action": {"data": {}}}))
    with pytest.raises(DecodingError, match=r"task\.fence"):
        await sender.task_fence_create(
            "t1", 1, PromiseCreateReq(id="p1", timeout_at=FAR, param=Value(), tags={})
        )


@pytest.mark.asyncio
async def test_a_null_value_field_collapses_to_an_empty_value() -> None:
    """Msgspec rejects ``null`` for a struct field; the server sends it anyway.

    The normalizer now runs recursively at the ``data`` boundary, so a nested
    record inside a typed page is covered too -- previously it was applied
    per-record by hand and could be forgotten.
    """
    sender = _sender(
        envelope(
            "task.acquire",
            "fixed",
            {
                "task": {"id": "t1", "state": "acquired", "version": 1},
                "promise": {
                    "id": "p1",
                    "state": "pending",
                    "timeoutAt": FAR,
                    "param": None,
                    "value": None,
                    "tags": {},
                },
                "preload": [
                    {
                        "id": "p2",
                        "state": "pending",
                        "timeoutAt": FAR,
                        "param": None,
                        "value": None,
                    }
                ],
            },
        )
    )
    result = await sender.task_acquire("t1", 1, "pid", 1000)
    assert result.promise.param == Value()
    assert result.preload[0].value == Value()


@pytest.mark.asyncio
async def test_preload_is_lenient_but_reports_what_it_drops() -> None:
    observer = RecordingObserver()
    sender = _sender(
        envelope(
            "task.acquire",
            "fixed",
            {
                "task": {"id": "t1", "state": "acquired", "version": 1},
                "promise": _promise(),
                "preload": [_promise("good"), {"id": "bad", "state": "nonsense"}],
            },
        ),
        observer,
    )
    result = await sender.task_acquire("t1", 1, "pid", 1000)

    assert [p.id for p in result.preload] == ["good"]
    assert observer.dropped("preload-record") == ["bad"]


@pytest.mark.asyncio
async def test_a_search_page_reports_its_cursor() -> None:
    sender = _sender(
        envelope(
            "promise.search",
            "fixed",
            {"promises": [_promise("a")], "cursor": "next-page"},
        )
    )
    page = await sender.promise_search(None, None, None, None)
    assert [p.id for p in page.promises] == ["a"]
    assert page.cursor == "next-page"


@pytest.mark.asyncio
async def test_an_absent_cursor_is_none() -> None:
    sender = _sender(envelope("promise.search", "fixed", {"promises": []}))
    assert (await sender.promise_search(None, None, None, None)).cursor is None


@pytest.mark.asyncio
async def test_a_schedule_page_decodes_and_stays_lenient() -> None:
    observer = RecordingObserver()
    sender = _sender(
        envelope(
            "schedule.search",
            "fixed",
            {
                "schedules": [
                    {
                        "id": "s1",
                        "cron": "* * * * *",
                        "promiseId": "p",
                        "promiseTimeout": 1,
                        "promiseParam": None,
                        "promiseTags": {},
                    },
                    {"id": "broken"},
                ]
            },
        ),
        observer,
    )
    page = await sender.schedule_search(None, None, None)
    assert [s.id for s in page.schedules] == ["s1"]
    assert observer.dropped("search-record") == ["broken"]


# ── Correlation ids ────────────────────────────────────────────────


def test_correlation_ids_are_unique_per_call() -> None:
    """A timestamp collides within a millisecond; a UUID does not.

    The correlation check in ``Transport.send`` is only meaningful if the id
    it compares is actually distinguishing -- previously two envelopes built in
    the same millisecond shared one, so the check validated nothing.
    """
    ids = {default_corr_id() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(i.startswith("sr-") for i in ids)


@pytest.mark.asyncio
async def test_a_mismatched_corr_id_is_rejected() -> None:
    transport = Transport(StubNetwork(envelope("k", "someone-elses", {})))
    with pytest.raises(ServerError, match="corrId mismatch"):
        await transport.send("k", "mine", "{}")


@pytest.mark.asyncio
async def test_a_mismatched_kind_is_rejected() -> None:
    transport = Transport(StubNetwork(envelope("other.kind", "c", {})))
    with pytest.raises(ServerError, match="kind mismatch"):
        await transport.send("expected.kind", "c", "{}")


@pytest.mark.asyncio
async def test_an_injected_corr_id_makes_the_wire_deterministic() -> None:
    """The seam golden-file tests depend on."""
    net = StubNetwork(envelope("promise.get", "pinned", {"promise": _promise()}))
    sender = Sender(Transport(net), None, corr_id=lambda: "pinned")
    await sender.promise_get("p1")
    assert net.bodies()[0]["head"]["corrId"] == "pinned"
