"""Pins how exceptions cross the durable-promise boundary.

What this file pins
-------------------

A step function may ``raise`` any Python exception. The rejection is stored on
the durable promise as JSON carrying two things (see
:func:`resonate.codec._encode_error`): a transport-safe ``message`` every SDK
can read, and a best-effort ``__py_pickle`` -- a base64 pickle of the original
exception. A Python awaiter that recovers the rejection (via ``ctx.run``,
``ctx.rpc``, or top-level ``handle.result()``) gets the **original exception
type and attributes back** whenever that pickle round-trips: the producing and
consuming sides share a Python runtime and the class is importable. When it
cannot round-trip -- a non-Python producer, an unimportable or locally-defined
class, an unpicklable payload, or a corrupt one -- the awaiter falls back to a
plain :class:`~resonate.error.ApplicationError` carrying ``message``.

Why we test this
----------------

The stored rejection also **deduplicates** recoveries: a worker resuming a
workflow may be a different process, on a different binary, weeks later, where a
user-defined class may not exist. The ``message`` field is what makes that
robust; ``__py_pickle`` is the additive enrichment that gives same-runtime
awaiters full fidelity without breaking the cross-SDK / cross-version contract.
Both the local (``ctx.run``, ``handle.result``) and remote (``ctx.rpc``) paths
funnel rejections through :func:`resonate.codec._deserialize_error`. The tests
below pin that contract at three layers:

1. **Codec** -- :func:`_encode_error` + :func:`_deserialize_error`: a picklable,
   importable class round-trips to its exact type; everything else degrades to
   :class:`ApplicationError` with the message preserved. This is the proof
   shared by both dispatch paths.
2. **Local dispatch (``ctx.run`` / ``handle.result``)** -- end-to-end against
   the in-process :class:`~resonate.network.LocalNetwork`.
3. **Discrimination patterns** -- catching the original class (now possible),
   "by position" (one ``await`` per ``try``), and "by message tag"
   (``raise ApplicationError('E_CODE: ...')``, the cross-SDK-safe idiom).
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import pickle
from typing import TYPE_CHECKING

import pytest

from resonate.codec import _deserialize_error, _encode_error
from resonate.error import ApplicationError, ServerError
from resonate.resonate import Resonate
from resonate.retry import Never
from resonate.types import Value

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from resonate.context import Context
    from resonate.types import PromiseRecord


# ── Harness ────────────────────────────────────────────────────────────────


@contextlib.asynccontextmanager
async def local() -> AsyncIterator[Resonate]:
    """Yield a local-mode Resonate, stopping it on exit."""
    # Pin ``Never``: these orchestrators run failing pure-leaf steps via
    # ``ctx.run``, which the SDK-default Exponential would retry forever.
    r = Resonate(retry_policy=Never())
    try:
        yield r
    finally:
        await r.stop()


async def wait_for_promise(r: Resonate, id: str, tries: int = 200) -> PromiseRecord:
    """Poll until the durable promise ``id`` exists.

    A top-level ``rpc`` creates its promise in the background and -- in local
    mode -- never executes it (no worker is subscribed to the target), so its
    creation can't be observed by awaiting a result. Mirrors the helper in
    ``test_resonate.py``.
    """
    for _ in range(tries):
        try:
            return await r.promises.get(id)
        except ServerError:
            await asyncio.sleep(0)
    msg = f"promise {id} was never created"
    raise AssertionError(msg)


# ── Domain exceptions used by the workflow library ─────────────────────────
#
# Defined at module scope so they are importable by qualified name, hence
# picklable: this is exactly what lets the original class survive the boundary
# in the same-runtime case the tests exercise.


class PaymentDeclinedError(Exception):
    """A custom domain exception. Survives the boundary via pickle."""


class FraudSuspectedError(PaymentDeclinedError):
    """Subclass of a custom exception. Its base relationship survives too."""


# ── Leaf step functions (settle once) ──────────────────────────────────────


async def step_raises_value_error(ctx: Context) -> str:
    msg = "bad input from step_raises_value_error"
    raise ValueError(msg)


async def step_raises_domain_error(ctx: Context) -> str:
    msg = "card declined for $42"
    raise PaymentDeclinedError(msg)


async def step_raises_domain_subclass(ctx: Context) -> str:
    msg = "suspected fraud on card 4242"
    raise FraudSuspectedError(msg)


async def step_raises_tagged_application_error(ctx: Context) -> str:
    # The cross-SDK-safe way to let callers discriminate: a parseable prefix in
    # the message text, robust even when the producer is not Python.
    msg = "E_NOT_FOUND: user 42 does not exist"
    raise ApplicationError(msg)


async def step_returns_ok(ctx: Context, x: int) -> int:
    return x * 2


# ── Orchestrators (no side effects: replay-safe) ───────────────────────────


async def orchestrator_lets_value_error_escape(ctx: Context) -> str:
    # No try/except: the rejection bubbles all the way out so we can pin what
    # ``handle.result()`` re-raises to the top-level caller.
    return await ctx.run(step_raises_value_error)


async def orchestrator_catches_value_error(ctx: Context) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_value_error)
    except ValueError as exc:
        # The original builtin type survives, so ``except ValueError`` matches.
        return type(exc).__name__, str(exc)
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_catches_domain_class(ctx: Context) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_domain_error)
    except PaymentDeclinedError as exc:
        # The custom domain class is reconstructed across the boundary.
        return type(exc).__name__, str(exc)
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_catches_domain_subclass(ctx: Context) -> str:
    # FraudSuspected is a PaymentDeclined, and the full class survives, so
    # ``except PaymentDeclined`` catches the reconstructed subclass.
    try:
        await ctx.run(step_raises_domain_subclass)
    except PaymentDeclinedError as exc:
        return type(exc).__name__
    return "not caught as PaymentDeclined"


async def orchestrator_parses_tagged_application_error(
    ctx: Context,
) -> tuple[str, str]:
    try:
        await ctx.run(step_raises_tagged_application_error)
    except ApplicationError as exc:
        code, _, detail = exc.message.partition(": ")
        return code, detail
    msg = "step unexpectedly succeeded"
    raise AssertionError(msg)


async def orchestrator_positional_discrimination(ctx: Context) -> str:
    # The "by position" pattern: one await per try block, so *which* block
    # raised tells you which step failed -- no class introspection needed, and
    # robust regardless of whether the original type round-tripped.
    await ctx.run(step_returns_ok, 1)  # step A: succeeds

    try:
        await ctx.run(step_raises_value_error)  # step B: fails
    except Exception:
        return "step B failed"

    try:
        await ctx.run(step_raises_domain_error)  # step C: never reached
    except Exception:
        return "step C failed"

    return "no failure"


async def orchestrator_raises_application_error_directly(ctx: Context) -> None:
    # The orchestrator itself raises ApplicationError without ever awaiting a
    # leaf. The top-level handle should still see it verbatim.
    msg = "orchestrator-level failure"
    raise ApplicationError(msg)


async def orchestrator_raises_value_error_directly(ctx: Context) -> None:
    # The orchestrator raises a plain Python exception. The core executor
    # settles the run-level promise ``rejected`` with the original exception,
    # identically to a step.
    msg = "orchestrator-level plain failure"
    raise ValueError(msg)


# =============================================================================
# Codec layer: _encode_error + _deserialize_error. A picklable, importable
# class round-trips to its exact type; everything else degrades to
# ApplicationError. This is the single decoder used by both ctx.run and
# ctx.rpc, so pinning it here proves the "same on either dispatch path" claim.
# =============================================================================


def test_codec_encode_error_keeps_message_and_pickles_class() -> None:
    err = PaymentDeclinedError("card declined for $42")
    encoded = _encode_error(err)
    assert encoded["__type"] == "error"
    assert encoded["message"] == "card declined for $42"
    # The class identity now survives via the best-effort pickle payload.
    assert "__py_pickle" in encoded


def test_codec_deserialize_full_envelope_recovers_original_type() -> None:
    # When ``__py_pickle`` round-trips, the recovering side gets the original
    # class back -- here PaymentDeclined, defined in this very module.
    rebuilt = _deserialize_error(_encode_error(PaymentDeclinedError("card declined")))
    assert isinstance(rebuilt, PaymentDeclinedError)
    assert str(rebuilt) == "card declined"


def test_codec_deserialize_message_only_falls_back_to_application_error() -> None:
    # A non-Python / legacy payload with no ``__py_pickle`` rebuilds as a plain
    # ApplicationError carrying the message. This is the cross-SDK contract.
    rebuilt = _deserialize_error({"__type": "error", "message": "card declined"})
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == "card declined"


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("bad input"),
        RuntimeError("kaboom"),
        PaymentDeclinedError("declined"),
        FraudSuspectedError("fraud"),
        ApplicationError("already an app error"),
    ],
)
def test_codec_roundtrip_preserves_picklable_type(exc: Exception) -> None:
    rebuilt = _deserialize_error(_encode_error(exc))
    assert type(rebuilt) is type(exc)
    assert str(rebuilt) == str(exc)


def test_codec_unpicklable_exception_falls_back_to_application_error() -> None:
    # A class defined inside a function has no importable qualified name, so
    # ``pickle.dumps`` fails -- the field is omitted and the awaiter degrades
    # to ApplicationError with the message intact.
    class LocallyDefinedError(Exception):
        pass

    encoded = _encode_error(LocallyDefinedError("nope"))
    assert "__py_pickle" not in encoded
    rebuilt = _deserialize_error(encoded)
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == "nope"


def test_codec_corrupt_pickle_falls_back_to_message() -> None:
    # ``__py_pickle`` present but undecodable (e.g. a different SDK version, or
    # a class missing on this worker): fall back to the message rather than
    # crashing the recovery.
    rebuilt = _deserialize_error(
        {
            "__type": "error",
            "message": "fallback message",
            "__py_pickle": "this-is-not-valid-base64-pickle!!",
        }
    )
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == "fallback message"


def test_codec_pickle_of_non_exception_falls_back() -> None:
    # The payload deserializes, but not to a BaseException. The type guard in
    # _deserialize_error rejects it rather than raising a non-exception.
    payload = base64.b64encode(pickle.dumps({"not": "an exception"})).decode("ascii")
    rebuilt = _deserialize_error(
        {"__type": "error", "message": "guarded", "__py_pickle": payload}
    )
    assert isinstance(rebuilt, ApplicationError)
    assert rebuilt.message == "guarded"


def test_codec_deserialize_error_unknown_shape_still_yields_application_error() -> None:
    # Defensive: a malformed/legacy payload still rehydrates as
    # ApplicationError so callers always face an exception.
    rebuilt = _deserialize_error("just a string")
    assert isinstance(rebuilt, ApplicationError)
    assert "unknown error" in rebuilt.message


# =============================================================================
# End-to-end: ctx.run inside a workflow. The orchestrator now sees the original
# exception type, so it can catch it directly.
# =============================================================================


@pytest.mark.asyncio
async def test_ctx_run_value_error_preserves_type() -> None:
    async with local() as r:
        r.register(orchestrator_catches_value_error)
        r.register(step_raises_value_error)

        caught, message = await r.run(
            "vh",
            orchestrator_catches_value_error,
        ).result()

    assert caught == "ValueError"
    assert message == "bad input from step_raises_value_error"


@pytest.mark.asyncio
async def test_ctx_run_domain_exception_preserves_type() -> None:
    async with local() as r:
        r.register(orchestrator_catches_domain_class)
        r.register(step_raises_domain_error)

        caught, message = await r.run(
            "dh",
            orchestrator_catches_domain_class,
        ).result()

    # The custom PaymentDeclined raised by the step is reconstructed for the
    # orchestrator: same runtime, importable class.
    assert caught == "PaymentDeclinedError"
    assert message == "card declined for $42"


@pytest.mark.asyncio
async def test_ctx_run_except_custom_class_matches() -> None:
    """``except PaymentDeclined`` catches the reconstructed rejection."""
    async with local() as r:
        r.register(orchestrator_catches_domain_class)
        r.register(step_raises_domain_error)

        caught, _ = await r.run("nc", orchestrator_catches_domain_class).result()

    assert caught == "PaymentDeclinedError"


@pytest.mark.asyncio
async def test_ctx_run_except_custom_subclass_matches() -> None:
    # FraudSuspected is a PaymentDeclined; the whole class survives, so the
    # orchestrator's ``except PaymentDeclined`` catches the subclass too.
    async with local() as r:
        r.register(orchestrator_catches_domain_subclass)
        r.register(step_raises_domain_subclass)

        result = await r.run("ns", orchestrator_catches_domain_subclass).result()

    assert result == "FraudSuspectedError"


@pytest.mark.asyncio
async def test_ctx_run_explicit_application_error_is_forwarded_verbatim() -> None:
    """``ApplicationError`` raised in a step is forwarded with its message intact."""
    async with local() as r:
        r.register(orchestrator_parses_tagged_application_error)
        r.register(step_raises_tagged_application_error)

        code, detail = await r.run(
            "tg",
            orchestrator_parses_tagged_application_error,
        ).result()

    # The tagged-message discrimination pattern -- still useful as the
    # cross-SDK-safe equivalent of ``except NotFoundError``.
    assert code == "E_NOT_FOUND"
    assert detail == "user 42 does not exist"


@pytest.mark.asyncio
async def test_ctx_run_positional_discrimination_identifies_failing_step() -> None:
    # Knowing *which* step failed without class introspection: each step has
    # its own try block, so the block that raised tells you the step.
    async with local() as r:
        r.register(orchestrator_positional_discrimination)
        r.register(step_returns_ok)
        r.register(step_raises_value_error)
        r.register(step_raises_domain_error)

        result = await r.run("pd", orchestrator_positional_discrimination).result()

    assert result == "step B failed"


# =============================================================================
# Top-level: handle.result() re-raises the original exception type, falling
# back to ApplicationError only when the rejection cannot round-trip.
# =============================================================================


@pytest.mark.asyncio
async def test_handle_result_re_raises_uncaught_step_failure() -> None:
    # A leaf raises; the orchestrator does not catch it; ``handle.result()``
    # surfaces the *original* ValueError to ``main``, message intact.
    async with local() as r:
        r.register(orchestrator_lets_value_error_escape)
        r.register(step_raises_value_error)

        with pytest.raises(ValueError, match="bad input") as excinfo:
            await r.run("ue", orchestrator_lets_value_error_escape).result()

    assert str(excinfo.value) == "bad input from step_raises_value_error"


@pytest.mark.asyncio
async def test_handle_result_re_raises_orchestrator_application_error() -> None:
    # The orchestrator itself raises ApplicationError (no step involved).
    # ``handle.result()`` sees it verbatim.
    async with local() as r:
        r.register(orchestrator_raises_application_error_directly)

        with pytest.raises(ApplicationError, match="orchestrator-level failure"):
            await r.run("oa", orchestrator_raises_application_error_directly).result()


@pytest.mark.asyncio
async def test_handle_result_re_raises_orchestrator_plain_exception() -> None:
    # The orchestrator raises a plain Python exception. The core executor
    # settles the top-level promise rejected with the original exception, so
    # the caller observes the original ValueError.
    async with local() as r:
        r.register(orchestrator_raises_value_error_directly)

        with pytest.raises(ValueError, match="orchestrator-level plain") as excinfo:
            await r.run("op", orchestrator_raises_value_error_directly).result()

    assert str(excinfo.value) == "orchestrator-level plain failure"


# =============================================================================
# Top-level rpc: the handle returned by ``r.rpc`` decodes a rejection through
# the same ``ResonateHandle._decode_result`` path as ``r.run`` -- the rejected
# arm is type-agnostic (the ``Any`` decode type only shapes a *resolved* value),
# so a remote rejection re-raises identically. A remote rpc promise never
# executes in local mode, so we settle it directly via ``r.promises.reject`` to
# stand in for the worker that would have rejected it.
# =============================================================================


@pytest.mark.asyncio
async def test_rpc_handle_re_raises_original_type_on_rejection() -> None:
    # Reject with a domain exception: ``promises.reject`` runs it through the
    # codec (``_encode_error``), so the stored value carries ``__py_pickle`` and
    # the handle recovers the original class -- same fidelity as ``ctx.run``.
    async with local() as r:
        h = r.rpc("rpc-reject-type", "remote_fn")
        await wait_for_promise(r, "rpc-reject-type")
        await r.promises.reject(
            "rpc-reject-type",
            Value(data=PaymentDeclinedError("card declined via rpc")),
        )

        with pytest.raises(PaymentDeclinedError, match="card declined via rpc"):
            await h.result()


@pytest.mark.asyncio
async def test_rpc_handle_falls_back_to_application_error_on_message_only() -> None:
    # A message-only rejection (the cross-SDK shape a non-Python worker would
    # write -- no ``__py_pickle``) degrades to ApplicationError with the message
    # intact, exactly as on the ``ctx.run`` / ``handle.result`` paths.
    async with local() as r:
        h = r.rpc("rpc-reject-msg", "remote_fn")
        await wait_for_promise(r, "rpc-reject-msg")
        await r.promises.reject(
            "rpc-reject-msg",
            Value(data={"__type": "error", "message": "remote worker failure"}),
        )

        with pytest.raises(ApplicationError, match="remote worker failure"):
            await h.result()
