"""One test per error variant, per boundary.

Python cannot put a failure set in a signature, so :mod:`resonate.error`
declares it as a union alias instead (``SenderError``, ``DurableOpError``,
``DispatchError``). A union is only worth declaring if it is walked: this
module walks every variant of every alias, so "the suite knows its own
contract" is literally true rather than aspirational.

The vocabulary is closed at every point but one:
:class:`~resonate_base.error.ConnectorError` stands for the substrate giving
up, whichever substrate that is. That is what lets ``SenderError`` stay closed
while the set of connectors stays open, so this module walks the *category*.
"""

from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any, TypeAliasType, get_args

import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.effects import ResonateEffects
from resonate.error import (
    AlreadyRegisteredError,
    ApplicationError,
    Base64DecodeError,
    ConnectorError,
    DecodingError,
    DispatchError,
    DurableOpError,
    FunctionNotFoundError,
    InvalidIdError,
    PlatformError,
    ResonateError,
    ResonateTimeoutError,
    SenderError,
    SerializationError,
    ServerError,
    StoppedError,
    Suspended,
)
from resonate.testing import FAR_FUTURE
from resonate.types import PromiseCreateReq, PromiseSettleReq, Value

if TYPE_CHECKING:
    from resonate.send import TaskFenceResult

# ── Every variant, constructed and rendered ────────────────────────
#
# Parameterized rather than hand-written so adding a variant to
# :mod:`resonate.error` without adding it here is visible as a coverage gap
# rather than passing silently.

_VARIANTS: list[tuple[ResonateError, str]] = [
    (FunctionNotFoundError("greet", 2), "function not found: greet (version 2)"),
    (InvalidIdError("a:b", "contains a colon"), "invalid id 'a:b': contains a colon"),
    (
        AlreadyRegisteredError("greet", 3),
        "function 'greet' (version 3) is already registered",
    ),
    (ServerError(503, "unavailable"), "server error (code=503): unavailable"),
    (StoppedError(), "execution stopped"),
    (DecodingError("bad shape"), "decoding error: bad shape"),
    (SerializationError(ValueError("nope")), "serialization error: nope"),
    (ConnectorError(OSError("refused")), "connector error: refused"),
    (Base64DecodeError(ValueError("pad")), "base64 decode error: pad"),
    (ApplicationError("domain failure"), "domain failure"),
    (ResonateTimeoutError(), "timeout"),
]


@pytest.mark.parametrize(("error", "rendered"), _VARIANTS, ids=lambda v: str(v)[:40])
def test_every_variant_renders_its_own_message(
    error: ResonateError, rendered: str
) -> None:
    assert str(error) == rendered


@pytest.mark.parametrize(
    "error", [v[0] for v in _VARIANTS], ids=lambda e: type(e).__name__
)
def test_every_variant_is_a_resonate_error(error: ResonateError) -> None:
    """The base class is the SDK's outermost catch -- every variant must fit it."""
    assert isinstance(error, ResonateError)
    assert isinstance(error, Exception)


@pytest.mark.parametrize(
    "error", [v[0] for v in _VARIANTS], ids=lambda e: type(e).__name__
)
def test_every_variant_survives_a_pickle_round_trip(error: ResonateError) -> None:
    """The codec pickles errors best-effort; the SDK's own must always make it.

    A variant whose ``__init__`` signature drifts from its ``super().__init__``
    args silently loses its pickle round-trip, and a rejected promise then
    degrades to a bare :class:`ApplicationError`. This catches that.
    """
    revived = pickle.loads(pickle.dumps(error))  # noqa: S301
    assert type(revived) is type(error)
    assert str(revived) == str(error)


# ── Union membership ───────────────────────────────────────────────


def members(alias: Any) -> set[type]:
    """Flatten a union alias to its concrete members, following nested aliases.

    ``DurableOpError`` is built *on top of* ``SenderError``, so a shallow
    ``get_args`` reports the alias rather than the four classes inside it. The
    nesting is deliberate -- it is what makes "a durable op inherits the
    sender's vocabulary" a structural fact instead of a copied list -- so the
    test flattens rather than the source duplicating.
    """
    out: set[type] = set()
    for arg in get_args(alias.__value__):
        if isinstance(arg, TypeAliasType):
            out |= members(arg)
        else:
            out.add(arg)
    return out


def test_sender_error_union_members() -> None:
    """The wire boundary's vocabulary: a status, a shape, and *any* transport.

    Three members, not one-per-transport-and-counting. Enumerating them here
    would mean this union has to know every connector that will ever exist.
    """
    assert members(SenderError) == {
        ServerError,
        DecodingError,
        ConnectorError,
    }


def test_durable_op_error_union_extends_the_sender_vocabulary() -> None:
    """A durable op inherits the sender's failures and adds the codec's."""
    durable = members(DurableOpError)
    assert members(SenderError) <= durable
    assert {SerializationError, Base64DecodeError, StoppedError} <= durable


def test_dispatch_error_union_covers_the_front_door() -> None:
    dispatch = members(DispatchError)
    assert {FunctionNotFoundError, AlreadyRegisteredError, InvalidIdError} <= dispatch
    assert members(SenderError) <= dispatch


# ── StoppedError: the circuit breaker's variant ────────────────────


def test_stopped_error_is_not_a_server_failure() -> None:
    """It means "we never touched the network", which is why it carries no code."""
    err = StoppedError()
    assert not hasattr(err, "code")
    assert err.args == ()


@pytest.mark.asyncio
async def test_stopped_error_is_the_cause_after_a_prior_durable_failure() -> None:
    """The second durable op in a broken attempt fails with ``StoppedError``.

    This is the circuit breaker: once one op has failed, every later one
    short-circuits so no further durable work happens before the task is
    released.
    """

    class Broken:
        """A :class:`~resonate.send.PromiseFencing` whose every op fails."""

        async def task_fence_create(
            self, id: str, version: int, req: PromiseCreateReq
        ) -> TaskFenceResult:
            raise ServerError(500, "down")

        async def task_fence_settle(
            self, id: str, version: int, req: PromiseSettleReq
        ) -> TaskFenceResult:
            raise ServerError(500, "down")

    effects = ResonateEffects(Broken(), Codec(NoopEncryptor()), "t", 1, [])
    req = PromiseCreateReq(id="p1", timeout_at=FAR_FUTURE, param=Value(), tags={})

    with pytest.raises(PlatformError) as first:
        await effects.create_promise(req)
    assert isinstance(first.value.cause, ServerError)

    with pytest.raises(PlatformError) as second:
        await effects.create_promise(
            PromiseCreateReq(id="p2", timeout_at=FAR_FUTURE, param=Value(), tags={})
        )
    assert isinstance(second.value.cause, StoppedError)

    with pytest.raises(PlatformError) as third:
        await effects.settle_promise("p1", 1)
    assert isinstance(third.value.cause, StoppedError)


# ── ConnectorError: the one open point ─────────────────────────────


def test_connector_error_wraps_and_exposes_its_cause() -> None:
    cause = TimeoutError("no responders")
    err = ConnectorError(cause)
    assert err.error is cause
    assert str(err) == "connector error: no responders"


# ── The BaseException pair ─────────────────────────────────────────


def test_platform_error_and_suspended_dodge_except_exception() -> None:
    """Both must survive a user's ``except Exception`` -- that is why they exist."""
    for control in (PlatformError([StoppedError()]), Suspended()):
        assert isinstance(control, BaseException)
        assert not isinstance(control, Exception)


def test_platform_error_refuses_an_empty_cause_list() -> None:
    """A defect, and raised as one -- ``assert`` would vanish under ``python -O``."""
    with pytest.raises(ValueError, match="at least one cause"):
        PlatformError([])


def test_platform_error_aggregates_and_exposes_a_primary_cause() -> None:
    a, b = ServerError(500, "a"), ConnectorError(OSError("b"))
    err = PlatformError([a, b])
    assert err.causes == [a, b]
    assert err.cause is a
    assert str(err) == (
        "platform error: server error (code=500): a; connector error: b"
    )
