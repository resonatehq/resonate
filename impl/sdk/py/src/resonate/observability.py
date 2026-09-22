"""A typed event stream for things the SDK does *not* raise about.

Several SDK behaviours are deliberately silent: a preloaded promise record
that fails to decode is skipped, a malformed record in a search response is
dropped, a background task's failure is logged and forgotten. Each is a
documented *contract* -- but a contract with no observable effect cannot be
asserted, so nothing stops a skip from silently becoming a crash.

:data:`Observer` is that seam. It is a plain callable taking one :data:`Event`,
defaulting to :func:`logging_observer`, which reproduces the SDK's historical
log output byte-for-byte (including the ``resonate.validation`` logger name the
external validation harness keys off). Tests inject a recorder and match on the
event structs instead of scraping log text.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Literal

import msgspec

#: The validation harness keys off this logger name rather than the module
#: name, so it must stay "resonate.validation".
validation_logger = logging.getLogger("resonate.validation")

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Events
# ═══════════════════════════════════════════════════════════════


class Dropped(msgspec.Struct, frozen=True, kw_only=True):
    """Something arrived, could not be understood, and was discarded.

    ``what`` names the drop site (``"preload-record"``, ``"search-record"``,
    ``"incoming-message"``); ``id`` identifies the discarded item where one is
    known, else ``""``. Every drop site in the SDK emits exactly one of these.
    """

    what: str
    id: str
    cause: str


class BackgroundTaskFailed(msgspec.Struct, frozen=True, kw_only=True):
    """A fire-and-forget task raised; nobody was there to receive it."""

    cause: str


class PromiseCreateRequested(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    invocation: Literal["run", "rpc", "unknown"]


class PromiseCreateReturned(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    invocation: Literal["run", "rpc", "unknown"]
    state: str


class PromiseSettleRequested(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    state: str


class PromiseSettleReturned(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    state: str


#: Everything an :data:`Observer` may be handed. A closed union, so an observer
#: that matches exhaustively is checked by the type checker.
type Event = (
    Dropped
    | BackgroundTaskFailed
    | PromiseCreateRequested
    | PromiseCreateReturned
    | PromiseSettleRequested
    | PromiseSettleReturned
)

#: Receives every :data:`Event`. Must not raise and must not block -- it is
#: called synchronously from the paths it reports on.
type Observer = Callable[[Event], None]


def logging_observer(event: Event) -> None:
    """Re-emit each event as a log record -- the default :data:`Observer`.

    Output is byte-identical to the SDK's historical logging, so the external
    validation harness that greps the ``resonate.validation`` stream keeps
    working unchanged.
    """
    match event:
        case Dropped(what=what, id=id, cause=cause):
            logger.debug("dropped %s id=%s: %s", what, id, cause)
        case BackgroundTaskFailed(cause=cause):
            logger.error("background task failed: %s", cause)
        case PromiseCreateRequested(id=id, invocation=invocation):
            validation_logger.info(
                "promise_create_request promise_id=%s invocation=%s", id, invocation
            )
        case PromiseCreateReturned(id=id, invocation=invocation, state=state):
            validation_logger.info(
                "promise_create_response promise_id=%s invocation=%s state=%s",
                id,
                invocation,
                state,
            )
        case PromiseSettleRequested(id=id, state=state):
            validation_logger.info(
                "promise_settle_request promise_id=%s state=%s", id, state
            )
        case PromiseSettleReturned(id=id, state=state):
            validation_logger.info(
                "promise_settle_response promise_id=%s state=%s", id, state
            )


def noop_observer(event: Event) -> None:
    """Discard every event -- the :data:`Observer` that reports nothing."""
