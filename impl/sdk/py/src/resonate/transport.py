from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import msgspec

from resonate.error import DecodingError, ServerError
from resonate.observability import Dropped, logging_observer
from resonate.types import PromiseRecord

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from resonate_base.connections import Network, Source

    from resonate.observability import Observer

logger = logging.getLogger(__name__)


# =============================================================================
# Incoming messages (recv path)
# =============================================================================


class TaskRef(msgspec.Struct, kw_only=True, frozen=True):
    id: str
    version: int = msgspec.field(default=0)


class ExecuteData(msgspec.Struct, kw_only=True, frozen=True):
    task: TaskRef


class ExecuteMsg(
    msgspec.Struct, tag="execute", tag_field="kind", kw_only=True, frozen=True
):
    data: ExecuteData

    @property
    def task_id(self) -> str:
        return self.data.task.id

    @property
    def version(self) -> int:
        return self.data.task.version


class UnblockData(msgspec.Struct, kw_only=True, frozen=True):
    promise: PromiseRecord


class UnblockMsg(
    msgspec.Struct, tag="unblock", tag_field="kind", kw_only=True, frozen=True
):
    data: UnblockData

    @property
    def promise(self) -> PromiseRecord:
        """Return the settled promise -- shorthand for ``data.promise``."""
        return self.data.promise


# A parsed incoming message from the network, discriminated by its ``kind``
# field.
Message = ExecuteMsg | UnblockMsg


# =============================================================================
# Outgoing responses (send path)
# =============================================================================


class ResponseHead(msgspec.Struct, kw_only=True, frozen=True, rename="camel"):
    """The ``head`` of a protocol response envelope.

    Every field is defaulted: a server that omits ``head`` entirely, or any
    field within it, is a *valid* response meaning "200, no correlation echo",
    and the mismatch checks in :meth:`Transport.send` catch the cases that
    matter. Defaulting here is what lets the rest of the SDK read
    ``resp.head.status`` unconditionally instead of re-deriving it behind
    ``isinstance`` guards at every call site.
    """

    corr_id: str = ""
    status: int = 200


class Response(msgspec.Struct, kw_only=True, frozen=True):
    """A parsed protocol response envelope: ``{ kind, head, data }``.

    The single parse boundary for everything arriving over the
    :class:`~resonate_base.connections.Network`. Below this point the SDK works
    with a typed value and never asks "is this a dict?" again -- the shape
    questions are all answered here, once.
    """

    kind: str = ""
    head: ResponseHead = msgspec.field(default_factory=ResponseHead)
    data: Any = msgspec.field(default_factory=dict)


# =============================================================================
# Transport
# =============================================================================


class Transport:
    """Adds JSON serialization, deserialization, and correlation validation.

    Resonate and its sub-components use the transport -- never the raw
    connections. Requests go out over the single ``network``; push messages
    come in over every ``source``.

    ``observer`` receives a :class:`~resonate.observability.Dropped` event for
    each incoming push message that cannot be parsed, so the drop is
    assertable rather than merely logged.
    """

    def __init__(
        self,
        network: Network,
        sources: Sequence[Source] = (),
        observer: Observer = logging_observer,
    ) -> None:
        self._network = network
        self._sources = tuple(sources)
        self._observer = observer

    async def send(
        self,
        kind: str,
        corr_id: str,
        body: str,
        headers: dict[str, str] | None = None,
    ) -> Response:
        """Send an already-serialized request, returning the parsed response.

        Parses at the edge: the raw JSON is decoded straight into
        :class:`Response`, so callers receive a typed value whose ``status``
        and ``corrId`` have already been validated.

        ``headers`` carry the request metadata, including the routing origin
        under :data:`~resonate_base.ORIGIN_HEADER`, and are passed straight to
        the connection (see :meth:`~resonate_base.connections.Network.send`) so
        a sharding substrate never has to open the payload. The caller owns
        that folding -- the transport only moves what it is handed.
        """
        logger.debug("transport send_req: %s", body)

        resp_str = await self._network.send(body, headers)
        logger.debug("transport send_res: %s", resp_str)

        try:
            response = msgspec.json.decode(resp_str, type=Response)
        except msgspec.MsgspecError as exc:
            msg = f"invalid response JSON: {exc}, resp: {resp_str}"
            raise DecodingError(msg) from exc

        if response.kind != kind:
            msg = f"response kind mismatch: expected '{kind}', got '{response.kind}'"
            raise ServerError(500, msg)

        if response.head.corr_id != corr_id:
            msg = (
                f"response corrId mismatch: expected '{corr_id}', "
                f"got '{response.head.corr_id}'"
            )
            raise ServerError(500, msg)

        return response

    def recv(self, callback: Callable[[Message], None]) -> None:
        """Register a callback for incoming messages on every source."""

        def on_raw(raw: str) -> None:
            try:
                msg = msgspec.json.decode(raw, type=Message)
            except msgspec.MsgspecError as exc:
                self._observer(Dropped(what="incoming-message", id="", cause=str(exc)))
                logger.warning(
                    "failed to parse incoming message: %s; raw: %s", exc, raw
                )
                return
            logger.debug("transport recv: %s", raw)
            callback(msg)

        for source in self._sources:
            source.recv(on_raw)

    def network(self) -> Network:
        """Access the underlying network."""
        return self._network
