from __future__ import annotations

import base64
import contextlib
import logging
import uuid
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from resonate_base import ORIGIN_HEADER, ConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

__all__ = ["NatsConnection"]

# =============================================================================
# CONSTANTS
# =============================================================================

#: Seconds to wait for a server reply before giving up.
DEFAULT_REQUEST_TIMEOUT_SECS = 30.0

#: Subject prefix the server's request stream is subscribed to. A request for
#: ``origin`` is published to ``{api_prefix}.{base64url(origin)}``; the token is
#: base64url-encoded so any origin maps to a single valid subject token and all
#: publishers hash the same origin to the same JetStream partition.
#: ponytail: must match the server's ``SubjectPrefix`` (resonate.requests).
DEFAULT_API_PREFIX = "resonate.requests"

#: Subject prefix the SDK subscribes on for execute/unblock messages. The
#: ``nats://`` delivery addresses this connection advertises round-trip through
#: the server's ``url.Parse(host+path)`` to these subjects.
DEFAULT_RECV_PREFIX = "resonate.recv"

#: Header the server reads to learn where to publish its reply. The NATS
#: request/reply ``reply`` subject is *not* used by resonate-on-nats.
REPLY_HEADER = "Resonate-Reply-To"


def _publish_subject(prefix: str, origin: str) -> str:
    """Map a routing origin to the subject its partition is subscribed on.

    The origin is base64url-encoded so that *any* origin -- dots, spaces,
    whatever a caller named their workflow -- becomes a single valid subject
    token, and so every publisher hashes one origin to one JetStream partition.
    """
    token = base64.urlsafe_b64encode(origin.encode("utf-8")).rstrip(b"=")
    return f"{prefix}.{token.decode('ascii')}"


# ═══════════════════════════════════════════════════════════════
#  The client seam
# ═══════════════════════════════════════════════════════════════
#
# ``NatsConnection`` used to take ``nats.aio.client.Client`` by name, which
# meant its own IO -- ``start``, ``stop``, ``send``, ``_on_msg`` -- could only
# be exercised against a real broker (or a hand-rolled mock of a class this
# module does not own). These three protocols describe exactly the surface
# this connection touches, nothing more, so a test can hand in a small fake
# instead: the real ``nats-py`` client already implements them structurally,
# with no inheritance required on either side.


class NatsMsg(Protocol):
    """The minimal ``nats.aio.msg.Msg`` surface :class:`NatsConnection` reads."""

    data: bytes
    subject: str


@runtime_checkable
class NatsSubscription(Protocol):
    """The minimal ``nats.aio.subscription.Subscription`` surface this needs."""

    async def unsubscribe(self, limit: int = 0) -> None: ...

    # ``timeout`` mirrors ``nats-py``'s own ``Subscription.next_msg`` parameter
    # name -- this seam calls into it by keyword, so the name must match.
    async def next_msg(self, timeout: float | None = 1.0) -> NatsMsg: ...  # noqa: ASYNC109


@runtime_checkable
class NatsClient(Protocol):
    """The minimal ``nats.aio.client.Client`` surface :class:`NatsConnection` needs.

    A structural seam, not a copy of ``nats-py``'s ``Client``: any object with
    these three methods satisfies it -- the real client, or a fake a test
    constructs in memory. Pass an already-connected real client in production;
    pass a fake in a test and ``start``/``stop``/``send``/message dispatch
    become as testable as the rest of the SDK's connection logic.
    """

    def new_inbox(self) -> str: ...

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Callable[[Any], Any] | None = None,
        max_msgs: int = 0,
    ) -> NatsSubscription: ...

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None: ...


class NatsConnection:
    """NATS connection to resonate-on-nats.

    Implements **both** protocols: :class:`~resonate_base.connections.Network` (the
    request/response ``send`` path) and :class:`~resonate_base.connections.Source` (the
    push-message ``recv`` path). It can therefore serve as the network, as a
    source, or as both at once. When used only as a source, :meth:`send` is
    simply never called; when used only as the network, no receiver is
    registered via :meth:`recv` and :meth:`start` opens no subscriptions.

    The NATS connection lifecycle lives **outside** the SDK: pass an already
    connected ``nats-py`` client; :meth:`stop` only tears down this
    connection's subscriptions and leaves the client for the caller to
    drain/close.

    - Requests are published to ``{api_prefix}.{base64url(origin)}`` -- the
      origin the SDK puts in the ``resonate:origin`` header handed to
      :meth:`send` -- with a ``Resonate-Reply-To``
      header naming a private inbox; the reply arrives on that inbox (the
      server ignores the NATS reply subject). The request body itself is never
      parsed: it is bytes to be moved.
    - Incoming execute/unblock messages arrive on a unicast subject
      (``{recv_prefix}.{group}.{pid}``) and an anycast subject
      (``{recv_prefix}.{group}``) queue-subscribed on ``group`` so exactly one
      group member receives each anycast message.
    - Addresses use the ``nats://`` scheme so the server's ``url.Parse`` maps
      ``nats://{subject}`` back to ``{subject}``: the destination already is an
      address in the NATS namespace, so nesting a second addressing scheme
      inside it would buy nothing.

    Install with ``uv add resonate-nats``; it depends on ``resonate-base`` and
    ``nats-py``, never on ``resonate-sdk``.
    """

    def __init__(
        self,
        conn: NatsClient,
        pid: str | None = None,
        group: str | None = None,
        *,
        server_topic: str = DEFAULT_API_PREFIX,
        worker_topic: str = DEFAULT_RECV_PREFIX,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECS,
    ) -> None:
        self._nc = conn
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._api_prefix = server_topic
        # ponytail: pid/group land in the NATS subject via the server's
        # url.Parse host, which Go lowercases -- uuid hex pids and lowercase
        # groups round-trip cleanly; uppercase custom values would not.
        self._uni_subject = f"{worker_topic}.{self._group}.{self._pid}"
        self._any_subject = f"{worker_topic}.{self._group}"
        self._unicast = f"nats://{self._uni_subject}"
        self._recv_prefix = worker_topic
        self._request_timeout = request_timeout

        self._subscribers: list[Callable[[str], None]] = []
        self._subs: list[NatsSubscription] = []
        self._running = False

    def unicast(self) -> str:
        return self._unicast

    def resolve_target(self, target: str) -> str:
        """Resolve a target group name to its ``nats://`` anycast address."""
        return f"nats://{self._recv_prefix}.{target}"

    async def start(self) -> None:
        """Subscribe to the unicast and anycast subjects on the shared connection.

        Subscriptions are only opened when a receiver has been registered via
        :meth:`recv` -- i.e. when this connection is used as a
        :class:`~resonate_base.connections.Source`. A network-only connection must not
        subscribe: it would consume messages off the group's queue
        subscription and drop them, delaying the task until the server
        re-delivers it elsewhere. Register receivers **before** calling
        ``start`` (:class:`~resonate.resonate.Resonate` wires dispatch before
        starting any connection).
        """
        self._running = True
        if not self._subscribers:
            return
        self._subs = [
            await self._nc.subscribe(self._uni_subject, cb=self._on_msg),
            await self._nc.subscribe(
                self._any_subject, queue=self._group, cb=self._on_msg
            ),
        ]
        logger.info(
            "NATS subscribed (uni=%s any=%s)", self._uni_subject, self._any_subject
        )

    async def stop(self) -> None:
        """Unsubscribe. The connection is owned by the caller and left open."""
        self._running = False
        for sub in self._subs:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        self._subs.clear()
        self._subscribers.clear()

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        """Publish a request to its origin's partition, await its reply.

        The routing origin rides in ``headers`` under ``resonate:origin``,
        already resolved, so this never opens ``req``: the body is published
        exactly as the SDK serialized it -- no decode, no re-encode, and no
        second copy of the SDK's envelope layout living here to drift out of
        step with it.

        ``headers`` are the request headers -- the origin key plus any
        caller-supplied NATS message headers -- merged alongside the
        reply-inbox header this method sets on every publish.
        """
        logger.debug("nats_connection req: %s", req)
        if not self._running:
            raise ConnectorError(RuntimeError("connection has been stopped"))
        origin = (headers or {}).get(ORIGIN_HEADER, "default")
        subject = _publish_subject(self._api_prefix, origin)
        payload = req.encode("utf-8")
        inbox = self._nc.new_inbox()
        publish_headers = dict(headers) if headers else {}
        publish_headers[REPLY_HEADER] = inbox
        try:
            sub = await self._nc.subscribe(inbox, max_msgs=1)
            await self._nc.publish(subject, payload, headers=publish_headers)
            msg = await sub.next_msg(timeout=self._request_timeout)
        except Exception as exc:
            raise ConnectorError(exc) from exc

        resp_str = msg.data.decode("utf-8")
        logger.debug("nats_connection res: %s", resp_str)
        return resp_str

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming execute/unblock messages."""
        self._subscribers.append(callback)

    # -- internals ------------------------------------------------------------

    async def _on_msg(self, msg: NatsMsg) -> None:
        try:
            data = msg.data.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("dropping non-utf8 NATS message on %s", msg.subject)
            return
        logger.debug("nats_connection recv: %s", data)
        for cb in list(self._subscribers):
            cb(data)
