from __future__ import annotations

import base64
import contextlib
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

from resonate.error import NatsError

if TYPE_CHECKING:
    from collections.abc import Callable

    from nats.aio.client import Client
    from nats.aio.subscription import Subscription

logger = logging.getLogger(__name__)

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
#: ``nats://`` delivery addresses advertised via :meth:`unicast` / :meth:`anycast`
#: round-trip through the server's ``url.Parse(host+path)`` to these subjects.
DEFAULT_RECV_PREFIX = "resonate.recv"

#: Header the server reads to learn where to publish its reply. The NATS
#: request/reply ``reply`` subject is *not* used by resonate-on-nats.
REPLY_HEADER = "Resonate-Reply-To"


def _id_to_origin(id: str) -> str:
    """Return the lineage origin: the substring before the first ``.``."""
    dot = id.find(".")
    return id if dot == -1 else id[:dot]


def _routing_origin(req: dict[str, Any]) -> str:
    """Derive the routing origin from a request, mirroring the server's client.

    The origin selects which origin-state partition the server loads, so it
    must be the lineage root of whatever id the request acts on.
    """
    kind = req.get("kind", "")
    data = req.get("data") or {}
    if kind in {
        "promise.get",
        "promise.create",
        "promise.settle",
        "task.acquire",
        "task.release",
        "task.suspend",
        "task.fulfill",
        "task.fence",
    }:
        return _id_to_origin(data.get("id", ""))
    if kind == "promise.register_listener":
        return _id_to_origin(data.get("awaited", ""))
    if kind == "task.create":
        return _id_to_origin(data["action"]["data"]["id"])
    if kind == "task.heartbeat":
        return _id_to_origin(data["tasks"][0]["id"])
    return "default"


def _publish_subject(prefix: str, origin: str) -> str:
    token = base64.urlsafe_b64encode(origin.encode("utf-8")).rstrip(b"=")
    return f"{prefix}.{token.decode('ascii')}"


class NatsNetwork:
    """:class:`Network` implementation that talks to resonate-on-nats over NATS.

    The NATS connection lifecycle lives **outside** the SDK: pass an already
    connected ``nats-py`` client; :meth:`stop` only tears down this network's
    subscriptions and leaves the connection for the caller to drain/close.

    - Requests are published to ``{api_prefix}.{base64url(origin)}`` with a
      ``Resonate-Reply-To`` header naming a private inbox; the reply arrives on
      that inbox (the server ignores the NATS reply subject).
    - Incoming execute/unblock messages arrive on a unicast subject
      (``{recv_prefix}.{group}.{pid}``) and an anycast subject
      (``{recv_prefix}.{group}``) queue-subscribed on ``group`` so exactly one
      group member receives each anycast message.
    - Addresses use the ``nats://`` scheme so the server's ``url.Parse`` maps
      ``nats://{subject}`` back to ``{subject}``.

    Requires the optional ``nats-py`` dependency (``uv add resonate-sdk[nats]``).
    """

    def __init__(
        self,
        conn: Client,
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
        self._anycast = f"nats://{self._any_subject}"
        self._recv_prefix = worker_topic
        self._request_timeout = request_timeout

        self._subscribers: list[Callable[[str], None]] = []
        self._subs: list[Subscription] = []
        self._running = False

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    async def start(self) -> None:
        """Subscribe to the unicast and anycast subjects on the shared connection."""
        self._running = True
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

    async def send(self, req: str) -> str:
        """Publish a request and await its reply on a private inbox."""
        logger.debug("nats_network req: %s", req)
        if not self._running:
            raise NatsError(RuntimeError("network has been stopped"))
        envelope = json.loads(req)
        origin = _routing_origin(envelope)
        # The server reads the origin from the head, not the subject; set both.
        envelope.setdefault("head", {})["resonate:origin"] = origin
        subject = _publish_subject(self._api_prefix, origin)
        payload = json.dumps(envelope).encode("utf-8")
        inbox = self._nc.new_inbox()
        try:
            sub = await self._nc.subscribe(inbox, max_msgs=1)
            await self._nc.publish(subject, payload, headers={REPLY_HEADER: inbox})
            msg = await sub.next_msg(timeout=self._request_timeout)
        except Exception as exc:
            raise NatsError(exc) from exc

        resp_str = msg.data.decode("utf-8")
        logger.debug("nats_network res: %s", resp_str)
        return resp_str

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming execute/unblock messages."""
        self._subscribers.append(callback)

    def target_resolver(self, target: str) -> str:
        """Resolve a target group name to its ``nats://`` anycast address."""
        return f"nats://{self._recv_prefix}.{target}"

    # -- internals ------------------------------------------------------------

    async def _on_msg(self, msg: Any) -> None:
        try:
            data = msg.data.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("dropping non-utf8 NATS message on %s", msg.subject)
            return
        logger.debug("nats_network recv: %s", data)
        for cb in list(self._subscribers):
            cb(data)
