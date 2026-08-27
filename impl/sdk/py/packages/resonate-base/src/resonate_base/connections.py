"""The two seams a Resonate connector implements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["Network", "Source"]


@runtime_checkable
class Network(Protocol):
    """The request/response channel to the server.

    Every request Resonate issues (promise/task/schedule operations) flows
    through :meth:`send` as a JSON string and returns the server's JSON
    response. Methods raise on error.

    A :class:`Resonate <resonate.resonate.Resonate>` instance uses exactly one
    network, paired with one or more :class:`Source` push channels:
    ``Resonate(network=network, sources=[source, ...])``.

    Implementations ship separately from this package: the SDK's default
    transports (:class:`~resonate.connections.HttpConnection`,
    :class:`~resonate.connections.LocalConnection`) in ``resonate-sdk``, and
    one per connector elsewhere (:class:`resonate_nats.NatsConnection`, which
    is also a :class:`Source`).
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...


@runtime_checkable
class Source(Protocol):
    """A push-message channel from the server.

    The server delivers ``execute``/``unblock`` messages to the addresses a
    source advertises: :meth:`unicast` reaches this process alone and
    :meth:`anycast` any member of its group. :meth:`target_resolver` maps a
    bare group name to an anycast address in this source's scheme, and
    :meth:`pid`/:meth:`group` expose the identity those addresses embed.
    Register receivers via :meth:`recv` **before** :meth:`start`; messages
    arrive as JSON strings.

    A :class:`Resonate <resonate.resonate.Resonate>` instance may listen on
    several sources at once; the first is the *primary* source, whose
    addresses are advertised for listener registration and target routing.

    Implementations ship separately from this package:
    :class:`~resonate.connections.SSEConnection` and
    :class:`~resonate.connections.LocalConnection` in ``resonate-sdk``,
    :class:`resonate_nats.NatsConnection` in ``resonate-nats``.
    """

    def pid(self) -> str: ...
    def group(self) -> str: ...
    def unicast(self) -> str: ...
    def anycast(self) -> str: ...
    def target_resolver(self, target: str) -> str: ...
    def recv(self, callback: Callable[[str], None]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
