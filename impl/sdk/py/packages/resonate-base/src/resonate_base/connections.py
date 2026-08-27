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
    response. Methods raise :class:`~resonate_base.error.ConnectorError` when
    the substrate fails.

    ``send`` takes the request and the ``headers`` that travel with it. The
    lineage ``origin`` the request routes by -- the value that selects the
    server's origin-state partition -- rides in ``headers`` under the
    ``resonate:origin`` key. A substrate that shards (one NATS subject per
    partition, say) reads it from there to place the request, so it never has
    to know the envelope layout or the promise id format. Two consequences
    worth stating plainly: ``req`` stays opaque -- a connector never has to
    parse it -- and every request carries an origin header, so there is no
    "unrouted" case to handle.

    A ``Resonate`` instance uses exactly one network, optionally paired with
    :class:`Source` push channels.
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str, headers: dict[str, str] | None = None) -> str: ...


@runtime_checkable
class Source(Protocol):
    """A push-message channel from the server.

    Two strings make a source routable, and the connector chooses both:

    :meth:`unicast`
        Where the server pushes messages meant for *this process alone*. It is
        handed to the server verbatim when a listener is registered.
    :meth:`resolve_target`
        Renders a caller-named routing group -- ``"workers"``, say -- as the
        address the server should deliver that group's work to. It is what the
        SDK stamps onto a promise's ``resonate:target`` tag.

    Both are opaque to the SDK and to the server alike: the server parses an
    address with Go's ``url.Parse``, dispatches on the scheme, and hands the
    rest straight back to the connector that minted it. So the *shape* of an
    address is the connector's own business -- ``resonate_nats`` advertises
    ``nats://resonate.recv.workers.7f3a`` because a NATS subject already is an
    address in the NATS namespace.

    Case matters. Whatever lands in the URL *host* is lowercased by the
    server's parser, so an address minted with an uppercase host does not round
    trip -- and nothing raises: the server accepts it, stores it, and the
    message is simply never delivered.

    Register receivers via :meth:`recv` **before** :meth:`start`; messages
    arrive as JSON strings.

    A ``Resonate`` instance may listen on several sources at once, or on none
    at all (a request/response-only deployment, where work arrives by some
    other means -- an HTTP handler, say).
    """

    def unicast(self) -> str: ...
    def resolve_target(self, target: str) -> str: ...
    def recv(self, callback: Callable[[str], None]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
