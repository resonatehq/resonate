from __future__ import annotations

from typing import TYPE_CHECKING

import resonate_base
from resonate_base import Network, Source

if TYPE_CHECKING:
    from collections.abc import Callable


class OpaqueConnection:
    """A connector that knows nothing about promises, ids, or address formats.

    It moves strings and names destinations in its own namespace -- which is
    the entire job. If this class ever stops satisfying both protocols, the
    seam has started leaking SDK concerns into connector code.
    """

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, str] | None]] = []

    async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
        self.sent.append((req, headers))
        return "{}"

    def unicast(self) -> str:
        return "mysub://inbox.7f3a"

    def resolve_target(self, target: str) -> str:
        return f"mysub://group.{target}"

    def recv(self, callback: Callable[[str], None]) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


def test_a_connector_needs_nothing_beyond_these_methods() -> None:
    conn = OpaqueConnection()
    assert isinstance(conn, Network)
    assert isinstance(conn, Source)


def test_the_two_halves_are_independent() -> None:
    """Request/response and push are separate protocols, satisfiable alone.

    A process that only sends implements ``Network``; a process that only
    listens implements ``Source``. Requiring both would force every send-only
    client to invent an address nobody reads.
    """

    class SendOnly:
        async def send(self, req: str, headers: dict[str, str] | None = None) -> str:
            return "{}"

        async def start(self) -> None: ...
        async def stop(self) -> None: ...

    class ListenOnly:
        def unicast(self) -> str:
            return "mysub://inbox.7f3a"

        def resolve_target(self, target: str) -> str:
            return f"mysub://group.{target}"

        def recv(self, callback: Callable[[str], None]) -> None: ...
        async def start(self) -> None: ...
        async def stop(self) -> None: ...

    assert isinstance(SendOnly(), Network)
    assert not isinstance(SendOnly(), Source)
    assert isinstance(ListenOnly(), Source)
    assert not isinstance(ListenOnly(), Network)


def test_the_seam_carries_no_id_or_address_vocabulary() -> None:
    """``resonate_base`` exports three names and a version string.

    The promise id format and the ``poll://`` address helpers used to live
    here. They are SDK formats -- a connector routes by the ``origin`` it is
    handed (in the ``resonate:origin`` header) and addresses its substrate in
    its own namespace -- so importing them from base must stay impossible.
    """
    assert sorted(resonate_base.__all__) == [
        "ConnectorError",
        "Network",
        "ORIGIN_HEADER",
        "PROTOCOL_VERSION",
        "ResonateError",
        "Source",
    ]
    assert not hasattr(resonate_base, "ids")
    assert not hasattr(resonate_base, "addresses")
