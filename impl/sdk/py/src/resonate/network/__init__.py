from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from resonate.network.http import HttpNetwork
from resonate.network.local import LocalNetwork

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["HttpNetwork", "LocalNetwork", "Network"]


class Network(Protocol):
    """The transport abstraction for all server communication.

    All communication between Resonate and the server (local or remote) flows
    through it as JSON strings. Methods raise on error.

    Two implementations are provided: :class:`LocalNetwork` runs an in-process
    server simulation, and :class:`HttpNetwork` talks to a Resonate server
    over HTTP.
    """

    def pid(self) -> str: ...
    def group(self) -> str: ...
    def unicast(self) -> str: ...
    def anycast(self) -> str: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...
    def recv(self, callback: Callable[[str], None]) -> None: ...
    def target_resolver(self, target: str) -> str: ...
