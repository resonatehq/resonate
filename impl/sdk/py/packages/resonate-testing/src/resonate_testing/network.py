"""Network / Source stand-ins.

One family of doubles, replacing the four near-identical ones the suite used to
carry (``StubNetwork`` twice, ``_FakeNetwork``, ``_RecordingNetwork``). All of
them implement both :class:`~resonate.connections.Network` and
:class:`~resonate.connections.Source`, because the in-process connection they
stand in for does too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


def envelope(kind: str, corr_id: str, data: Any, status: int | None = None) -> str:
    """Build a wire response envelope. ``status`` is omitted when ``None``."""
    head: dict[str, Any] = {"corrId": corr_id}
    if status is not None:
        head["status"] = status
    return msgspec.json.encode({"kind": kind, "head": head, "data": data}).decode(
        "utf-8"
    )


class StubNetwork:
    """Dual-role connection returning one canned response to every request.

    ``send`` records the request body and replies with ``response``; ``recv``
    captures each registered callback so a test can push raw messages in.
    """

    def __init__(self, response: str = "") -> None:
        self.response = response
        self.sent: list[str] = []
        self.callbacks: list[Callable[[str], None]] = []
        self.started = 0
        self.stopped = 0
        self._next_error: BaseException | None = None

    # -- Source identity ------------------------------------------------------

    def pid(self) -> str:
        return "test"

    def group(self) -> str:
        return "default"

    def unicast(self) -> str:
        return "local://uni@default/test"

    def anycast(self) -> str:
        return "local://any@default/test"

    def target_resolver(self, target: str) -> str:
        return f"local://any@{target}"

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    # -- traffic --------------------------------------------------------------

    async def send(self, req: str) -> str:
        self.sent.append(req)
        self._raise_if_armed()
        return self.response

    def recv(self, callback: Callable[[str], None]) -> None:
        self.callbacks.append(callback)

    def fail_next(self, error: BaseException) -> None:
        """Arm the *next* ``send`` to raise ``error``, then recover.

        A one-shot failure is the shape most resilience tests want -- "does the
        loop survive one bad round trip" -- and expressing it here keeps every
        test from re-rolling its own patched session.
        """
        self._next_error = error

    def _raise_if_armed(self) -> None:
        error, self._next_error = self._next_error, None
        if error is not None:
            raise error

    # -- test affordances -----------------------------------------------------

    def push(self, raw: str) -> None:
        """Deliver ``raw`` to every registered receiver, as a source would."""
        for cb in list(self.callbacks):
            cb(raw)

    def kinds(self) -> list[str]:
        """Return the ``kind`` of every request sent, in order."""
        return [msgspec.json.decode(body)["kind"] for body in self.sent]

    def bodies(self) -> list[Any]:
        """Every request sent, decoded from JSON."""
        return [msgspec.json.decode(body) for body in self.sent]


class ScriptedNetwork(StubNetwork):
    """Replies with a different canned response per request, in order.

    Once the script is exhausted the last entry repeats, so a test only has to
    script the prefix it cares about.
    """

    def __init__(self, responses: Sequence[str]) -> None:
        super().__init__()
        if not responses:
            msg = "ScriptedNetwork needs at least one response"
            raise AssertionError(msg)
        self.responses = list(responses)

    async def send(self, req: str) -> str:
        self.sent.append(req)
        self._raise_if_armed()
        index = min(len(self.sent) - 1, len(self.responses) - 1)
        return self.responses[index]


class RecordingNetwork(StubNetwork):
    """Records requests and replies with a bare success envelope.

    The default for tests that care only about *what was sent* -- heartbeats,
    releases, listener registrations.
    """

    async def send(self, req: str) -> str:
        self.sent.append(req)
        self._raise_if_armed()
        decoded = msgspec.json.decode(req)
        return envelope(decoded["kind"], decoded["head"]["corrId"], {})


class FailingNetwork(StubNetwork):
    """Raises ``error`` from every ``send``. For the platform-failure paths."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    async def send(self, req: str) -> str:
        self.sent.append(req)
        raise self.error


# =============================================================================
# Connection-selection doubles
# =============================================================================
#
# These carry a ``fake://`` address scheme so a test can prove an address came
# from the source it injected rather than from a real connection.


class FakeSource:
    """Minimal :class:`~resonate.connections.Source` for construction tests."""

    def __init__(self, pid: str = "src", group: str = "grp") -> None:
        self._pid = pid
        self._group = group
        self.callbacks: list[Callable[[str], None]] = []
        self.started = False
        self.stopped = False

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return f"fake://uni@{self._group}/{self._pid}"

    def anycast(self) -> str:
        return f"fake://any@{self._group}/{self._pid}"

    def target_resolver(self, target: str) -> str:
        return f"fake://any@{target}"

    def recv(self, callback: Callable[[str], None]) -> None:
        self.callbacks.append(callback)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def push(self, raw: str) -> None:
        """Deliver ``raw`` to every registered receiver."""
        for cb in list(self.callbacks):
            cb(raw)


class FakeNetwork(FakeSource):
    """A non-Local dual-role connection: ``Network`` *and* ``Source``.

    Used to check the branches that treat a remote network differently from a
    :class:`~resonate.connections.LocalConnection` -- notably heartbeat
    selection.
    """

    def __init__(self, pid: str = "fake", group: str = "g") -> None:
        super().__init__(pid, group)
        self.sent: list[str] = []

    async def send(self, req: str) -> str:
        self.sent.append(req)
        return "{}"


class SendOnlyNetwork:
    """A :class:`~resonate.connections.Network` with no source half."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, req: str) -> str:
        self.sent.append(req)
        return "{}"
