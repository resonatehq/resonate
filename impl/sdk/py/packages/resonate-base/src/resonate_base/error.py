"""The three errors that cross the connector seam.

A connector raises exactly one of these -- :class:`ConnectorError`, when its
substrate fails -- and never sees the rest. The SDK's own vocabulary (the
codec's failures, the circuit breaker, the durable-execution control flow, and
the per-boundary union aliases that pin them) describes *executing durable
functions*, so it lives in :mod:`resonate.error` and is re-exported from there
alongside these three. SDK users have one import path; connector authors have
one class to subclass.

:class:`ResonateError` is here because it is the root every Resonate failure
shares, and :class:`InvalidIdError` because :mod:`resonate_base.ids` and
:mod:`resonate_base.addresses` raise it.
"""

from __future__ import annotations

__all__ = ["ConnectorError", "InvalidIdError", "ResonateError"]


class ResonateError(Exception):
    """Top-level error type for Resonate.

    The SDK's outermost catch is ``except ResonateError``, so anything a
    connector raises through the :class:`~resonate_base.connections.Network`
    seam must derive from it or it will kill the worker instead of releasing
    the task.
    """


class ConnectorError(ResonateError):
    """The substrate carrying a request to the server failed.

    The **one** error a connector needs. Base names the category; each
    connector either raises it directly or subclasses it to name itself::

        class NatsError(ConnectorError):
            label = "nats"          # -> "nats error: no responders"

    Subclassing is optional -- the SDK's own HTTP transport just raises
    ``ConnectorError(exc)``. It is worth doing when the connector ships as its
    own distribution and its users benefit from a name they can catch
    specifically.

    Either way callers can handle *any* transport failure, from a connector
    that did not exist when their code was written, without importing it::

        try:
            await resonate.run("greet", ...)
        except ConnectorError as exc:
            ...

    That is what lets :data:`resonate.error.SenderError` stay a closed,
    exhaustively type-checked union while the set of connectors stays open.
    Subclasses override :attr:`label` and nothing else: the wrapped ``error``,
    the ``args`` tuple (and so the pickle round-trip the codec depends on), and
    the message shape are all inherited.
    """

    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(error)

    def __str__(self) -> str:
        return f"connector error: {self.error}"


class InvalidIdError(ResonateError):
    """A caller-supplied id or address the server's format cannot carry.

    See :func:`resonate_base.ids.validate_root_id` and
    :mod:`resonate_base.addresses`.
    """

    def __init__(self, id: str, reason: str) -> None:
        self.id = id
        self.reason = reason
        super().__init__(id, reason)

    def __str__(self) -> str:
        return f"invalid id {self.id!r}: {self.reason}"
