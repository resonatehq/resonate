from __future__ import annotations

__all__ = ["ConnectorError", "ResonateError"]


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

        class NatsError(ConnectorError): ...

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
    A subclass adds nothing but its name: the wrapped ``error``, the ``args``
    tuple (and so the pickle round-trip the codec depends on), and the message
    shape are all inherited, so there is no ``__init__`` to forget to forward.
    """

    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(error)

    def __str__(self) -> str:
        return f"connector error: {self.error}"
