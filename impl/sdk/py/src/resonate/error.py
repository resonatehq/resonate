"""The SDK's error vocabulary.

Everything here describes *executing durable functions*: the codec's failures,
the circuit breaker that skips work after a prior failure, the two control-flow
signals that must dodge ``except Exception``, and the per-boundary union
aliases that pin the whole set.

Two names come from :mod:`resonate_base.error` and are re-exported so this
module is the single import path for SDK users: :class:`ResonateError` (the
root) and :class:`ConnectorError` (what a transport raises -- see below).

The split is by *who raises it*. A connector -- NATS, or something that does
not exist yet -- raises :class:`ConnectorError` and needs nothing else, so that
is all ``resonate-base`` ships. :class:`InvalidIdError` is defined here rather
than there because the format that raises it -- the promise id -- belongs to
whoever defines it, not to the seam: a connector moves opaque strings and names
its destinations however its substrate does, so no connector raises this about
an address of its own. It is also the one open point in the vocabulary:
because it names the category rather than each transport, :data:`SenderError`
below can stay closed and exhaustively type-checked while the set of connectors
stays open.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from resonate_base.error import ConnectorError, ResonateError

__all__ = [
    "AlreadyRegisteredError",
    "ApplicationError",
    "Base64DecodeError",
    "ConnectorError",
    "DecodingError",
    "DispatchError",
    "DurableOpError",
    "FunctionNotFoundError",
    "InvalidIdError",
    "PlatformError",
    "ResonateError",
    "ResonateTimeoutError",
    "SenderError",
    "SerializationError",
    "ServerError",
    "StoppedError",
    "Suspended",
]


class InvalidIdError(ResonateError):
    """A caller-supplied id the server's format cannot carry.

    See :func:`resonate.ids.validate_root_id`. Addresses are *not* in scope:
    connector address minting is total by design (see
    :func:`resonate.connections.sse.unicast`), and a malformed one is the
    server's judgement to make.
    """

    def __init__(self, id: str, reason: str) -> None:
        self.id = id
        self.reason = reason
        super().__init__(id, reason)

    def __str__(self) -> str:
        return f"invalid id {self.id!r}: {self.reason}"


class FunctionNotFoundError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(name, version)

    def __str__(self) -> str:
        return f"function not found: {self.name} (version {self.version})"


class AlreadyRegisteredError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(name, version)

    def __str__(self) -> str:
        return f"function '{self.name}' (version {self.version}) is already registered"


class ServerError(ResonateError):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(code, message)

    def __str__(self) -> str:
        return f"server error (code={self.code}): {self.message}"


class StoppedError(ResonateError):
    """Skipped op after a prior failure stopped the execution.

    Not a server failure -- the network was never touched.
    """

    def __init__(self) -> None:
        super().__init__()

    def __str__(self) -> str:
        return "execution stopped"


class DecodingError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"decoding error: {self.message}"


class SerializationError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(error)

    def __str__(self) -> str:
        return f"serialization error: {self.error}"


class Base64DecodeError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(error)

    def __str__(self) -> str:
        return f"base64 decode error: {self.error}"


class PlatformError(BaseException):
    """A Resonate platform failure inside a durable execution.

    Extends ``BaseException`` (like :class:`Suspended`) so user code's
    ``except Exception`` cannot swallow it; the task must be released, not
    fulfilled. Always raised ``from`` the original :class:`ResonateError`,
    which is also kept on ``causes``.

    Always carries a *list* of causes: a single durable op failing wraps one
    error, while ``flush_local_work`` aggregates every concurrent failure into
    one error with all causes. ``cause`` returns the first (primary) one so the
    outer-boundary unwrap keeps surfacing a single ``ResonateError``.
    """

    def __init__(self, causes: list[ResonateError]) -> None:
        if not causes:
            # Not an assert: asserts are stripped under ``python -O``, which
            # would turn this into a later ``IndexError`` on ``cause``.
            msg = "PlatformError needs at least one cause"
            raise ValueError(msg)
        self.causes: list[ResonateError] = causes
        super().__init__(causes)

    def __str__(self) -> str:
        return "platform error: " + "; ".join(str(c) for c in self.causes)

    @property
    def cause(self) -> ResonateError:
        """The first (primary) cause -- what the outer boundary unwraps to."""
        return self.causes[0]


class Suspended(BaseException):
    """Signals that an execution has suspended.

    Extends ``BaseException`` so that a ``try/except Exception`` does not
    swallow it.
    """

    def __init__(self) -> None:
        super().__init__()

    def __str__(self) -> str:
        return "execution suspended"


class ApplicationError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return self.message


class ResonateTimeoutError(ResonateError):
    def __init__(self) -> None:
        super().__init__()

    def __str__(self) -> str:
        return "timeout"


# ═══════════════════════════════════════════════════════════════
#  Per-boundary error vocabularies
# ═══════════════════════════════════════════════════════════════
#
# Python models failure with exceptions, not ``Result`` values, so a function's
# signature cannot carry its failure set. These aliases carry it instead: each
# names the closed union one *boundary* is allowed to fail with, so the failure
# space is documented in one place and the test plan falls out of the union --
# one test per variant.
#
# They are checked, not decorative: :func:`_pin_unions` below makes the type
# checker reject a variant added to a boundary without being added here.


#: What a :class:`~resonate.send.Sender` call can fail with. Every one of these
#: reaches the caller from the wire: a non-2xx status, an unparseable response,
#: or the underlying connection giving up.
#:
#: :class:`ConnectorError` stands for *every* transport, present and future --
#: which is what keeps this union closed while the set of connectors stays
#: open. Naming each transport here instead would mean a new entry, and a
#: release, per connector.
type SenderError = ServerError | DecodingError | ConnectorError

#: What one durable operation (:mod:`resonate.effects`) can fail with -- the
#: sender's vocabulary plus the codec's, plus the circuit-breaker's
#: :class:`StoppedError` for an op skipped after a prior failure. This is the
#: set that arrives wrapped in a :class:`PlatformError`'s ``causes``.
type DurableOpError = (
    SenderError | SerializationError | Base64DecodeError | StoppedError
)

#: What the SDK's *front door* (register / run / rpc / get) can fail with
#: before any durable work starts.
type DispatchError = (
    FunctionNotFoundError | AlreadyRegisteredError | InvalidIdError | SenderError
)


if TYPE_CHECKING:
    from typing import assert_never

    def _pin_unions(
        sender: SenderError,
        durable: DurableOpError,
        dispatch: DispatchError,
    ) -> None:
        """Compile-time test: the unions above cannot drift silently.

        Never called. ``ty`` (run in CI) rejects this file if a member is
        removed from a union without its ``case`` arm going too, or if an arm
        is added that the union does not contain -- the Python equivalent of
        pinning a signature with a ``const`` coercion.
        """
        match sender:
            case ServerError() | DecodingError() | ConnectorError():
                pass
            case _:
                assert_never(sender)

        match durable:
            case (
                ServerError()
                | DecodingError()
                | ConnectorError()
                | SerializationError()
                | Base64DecodeError()
                | StoppedError()
            ):
                pass
            case _:
                assert_never(durable)

        match dispatch:
            case (
                FunctionNotFoundError()
                | AlreadyRegisteredError()
                | InvalidIdError()
                | ServerError()
                | DecodingError()
                | ConnectorError()
            ):
                pass
            case _:
                assert_never(dispatch)
