from __future__ import annotations


class ResonateError(Exception):
    """Top-level error type for the Resonate SDK."""


class FunctionNotFoundError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(f"function not found: {name} (version {version})")


class AlreadyRegisteredError(ResonateError):
    def __init__(self, name: str, version: int = 1) -> None:
        self.name = name
        self.version = version
        super().__init__(
            f"function '{self.name}' (version {self.version}) is already registered"
        )


class ServerError(ResonateError):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"server error (code={self.code}): {self.message}")


class StoppedError(ResonateError):
    """Skipped op after a prior failure stopped the execution.

    Not a server failure -- the network was never touched.
    """

    def __init__(self) -> None:
        super().__init__("execution stopped")


class DecodingError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"decoding error: {self.message}")


class SerializationError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"serialization error: {error}")


class HttpError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"http error: {error}")


class Base64DecodeError(ResonateError):
    def __init__(self, error: Exception) -> None:
        self.error = error
        super().__init__(f"base64 decode error: {error}")


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
        super().__init__("platform error: " + "; ".join(str(c) for c in causes))

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
        super().__init__("execution suspended")


class ApplicationError(ResonateError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class TimeoutError(ResonateError):
    def __init__(self) -> None:
        super().__init__("timeout")
