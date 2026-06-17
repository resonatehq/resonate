"""Pins that every SDK error type survives a pickle round-trip.

Why this matters
----------------

Exceptions cross the durable-promise boundary as a base64 ``__py_pickle`` of the
original object (see :func:`resonate.codec._encode_error`): a same-runtime
awaiter that recovers a rejection gets the **exact type and attributes** back
only if the class round-trips through ``pickle``. ``pickle`` reconstructs a
``BaseException`` via ``cls(*self.args)``, so every error's ``__init__`` must
forward its real constructor arguments to ``super().__init__`` -- forwarding a
pre-formatted string instead silently breaks reconstruction (wrong arity). These
tests are the regression guard: construct one of every error type, round-trip
it, and assert the type, ``str``, and public attributes all come back intact.
"""

from __future__ import annotations

import pickle

import pytest

from resonate.error import (
    AlreadyRegisteredError,
    ApplicationError,
    Base64DecodeError,
    DecodingError,
    FunctionNotFoundError,
    HttpError,
    PlatformError,
    ResonateError,
    ResonateTimeoutError,
    SerializationError,
    ServerError,
    StoppedError,
    Suspended,
)

# One representative instance of every error type the SDK defines. The wrapper
# errors carry a plain ``ValueError`` -- a picklable, importable nested
# exception -- so the round-trip exercises the error itself, not its payload.
ERRORS = [
    ResonateError("top-level"),
    FunctionNotFoundError("foo", 2),
    AlreadyRegisteredError("bar"),
    ServerError(500, "boom"),
    StoppedError(),
    DecodingError("bad bytes"),
    SerializationError(ValueError("nope")),
    HttpError(ValueError("net down")),
    Base64DecodeError(ValueError("bad b64")),
    PlatformError([ServerError(500, "boom"), ResonateTimeoutError()]),
    Suspended(),
    ApplicationError("app boom"),
    ResonateTimeoutError(),
]


@pytest.mark.parametrize("err", ERRORS, ids=lambda e: type(e).__name__)
def test_pickle_roundtrip_preserves_type_and_message(err: BaseException) -> None:
    restored = pickle.loads(pickle.dumps(err))  # noqa: S301
    assert type(restored) is type(err)
    assert str(restored) == str(err)
    # A second round-trip proves reconstruction is stable: pickle rebuilds via
    # ``cls(*self.args)``, so wrong arity would raise here even if the first
    # ``dumps`` succeeded.
    again = pickle.loads(pickle.dumps(restored))  # noqa: S301
    assert type(again) is type(err)
    assert str(again) == str(err)


def test_pickle_roundtrip_preserves_attributes() -> None:
    err = FunctionNotFoundError("foo", 2)
    restored = pickle.loads(pickle.dumps(err))  # noqa: S301
    assert restored.name == "foo"
    assert restored.version == 2


def test_platform_error_causes_and_cause_survive_roundtrip() -> None:
    err = PlatformError([ServerError(1, "first"), ResonateTimeoutError()])
    restored = pickle.loads(pickle.dumps(err))  # noqa: S301
    assert [type(c) for c in restored.causes] == [ServerError, ResonateTimeoutError]
    assert isinstance(restored.cause, ServerError)
    assert restored.cause.code == 1
