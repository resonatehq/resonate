from __future__ import annotations

import base64
import binascii
import contextlib
import pickle
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

from resonate.error import (
    ApplicationError,
    Base64DecodeError,
    SerializationError,
)
from resonate.types import PromiseRecord, Value

if TYPE_CHECKING:
    from pydantic import BaseModel
else:
    try:
        from pydantic import BaseModel
    except ModuleNotFoundError:  # pydantic is an opt-in extra
        BaseModel = None


def enc_hook(obj: Any) -> Any:
    """Serialize types msgspec can't handle natively (opt-in Pydantic support)."""
    if BaseModel is not None and isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    msg = f"cannot encode object of type {type(obj).__name__}"
    raise TypeError(msg)


def dec_hook(expected: type, obj: Any) -> Any:
    """Reshape decoded builtins into a Pydantic model when that's the target."""
    if (
        BaseModel is not None
        and isinstance(expected, type)
        and issubclass(expected, BaseModel)
    ):
        return expected.model_validate(obj)
    msg = f"cannot decode object of type {expected}"
    raise TypeError(msg)


class Encryptor(Protocol):
    def encrypt(self, data: bytes) -> bytes:
        """Encrypts the given byte data."""

    def decrypt(self, data: bytes) -> bytes:
        """Decrypts the given byte data."""


class NoopEncryptor:
    """No-op encryptor (passthrough)."""

    def encrypt(self, data: bytes) -> bytes:
        return data

    def decrypt(self, data: bytes) -> bytes:
        return data


class Codec:
    """Handles encoding/decoding of values for the durability boundary.

    Encode: value -> JSON -> encrypt -> base64 -> ``Value { headers, data }``
    Decode: ``Value { headers, data }`` -> base64 -> decrypt -> JSON -> value
    """

    def __init__(self, encryptor: Encryptor) -> None:
        self.encryptor = encryptor

    def encode(self, value: Any | Exception) -> Value:
        """Encode a serializable value into the wire format.

        An ``Exception`` is always treated as a rejection: it is flattened to
        the error shape by :func:`_encode_error` (a plain ``message`` string,
        plus a best-effort ``__py_pickle`` payload so the awaiter can recover
        the original exception). A resolved value is never an exception object.
        """
        if value is None:
            return Value(headers=None, data=None)

        try:
            json_bytes = msgspec.json.encode(
                _encode_error(value) if isinstance(value, Exception) else value,
                enc_hook=enc_hook,
            )
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

        encrypted = self.encryptor.encrypt(json_bytes)
        return Value(headers=None, data=base64.b64encode(encrypted).decode("ascii"))

    def decode(self, value: Value) -> Any:
        """Decode a wire-format value into plain Python builtins.

        Crosses the wire (base64 -> decrypt -> JSON) and stops at builtins;
        returns ``None`` for empty or ``null`` ``data``. Reshaping the decoded
        builtins into a concrete type is :meth:`convert`'s job.
        """
        if not value.data:
            return None

        try:
            data = base64.b64decode(value.data, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise Base64DecodeError(exc) from exc

        try:
            return msgspec.json.decode(self.encryptor.decrypt(data))
        except msgspec.MsgspecError as exc:
            raise SerializationError(exc) from exc

    def convert[T](self, value: Any, type: type[T]) -> T:
        """Coerce an already-decoded value into ``type``.

        Wraps :func:`msgspec.convert` so type-shaping of decoded payloads stays
        inside the codec. Distinct from :meth:`decode`, which crosses the wire
        boundary: this only reshapes a value already on the Python side.
        """
        try:
            return msgspec.convert(value, type, dec_hook=dec_hook)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            raise SerializationError(exc) from exc

    def decode_error(self, value: Value) -> BaseException:
        """Decode a rejected promise's wire ``value`` into its originating error.

        Returns the original exception when its ``__py_pickle`` payload
        round-trips (same Python process / importable class), otherwise an
        :class:`ApplicationError`.
        """
        return _deserialize_error(self.decode(value))

    def decode_promise(self, promise: PromiseRecord) -> PromiseRecord:
        """Decode a promise's ``param`` and ``value`` fields in place.

        Only the two ``Value.data`` payloads change; ``msgspec.structs.replace``
        carries every other field (including each ``Value.headers``) through
        untouched, so the record stays robust to new fields.
        """
        return msgspec.structs.replace(
            promise,
            param=msgspec.structs.replace(
                promise.param, data=self.decode(promise.param)
            ),
            value=msgspec.structs.replace(
                promise.value, data=self.decode(promise.value)
            ),
        )


_PICKLE_KEY = "__py_pickle"


def _encode_error(err: Exception) -> dict[str, str]:
    """Encode an error for durable storage.

    ``message`` is a plain string and is always present. ``__py_pickle`` is a
    best-effort extra field -- a base64 pickle of the original exception so an
    awaiter can recover its exact type and attributes. Pickling can fail
    (unpicklable closures, locks, file handles, ...); on failure the field is
    simply omitted and the awaiter falls back to ``message``.
    """
    encoded = {"__type": "error", "message": str(err)}
    # Best-effort: any pickling failure (unpicklable closures, locks, ...)
    # leaves only ``message``, and the awaiter falls back to ApplicationError.
    with contextlib.suppress(Exception):
        encoded[_PICKLE_KEY] = base64.b64encode(pickle.dumps(err)).decode("ascii")
    return encoded


def _deserialize_error(value: Any) -> BaseException:
    """Deserialize an error value from a rejected promise.

    Prefers the original exception via ``__py_pickle`` when it round-trips
    (same Python runtime, importable class, intact payload); otherwise -- a
    non-Python producer, an unimportable class, a corrupt or absent payload --
    falls back to :class:`ApplicationError` carrying ``message``. The unpickled
    object is type-checked: a payload that does not deserialize to a
    ``BaseException`` is treated as a failure, not raised verbatim.

    Note: ``pickle.loads`` executes arbitrary code from the promise value;
    this trusts the Resonate server and peer workers as the payload source.
    """
    if isinstance(value, dict):
        pickled = value.get(_PICKLE_KEY)
        if isinstance(pickled, str):
            try:
                obj = pickle.loads(base64.b64decode(pickled, validate=True))  # noqa: S301
            except Exception:  # best-effort; fall back to message
                obj = None
            if isinstance(obj, BaseException):
                return obj

        msg = value.get("message")
        if isinstance(msg, str):
            return ApplicationError(msg)

    # msgspec.json.encode outputs bytes, so .decode() is necessary here to format the string
    rendered = msgspec.json.encode(value).decode("utf-8")
    return ApplicationError(f"unknown error: {rendered}")


def decode_settled(record: PromiseRecord) -> Any:
    """Map an already-decoded, settled record to its value, raising on rejection.

    The record's ``value`` has already been decoded by
    :meth:`Codec.decode_promise`, so a resolved payload is returned as-is and
    any rejected payload is turned back into the originating error via
    :func:`_deserialize_error`.
    """
    assert record.state != "pending"
    match record.state:
        case "resolved":
            return record.value.data
        case "rejected" | "rejected_canceled" | "rejected_timedout":
            raise _deserialize_error(record.value.data)
