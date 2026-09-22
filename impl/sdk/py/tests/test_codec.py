from __future__ import annotations

import base64
import dataclasses
import enum
from typing import Any, NamedTuple, TypedDict

import attrs
import msgspec
import pydantic
import pytest

from resonate.codec import (
    Codec,
    NoopEncryptor,
    _deserialize_error,
    _encode_error,
    decode_settled,
)
from resonate.error import (
    ApplicationError,
    Base64DecodeError,
    ResonateError,
    SerializationError,
)
from resonate.types import PromiseRecord, PromiseState, Value


def codec() -> Codec:
    return Codec(NoopEncryptor())


# -- encode/decode roundtrip for primitives -----------------------------------


def test_roundtrip_integer() -> None:
    c = codec()
    encoded = c.encode(42)
    assert c.decode(encoded) == 42


def test_roundtrip_string() -> None:
    c = codec()
    encoded = c.encode("hello")
    assert c.decode(encoded) == "hello"


def test_decode_from_encoded_data_roundtrip() -> None:
    c = codec()
    encoded = c.encode({"x": 1})
    assert isinstance(encoded.data, str)
    assert c.decode(Value(data=encoded.data)) == {"x": 1}


def test_roundtrip_bool() -> None:
    c = codec()
    encoded = c.encode(value=True)
    assert c.decode(encoded) is True


# -- encode/decode roundtrip for objects --------------------------------------


def test_roundtrip_object() -> None:
    c = codec()
    obj = {"func": "f", "args": [1, "two"]}
    encoded = c.encode(obj)
    assert c.decode(encoded) == obj


# -- encode/decode roundtrip for arrays ---------------------------------------


def test_roundtrip_array() -> None:
    c = codec()
    arr = [1, 2, 3]
    encoded = c.encode(arr)
    assert c.decode(encoded) == arr


# -- encode None/null produces empty data -------------------------------------


def test_encode_null_produces_empty_data() -> None:
    c = codec()
    encoded = c.encode(None)
    assert encoded.data is None
    assert c.decode(encoded) is None


# -- decode_promise decodes both param and value ------------------------------


def test_decode_promise_decodes_param_and_value() -> None:
    c = codec()
    param_encoded = c.encode({"func": "f"})
    value_encoded = c.encode({"result": 42})

    record = PromiseRecord(
        id="test",
        state="resolved",
        timeout_at=0,
        param=param_encoded,
        value=value_encoded,
        tags={},
        created_at=0,
        settled_at=1,
    )

    decoded = c.decode_promise(record)
    assert decoded.param.data == {"func": "f"}
    assert decoded.value.data == {"result": 42}


# -- decode invalid base64 returns error --------------------------------------


def test_decode_invalid_base64_returns_error() -> None:
    c = codec()
    bad_value = Value(headers=None, data="not-base64!!!")
    with pytest.raises(ResonateError):
        c.decode(bad_value)


# -- encode error produces correct shape --------------------------------------


def test_encode_error_produces_correct_shape() -> None:
    err = ApplicationError("boom")
    encoded = _encode_error(err)
    assert encoded["__type"] == "error"
    assert encoded["message"] == "boom"


# =============================================================================
# User-defined struct styles round-trip through the codec
#
# Users declare the values they pass as durable-function arguments and return
# as results in many different ways. The codec serializes via
# ``msgspec.to_builtins`` and reshapes the decoded builtins via ``convert``
# (``msgspec.convert``), so a style survives the durability boundary iff msgspec
# supports it -- natively, or through the codec's enc/dec hooks (opt-in pydantic
# support). These tests pin which popular styles round-trip -- preserving both
# value and type. See also the end-to-end argument-coercion variants in
# ``test_durable.py``.
# =============================================================================


def _roundtrip[T](value: Any, type_: type[T]) -> T | None:
    c = codec()
    return c.convert(c.decode(c.encode(value)), type_)


# -- msgspec.Struct (the SDK's own convention) --------------------------------


class MsgspecPoint(msgspec.Struct, frozen=True):
    x: int
    y: int


def test_roundtrip_msgspec_struct() -> None:
    p = MsgspecPoint(x=3, y=4)
    out = _roundtrip(p, MsgspecPoint)
    assert out == p
    assert isinstance(out, MsgspecPoint)


# -- stdlib dataclass ---------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class DataclassPoint:
    x: int
    y: str


def test_roundtrip_dataclass() -> None:
    p = DataclassPoint(x=1, y="a")
    out = _roundtrip(p, DataclassPoint)
    assert out == p
    assert isinstance(out, DataclassPoint)


# -- attrs --------------------------------------------------------------------


@attrs.frozen
class AttrsPoint:
    x: int
    y: str


def test_roundtrip_attrs() -> None:
    p = AttrsPoint(x=1, y="a")
    out = _roundtrip(p, AttrsPoint)
    assert out == p
    assert isinstance(out, AttrsPoint)


# -- typing.NamedTuple (encodes as a JSON array, decodes back to the tuple) ---


class NamedTuplePoint(NamedTuple):
    x: int
    y: str


def test_roundtrip_namedtuple() -> None:
    p = NamedTuplePoint(1, "a")
    # NamedTuple serializes positionally as a tuple (JSON-encoded as an array),
    # not as an object.
    assert msgspec.to_builtins(p) == (1, "a")
    out = _roundtrip(p, NamedTuplePoint)
    assert out == p
    assert isinstance(out, NamedTuplePoint)


# -- typing.TypedDict (decodes back to a plain dict) --------------------------


class TypedDictPoint(TypedDict):
    x: int
    y: str


def test_roundtrip_typeddict() -> None:
    p: TypedDictPoint = {"x": 1, "y": "a"}
    out = _roundtrip(p, TypedDictPoint)
    assert out == {"x": 1, "y": "a"}


# -- enum.Enum / enum.IntEnum -------------------------------------------------


class Color(enum.Enum):
    RED = "red"


class Size(enum.IntEnum):
    LARGE = 2


def test_roundtrip_enum() -> None:
    assert msgspec.to_builtins(Color.RED) == "red"
    out = _roundtrip(Color.RED, Color)
    assert out is Color.RED


def test_roundtrip_int_enum() -> None:
    assert msgspec.to_builtins(Size.LARGE) == 2
    out = _roundtrip(Size.LARGE, Size)
    assert out is Size.LARGE


# -- nested / mixed styles ----------------------------------------------------


@dataclasses.dataclass(frozen=True)
class MixedShape:
    corner: MsgspecPoint
    label: str | None = None


def test_roundtrip_nested_mixed_styles() -> None:
    shape = MixedShape(corner=MsgspecPoint(x=1, y=2), label="tri")
    out = _roundtrip(shape, MixedShape)
    assert out == shape
    assert isinstance(out, MixedShape)
    assert isinstance(out.corner, MsgspecPoint)


def test_roundtrip_nested_optional_default_none() -> None:
    shape = MixedShape(corner=MsgspecPoint(x=0, y=0))
    out = _roundtrip(shape, MixedShape)
    assert isinstance(out, MixedShape)
    assert out == shape
    assert out.label is None


def test_roundtrip_list_of_dataclasses() -> None:
    points = [DataclassPoint(x=1, y="a"), DataclassPoint(x=2, y="b")]
    out = _roundtrip(points, list[DataclassPoint])
    assert out is not None
    assert out == points
    assert all(isinstance(p, DataclassPoint) for p in out)


# -- pydantic: opt-in native support via enc_hook/dec_hook --------------------


class PydanticPoint(pydantic.BaseModel):
    x: int
    y: str


def test_pydantic_model_roundtrips_to_its_type() -> None:
    # With pydantic installed the codec encodes a BaseModel (via model_dump) and
    # reconstructs the exact model type when convert is given that type.
    p = PydanticPoint(x=1, y="a")
    out = _roundtrip(p, PydanticPoint)
    assert out == p
    assert isinstance(out, PydanticPoint)


def test_pydantic_model_encodes_to_plain_json() -> None:
    # A BaseModel encodes to the same wire bytes as its model_dump mapping, so
    # decoding without a target type yields plain builtins.
    c = codec()
    encoded = c.encode(PydanticPoint(x=1, y="a"))
    assert c.decode(encoded) == {"x": 1, "y": "a"}


def test_pydantic_model_nested_in_container() -> None:
    # A model nested inside a normal container still serializes via the enc_hook.
    c = codec()
    encoded = c.encode({"pts": [PydanticPoint(x=1, y="a")]})
    assert c.decode(encoded) == {"pts": [{"x": 1, "y": "a"}]}


# =============================================================================
# decode returns None for absent/empty/null data
#
# ``Value.data`` is omitted on the wire when empty (``omit_defaults=True``), and
# Python collapses an absent field, an explicit JSON ``null``, and the empty
# string to the same ``None`` on decode. ``not value.data`` is the single guard
# for all three, so each must short-circuit before base64 is even attempted.
# =============================================================================


def test_decode_none_data_returns_none() -> None:
    assert codec().decode(Value(data=None)) is None


def test_decode_empty_string_data_returns_none() -> None:
    # An empty string is falsy, so the base64 step is skipped (an empty string is
    # not valid base64 under ``validate=True`` and would otherwise raise).
    assert codec().decode(Value(data="")) is None


def test_decode_default_value_returns_none() -> None:
    # The default-constructed ``Value`` (used as a promise's param/value default)
    # has no data and must decode to ``None`` rather than raising.
    assert codec().decode(Value()) is None


def test_roundtrip_explicit_null() -> None:
    # Encoding a literal ``None`` never produces data, so it is indistinguishable
    # from an absent value -- both decode back to ``None``.
    c = codec()
    encoded = c.encode(None)
    assert encoded.data is None
    assert c.decode(encoded) is None


# =============================================================================
# decode surfaces corruption as a loud, typed error
#
# Past the empty-data guard, the wire payload runs base64 -> decrypt -> JSON. A
# payload that survives base64 but is not valid JSON (e.g. a foreign encoding,
# or bytes a buggy peer wrote) must raise ``SerializationError`` rather than
# return a half-decoded value.
# =============================================================================


def test_decode_valid_base64_but_not_json_raises() -> None:
    c = codec()
    # Valid base64, but the bytes (0xff 0xff) are not valid UTF-8/JSON.
    not_json = base64.b64encode(b"\xff\xff").decode("ascii")
    with pytest.raises(SerializationError):
        c.decode(Value(data=not_json))


def test_decode_base64_with_whitespace_raises() -> None:
    # ``validate=True`` rejects embedded whitespace rather than silently
    # stripping it, so a mangled payload fails fast.
    c = codec()
    valid = c.encode({"x": 1}).data
    assert isinstance(valid, str)
    with pytest.raises(Base64DecodeError):
        c.decode(Value(data=valid[:2] + " " + valid[2:]))


# =============================================================================
# convert reshapes builtins and fails loudly on a type mismatch
#
# ``convert`` is the second half of the read path (``decode`` -> ``convert``).
# It coerces already-decoded builtins into the target type and, like ``encode``,
# must raise ``SerializationError`` rather than return a wrong-typed value.
# =============================================================================


def test_convert_coerces_builtins_into_type() -> None:
    assert codec().convert({"x": 1, "y": 2}, MsgspecPoint) == MsgspecPoint(x=1, y=2)


def test_convert_type_mismatch_raises() -> None:
    with pytest.raises(SerializationError):
        codec().convert("not-an-int", int)


def test_convert_missing_field_raises() -> None:
    with pytest.raises(SerializationError):
        codec().convert({"x": 1}, MsgspecPoint)


# =============================================================================
# ApplicationError encodes to the tagged error envelope and decodes back
#
# ``encode`` special-cases ``ApplicationError`` so a rejected promise's value is
# stored as ``{"__type": "error", "message": ...}``. ``decode_error`` is the
# inverse, reconstructing the originating ``ApplicationError``. This is the
# whole rejected-value path, kept behind the codec.
# =============================================================================


def test_encode_application_error_produces_envelope() -> None:
    c = codec()
    encoded = c.encode(ApplicationError("boom"))
    decoded = c.decode(encoded)
    assert decoded["__type"] == "error"
    assert decoded["message"] == "boom"
    # The original exception is also pickled best-effort so a Python awaiter
    # can recover its exact type (see codec._encode_error).
    assert "__py_pickle" in decoded


def test_decode_error_roundtrips_application_error() -> None:
    c = codec()
    encoded = c.encode(ApplicationError("boom"))
    err = c.decode_error(encoded)
    assert isinstance(err, ApplicationError)
    assert str(err) == "boom"


def test_decode_error_on_empty_value() -> None:
    # An empty rejected value decodes to ``None``; ``_deserialize_error`` then
    # renders it as an unknown error rather than crashing.
    c = codec()
    err = c.decode_error(Value())
    assert isinstance(err, ApplicationError)
    assert "unknown error" in str(err)


# =============================================================================
# _deserialize_error: only a string ``message`` is trusted
#
# The reconstructor recovers an ``ApplicationError(message)`` only when the
# decoded value is a dict carrying a string ``message``. Every other shape -- a
# dict without ``message``, a non-string ``message``, or a non-dict entirely --
# is wrapped as an opaque "unknown error" so malformed payloads never raise the
# wrong exception type or leak a non-string into the message.
# =============================================================================


def test_deserialize_error_dict_with_string_message() -> None:
    err = _deserialize_error({"__type": "error", "message": "boom"})
    assert isinstance(err, ApplicationError)
    assert str(err) == "boom"


def test_deserialize_error_dict_without_message() -> None:
    err = _deserialize_error({"code": 500})
    assert isinstance(err, ApplicationError)
    assert "unknown error" in str(err)
    assert "500" in str(err)


def test_deserialize_error_dict_with_non_string_message() -> None:
    err = _deserialize_error({"message": 42})
    assert isinstance(err, ApplicationError)
    assert "unknown error" in str(err)


def test_deserialize_error_non_dict_values() -> None:
    for value in ("plain string", 42, [1, 2, 3], None):
        err = _deserialize_error(value)
        assert isinstance(err, ApplicationError)
        assert "unknown error" in str(err)


# =============================================================================
# decode_settled maps a decoded record to its result or the matching error
#
# By the time ``decode_settled`` runs, ``decode_promise`` has already turned the
# wire value into builtins, so it works purely on decoded data. It returns the
# value for a resolved record, raises the reconstructed error for any rejected
# state, and raises on a still-pending (unexpected) record.
# =============================================================================


def _settled(state: PromiseState, data: Any) -> PromiseRecord:
    return PromiseRecord(
        id="p",
        state=state,
        timeout_at=0,
        value=Value(data=data),
    )


def test_decode_settled_resolved_returns_value() -> None:
    assert decode_settled(_settled("resolved", {"result": 42})) == {"result": 42}


def test_decode_settled_resolved_none_value() -> None:
    assert decode_settled(_settled("resolved", None)) is None


@pytest.mark.parametrize(
    "state",
    ["rejected", "rejected_canceled", "rejected_timedout"],
)
def test_decode_settled_rejected_states_raise_error(state: PromiseState) -> None:
    record = _settled(state, {"__type": "error", "message": "boom"})
    with pytest.raises(ApplicationError, match="boom"):
        decode_settled(record)


def test_decode_settled_pending_raises_unexpected_state() -> None:
    with pytest.raises(AssertionError):
        decode_settled(_settled("pending", None))


# =============================================================================
# Full rejected-value path: encode error -> decode_promise -> decode_settled
#
# Pins the pieces fitting together end-to-end: an ``ApplicationError`` stored on
# a rejected promise survives the wire encode, is decoded in place by
# ``decode_promise``, and is raised back as the originating error by
# ``decode_settled``.
# =============================================================================


def test_rejected_promise_roundtrip_raises_originating_error() -> None:
    c = codec()
    record = PromiseRecord(
        id="p",
        state="rejected",
        timeout_at=0,
        value=c.encode(ApplicationError("kaboom")),
    )
    decoded = c.decode_promise(record)
    with pytest.raises(ApplicationError, match="kaboom"):
        decode_settled(decoded)


# =============================================================================
# decode_promise carries every field but the two decoded payloads
#
# ``decode_promise`` swaps only ``param.data`` and ``value.data`` for their
# decoded forms; ``msgspec.structs.replace`` must carry id, state, timeouts,
# tags, and -- crucially -- each ``Value.headers`` through untouched, so the
# record stays robust as new fields are added.
# =============================================================================


def test_decode_promise_preserves_other_fields() -> None:
    c = codec()
    record = PromiseRecord(
        id="abc",
        state="resolved",
        timeout_at=99,
        param=Value(headers={"h": "1"}, data=c.encode({"a": 1}).data),
        value=Value(headers={"h": "2"}, data=c.encode({"b": 2}).data),
        tags={"k": "v"},
        created_at=7,
        settled_at=8,
    )
    decoded = c.decode_promise(record)

    assert decoded.id == "abc"
    assert decoded.state == "resolved"
    assert decoded.timeout_at == 99
    assert decoded.tags == {"k": "v"}
    assert decoded.created_at == 7
    assert decoded.settled_at == 8
    # Headers ride through untouched while only data is decoded.
    assert decoded.param.headers == {"h": "1"}
    assert decoded.value.headers == {"h": "2"}
    assert decoded.param.data == {"a": 1}
    assert decoded.value.data == {"b": 2}


def test_decode_promise_with_default_values() -> None:
    # A pending promise has the default (empty) param/value; both decode to
    # ``None`` without raising.
    c = codec()
    record = PromiseRecord(id="p", state="pending", timeout_at=0)
    decoded = c.decode_promise(record)
    assert decoded.param.data is None
    assert decoded.value.data is None


# =============================================================================
# Encryptor integration pins the pipeline order
#
# encode is value -> JSON -> encrypt -> base64; decode is base64 -> decrypt ->
# JSON. A non-trivial encryptor proves both that the codec actually calls
# encrypt/decrypt (not just the JSON+base64 layers) and that it calls them on
# the right side of base64 -- a reversing encryptor only round-trips if decrypt
# runs after base64-decode, on the same bytes encrypt produced before encode.
# =============================================================================


class ReversingEncryptor:
    """Reverses bytes on encrypt and back on decrypt (its own inverse)."""

    def encrypt(self, data: bytes) -> bytes:
        return data[::-1]

    def decrypt(self, data: bytes) -> bytes:
        return data[::-1]


def test_noop_encryptor_is_identity() -> None:
    enc = NoopEncryptor()
    assert enc.encrypt(b"hello") == b"hello"
    assert enc.decrypt(b"hello") == b"hello"


def test_roundtrip_with_nontrivial_encryptor() -> None:
    c = Codec(ReversingEncryptor())
    value = {"func": "f", "args": [1, "two"]}
    assert c.decode(c.encode(value)) == value


def test_encryptor_actually_transforms_wire_data() -> None:
    # The encrypted wire bytes differ from the plain (noop) encoding, proving the
    # encryptor is applied -- and that base64 wraps the encrypted bytes.
    value = {"x": 1}
    plain = Codec(NoopEncryptor()).encode(value)
    encrypted = Codec(ReversingEncryptor()).encode(value)
    assert encrypted.data != plain.data


def test_decode_fails_when_decryptor_does_not_match_encryptor() -> None:
    # Bytes encrypted by the reversing encryptor are gibberish to the noop
    # decryptor: the JSON parse fails loudly rather than returning junk.
    encrypted = Codec(ReversingEncryptor()).encode({"x": 1})
    with pytest.raises(SerializationError):
        Codec(NoopEncryptor()).decode(encrypted)
