from __future__ import annotations

import msgspec
import pytest

from resonate.types import (
    Args,
    Info,
    PromiseCreateReq,
    PromiseRecord,
    PromiseRegisterCallbackData,
    PromiseSettleReq,
    TaskData,
    TaskRecord,
    Value,
)

# --- serialization: omit_defaults drops None fields from the wire format ---


def test_encode_empty_value_is_empty_object() -> None:
    assert msgspec.json.encode(Value()) == b"{}"


def test_encode_headers_only() -> None:
    assert msgspec.json.encode(Value(headers={"a": "b"})) == b'{"headers":{"a":"b"}}'


def test_encode_data_only() -> None:
    assert msgspec.json.encode(Value(data=42)) == b'{"data":42}'


def test_encode_both_fields_in_field_order() -> None:
    encoded = msgspec.json.encode(Value(headers={"a": "b"}, data=42))
    assert encoded == b'{"headers":{"a":"b"},"data":42}'


# --- data_or_null ---


def test_data_or_null_defaults_to_null() -> None:
    assert Value().data is None


def test_data_or_null_returns_data() -> None:
    assert Value(data=42).data == 42


# --- wire decode: msgspec decodes the {headers, data} struct natively ---


def test_value_decodes_from_wire_object() -> None:
    v = msgspec.json.decode(b'{"headers":{"a":"b"},"data":1}', type=Value)
    assert v.headers == {"a": "b"}
    assert v.data == 1


def test_value_decodes_empty_object() -> None:
    v = msgspec.json.decode(b"{}", type=Value)
    assert v.headers is None
    assert v.data is None


def test_promise_record_decode_minimal_applies_defaults() -> None:
    # Only the required fields; the rest fall back to struct defaults.
    r = msgspec.json.decode(
        b'{"id":"p1","state":"pending","timeoutAt":10}', type=PromiseRecord
    )
    assert r.param.data is None
    assert r.param.headers is None
    assert r.value.data is None
    assert r.tags == {}
    assert r.created_at == 0
    assert r.settled_at is None


def test_promise_record_decode_missing_required_field_raises() -> None:
    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"id":"p1","state":"pending"}', type=PromiseRecord)


def test_promise_record_encode_camel_and_field_order() -> None:
    r = PromiseRecord(
        id="p1",
        state="pending",
        param=Value(data=1),
        tags={"k": "v"},
        timeout_at=10,
        created_at=5,
    )
    assert msgspec.json.encode(r) == (
        b'{"id":"p1","state":"pending","param":{"data":1},"value":{},'
        b'"tags":{"k":"v"},"timeoutAt":10,"createdAt":5,"settledAt":null}'
    )


# --- TaskRecord ---


def test_task_record_decode_minimal_applies_defaults() -> None:
    r = msgspec.json.decode(
        b'{"id":"t1","state":"pending","version":1}', type=TaskRecord
    )
    assert r.resumes is None
    assert r.ttl is None
    assert r.pid is None


def test_task_record_resumes_variants() -> None:
    cases: list[tuple[bytes, list[str] | int | bool | None]] = [
        (b'["a","b"]', ["a", "b"]),
        (b"5", 5),
        (b"true", True),
        (b"null", None),
    ]
    for raw, expected in cases:
        r = msgspec.json.decode(
            b'{"id":"t","state":"pending","version":1,"resumes":' + raw + b"}",
            type=TaskRecord,
        )
        assert r.resumes == expected
        # bool is a subclass of int -- guard against int/bool confusion.
        assert type(r.resumes) is type(expected)


def test_task_record_encode() -> None:
    r = TaskRecord(id="t1", state="acquired", version=2, resumes=["a"], ttl=30, pid="x")
    assert (
        msgspec.json.encode(r)
        == b'{"id":"t1","state":"acquired","version":2,"resumes":["a"],"ttl":30,"pid":"x"}'
    )


def test_task_record_encode_defaults_emit_null() -> None:
    r = TaskRecord(id="t1", state="pending", version=1)
    assert (
        msgspec.json.encode(r)
        == b'{"id":"t1","state":"pending","version":1,"resumes":null,"ttl":null,"pid":null}'
    )


# --- PromiseCreateReq: camelCase wire format + default_with_id ---


def test_promise_create_req_encode_camel_and_field_order() -> None:
    r = PromiseCreateReq(id="p1", timeout_at=10, param=Value(data=1), tags={"k": "v"})
    assert msgspec.json.encode(r) == (
        b'{"id":"p1","timeoutAt":10,"param":{"data":1},"tags":{"k":"v"}}'
    )


def test_promise_create_req_decode_camel() -> None:
    r = msgspec.json.decode(
        b'{"id":"p1","timeoutAt":10,"param":{"data":1},"tags":{"k":"v"}}',
        type=PromiseCreateReq,
    )
    assert r.id == "p1"
    assert r.timeout_at == 10
    assert r.param.data == 1
    assert r.tags == {"k": "v"}


def test_promise_create_req_default_with_id() -> None:
    r = PromiseCreateReq(id="p1")
    assert r.id == "p1"
    assert r.timeout_at == 0
    assert r.param.headers is None
    assert r.param.data is None
    assert r.tags == {}


# --- PromiseSettleReq ---


def test_promise_settle_req_encode() -> None:
    r = PromiseSettleReq(id="p1", state="resolved", value=Value(data=1))
    assert (
        msgspec.json.encode(r) == b'{"id":"p1","state":"resolved","value":{"data":1}}'
    )


def test_promise_settle_req_decode() -> None:
    r = msgspec.json.decode(
        b'{"id":"p1","state":"rejected_canceled","value":{}}', type=PromiseSettleReq
    )
    assert r.id == "p1"
    assert r.state == "rejected_canceled"
    assert r.value.data is None


# --- PromiseRegisterCallbackData ---


def test_promise_register_callback_data_roundtrip() -> None:
    d = PromiseRegisterCallbackData(awaited="a", awaiter="b")
    encoded = msgspec.json.encode(d)
    assert encoded == b'{"awaited":"a","awaiter":"b"}'
    back = msgspec.json.decode(encoded, type=PromiseRegisterCallbackData)
    assert back.awaited == "a"
    assert back.awaiter == "b"


# --- Args: the packed *args / **kwargs slot (defaults empty) ---


def test_args_decode_minimal_applies_defaults() -> None:
    a = msgspec.json.decode(b"{}", type=Args)
    assert a.args == ()
    assert a.kwargs == {}


def test_args_decode_full() -> None:
    a = msgspec.json.decode(b'{"args":[1,2],"kwargs":{"k":3}}', type=Args)
    assert a.args == (1, 2)
    assert a.kwargs == {"k": 3}


def test_args_encode() -> None:
    a = Args(args=(1, 2), kwargs={"k": 3})
    assert msgspec.json.encode(a) == b'{"args":[1,2],"kwargs":{"k":3}}'


# --- TaskData: Args fields flattened + `func` / `version` + into_value ---


def test_task_data_decode_minimal_applies_default_args() -> None:
    d = msgspec.json.decode(b'{"func":"f","version":2}', type=TaskData)
    assert d.func == "f"
    assert d.args == ()
    assert d.kwargs == {}
    assert d.version == 2


def test_task_data_decode_full() -> None:
    d = msgspec.json.decode(
        b'{"func":"f","args":[1,2],"kwargs":{"k":3},"version":1}', type=TaskData
    )
    assert d.func == "f"
    assert d.args == (1, 2)
    assert d.kwargs == {"k": 3}
    assert d.version == 1


def test_task_data_version_defaults_to_one() -> None:
    # ``version`` defaults to 1: an omitted version (e.g. a foreign-SDK payload)
    # resolves deterministically to the first registered version. Version 0 --
    # which once meant "latest registered" -- is no longer used.
    d = msgspec.json.decode(b'{"func":"f"}', type=TaskData)
    assert d.func == "f"
    assert d.version == 1


def test_task_data_encode() -> None:
    # Inherited Args fields encode first, then ``func`` / ``version``.
    d = TaskData(func="f", args=(1, 2), kwargs={"k": 3}, version=2)
    assert (
        msgspec.json.encode(d)
        == b'{"args":[1,2],"kwargs":{"k":3},"func":"f","version":2}'
    )


def test_task_data_encode_defaults_emit_empty() -> None:
    d = TaskData(func="f")
    assert msgspec.json.encode(d) == b'{"args":[],"kwargs":{},"func":"f","version":1}'


def info() -> Info:
    return Info(
        id="id",
        parent_id="parent",
        origin_id="origin",
        branch_id="branch",
        timeout_at=123,
        func_name="func",
        tags={"k": "v"},
    )


def test_info_fields() -> None:
    i = info()
    assert i.id == "id"
    assert i.parent_id == "parent"
    assert i.origin_id == "origin"
    assert i.branch_id == "branch"
    assert i.timeout_at == 123
    assert i.func_name == "func"
    assert i.tags == {"k": "v"}
