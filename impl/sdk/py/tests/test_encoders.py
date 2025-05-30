from __future__ import annotations

from types import NoneType
from typing import Any

import pytest

from resonate.encoders import Base64Encoder, HeaderEncoder, JsonEncoder, JsonPickleEncoder, PairEncoder


@pytest.mark.parametrize(
    "value",
    ["", "foo", "bar", "baz", None],
)
def test_base64_enconder(value: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(value)
    assert isinstance(encoded, (str, NoneType))

    decoded = encoder.decode(encoded)
    assert type(decoded) is type(value)
    assert decoded == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "foo",
        "bar",
        "baz",
        0,
        1,
        2,
        [],
        ["foo", "bar", "baz"],
        [0, 1, 2],
        {},
        {"foo": "foo", "bar": "bar", "baz": "baz"},
        {"foo": 0, "bar": 1, "baz": 2},
        None,
        (),
        ("foo", "bar", "baz"),
        (0, 1, 2),
        Exception("foo"),
        ValueError("bar"),
        BaseException("baz"),
    ],
)
def test_json_encoder(value: Any) -> None:
    encoder = JsonEncoder()
    encoded = encoder.encode(value)
    assert isinstance(encoded, (str, NoneType))

    match decoded := encoder.decode(encoded):
        case BaseException() as e:
            # all exceptions are flattened to Exception
            assert isinstance(decoded, Exception)
            assert str(e) == str(value)
        case list(items):
            # tuples are converted to lists
            assert isinstance(value, (list, tuple))
            assert items == list(value)
        case _:
            assert type(decoded) is type(value)
            assert decoded == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "foo",
        "bar",
        "baz",
        0,
        1,
        2,
        [],
        ["foo", "bar", "baz"],
        [0, 1, 2],
        {},
        {"foo": "foo", "bar": "bar", "baz": "baz"},
        {"foo": 0, "bar": 1, "baz": 2},
        None,
        (),
        ("foo", "bar", "baz"),
        (0, 1, 2),
        Exception("foo"),
        ValueError("bar"),
        BaseException("baz"),
    ],
)
def test_jsonpickle_encoder(value: Any) -> None:
    encoder = JsonPickleEncoder()
    encoded = encoder.encode(value)
    assert isinstance(encoded, (str, NoneType))

    decoded = encoder.decode(encoded)
    assert type(decoded) is type(value)

    if not isinstance(value, BaseException):
        assert decoded == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "foo",
        "bar",
        "baz",
        0,
        1,
        2,
        [],
        ["foo", "bar", "baz"],
        [0, 1, 2],
        {},
        {"foo": "foo", "bar": "bar", "baz": "baz"},
        {"foo": 0, "bar": 1, "baz": 2},
        None,
        (),
        ("foo", "bar", "baz"),
        (0, 1, 2),
        Exception("foo"),
        ValueError("bar"),
    ],
)
def test_default_encoder(value: Any) -> None:
    encoder = PairEncoder(HeaderEncoder("resonate:format-py", JsonPickleEncoder()), JsonEncoder())
    headers, encoded = encoder.encode(value)
    assert isinstance(headers, dict)
    assert "resonate:format-py" in headers
    assert isinstance(headers["resonate:format-py"], str)
    assert isinstance(encoded, (str, NoneType))

    # primary decoder
    for decoded in (encoder.decode((headers, encoded)), JsonPickleEncoder().decode(headers["resonate:format-py"])):
        assert type(decoded) is type(value)
        if not isinstance(value, Exception):
            assert decoded == value

    # backup decoder
    match decoded := JsonEncoder().decode(encoded):
        case BaseException() as e:
            # all exceptions are flattened to Exception
            assert isinstance(decoded, Exception)
            assert str(e) == str(value)
        case list(items):
            # tuples are converted to lists
            assert isinstance(value, (list, tuple))
            assert items == list(value)
        case _:
            assert type(decoded) is type(value)
            assert decoded == value
