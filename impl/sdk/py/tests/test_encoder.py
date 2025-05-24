from __future__ import annotations

from typing import Any

import pytest

from resonate.encoders import Base64Encoder, ChainEncoder, JsonEncoder
from resonate.errors.errors import ResonateCanceledError, ResonateShutdownError, ResonateStoreError, ResonateTimedoutError


@pytest.mark.parametrize(
    "value",
    ["12321", "hi", "by", None],
)
def test_base64_enconder(value: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(value)

    assert value == encoder.decode(encoded)


class CustomError(Exception):
    def __init__(self, status: int, mesg: str) -> None:
        self._status = status
        self._mesg = mesg
        super().__init__(f"{status}: {mesg}")

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.__class__, (self._status, self._mesg))


@pytest.mark.parametrize(
    "value",
    [
        {"value": 1},
        2,
        "three",
        ["four", 5],
        ("six",),
        CustomError(10, "abc"),
        TypeError("HERE"),
        None,
        ResonateShutdownError("foo"),
        ResonateStoreError(1213, 121),
        ResonateCanceledError("12"),
        ResonateTimedoutError("12", 12),
    ],
)
def test_json_encoder(value: Any) -> None:
    encoder = JsonEncoder()
    encoded = encoder.encode(value)
    match encoder.decode(encoded):
        case Exception() as decoded:
            assert isinstance(decoded, type(value))
            assert decoded.args == value.args
        case _ as decoded:
            assert value == decoded


@pytest.mark.parametrize("value", [{"value": 1}, CustomError(10, "abc"), TypeError("HERE"), None])
def test_chain_encoder(value: Any) -> None:
    encoder = ChainEncoder(JsonEncoder(), Base64Encoder())
    encoded = encoder.encode(value)
    match encoder.decode(encoded):
        case Exception() as decoded:
            assert isinstance(decoded, type(value))
            assert decoded.args == value.args
        case _ as decoded:
            assert value == decoded
