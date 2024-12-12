from __future__ import annotations

from typing import Any

import pytest

from resonate.encoders import Base64Encoder, JsonEncoder
from resonate.errors import ResonateError


@pytest.mark.parametrize("data", ["hi", "bye", "1"])
def test_base64_encoder(data: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(data)
    assert data == encoder.decode(encoded)


@pytest.mark.parametrize("data", [1, {"value": 3}, {"number": "1", "text": 312}])
def test_json_encoder(data: Any) -> None:  # noqa: ANN401
    encoder = JsonEncoder()
    encoded = encoder.encode(data=data)
    assert data == encoder.decode(encoded)


class _CustomerError(Exception):
    def __init__(self, age: int, *, testable: bool) -> None:
        super().__init__(f"I'm {age} centuries old")
        self.age = age
        self.testable = testable


@pytest.mark.parametrize(
    "data",
    [
        ResonateError("I'm here for testing", code="UNKNOWN", retriable=True),
        _CustomerError(age=10232, testable=False),
    ],
)
def test_error_encoder(data: Exception) -> None:
    encoder = JsonEncoder()
    encoded = encoder.encode(data=data)
    assert isinstance(encoder.decode(encoded), type(data))
