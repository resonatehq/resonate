from __future__ import annotations

import json

import pytest
from resonate.encoders import Base64Encoder


@pytest.mark.parametrize("data", ["hi", "bye", json.dumps(1)])
def test_base64_encoder(data: str) -> None:
    encoder = Base64Encoder()
    encoded = encoder.encode(data)
    assert data == encoder.decode(encoded)
