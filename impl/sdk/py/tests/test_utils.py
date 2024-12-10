from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from resonate import utils

if TYPE_CHECKING:
    from uuid import UUID


@pytest.mark.parametrize(
    ("string", "expected"),
    [
        ("hi", "af4c"),
        ("bye", "df36"),
        ("money-transfer-13112-1231", "f299"),
        ("greeting-peter", "050d"),
    ],
)
def test_string_to_uuid(string: str, expected: UUID) -> None:
    assert utils.string_to_uuid(string) == expected
