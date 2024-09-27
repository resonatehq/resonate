from __future__ import annotations

from resonate.collections import DoubleDict


def test_double_dict() -> None:
    dd = DoubleDict[str, int]({"a": 1, "b": 2})
    assert dd.get("a") == 1
    dd.add("c", 3)
    assert dd.get("c") == 3  # noqa: PLR2004
    assert dd.get_from_value(3) == "c"
    assert dd.pop("a") == 1
    assert dd.data == {"b": 2, "c": 3}
