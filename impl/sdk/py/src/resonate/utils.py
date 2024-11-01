from __future__ import annotations

import hashlib
from typing import Any, TypeVar
from uuid import UUID

T = TypeVar("T")


def string_to_ikey(string: str) -> str:
    return UUID(bytes=hashlib.sha1(string.encode("utf-8")).digest()[:16]).hex[-4:]  # noqa: S324


def recv_url(group: str, pid: str | None) -> dict[str, Any]:
    recv: dict[str, Any] = {"type": "poll", "data": {"group": group}}
    if pid is not None:
        recv["data"]["id"] = pid
    return recv
