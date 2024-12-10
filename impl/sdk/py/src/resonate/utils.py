from __future__ import annotations

import hashlib
from typing import TypeVar
from uuid import UUID

T = TypeVar("T")


def string_to_uuid(string: str) -> str:
    return UUID(bytes=hashlib.sha1(string.encode("utf-8")).digest()[:16]).hex[-4:]  # noqa: S324
