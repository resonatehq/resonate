from __future__ import annotations

from typing import Any, Literal

import msgspec

PromiseState = Literal[
    "pending",
    "resolved",
    "rejected",
    "rejected_canceled",
    "rejected_timedout",
]


class Value(msgspec.Struct, omit_defaults=True, kw_only=True, frozen=True):
    """The wire format for data crossing the durability boundary.

    On the wire, ``data`` is a base64-encoded JSON string (or omitted).
    Internally, after decoding by the Codec, ``data`` holds the deserialized
    value; before encoding, it holds the plaintext value the
    :class:`~resonate.codec.Codec` will serialize.

    Both fields default to ``None`` and, with ``omit_defaults=True``, are left
    out of the encoded output entirely.

    Note: Python uses ``None`` for JSON ``null``, so an absent ``data`` field
    and an explicit ``null`` collapse to the same value.
    """

    headers: dict[str, str] | None = None
    data: Any | None = None


class PromiseRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    """A durable promise record as stored by the server.

    ``kw_only=True`` lets ``timeout_at`` stay required even though it is
    declared after fields with defaults, keeping the declaration order aligned
    with the wire format. Defaulted fields are filled in when the server omits
    them.
    """

    id: str
    state: PromiseState
    param: Value = msgspec.field(default_factory=Value)
    value: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)
    timeout_at: int
    created_at: int = msgspec.field(default=0)
    settled_at: int | None = msgspec.field(default=None)


class TaskRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    state: Literal["pending", "acquired", "suspended", "halted", "fulfilled"]
    version: int
    resumes: list[str] | int | bool | None = msgspec.field(default=None)
    ttl: int | None = msgspec.field(default=None)
    pid: str | None = msgspec.field(default=None)


class ScheduleRecord(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    cron: str
    promise_id: str
    promise_timeout: int
    promise_param: Value = msgspec.field(default_factory=Value)
    promise_tags: dict[str, str] = msgspec.field(default_factory=dict)
    created_at: int = msgspec.field(default=0)


class PromiseCreateReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    timeout_at: int = msgspec.field(default=0)
    param: Value = msgspec.field(default_factory=Value)
    tags: dict[str, str] = msgspec.field(default_factory=dict)


class PromiseSettleReq(msgspec.Struct, rename="camel", kw_only=True, frozen=True):
    id: str
    state: Literal["resolved", "rejected", "rejected_canceled"]
    value: Value


class PromiseRegisterCallbackData(
    msgspec.Struct, rename="camel", kw_only=True, frozen=True
):
    awaited: str
    awaiter: str


class Args(msgspec.Struct, kw_only=True, frozen=True):
    args: tuple[Any, ...] = msgspec.field(default_factory=tuple)
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)


class TaskData(Args, kw_only=True, frozen=True):
    func: str
    version: int = msgspec.field(default=1)


Status = Literal["done", "suspended", "error"]


class Info(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    parent_id: str
    origin_id: str
    branch_id: str
    timeout_at: int
    func_name: str
    tags: dict[str, str]
