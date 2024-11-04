from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from resonate.context import Context


class Command:
    def __call__(self, ctx: Context) -> None:
        # This is not meant to be call. We are making the type system happy.
        _ = ctx
        msg = "You should never be here!"
        raise AssertionError(msg)


class CreateDurablePromiseReq(Command):
    def __init__(
        self,
        promise_id: str | None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        self.promise_id = promise_id
        self.data = data
        self.headers = headers
        self.tags = tags


def manual_completion(promise_id: str | None) -> CreateDurablePromiseReq:
    return CreateDurablePromiseReq(promise_id=promise_id)


def remote_function(
    func_name: str,
    args: list[Any],
    *,
    target: str,
    promise_id: str | None = None,
) -> CreateDurablePromiseReq:
    return CreateDurablePromiseReq(
        promise_id=promise_id,
        data={"func": func_name, "args": args, "kwargs": {}},
        headers=None,
        tags={"resonate:invoke": f"poll://{target}"},
    )
