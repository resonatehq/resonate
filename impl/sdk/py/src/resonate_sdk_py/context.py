from __future__ import annotations


class Context:
    def __init__(self, parent_ctx: Context | None = None, *, dst: bool = False) -> None:
        self.parent_ctx = parent_ctx
        self.dst = dst

    def new_child(self) -> Context:
        return Context(parent_ctx=self, dst=self.dst)

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        if not self.dst:
            return
        assert stmt, msg
