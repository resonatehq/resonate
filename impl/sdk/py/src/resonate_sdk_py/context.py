from __future__ import annotations


class Context:
    def __init__(self, parent_ctx: Context | None = None) -> None:
        self.parent_ctx = parent_ctx

    def new_child(self) -> Context:
        return Context(parent_ctx=self)

    def assert_statement(self, stmt: bool, msg: str) -> None:  # noqa: FBT001
        assert stmt, msg
