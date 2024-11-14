from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

from resonate.commands import Command

if TYPE_CHECKING:
    from resonate.promise import Promise

T = TypeVar("T")
C = TypeVar("C", bound=Command)


@final
class CommandBuffer(Generic[C]):
    def __init__(self, maxlen: int | None) -> None:
        self._maxlen = maxlen
        self.cmds: list[C] = []
        self.promises: list[Promise[Any]] = []

    def add(self, cmd: C, promise: Promise[Any]) -> None:
        assert len(self.cmds) == len(self.promises)
        assert not self.is_full()
        self.cmds.append(cmd)
        self.promises.append(promise)

    def is_full(self) -> bool:
        if self._maxlen is None:
            return False
        return self._maxlen == len(self.cmds)

    def is_empty(self) -> bool:
        return len(self.cmds) == 0

    def pop_all(self) -> tuple[list[C], list[Promise[Any]]]:
        cmds, self.cmds = self.cmds, []
        promises, self.promises = self.promises, []
        assert len(cmds) == len(promises)
        return cmds, promises
