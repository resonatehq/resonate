from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Self

from resonate.errors import ResonateValidationError
from resonate.models.result import Ko, Ok, Result
from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from resonate.models.convention import Convention
    from resonate.models.retry_policy import RetryPolicy


@dataclass
class LFX:
    id: str
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)
    versions: dict[int, Callable] | None = None

    def __post_init__(self) -> None:
        # Initially, timeout is set to the parent context timeout. This is the upper bound for the timeout.
        self._max_timeout = self.opts.timeout

    def options(
        self,
        *,
        id: str | None = None,
        durable: bool | None = None,
        retry_policy: RetryPolicy | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        if version is not None and self.versions is not None:
            if version not in self.versions:
                msg = f"version={version} not found."
                raise ResonateValidationError(msg)
            self.func = self.versions[version]

        if timeout is not None:
            timeout = min(self._max_timeout, timeout)

        self.id = id or self.id
        self.opts = self.opts.merge(timeout=timeout, version=version, tags=tags, retry_policy=retry_policy, durable=durable)
        return self


@dataclass
class LFI(LFX):
    pass


@dataclass
class LFC(LFX):
    pass


@dataclass
class RFX:
    id: str
    convention: Convention

    def options(
        self,
        *,
        id: str | None = None,
        send_to: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: int | None = None,
        version: int | None = None,
    ) -> Self:
        self.id = id or self.id
        self.convention.options(send_to=send_to, tags=tags, timeout=timeout, version=version)

        return self


@dataclass
class RFI(RFX):
    mode: Literal["attached", "detached"] = "attached"


@dataclass
class RFC(RFX):
    pass


@dataclass
class AWT:
    id: str


@dataclass
class TRM:
    id: str
    result: Result


@dataclass(frozen=True)
class Promise:
    id: str
    cid: str


type Yieldable = LFI | LFC | RFI | RFC | Promise


class Coroutine:
    def __init__(self, id: str, cid: str, gen: Generator[Yieldable, Any, Any]) -> None:
        self.id = id
        self.cid = cid
        self.gen = gen

        self.done = False
        self.skip = False
        self.next: type[None | AWT] | tuple[type[Result], ...] = type(None)
        self.unyielded: list[AWT | TRM] = []

    def __repr__(self) -> str:
        return f"Coroutine(done={self.done})"

    def send(self, value: None | AWT | Result) -> LFI | RFI | AWT | TRM:
        assert self.done or isinstance(value, self.next), "AWT must follow LFI/RFI. Value must follow AWT."
        assert not self.skip or isinstance(value, AWT), "If skipped, value must be an AWT."

        if self.done:
            # When done, yield all unyielded values to enforce structured concurrency, the final
            # value must be a TRM.

            match self.unyielded:
                case []:
                    raise StopIteration
                case [trm]:
                    assert isinstance(trm, TRM), "Last unyielded value must be a TRM."
                    self.unyielded = []
                    return trm
                case [head, *tail]:
                    self.unyielded = tail
                    return head
        try:
            match value, self.skip:
                case None, _:
                    yielded = next(self.gen)
                case Ok(v), _:
                    yielded = self.gen.send(v)
                case Ko(e), _:
                    yielded = self.gen.throw(e)
                case awt, True:
                    # When skipped, pretend as if the generator yielded a promise
                    self.skip = False
                    yielded = Promise(awt.id, self.cid)
                case awt, False:
                    yielded = self.gen.send(Promise(awt.id, self.cid))

            match yielded:
                case LFI(id) | RFI(id, mode="attached"):
                    # LFIs and attached RFIs require an AWT
                    self.next = AWT
                    self.unyielded.append(AWT(id))
                    command = yielded
                case LFC(id, func, args, kwargs, opts):
                    # LFCs can be converted to an LFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = LFI(id, func, args, kwargs, opts)
                case RFC(id, convention):
                    # RFCs can be converted to an RFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = RFI(id, convention)
                case Promise(id):
                    # When a promise is yielded we can remove it from unyielded
                    self.next = (Ok, Ko)
                    self.unyielded = [y for y in self.unyielded if y.id != id]
                    command = AWT(id)
                case _:
                    assert isinstance(yielded, RFI), "Yielded must be an RFI."
                    assert yielded.mode == "detached", "RFI must be detached."
                    self.next = AWT
                    command = yielded

        except StopIteration as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ok(e.value)))
            return self.unyielded.pop(0)
        except Exception as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ko(e)))
            return self.unyielded.pop(0)
        else:
            assert not isinstance(yielded, Promise) or yielded.cid == self.cid, "If promise, cids must match."
            return command
