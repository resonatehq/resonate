from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Self

from resonate.models.result import Ko, Ok, Result
from resonate.options import Options

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from resonate.models.convention import Convention
    from resonate.models.encoder import Encoder
    from resonate.models.retry_policy import RetryPolicy


@dataclass
class LFX[T]:
    conv: Convention
    func: Callable[..., Generator[Any, Any, T] | T]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: Options = field(default_factory=Options)

    @property
    def id(self) -> str:
        return self.conv.id

    def options(
        self,
        *,
        durable: bool | None = None,
        encoder: Encoder[Any, str | None] | None = None,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        non_retryable_exceptions: tuple[type[Exception], ...] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Self:
        # Note: we deliberately ignore the version for LFX
        self.conv = self.conv.options(
            id=id,
            idempotency_key=idempotency_key,
            tags=tags,
            timeout=timeout,
        )
        self.opts = self.opts.merge(
            durable=durable,
            encoder=encoder,
            non_retryable_exceptions=non_retryable_exceptions,
            retry_policy=retry_policy,
        )
        return self


@dataclass
class LFI[T](LFX[T]):
    pass


@dataclass
class LFC[T](LFX[T]):
    pass


@dataclass
class RFX[T]:
    conv: Convention
    opts: Options = field(default_factory=Options)

    @property
    def id(self) -> str:
        return self.conv.id

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        target: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Self:
        self.conv = self.conv.options(
            id=id,
            idempotency_key=idempotency_key,
            target=target,
            tags=tags,
            timeout=timeout,
            version=version,
        )
        self.opts = self.opts.merge(
            encoder=encoder,
        )
        return self


@dataclass
class RFI[T](RFX[T]):
    mode: Literal["attached", "detached"] = "attached"


@dataclass
class RFC[T](RFX[T]):
    pass


@dataclass
class AWT:
    id: str


@dataclass
class TRM:
    id: str
    result: Result


@dataclass(frozen=True)
class Promise[T]:
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
                case LFI(conv) | RFI(conv, mode="attached"):
                    # LFIs and attached RFIs require an AWT
                    self.next = AWT
                    self.unyielded.append(AWT(conv.id))
                    command = yielded
                case LFC(conv, func, args, kwargs, opts):
                    # LFCs can be converted to an LFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = LFI(conv, func, args, kwargs, opts)
                case RFC(conv):
                    # RFCs can be converted to an RFI+AWT
                    self.next = AWT
                    self.skip = True
                    command = RFI(conv)
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
