from __future__ import annotations

from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any

from resonate.encoders import HeaderEncoder, JsonEncoder, JsonPickleEncoder, NoopEncoder, PairEncoder
from resonate.models.encoder import Encoder
from resonate.models.retry_policy import RetryPolicy
from resonate.retry_policies import Exponential, Never

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class Options:
    durable: bool = True
    encoder: Encoder[Any, str | None] | None = None
    id: str | None = None
    idempotency_key: str | Callable[[str], str] | None = lambda id: id
    non_retryable_exceptions: tuple[type[Exception], ...] = ()
    retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] = lambda f: Never() if isgeneratorfunction(f) else Exponential()
    target: str = "default"
    tags: dict[str, str] = field(default_factory=dict)
    timeout: float = 31536000  # relative time in seconds, default 1 year
    version: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.durable, bool):
            msg = f"durable must be `bool`, got {type(self.durable).__name__}"
            raise TypeError(msg)

        if self.encoder is not None and not isinstance(self.encoder, Encoder):
            msg = f"encoder must be `Encoder | None`, got {type(self.encoder).__name__}"
            raise TypeError(msg)

        if self.id is not None and not isinstance(self.id, str):
            msg = f"id must be `str | None`, got {type(self.id).__name__}"
            raise TypeError(msg)

        if self.idempotency_key is not None and not (isinstance(self.idempotency_key, str) or callable(self.idempotency_key)):
            msg = f"idempotency_key must be `Callable | str | None`, got {type(self.idempotency_key).__name__}"
            raise TypeError(msg)

        if not isinstance(self.non_retryable_exceptions, tuple):
            msg = f"non_retryable_exceptions must be `tuple`, got {type(self.non_retryable_exceptions).__name__}"
            raise TypeError(msg)

        if not (isinstance(self.retry_policy, RetryPolicy) or callable(self.retry_policy)):
            msg = f"retry_policy must be `Callable | RetryPolicy | None`, got {type(self.retry_policy).__name__}"
            raise TypeError(msg)

        if not isinstance(self.target, str):
            msg = f"target must be `str`, got {type(self.target).__name__}"
            raise TypeError(msg)

        if not isinstance(self.tags, dict):
            msg = f"tags must be `dict`, got {type(self.tags).__name__}"
            raise TypeError(msg)

        if not isinstance(self.timeout, int | float):
            msg = f"timeout must be `float`, got {type(self.timeout).__name__}"
            raise TypeError(msg)

        if not isinstance(self.version, int):
            msg = f"version must be `int`, got {type(self.version).__name__}"
            raise TypeError(msg)

        if not (self.version >= 0):
            msg = "version must be greater than or equal to zero"
            raise ValueError(msg)
        if not (self.timeout >= 0):
            msg = "timeout must be greater than or equal to zero"
            raise ValueError(msg)

    def merge(
        self,
        *,
        durable: bool | None = None,
        encoder: Encoder[Any, str | None] | None = None,
        id: str | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        non_retryable_exceptions: tuple[type[Exception], ...] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        target: str | None = None,
        tags: dict[str, str] | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Options:
        return Options(
            durable=durable if durable is not None else self.durable,
            encoder=encoder if encoder is not None else self.encoder,
            id=id if id is not None else self.id,
            idempotency_key=idempotency_key if idempotency_key is not None else self.idempotency_key,
            non_retryable_exceptions=non_retryable_exceptions if non_retryable_exceptions is not None else self.non_retryable_exceptions,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            target=target if target is not None else self.target,
            tags=tags if tags is not None else self.tags,
            timeout=timeout if timeout is not None else self.timeout,
            version=version if version is not None else self.version,
        )

    def get_encoder(self) -> Encoder[Any, tuple[dict[str, str] | None, str | None]]:
        l = NoopEncoder() if self.encoder else HeaderEncoder("resonate:format-py", JsonPickleEncoder())
        r = self.encoder or JsonEncoder()
        return PairEncoder(l, r)

    def get_idempotency_key(self, id: str) -> str | None:
        return self.idempotency_key(id) if callable(self.idempotency_key) else self.idempotency_key

    def get_retry_policy(self, func: Callable) -> RetryPolicy:
        return self.retry_policy(func) if callable(self.retry_policy) else self.retry_policy
