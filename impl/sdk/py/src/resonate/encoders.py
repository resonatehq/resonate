from __future__ import annotations

import base64
from abc import ABC, abstractmethod
from typing import Generic

from typing_extensions import TypeVar

In = TypeVar("In")
Out = TypeVar("Out")


class IEncoder(Generic[In, Out], ABC):
    @abstractmethod
    def encode(self, data: In) -> Out: ...

    @abstractmethod
    def decode(self, data: Out) -> In: ...


class Base64Encoder(IEncoder[str, str]):
    def encode(self, data: str) -> str:
        return base64.b64encode(data.encode()).decode()

    def decode(self, data: str) -> str:
        return base64.b64decode(data).decode()
