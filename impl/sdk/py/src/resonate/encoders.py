from __future__ import annotations

import base64
import importlib
import json
from abc import ABC, abstractmethod
from typing import Any, Generic, final

from typing_extensions import TypeVar

In = TypeVar("In")
Out = TypeVar("Out")


class IEncoder(Generic[In, Out], ABC):
    @abstractmethod
    def encode(self, data: In) -> Out: ...

    @abstractmethod
    def decode(self, data: Out) -> In: ...


@final
class Base64Encoder(IEncoder[str, str]):
    def encode(self, data: str) -> str:
        return base64.b64encode(data.encode()).decode()

    def decode(self, data: str) -> str:
        return base64.b64decode(data).decode()


@final
class JsonEncoder(IEncoder[Any, str]):
    def encode(self, data: Any) -> str:  # noqa: ANN401
        return json.dumps(data)

    def decode(self, data: str) -> Any:  # noqa: ANN401
        return json.loads(data)


def _classname(obj: object) -> str:
    cls = type(obj)
    module = cls.__module__
    name = cls.__qualname__
    if module is not None and module != "__builtin__":
        name = module + "." + name
    return name


def _import_class_from_qualified_name(qualified_name: str) -> Any:  # noqa: ANN401
    module_name, class_name = qualified_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


@final
class ErrorEncoder(IEncoder[Exception, str]):
    @classmethod
    def encode(cls, data: Exception) -> str:
        return json.dumps(
            {
                "type": _classname(data),
                "attributes": data.__dict__,
            }
        )

    @classmethod
    def decode(cls, data: str) -> Exception:
        ex_data: dict[str, Any] = json.loads(data)
        error_class = _import_class_from_qualified_name(ex_data["type"])
        return error_class(**ex_data["attributes"])
