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


# Helper method to make Errors serializable
def _default(data: Any) -> Any:  # noqa: ANN401
    if isinstance(data, Exception):
        return {
            "__type": _classname(data),
            "attributes": data.__dict__,
        }

    return data


# Helper method to deserialize errors
def _object_hook(data: dict[str, Any]) -> Any:  # noqa: ANN401
    if "__type" in data:
        error_cls = _import_class_from_qualified_name(data["__type"])
        return error_cls(**data["attributes"])

    return data


@final
class JsonEncoder(IEncoder[Any, str]):
    def encode(self, data: Any) -> str:  # noqa: ANN401
        return json.dumps(data, default=_default)

    def decode(self, data: str) -> Any:  # noqa: ANN401
        return json.loads(data, object_hook=_object_hook)


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
