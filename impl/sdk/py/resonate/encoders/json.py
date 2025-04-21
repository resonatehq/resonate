from __future__ import annotations

import importlib
import json
from typing import Any


class JsonEncoder:
    def encode(self, obj: Any) -> str | None:
        if obj is None:
            return None

        match obj:
            case Exception():
                obj = {
                    "__exception__": True,
                    "type": type(obj).__name__,
                    "module": type(obj).__module__,
                    "args": obj.args,
                }
                return json.dumps(obj)
            case _:
                return json.dumps(obj)

    def decode(self, obj: str | None) -> Any:
        if obj is None:
            return None

        obj = json.loads(obj)
        match obj:
            case {"__exception__": _}:
                module_name: str = obj.get("module", "builtins")
                class_name: str = obj["type"]
                args = obj.get("args", ())

                module = importlib.import_module(module_name)
                exc_class = getattr(module, class_name)
                assert issubclass(exc_class, Exception)
                return exc_class(*args)
            case _:
                return obj
