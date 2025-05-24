from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import overload


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, tuple[str, Callable, int]]] = {}
        self._reverse_registry: dict[Callable, tuple[str, Callable, int]] = {}

    def add(self, func: Callable, name: str | None = None, version: int = 1) -> None:
        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        if not name and func.__name__ == "<lambda>":
            msg = "name required when registering a lambda function"
            raise ValueError(msg)

        if not version > 0:
            msg = "provided version must be greater than zero"
            raise ValueError(msg)

        name = name or func.__name__
        if version in self._forward_registry.get(name, {}) or func in self._reverse_registry:
            msg = f"function {name} already registered"
            raise ValueError(msg)

        item = (name, func, version)
        self._forward_registry.setdefault(name, {})[version] = item
        self._reverse_registry[func] = item

    @overload
    def get(self, func: str, version: int = 0) -> tuple[str, Callable, int]: ...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, Callable, int]: ...
    def get(self, func: str | Callable, version: int = 0) -> tuple[str, Callable, int]:
        if func not in (self._forward_registry if isinstance(func, str) else self._reverse_registry):
            msg = f"function {func if isinstance(func, str) else getattr(func, '__name__', 'unknown')} not found in registry"
            raise ValueError(msg)

        if version != 0 and version not in (self._forward_registry[func] if isinstance(func, str) else (self._reverse_registry[func][2],)):
            msg = f"function {func if isinstance(func, str) else getattr(func, '__name__', 'unknown')} version {version} not found in registry"
            raise ValueError(msg)

        match func:
            case str():
                vers = max(self._forward_registry[func]) if version == 0 else version
                return self._forward_registry[func][vers]

            case Callable():
                return self._reverse_registry[func]

    @overload
    def latest(self, func: str, default: int = 1) -> int: ...
    @overload
    def latest(self, func: Callable, default: int = 1) -> int: ...
    def latest(self, func: str | Callable, default: int = 1) -> int:
        match func:
            case str():
                return max(self._forward_registry.get(func, [default]))
            case Callable():
                _, _, version = self._reverse_registry.get(func, (None, None, default))
                return version
