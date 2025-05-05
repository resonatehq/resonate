from __future__ import annotations

from collections.abc import Callable
from typing import overload

from resonate.errors import ResonateValidationError


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, tuple[str, Callable, int]]] = {}
        self._reverse_registry: dict[Callable, tuple[str, Callable, int]] = {}

    def add(self, func: Callable, name: str, version: int = 1) -> None:
        if name == "<lambda>":
            msg = "Registering a lambda requires setting a name."
            raise ResonateValidationError(msg)

        if not version > 0:
            msg = "Version must be greatder than 0."
            raise ResonateValidationError(msg)

        if version in self._forward_registry.get(name, {}) or func in self._reverse_registry:
            msg = f"Function {func.__name__} already registered."
            raise ResonateValidationError(msg)

        item = (name, func, version)
        self._forward_registry.setdefault(name, {})[version] = item
        self._reverse_registry[func] = item

    @overload
    def get(self, func: str, version: int = 0) -> tuple[str, Callable, int]: ...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, Callable, int]: ...
    def get(self, func: str | Callable, version: int = 0) -> tuple[str, Callable, int]:
        if func not in (self._forward_registry if isinstance(func, str) else self._reverse_registry):
            msg = f"Function {func if isinstance(func, str) else func.__name__} not found in registry."
            raise ResonateValidationError(msg)

        if version != 0 and version not in (self._forward_registry[func] if isinstance(func, str) else (self._reverse_registry[func][2],)):
            msg = f"Function {func if isinstance(func, str) else func.__name__} version {version} not found in registry."
            raise ResonateValidationError(msg)

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
