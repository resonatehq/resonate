from __future__ import annotations

from collections.abc import Callable
from typing import overload

from resonate.errors import ResonateValidationError

# Registry


class Registry:
    def __init__(self) -> None:
        self._forward_registry: dict[str, dict[int, Callable]] = {}
        self._reverse_registry: dict[Callable, tuple[str, set[int]]] = {}

    def add(self, func: Callable, name: str, version: int = 1) -> None:
        if name == "<lambda>":
            msg = "registering a lambda requires setting a name."
            raise ResonateValidationError(msg)

        if not version > 0:
            msg = "version must be greater than 0"
            raise ResonateValidationError(msg)

        if version in self._forward_registry.get(name, {}):
            msg = f"function {func.__name__} already registered under name={name} with version {version}."
            raise ResonateValidationError(msg)

        r_name, r_versions = self._reverse_registry.get(func, (name, set()))
        if name != r_name:
            msg = f"function={func.__name__} is already being registered with name={r_name}"
            raise ResonateValidationError(msg)

        if version in r_versions:
            msg = f"function {func.__name__} already registered under name={self._reverse_registry[func][version]} with version {version}."
            raise ResonateValidationError(msg)

        self._forward_registry.setdefault(name, {})[version] = func
        self._reverse_registry[func] = (name, r_versions.union({version}))

    @overload
    def get(self, func: str, version: int = 0) -> tuple[Callable, int]: ...
    @overload
    def get(self, func: Callable, version: int = 0) -> tuple[str, int]: ...
    def get(self, func: str | Callable, version: int = 0) -> tuple[str | Callable, int]:
        if not version >= 0:
            msg = "version must be greater or equal than 0"
            raise ResonateValidationError(msg)

        if func not in (self._forward_registry if isinstance(func, str) else self._reverse_registry):
            msg = f"function={func if isinstance(func, str) else func.__name__} is not registered."
            raise ResonateValidationError(msg)

        match func:
            case str():
                versions = self._forward_registry[func]
                version = max(versions.keys()) if version == 0 else version
                name = versions.get(version)
            case Callable():
                name, versions = self._reverse_registry[func]
                version = max(versions) if version == 0 else version

        if version not in versions:
            msg = f"version={version} is not registered for function={func if isinstance(func, str) else func.__name__}"
            raise ResonateValidationError(msg)

        assert name is not None
        return name, version

    @overload
    def all(self, func: str) -> dict[int, Callable]: ...
    @overload
    def all(self, func: Callable) -> set[int]: ...
    def all(self, func: str | Callable) -> dict[int, Callable] | set[int]:
        match func:
            case str():
                return self._forward_registry.get(func, {})
            case Callable():
                return self._reverse_registry.get(func, ("", set()))[1]

    @overload
    def latest(self, func: str) -> int: ...
    @overload
    def latest(self, func: Callable) -> int: ...
    def latest(self, func: str | Callable) -> int:
        match func:
            case str():
                return max(self._forward_registry[func]) if func in self._forward_registry else 0
            case Callable():
                return max(self._reverse_registry[func][1]) if func in self._reverse_registry else 0
