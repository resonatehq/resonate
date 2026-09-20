from __future__ import annotations

from typing import TYPE_CHECKING, Concatenate

from resonate.durable import DurableFunction
from resonate.error import AlreadyRegisteredError

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from resonate.context import Context
    from resonate.retry import RetryPolicy


class Registry:
    """Maps a ``(name, version)`` pair to a validated :class:`DurableFunction`.

    The version is explicit -- never "latest" -- so a lookup is deterministic
    regardless of what is registered afterwards: a task records its version at
    create time and resolves the same implementation on every replay.

    Names are passed at register time rather than derived from the Python
    function, so they stay stable across renames. The callable is wrapped in a
    :class:`DurableFunction`, which validates the ctx-first convention and
    handles serializing/deserializing its arguments and return value.
    """

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, int], DurableFunction] = {}
        #: Per-function retry policy override, looked up by ``(name, version)``
        #: when a root task is dispatched (a remote dispatch carries no policy
        #: on the wire). ``None`` (or absent) means "no override" -- the
        #: SDK-wide default applies.
        self._policy_by_key: dict[tuple[str, int], RetryPolicy | None] = {}
        #: Reverse of ``_by_key``: function object -> its registered
        #: ``(name, version)``. Lets a caller holding the function object
        #: recover what to dispatch by (e.g. a by-object ``rpc`` or ``run``).
        #: Registering the same object under several keys keeps the last one.
        self._by_fn: dict[
            Callable[Concatenate[Context, ...], Any], tuple[str, int]
        ] = {}

    def register(
        self,
        name: str,
        fn: Callable[Concatenate[Context, ...], Any],
        version: int = 1,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """Validate ``fn`` and store it under ``(name, version)``.

        ``retry_policy`` is a per-function override, applied when this function
        fails as a pure (leaf) function running as a root task. ``None`` (the
        default) means no override -- the SDK-wide default applies. A workflow
        body never retries regardless of the policy.
        """
        if not name:
            msg = "name is required"
            raise ValueError(msg)
        if version < 1:
            msg = "version must be >= 1"
            raise ValueError(msg)
        key = (name, version)
        if key in self._by_key:
            raise AlreadyRegisteredError(name, version)
        self._by_key[key] = DurableFunction(fn)
        self._policy_by_key[key] = retry_policy
        self._by_fn[fn] = (name, version)

    def get(self, name: str, version: int = 1) -> DurableFunction | None:
        return self._by_key.get((name, version))

    def reverse(
        self, fn: Callable[Concatenate[Context, ...], Any]
    ) -> tuple[str, int] | None:
        """Return the ``(name, version)`` ``fn`` was registered under, or ``None``.

        The inverse of :meth:`get`: a caller holding the function object rather
        than its name uses this to recover what to dispatch by. ``None`` means
        the object was never registered.
        """
        return self._by_fn.get(fn)

    def get_policy(self, name: str, version: int = 1) -> RetryPolicy | None:
        """Return the per-function policy override, or ``None`` if there is none."""
        return self._policy_by_key.get((name, version))
