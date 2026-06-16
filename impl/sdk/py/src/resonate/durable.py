"""Runtime representation of user functions registered as durable.

Python annotations are optional, so ``def fn(ctx, x):`` carries nothing that
says ``ctx`` is a :class:`Context`. The SDK therefore adopts one convention:
**every** durable function -- workflow or leaf -- takes a :class:`Context` as
its first positional argument. The runtime strips that first parameter and
injects the ``Context`` on each call; it never inspects annotations to decide
*shape*.

Annotations drive exactly one thing: **symmetric serialize/deserialize**. For
the root / dispatched path a call's arguments are JSON-encoded into the durable
promise's ``param`` and, on every (re-)invocation, coerced back to their declared
parameter types (:meth:`DurableFunction.invoke` with ``coerce_args=True``); a
recovered return value is likewise coerced back to the declared return type
(:meth:`DurableFunction.coerce_result`). Both run through the one primitive
:meth:`DurableFunction._coerce_value`, the same type-shaping step the top-level
:class:`~resonate.handle.ResonateHandle` applies. A missing /
``Any`` / unresolved annotation is a pass-through, so an untyped function behaves
like ``rpc`` (raw builtins, the caller's responsibility).

A local ``ctx.run`` child is the exception: its arguments are never serialized
into the param (nothing reads it back -- see :meth:`Context.run`), so
:meth:`DurableFunction.invoke` is called with ``coerce_args=False`` and the
in-memory objects reach the function verbatim. That is what lets ``ctx.run``
accept non-serializable arguments. The return value still round-trips and is
coerced, so it must remain serializable.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Concatenate

import msgspec

from resonate.error import ApplicationError, SerializationError
from resonate.types import Args

if TYPE_CHECKING:
    from collections.abc import Callable

    from resonate.context import Context


class DurableFunction:
    """A user function adapted for durable execution.

    Built once per function (at registration / ``ctx.run`` time). It remembers
    how to re-bind serialized arguments on every (re-)invocation so a
    suspended-then-resumed call reconstructs the same Python call it was first
    dispatched with. The :class:`Context` is always injected as the first
    positional argument; only the *user* arguments round-trip through the durable
    promise's ``param`` field.
    """

    def __init__(self, fn: Callable[Concatenate[Context, ...], Any]) -> None:
        if not callable(fn):
            msg = f"expected a callable, got {type(fn).__name__}"
            raise ApplicationError(msg)

        name = getattr(fn, "__name__", "unknown")
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if not params:
            msg = (
                f"{name}: durable function must accept a Context as its first argument"
            )
            raise ApplicationError(msg)

        # ``ctx`` is injected positionally (``self._fn(ctx, ...)``) and stripped by
        # position (``params[1:]``), so the first parameter must be a plain
        # positional one. A ``*args`` / ``**kwargs`` / keyword-only leading
        # parameter cannot hold it -- and a bare ``(*args, **kwargs)`` is the
        # tell-tale of a decorator applied without :func:`functools.wraps`, which
        # erases the real signature (and with it arity validation and coercion).
        # Reject it with a clear message rather than failing confusingly later.
        if params[0].kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            msg = (
                f"{name}: durable function must accept a Context as its first "
                f"positional argument, but its first parameter is "
                f"{params[0].kind.description!r} (if this is a decorated function, "
                f"apply functools.wraps so its real signature is visible)"
            )
            raise ApplicationError(msg)

        # Resolve string annotations (``from __future__ import annotations`` turns
        # them all into strings) tolerantly and per-annotation: a name that is not
        # importable at runtime -- e.g. a ``Context`` under ``TYPE_CHECKING`` -- is
        # left as a string and treated as pass-through, without disabling coercion
        # of its resolvable siblings. Only the *user* parameters and the return are
        # resolved; the stripped ``ctx`` slot is never coerced, so its (often
        # unresolvable) annotation is ignored.
        globalns = _func_globals(fn)

        self._fn = fn
        #: Signature of the *user* arguments (leading ``ctx`` removed, annotations
        #: resolved). The runtime owns the ``ctx`` slot and never exposes it.
        self._user_sig = sig.replace(
            parameters=[
                p.replace(annotation=_resolve_annotation(p.annotation, globalns))
                for p in params[1:]
            ]
        )
        #: Resolved return annotation, used by :meth:`coerce_result`.
        self._return_annotation = _resolve_annotation(sig.return_annotation, globalns)
        #: Source name (child context ``func_name`` + error messages). The registry
        #: key is supplied separately at register time and may differ.
        self.name = name

    def pack_args(self, *args: Any, **kwargs: Any) -> Args:
        """Validate a call and pack it into a serializable :class:`Args`.

        Called at dispatch (``ctx.run``) time. :class:`Args` is the single slot
        able to round-trip Python ``*args`` / ``**kwargs`` through the durable
        promise's one ``param`` field; an empty call yields ``Args()``. The
        injected ``Context`` is never part of the payload. Raises
        :class:`ApplicationError` if the call does not match the signature.
        """
        try:
            self._user_sig.bind(*args, **kwargs)
        except TypeError as exc:
            msg = f"{self.name}: {exc}"
            raise ApplicationError(msg) from exc

        return Args(args=args, kwargs=kwargs)

    async def invoke(
        self, ctx: Context, packed: Args, *, coerce_args: bool = True
    ) -> Any:
        """Re-bind ``packed`` to the signature and execute the function.

        ``packed`` is the :class:`Args` :meth:`pack_args` produced (a local child)
        or the :class:`~resonate.types.TaskData` Core decoded from the root promise
        param -- ``TaskData`` is an ``Args``, so both arrive through the same slot.

        When ``coerce_args`` is ``True`` (the root / dispatched path) arguments are
        coerced to their declared types: that path's args arrive as JSON builtins
        decoded from the persisted param, so they must be reshaped, and msgspec
        strictness applies (e.g. ``True`` is rejected for an ``int`` parameter, an
        ``int`` is widened to a declared ``float``).

        A local ``ctx.run`` child passes ``coerce_args=False``: its arguments are
        never serialized into the promise param (see :meth:`Context.run`), so the
        in-memory objects reach the function verbatim -- nothing to reshape, and
        no annotation can reject them. This is what lets ``ctx.run`` accept
        non-serializable arguments. Symmetry with replay is preserved because the
        parent re-derives the *same* in-memory objects on every run, so neither
        path round-trips. ``bind`` + ``apply_defaults`` still run, so arity is
        validated and defaults are filled regardless.

        ``ctx`` is injected as the first positional argument; sync and ``async``
        functions are both supported.
        """
        try:
            bound = self._user_sig.bind(*packed.args, **packed.kwargs)
        except TypeError as exc:
            msg = f"{self.name}: {exc}"
            raise ApplicationError(msg) from exc

        # Snapshot which arguments were actually supplied *before* apply_defaults
        # fills the rest: only supplied values crossed the serialization boundary
        # and need coercion. A default is a live object (possibly a sentinel whose
        # type deliberately violates its annotation), so it is left untouched.
        provided = set(bound.arguments)
        bound.apply_defaults()
        if coerce_args:
            self._coerce(bound, provided)

        result = self._fn(ctx, *bound.args, **bound.kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def coerce_result(self, value: Any) -> Any:
        """Reshape a recovered return value into the declared return type.

        The symmetric counterpart of argument coercion. ``ctx.run`` calls this on
        both paths -- the recovery short-circuit (the settled child value comes
        back as JSON builtins) and the live path (the raw object the function
        returned) -- so the parent observes the same in-memory value either way.

        .. warning::
            A pass-through annotation (unannotated / ``Any`` / ``None`` /
            unresolved) reshapes nothing, so a non-JSON-native value round-trips
            faithfully only when the annotation can rebuild it: an untyped (or
            ``Any``) ``datetime`` / ``set`` return yields the live object on the
            first run but the decoded builtin (``str`` / ``list``) on recovery.
            Annotate the return type to keep the two paths symmetric.
        """
        return self._coerce_value(value, self._return_annotation)

    @property
    def return_type(self) -> Any:
        """The declared return type, as a value safe to hand to ``msgspec.convert``.

        The type-shaped view of the resolved return annotation, for a caller that
        decodes a settled value against a *type* rather than calling
        :meth:`coerce_result` -- namely the top-level
        :class:`~resonate.handle.ResonateHandle`, whose generic ``T`` is erased at
        runtime. A pass-through annotation collapses to ``Any`` so
        ``msgspec.convert(value, return_type)`` is the same no-op
        :meth:`coerce_result` performs for those cases, keeping the top-level
        decode and the ``ctx.run`` recovery decode in lock-step (and resolving the
        annotation through the one tolerant path that also handles callable
        instances, unlike a hand-rolled re-resolution).
        """
        if _is_passthrough_annotation(self._return_annotation):
            return Any
        return self._return_annotation

    def _coerce(self, bound: inspect.BoundArguments, provided: set[str]) -> None:
        """Coerce each *supplied* argument to its declared type, in place.

        ``*args`` / ``**kwargs`` coerce element-wise (their annotation is the
        element type); every other parameter coerces its single value. Defaults
        filled by ``apply_defaults`` are not in ``provided`` and are skipped.
        """
        for name in provided:
            param = self._user_sig.parameters[name]
            annotation = param.annotation
            value = bound.arguments[name]
            if param.kind is inspect.Parameter.VAR_POSITIONAL:
                bound.arguments[name] = tuple(
                    self._coerce_value(v, annotation) for v in value
                )
            elif param.kind is inspect.Parameter.VAR_KEYWORD:
                bound.arguments[name] = {
                    k: self._coerce_value(v, annotation) for k, v in value.items()
                }
            else:
                bound.arguments[name] = self._coerce_value(value, annotation)

    def _coerce_value(self, value: Any, annotation: Any) -> Any:
        """Reshape one value to ``annotation`` via msgspec, or pass it through.

        The single coercion primitive shared by argument coercion (:meth:`_coerce`)
        and return coercion (:meth:`coerce_result`), so the serialize/deserialize
        pair cannot drift. An annotation that carries nothing to reshape (see
        :func:`_is_passthrough_annotation`) leaves the value untouched.
        """
        if _is_passthrough_annotation(annotation):
            return value

        try:
            return msgspec.convert(value, annotation)
        except (TypeError, ValueError, msgspec.MsgspecError) as exc:
            error = SerializationError(exc)
            error.add_note(f"while binding arguments for {self.name}")
            raise error from exc


def _is_passthrough_annotation(annotation: Any) -> bool:
    """Whether ``annotation`` carries no information to reshape a value.

    Shared by :meth:`DurableFunction._coerce_value` (argument + return coercion)
    and :meth:`DurableFunction.return_type` (the top-level decode type), so every
    view of "is this typed?" agrees. Four cases pass through untouched:

    * ``empty``  -- the parameter / return is genuinely unannotated.
    * ``Any``    -- explicitly opted out of typing.
    * ``None``   -- ``-> None`` / ``x: None``; the value is already ``None``.
    * a ``str``  -- a forward ref :func:`_resolve_annotation` could not evaluate.
    """
    return (
        annotation is inspect.Parameter.empty
        or annotation is Any
        or annotation is None
        or isinstance(annotation, str)
    )


def _func_globals(fn: Callable[Concatenate[Context, ...], Any]) -> dict[str, Any]:
    """Best-effort global namespace for resolving ``fn``'s string annotations.

    A plain function or bound method exposes ``__globals__`` directly. A *callable
    instance* does not -- its annotations live on ``type(fn).__call__`` -- so fall
    back to that callable's globals. Without this fallback an instance's
    annotations resolve against an empty namespace, so every non-builtin
    annotation silently stays an unresolved string and coercion is skipped,
    handing the function a raw ``dict`` / ``list`` instead of the declared type.
    """
    globalns = getattr(fn, "__globals__", None)
    if globalns is not None:
        return globalns
    return getattr(type(fn).__call__, "__globals__", {})


def _resolve_annotation(annotation: Any, globalns: dict[str, Any]) -> Any:
    """Evaluate one string annotation, leaving it as-is if it cannot resolve.

    Resolution is per-annotation and best-effort: ``inspect.signature(eval_str=True)``
    is all-or-nothing (a single unresolvable name aborts the whole signature),
    which would silently disable coercion of every sibling whenever a function
    follows the standard ``TYPE_CHECKING``-only ``Context`` import. Here each
    annotation is evaluated alone; one that fails yields its raw string, which
    :meth:`DurableFunction._coerce_value` then treats as pass-through.

    Any evaluation failure is swallowed, not just ``NameError`` -- an annotation
    string is arbitrary Python that ``eval_str=True`` already runs, so a broad
    ``except`` adds no new risk, and a value that cannot be reshaped is left
    un-coerced rather than aborting registration of an otherwise-valid function.
    """
    if not isinstance(annotation, str):
        return annotation
    holder = type("_", (), {"__annotations__": {"a": annotation}})
    try:
        return inspect.get_annotations(holder, globals=globalns, eval_str=True)["a"]
    except Exception:
        return annotation
