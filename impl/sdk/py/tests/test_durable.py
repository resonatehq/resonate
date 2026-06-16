"""Behaviour tests for :mod:`resonate.durable`.

These tests pin the contract: every durable
function -- workflow or leaf -- receives a :class:`Context` as its first
positional argument, the runtime never inspects annotations, and the
``*args``/``**kwargs`` round trip through
:meth:`~resonate.durable.DurableFunction.pack_args` /
:meth:`~resonate.durable.DurableFunction.invoke` (a typed
:class:`~resonate.types.Args` slot) preserves the call across the durability
boundary.
"""

from __future__ import annotations

import dataclasses
import enum
import functools
import inspect
from typing import Any

import attrs
import msgspec
import pydantic
import pytest

from resonate.codec import Codec, NoopEncryptor
from resonate.context import Context
from resonate.dependencies import DependencyMap
from resonate.durable import DurableFunction
from resonate.effects import ResonateEffects
from resonate.error import ApplicationError, SerializationError
from resonate.network import LocalNetwork
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import Args

I64_MAX = 2**63 - 1


# =============================================================================
# Fixtures: a root ``Context`` (the only env a durable function ever sees)
# =============================================================================


def _context() -> Context:
    sender = Sender(Transport(LocalNetwork()), None)
    effects = ResonateEffects(sender, Codec(NoopEncryptor()), [])
    return Context.root(
        id="root",
        origin_id="root",
        prefix_id="root",
        timeout_at=I64_MAX,
        func_name="root",
        effects=effects,
        target_resolver=lambda target: target or "",
        deps=DependencyMap(),
    )


# =============================================================================
# Registration: ctx-first convention, no annotation inspection
# =============================================================================


async def leaf(ctx: Context, x: int) -> int:
    return x * 2


async def workflow(ctx: Context, x: int) -> str:
    return f"{ctx.info.id}:{x}"


async def ctx_only(ctx: Context) -> int:
    return 42


def test_leaf_registered_without_inspecting_annotations() -> None:
    df = DurableFunction(leaf)
    assert df.name == "leaf"


def test_workflow_registered_without_inspecting_annotations() -> None:
    df = DurableFunction(workflow)
    assert df.name == "workflow"


def test_ctx_only_function_registered() -> None:
    df = DurableFunction(ctx_only)
    assert df.name == "ctx_only"


def test_non_callable_rejected() -> None:
    not_callable: Any = 42
    with pytest.raises(ApplicationError, match="expected a callable"):
        DurableFunction(not_callable)


def test_zero_arg_function_rejected() -> None:
    async def no_args() -> int:
        return 42

    # A function with no ctx slot violates the typed contract; the runtime
    # rejects it. (Typed as ``Any`` so the negative case still type-checks.)
    bad: Any = no_args
    with pytest.raises(ApplicationError, match="must accept a Context"):
        DurableFunction(bad)


def test_first_param_treated_as_ctx_regardless_of_annotation() -> None:
    # The first parameter is the runtime-injected ctx slot: it is stripped by
    # position, and the runtime never inspects its name or annotation (here it
    # is annotated ``object``, not ``Context``). Everything after it is a
    # user-facing argument.
    async def fn(ctx: object, _: int, x: int) -> int:
        return x

    df = DurableFunction(fn)
    # ``ctx`` is stripped; ``_`` and ``x`` remain as the two user arguments.
    assert df.pack_args(7, 8) == Args(args=(7, 8), kwargs={})
    # Passing more positionals than the env-stripped signature takes fails.
    with pytest.raises(ApplicationError):
        df.pack_args(1, 2, 3)


# =============================================================================
# invoke: ctx is always injected as the first positional argument
# =============================================================================


@pytest.mark.asyncio
async def test_invoke_injects_context() -> None:
    df = DurableFunction(workflow)
    ctx = _context()
    assert await df.invoke(ctx, df.pack_args(7)) == "root:7"


@pytest.mark.asyncio
async def test_invoke_ctx_only_no_user_args() -> None:
    df = DurableFunction(ctx_only)
    assert df.pack_args() == Args()
    assert await df.invoke(_context(), Args()) == 42


@pytest.mark.asyncio
async def test_leaf_can_ignore_ctx() -> None:
    df = DurableFunction(leaf)
    assert await df.invoke(_context(), df.pack_args(21)) == 42


# =============================================================================
# invoke: sync functions, errors
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_supported() -> None:
    def sync_leaf(ctx: Context, x: int) -> int:
        return x + 1

    df = DurableFunction(sync_leaf)
    assert await df.invoke(_context(), df.pack_args(41)) == 42


@pytest.mark.asyncio
async def test_raising_function_propagates() -> None:
    async def boom(ctx: Context, x: int) -> int:
        msg = "boom"
        raise ApplicationError(msg)

    df = DurableFunction(boom)
    with pytest.raises(ApplicationError, match="boom"):
        await df.invoke(_context(), df.pack_args(1))


# =============================================================================
# pack_args / invoke: *args / **kwargs round trip
# =============================================================================


async def variadic(ctx: Context, *args: int, **kwargs: int) -> int:
    return sum(args) + sum(kwargs.values())


async def keyword_only(ctx: Context, *, a: int, b: int = 10) -> int:
    return a + b


def test_pack_args_validates_arity() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, 2)  # leaf takes a single user positional


def test_pack_args_packs_into_typed_args() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    assert payload == Args(args=(1, 2, 3), kwargs={"k": 4})


@pytest.mark.asyncio
async def test_variadic_round_trip() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3, k=4)
    # Simulate the durability boundary: payload is JSON-decoded back into Args.
    assert await df.invoke(_context(), _roundtrip(payload)) == 1 + 2 + 3 + 4  # seq==0


@pytest.mark.asyncio
async def test_keyword_only_round_trip() -> None:
    df = DurableFunction(keyword_only)
    assert await df.invoke(_context(), df.pack_args(a=5)) == 15  # b defaults to 10
    assert await df.invoke(_context(), df.pack_args(a=5, b=6)) == 11


# =============================================================================
# invoke: argument coercion on recovery (JSON builtins -> declared types)
# =============================================================================


class Point(msgspec.Struct, frozen=True):
    x: int
    y: int


async def sum_point(ctx: Context, p: Point) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_struct_arg_coerced_from_builtins() -> None:
    df = DurableFunction(sum_point)
    # On recovery the arg arrives as a plain dict, not a Point.
    payload = Args(args=({"x": 3, "y": 4},), kwargs={})
    assert await df.invoke(_context(), payload) == 7


@pytest.mark.asyncio
async def test_coercion_failure_raises_serialization_error() -> None:
    df = DurableFunction(sum_point)
    payload = Args(args=({"x": "not-an-int", "y": 4},), kwargs={})
    with pytest.raises(SerializationError):
        await df.invoke(_context(), payload)


# =============================================================================
# coerce_result: return-value coercion on recovery (the symmetric counterpart of
# argument coercion). On the ``ctx.run`` recovery short-circuit a settled child's
# value comes back as JSON builtins; the declared return type reshapes it into
# the same in-memory object the live path would have produced.
# =============================================================================


async def make_point(ctx: Context, x: int, y: int) -> Point:
    return Point(x=x, y=y)


async def make_pair(ctx: Context, a: int, b: int) -> tuple[int, str]:
    return a, str(b)


def test_coerce_result_struct_from_builtins() -> None:
    df = DurableFunction(make_point)
    assert df.coerce_result({"x": 1, "y": 2}) == Point(x=1, y=2)


def test_coerce_result_list_to_tuple() -> None:
    # JSON has no tuple, so a tuple return round-trips as a list; the annotation
    # drives coercion back to a real tuple.
    df = DurableFunction(make_pair)
    result = df.coerce_result([1, "2"])
    assert result == (1, "2")
    assert isinstance(result, tuple)


def test_coerce_result_failure_raises_serialization_error() -> None:
    df = DurableFunction(make_point)
    with pytest.raises(SerializationError):
        df.coerce_result({"x": "not-an-int", "y": 2})


def test_coerce_result_passthrough_when_unannotated() -> None:
    df = DurableFunction(leaf)
    # Drop the resolved return annotation to simulate an unannotated function.
    df._return_annotation = inspect.Parameter.empty
    sentinel = {"raw": "dict"}
    # Untyped: recovers raw builtins, exactly like rpc -- not coerced.
    assert df.coerce_result(sentinel) is sentinel


def test_coerce_result_passthrough_when_any() -> None:
    async def any_ret(ctx: Context, x: int) -> Any:
        return x

    df = DurableFunction(any_ret)
    sentinel = {"raw": "dict"}
    assert df.coerce_result(sentinel) is sentinel


def test_coerce_result_passthrough_for_none_annotation() -> None:
    df = DurableFunction(ctx_only)
    df._return_annotation = None  # as resolved from ``-> None``
    assert df.coerce_result(None) is None


def test_coerce_result_passthrough_for_unresolved_return() -> None:
    # A return annotation unresolvable at runtime is left as a string and skipped
    # -- the same tolerance _signature applies to parameter annotations.
    async def fn(ctx: Context, x: int) -> int:
        return x

    fn.__annotations__["return"] = "DefinitelyNotARealName"
    df = DurableFunction(fn)
    sentinel = {"raw": "dict"}
    assert df.coerce_result(sentinel) is sentinel


# =============================================================================
# Replay parity: fresh in-process objects vs JSON-recovered builtins
# =============================================================================


def _roundtrip(packed: Args) -> Args:
    """Simulate the durability boundary: JSON-encode the packed Args, decode back.

    Mirrors recovery: Core decodes the promise param into a typed
    :class:`~resonate.types.TaskData` (an :class:`Args`), so the recovered slot
    carries JSON builtins for its element values rather than live objects.
    """
    return msgspec.json.decode(msgspec.json.encode(packed), type=Args)


class Line(msgspec.Struct, frozen=True):
    start: Point
    end: Point


async def manhattan(ctx: Context, line: Line) -> int:
    return abs(line.start.x - line.end.x) + abs(line.start.y - line.end.y)


@pytest.mark.asyncio
async def test_replay_parity_positional() -> None:
    df = DurableFunction(leaf)
    fresh = df.pack_args(21)
    assert await df.invoke(_context(), fresh) == await df.invoke(
        _context(), _roundtrip(fresh)
    )


@pytest.mark.asyncio
async def test_replay_parity_struct() -> None:
    df = DurableFunction(sum_point)
    fresh = df.pack_args(Point(x=3, y=4))
    # Fresh dispatch carries a real Point; recovery carries a dict -- both -> 7.
    assert await df.invoke(_context(), fresh) == 7
    assert await df.invoke(_context(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_parity_nested_struct() -> None:
    df = DurableFunction(manhattan)
    fresh = df.pack_args(Line(start=Point(x=0, y=0), end=Point(x=3, y=4)))
    assert await df.invoke(_context(), _roundtrip(fresh)) == 7


@pytest.mark.asyncio
async def test_replay_is_idempotent_across_repeats() -> None:
    df = DurableFunction(variadic)
    payload = _roundtrip(df.pack_args(1, 2, 3, k=4))
    results = [await df.invoke(_context(), payload) for _ in range(5)]
    assert results == [10, 10, 10, 10, 10]
    # The DurableFunction carries no per-call state: its metadata is unchanged.
    assert df.name == "variadic"


@pytest.mark.asyncio
async def test_distinct_payloads_do_not_interfere() -> None:
    df = DurableFunction(leaf)
    a = df.pack_args(1)
    b = df.pack_args(100)
    assert await df.invoke(_context(), a) == 2
    assert await df.invoke(_context(), b) == 200
    assert await df.invoke(_context(), a) == 2  # re-running A stays stable


def test_pack_args_produces_immutable_args() -> None:
    df = DurableFunction(variadic)
    payload = df.pack_args(1, 2, 3)
    assert payload == Args(args=(1, 2, 3), kwargs={})
    # ``args`` is a tuple: the packed payload is immutable and cannot be aliased
    # or mutated by the caller after the fact.
    assert isinstance(payload.args, tuple)


# =============================================================================
# Registration: callables, methods, lambdas
# =============================================================================


class _Adder:
    """A callable object (not a function) used as a durable function."""

    def __call__(self, ctx: Context, x: int) -> int:
        return x + 100


class _Service:
    async def step(self, ctx: Context, x: int) -> str:
        return f"{ctx.info.id}:{x}"


@pytest.mark.asyncio
async def test_callable_instance_supported() -> None:
    df = DurableFunction(_Adder())
    assert df.name == "unknown"  # an instance has no __name__
    assert await df.invoke(_context(), df.pack_args(1)) == 101


@pytest.mark.asyncio
async def test_bound_method_drops_self() -> None:
    # The bound method's first user-visible param is ``ctx`` -- ``self`` is
    # already bound, so the convention still holds.
    df = DurableFunction(_Service().step)
    assert df.name == "step"
    assert await df.invoke(_context(), df.pack_args(9)) == "root:9"


class _PointAdder:
    """A callable instance whose argument and return are user-defined structs.

    Regression fixture: a callable instance has no ``__globals__`` of its own, so
    its string annotations (under ``from __future__ import annotations``) must be
    resolved against ``type(self).__call__``'s globals -- otherwise coercion is
    silently skipped and recovery hands ``__call__`` a raw ``dict`` for ``p``.
    """

    async def __call__(self, ctx: Context, p: Point) -> Point:
        return Point(x=p.x + 1, y=p.y + 1)


@pytest.mark.asyncio
async def test_callable_instance_coerces_struct_arg_on_recovery() -> None:
    # On recovery the arg arrives as a plain dict. The annotation lives on
    # ``__call__`` (the instance has no ``__globals__``); coercion must still
    # resolve ``Point`` and rebuild it before the call.
    df = DurableFunction(_PointAdder())
    payload = Args(args=({"x": 3, "y": 4},), kwargs={})
    assert await df.invoke(_context(), payload) == Point(x=4, y=5)


def test_callable_instance_coerces_struct_result() -> None:
    # The return annotation is likewise resolved against ``__call__``'s globals,
    # so a recovered (JSON-builtin) value is reshaped into the declared Point.
    df = DurableFunction(_PointAdder())
    assert df.coerce_result({"x": 1, "y": 2}) == Point(x=1, y=2)


def test_lambda_supported() -> None:
    df = DurableFunction(lambda _ctx, x: x)
    assert df.name == "<lambda>"


# =============================================================================
# pack_args: signature validation
# =============================================================================


def test_pack_args_missing_required_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError, match="leaf"):
        df.pack_args()


def test_pack_args_unexpected_keyword_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        df.pack_args(1, nope=2)


def test_pack_args_positional_as_keyword() -> None:
    df = DurableFunction(leaf)
    assert df.pack_args(x=21) == Args(args=(), kwargs={"x": 21})


@pytest.mark.asyncio
async def test_pack_args_positional_as_keyword_invokes() -> None:
    df = DurableFunction(leaf)
    assert await df.invoke(_context(), df.pack_args(x=21)) == 42


def test_pack_args_excludes_ctx_param() -> None:
    df = DurableFunction(workflow)
    # ``workflow`` is (ctx, x); only ``x`` is a user arg, so passing two
    # positionals (as if including ctx) overflows the env-stripped signature.
    with pytest.raises(ApplicationError):
        df.pack_args(_context(), 7)


# =============================================================================
# *args / **kwargs / positional-only: empties and round trips
# =============================================================================


async def star_args(ctx: Context, *args: int) -> int:
    return sum(args)


async def star_kwargs(ctx: Context, **kwargs: int) -> int:
    return sum(kwargs.values())


async def posonly(ctx: Context, x: int, /, y: int) -> int:
    return x - y


def test_empty_variadic_packs_to_empty_args() -> None:
    assert DurableFunction(star_args).pack_args() == Args()
    assert DurableFunction(star_kwargs).pack_args() == Args()


@pytest.mark.asyncio
async def test_star_args_round_trip() -> None:
    df = DurableFunction(star_args)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(1, 2, 3))) == 6
    assert await df.invoke(_context(), Args()) == 0  # no args supplied


@pytest.mark.asyncio
async def test_star_kwargs_round_trip() -> None:
    df = DurableFunction(star_kwargs)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(a=1, b=2))) == 3


@pytest.mark.asyncio
async def test_positional_only_round_trip() -> None:
    df = DurableFunction(posonly)
    assert await df.invoke(_context(), _roundtrip(df.pack_args(10, 3))) == 7


# =============================================================================
# invoke: a typed Args slot removes the old envelope-detection ambiguity
# =============================================================================


@pytest.mark.asyncio
async def test_user_dict_shaped_like_args_is_unambiguous() -> None:
    # With a typed ``Args`` slot there is no envelope-detection heuristic: a user
    # dict that happens to look like ``{"args", "kwargs"}`` is just an ordinary
    # positional argument, so it round-trips with no special-casing.
    async def echo(ctx: Context, d: dict[str, Any]) -> dict[str, Any]:
        return d

    df = DurableFunction(echo)
    tricky = {"args": [1, 2], "kwargs": {"z": 9}}
    payload = df.pack_args(tricky)
    assert payload == Args(args=(tricky,), kwargs={})
    assert await df.invoke(_context(), _roundtrip(payload)) == tricky


# =============================================================================
# invoke: coercion edge cases
# =============================================================================


@pytest.mark.asyncio
async def test_unannotated_param_passes_through() -> None:
    def fn(ctx: Context, x: int) -> object:
        return x

    del fn.__annotations__["x"]  # a genuinely unannotated parameter
    df = DurableFunction(fn)
    payload = Args(args=({"keep": "me"},), kwargs={})
    assert await df.invoke(_context(), payload) == {"keep": "me"}


@pytest.mark.asyncio
async def test_any_annotation_passes_through() -> None:
    async def any_leaf(ctx: Context, x: Any) -> Any:
        return x

    df = DurableFunction(any_leaf)
    assert await df.invoke(_context(), Args(args=({"a": 1},), kwargs={})) == {"a": 1}


@pytest.mark.asyncio
async def test_unresolvable_ctx_annotation_does_not_block_sibling_coercion() -> None:
    # The standard convention imports Context only under TYPE_CHECKING, so
    # `ctx: Context` is unresolvable at runtime. That must NOT disable coercion
    # of a sibling argument: annotations resolve per parameter, not all-or-none.
    async def fn(ctx: Context, p: Point) -> int:
        return p.x + p.y

    # Mimic a TYPE_CHECKING-only import: ctx's annotation references a name that
    # does not exist at runtime.
    fn.__annotations__["ctx"] = "TypeCheckingOnlyContext"
    df = DurableFunction(fn)
    # p is still coerced from the JSON-builtins dict into a Point.
    payload = Args(args=({"x": 1, "y": 2},), kwargs={})
    assert await df.invoke(_context(), payload) == 3


@pytest.mark.asyncio
async def test_optional_annotation_accepts_none() -> None:
    async def maybe(ctx: Context, x: int | None) -> bool:
        return x is None

    df = DurableFunction(maybe)
    assert await df.invoke(_context(), df.pack_args(None)) is True
    assert await df.invoke(_context(), df.pack_args(5)) is False


@pytest.mark.asyncio
async def test_list_annotation_coerces_elements() -> None:
    async def total(ctx: Context, xs: list[int]) -> int:
        return sum(xs)

    df = DurableFunction(total)
    assert await df.invoke(_context(), Args(args=([1, 2, 3],), kwargs={})) == 6


@pytest.mark.asyncio
async def test_var_positional_struct_coercion() -> None:
    async def sum_points(ctx: Context, *points: Point) -> int:
        return sum(p.x + p.y for p in points)

    df = DurableFunction(sum_points)
    payload = Args(args=({"x": 1, "y": 2}, {"x": 3, "y": 4}), kwargs={})
    assert await df.invoke(_context(), payload) == 10


@pytest.mark.asyncio
async def test_var_keyword_struct_coercion() -> None:
    async def gather(ctx: Context, **points: Point) -> int:
        return sum(p.x for p in points.values())

    df = DurableFunction(gather)
    payload = Args(args=(), kwargs={"a": {"x": 5, "y": 0}, "b": {"x": 6, "y": 0}})
    assert await df.invoke(_context(), payload) == 11


# =============================================================================
# invoke: defaults are not coerced (sentinel-default regression)
# =============================================================================


_SENTINEL: Any = object()


@pytest.mark.asyncio
async def test_unprovided_default_is_not_coerced() -> None:
    # Regression: a sentinel default whose type does not match its annotation
    # must survive untouched when the caller omits the argument (it never
    # crossed the serialization boundary, so it needs no coercion).
    async def with_sentinel(ctx: Context, x: int = _SENTINEL) -> bool:
        return x is _SENTINEL

    df = DurableFunction(with_sentinel)
    assert await df.invoke(_context(), Args()) is True  # default kept as-is
    assert await df.invoke(_context(), df.pack_args(7)) is False  # provided value used


@pytest.mark.asyncio
async def test_provided_value_coerced_with_defaults_present() -> None:
    async def add(ctx: Context, a: int, b: int = 100) -> int:
        return a + b

    df = DurableFunction(add)
    assert await df.invoke(_context(), Args(args=(1,), kwargs={})) == 101  # b default
    assert await df.invoke(_context(), Args(args=(1, 2), kwargs={})) == 3


# =============================================================================
# invoke: resilience to incompatible / corrupt payloads (signature drift)
# =============================================================================


@pytest.mark.asyncio
async def test_invoke_too_many_args_raises() -> None:
    df = DurableFunction(leaf)  # (ctx, x: int)
    with pytest.raises(ApplicationError, match="leaf"):
        await df.invoke(_context(), Args(args=(1, 2), kwargs={}))


@pytest.mark.asyncio
async def test_invoke_unexpected_keyword_raises() -> None:
    df = DurableFunction(leaf)
    with pytest.raises(ApplicationError):
        await df.invoke(_context(), Args(args=(1,), kwargs={"nope": 9}))


@pytest.mark.asyncio
async def test_coercion_error_notes_function_name() -> None:
    df = DurableFunction(sum_point)
    payload = Args(args=({"x": "bad", "y": 1},), kwargs={})
    with pytest.raises(SerializationError) as excinfo:
        await df.invoke(_context(), payload)
    notes = getattr(excinfo.value, "__notes__", [])
    assert any("sum_point" in note for note in notes)


# =============================================================================
# invoke: sync/async parity and None results
# =============================================================================


@pytest.mark.asyncio
async def test_sync_function_raising_propagates() -> None:
    def boom(ctx: Context, x: int) -> int:
        msg = "sync boom"
        raise ApplicationError(msg)

    df = DurableFunction(boom)
    with pytest.raises(ApplicationError, match="sync boom"):
        await df.invoke(_context(), df.pack_args(1))


@pytest.mark.asyncio
async def test_function_returning_none() -> None:
    captured: list[int] = []

    async def sink(ctx: Context, x: int) -> None:
        captured.append(x)

    df = DurableFunction(sink)
    assert await df.invoke(_context(), df.pack_args(5)) is None
    assert captured == [5]


# =============================================================================
# Argument coercion across popular struct-definition styles
#
# The codec-level matrix lives in ``test_codec.py``; these pin the same support
# from the angle a user actually hits it -- a durable function whose parameter
# is annotated with some struct style. On recovery the argument arrives as JSON
# builtins, and ``invoke`` coerces it back to the declared type (via
# ``msgspec.convert``) before the call. msgspec supports dataclasses, attrs, and
# enums natively; pydantic is unsupported and must raise rather than silently
# hand the function a raw dict.
# =============================================================================


@dataclasses.dataclass(frozen=True)
class DataclassArg:
    x: int
    y: int


async def sum_dataclass(ctx: Context, p: DataclassArg) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_dataclass_arg_coerced_from_builtins() -> None:
    df = DurableFunction(sum_dataclass)
    # On recovery the arg arrives as a plain dict, not a DataclassArg.
    payload = Args(args=({"x": 3, "y": 4},), kwargs={})
    assert await df.invoke(_context(), payload) == 7


@pytest.mark.asyncio
async def test_dataclass_arg_replay_parity() -> None:
    df = DurableFunction(sum_dataclass)
    fresh = df.pack_args(DataclassArg(x=3, y=4))
    assert await df.invoke(_context(), fresh) == 7
    assert await df.invoke(_context(), _roundtrip(fresh)) == 7


@attrs.frozen
class AttrsArg:
    x: int
    y: int


async def sum_attrs(ctx: Context, p: AttrsArg) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_attrs_arg_replay_parity() -> None:
    df = DurableFunction(sum_attrs)
    fresh = df.pack_args(AttrsArg(x=5, y=6))
    assert await df.invoke(_context(), fresh) == 11
    assert await df.invoke(_context(), _roundtrip(fresh)) == 11


class Priority(enum.IntEnum):
    HIGH = 2


async def double_priority(ctx: Context, p: Priority) -> int:
    return int(p) * 2


@pytest.mark.asyncio
async def test_enum_arg_coerced_from_builtins() -> None:
    df = DurableFunction(double_priority)
    fresh = df.pack_args(Priority.HIGH)
    # On recovery the enum arrives as its raw value (2) and is coerced back.
    recovered = _roundtrip(fresh)
    assert recovered == Args(args=(2,), kwargs={})
    assert await df.invoke(_context(), recovered) == 4


class PydanticArg(pydantic.BaseModel):
    x: int
    y: int


async def sum_pydantic(ctx: Context, p: PydanticArg) -> int:
    return p.x + p.y


@pytest.mark.asyncio
async def test_pydantic_arg_coercion_is_unsupported() -> None:
    # msgspec cannot build a pydantic model from the recovered dict, so coercion
    # raises rather than handing the function an un-coerced mapping.
    df = DurableFunction(sum_pydantic)
    payload = Args(args=({"x": 3, "y": 4},), kwargs={})
    with pytest.raises(SerializationError):
        await df.invoke(_context(), payload)


# =============================================================================
# invoke: coercion runs on the live path too, not just recovery
#
# ``invoke`` coerces on *every* call -- the first live execution as well as a
# replay -- so a payload that cannot satisfy the declared type fails fast at
# dispatch instead of only after a crash, and the live and replay paths stay
# byte-for-byte symmetric. The flip side is msgspec strictness: a value plain
# Python would accept can be rejected or reshaped, *identically* on both paths.
# =============================================================================


async def needs_int(ctx: Context, x: int) -> int:
    return x + 1


@pytest.mark.asyncio
async def test_bool_for_int_rejected_consistently_live_and_replay() -> None:
    # ``True`` satisfies ``x: int`` for plain Python (``True + 1 == 2``) but
    # msgspec rejects bool-for-int. The rejection happens on the live path
    # (freshly-packed bool) and identically on replay (the same bool after a
    # JSON round-trip) -- failing fast at dispatch, not only after recovery.
    df = DurableFunction(needs_int)
    live = df.pack_args(True)  # noqa: FBT003
    with pytest.raises(SerializationError):
        await df.invoke(_context(), live)
    with pytest.raises(SerializationError):
        await df.invoke(_context(), _roundtrip(live))


@pytest.mark.asyncio
async def test_int_widened_to_float_reshaped_consistently() -> None:
    # msgspec widens int -> float. The reshape is applied on the live path and
    # on replay alike, so the function sees the same ``1.0`` either way.
    async def needs_float(ctx: Context, x: float) -> bool:
        return isinstance(x, float)

    df = DurableFunction(needs_float)
    live = df.pack_args(1)
    assert await df.invoke(_context(), live) is True
    assert await df.invoke(_context(), _roundtrip(live)) is True


# =============================================================================
# Registration: the first parameter must be able to hold the injected ctx
#
# ``ctx`` is injected positionally and stripped by position, so the leading
# parameter must be a plain positional one. ``*args``/``**kwargs``/keyword-only
# leading parameters cannot -- and a bare ``(*args, **kwargs)`` is the tell-tale
# shape of a decorator applied without functools.wraps -- so registration
# rejects them with a clear message instead of failing confusingly later.
# =============================================================================


def test_unwrapped_decorator_rejected() -> None:
    # A decorator that forgets functools.wraps erases the real signature,
    # leaving ``(*args, **kwargs)``. Its first parameter is variadic-positional,
    # so it cannot be the ctx slot.
    def deco(f: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return f(*args, **kwargs)

        return wrapper

    @deco
    def fn(ctx: Context, x: int) -> int:
        return x

    bad: Any = fn
    with pytest.raises(ApplicationError, match="first positional argument"):
        DurableFunction(bad)


def test_keyword_only_ctx_rejected() -> None:
    async def fn(*, ctx: Context, x: int) -> int:
        return x

    bad: Any = fn
    with pytest.raises(ApplicationError, match="first positional argument"):
        DurableFunction(bad)


@pytest.mark.asyncio
async def test_wrapped_decorator_preserves_signature() -> None:
    # With functools.wraps the real signature survives, so the convention holds
    # and coercion still works.

    def deco(f: Any) -> Any:
        @functools.wraps(f)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await f(*args, **kwargs)

        return wrapper

    @deco
    async def fn(ctx: Context, x: int) -> int:
        return x + 1

    df = DurableFunction(fn)
    assert await df.invoke(_context(), df.pack_args(41)) == 42


# =============================================================================
# coercion: ``None``/``NoneType`` annotation is pass-through on BOTH sides
#
# ``_coerce`` (arguments) and ``coerce_result`` (return) share one predicate, so
# a ``None`` annotation -- ``-> None`` or the rare ``x: None`` -- passes through
# untouched on either side rather than the two diverging.
# =============================================================================


@pytest.mark.asyncio
async def test_none_annotated_param_passes_through() -> None:
    async def takes_none(ctx: Context, x: None) -> bool:
        return x is None

    df = DurableFunction(takes_none)
    # The recovered value is already ``None`` (JSON ``null``); pass it through
    # rather than running it through ``msgspec.convert(None, None)``. The return
    # side (``-> None``) is covered by test_coerce_result_passthrough_for_none.
    assert await df.invoke(_context(), df.pack_args(None)) is True


# =============================================================================
# coercion: untyped (Any) non-JSON-native return diverges live vs recovery
#
# Pins the documented footgun -- a pass-through return reshapes nothing, so the
# recovery short-circuit yields the decoded builtin, not the live object.
# =============================================================================


def test_any_return_recovers_raw_builtin_not_original_type() -> None:
    async def make_set(ctx: Context, n: int) -> Any:
        return set(range(n))

    df = DurableFunction(make_set)
    # On recovery a set has already decoded to a list; the ``Any`` annotation
    # carries nothing to rebuild it, so the caller gets the list as-is. (Annotate
    # ``-> set[int]`` to recover a real set -- that is the symmetric path.)
    assert df.coerce_result([0, 1, 2]) == [0, 1, 2]


# =============================================================================
# _resolve_annotation: any evaluation failure is tolerated, not just NameError
# =============================================================================


@pytest.mark.asyncio
async def test_annotation_eval_error_does_not_break_registration() -> None:
    # ``x``'s annotation evaluates to a ZeroDivisionError -- outside the old
    # (NameError, AttributeError, SyntaxError, TypeError) set. Registration must
    # still succeed, leaving ``x`` un-coerced (pass-through) rather than aborting.
    async def fn(ctx: Context, x: int) -> int:
        return x

    fn.__annotations__["x"] = "1 / 0"  # raises ZeroDivisionError on eval
    df = DurableFunction(fn)
    # Un-coerced: the raw builtin reaches the function untouched.
    assert await df.invoke(_context(), Args(args=(7,), kwargs={})) == 7
