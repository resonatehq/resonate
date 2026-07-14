"""Behaviour tests for :mod:`resonate.ext.pydantic_ai`.

End-to-end tests drive a real ``ResonateAgent`` over the in-process
:class:`~resonate.network.LocalNetwork` -- "real server, real wire", no mocks
-- with Pydantic AI's offline ``TestModel`` / ``FunctionModel`` standing in for
a provider. Replay tests reuse the ``_root`` preload harness from
``test_context``: a pre-settled child promise is what a crash-recovery replay
observes, so serving the checkpoint without re-invoking the wrapped model is
asserted directly at the durability boundary.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Self

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.exceptions import UserError
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.models.wrapper import CompletedStreamedResponse
from pydantic_ai.tools import RunContext
from pydantic_ai.usage import RunUsage

from resonate.codec import Codec, NoopEncryptor
from resonate.context import Context
from resonate.dependencies import DependencyMap
from resonate.effects import ResonateEffects
from resonate.error import ApplicationError, ServerError
from resonate.ext.pydantic_ai import ResonateAgent, ResonateModel
from resonate.ext.pydantic_ai.context import (
    reset_workflow_context,
    set_workflow_context,
)
from resonate.ext.pydantic_ai.types import ModelResponseEnvelope
from resonate.network import LocalNetwork
from resonate.network.local import Task
from resonate.resonate import Resonate
from resonate.retry import Never
from resonate.send import Sender
from resonate.transport import Transport
from resonate.types import PromiseRecord, Value

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, AsyncIterator

    from pydantic_ai.messages import AgentStreamEvent

I64_MAX = 2**63 - 1


# =============================================================================
# Harness
# =============================================================================


@contextlib.asynccontextmanager
async def local() -> AsyncIterator[Resonate]:
    """Yield a local-mode Resonate, stopping it on exit.

    Pins ``Never`` so a failing leaf settles immediately instead of retrying
    through the SDK-wide exponential default.
    """
    r = Resonate(retry_policy=Never())
    try:
        yield r
    finally:
        await r.stop()


def _codec() -> Codec:
    return Codec(NoopEncryptor())


def _root(preload: list[PromiseRecord] | None = None) -> Context:
    """Build a root ``Context`` over a fresh ``LocalNetwork``, as a workflow would see it."""
    net = LocalNetwork()
    net.state.tasks["root"] = Task(
        id="root",
        state="acquired",
        version=1,
        pid="test-pid",
        ttl=60_000,
        resumes=set(),
    )
    sender = Sender(Transport(net), None)
    effects = ResonateEffects(sender, _codec(), "root", 1, preload or [])
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


def _resolved(id: str, value: Any) -> PromiseRecord:
    """Build a pre-settled *resolved* record, wire-encoded for the preload cache."""
    return PromiseRecord(
        id=id,
        state="resolved",
        timeout_at=I64_MAX,
        param=Value(),
        value=_codec().encode(value),
        tags={},
        created_at=0,
        settled_at=1,
    )


def _text_response(content: str) -> ModelResponse:
    return ModelResponse(parts=[TextPart(content=content)])


# =============================================================================
# Durable runs, end to end
# =============================================================================


@pytest.mark.asyncio
async def test_run_returns_output_and_messages() -> None:
    async with local() as r:
        agent = Agent(TestModel(custom_output_text="Mexico City"), name="basic")
        durable = ResonateAgent(agent, r)

        result = await durable.run("What is the capital of Mexico?", id="basic-1")

        assert result.output == "Mexico City"
        assert len(result.all_messages()) == 2
        assert result.usage.requests == 1
        assert result.new_messages()  # the reconstructed result keeps the message index


@pytest.mark.asyncio
async def test_run_same_id_returns_same_result_without_rerunning() -> None:
    calls = 0

    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal calls
        calls += 1
        return _text_response(f"answer #{calls}")

    async with local() as r:
        agent = Agent(FunctionModel(model_logic), name="dedup")
        durable = ResonateAgent(agent, r)

        first = await durable.run("question", id="dedup-1")
        second = await durable.run("a different prompt", id="dedup-1")

        assert calls == 1
        assert first.output == "answer #1"
        assert second.output == first.output
        assert second.all_messages() == first.all_messages()


class City(BaseModel):
    name: str
    country: str


@pytest.mark.asyncio
async def test_run_structured_output_is_rebuilt_as_model() -> None:
    async with local() as r:
        agent = Agent(TestModel(), name="structured", output_type=City)
        durable = ResonateAgent(agent, r)

        result = await durable.run("capital of Mexico?", id="structured-1")
        assert isinstance(result.output, City)

        # Served from the journal: the output is rebuilt as the model, not a dict.
        recovered = await durable.run("capital of Mexico?", id="structured-1")
        assert isinstance(recovered.output, City)
        assert recovered.output == result.output


@pytest.mark.asyncio
async def test_run_tool_loop_executes_function_tools_inline() -> None:
    tool_calls: list[str] = []

    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(
                parts=[ToolCallPart(tool_name="get_weather", args={"city": "SF"})]
            )
        return _text_response("sunny in SF")

    async with local() as r:
        agent = Agent(FunctionModel(model_logic), name="tools")

        @agent.tool_plain
        def get_weather(city: str) -> str:
            tool_calls.append(city)
            return "sunny"

        durable = ResonateAgent(agent, r)
        result = await durable.run("weather in SF?", id="tools-1")

        assert result.output == "sunny in SF"
        assert tool_calls == ["SF"]
        returns = [
            part
            for message in result.all_messages()
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        ]
        assert len(returns) == 1
        assert returns[0].content == "sunny"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["parallel_ordered_events", "sequential"])
async def test_parallel_execution_mode_runs_all_tool_calls(mode: Any) -> None:
    """Both durable execution modes drive a multi-tool-call step to completion.

    `parallel_ordered_events` (the default) and `sequential` are the two modes
    that keep checkpoint order deterministic across replays; plain `parallel`
    is intentionally unavailable. Ordered events additionally guarantee the
    tool results land in the model's call order.
    """
    names = ["t1", "t2", "t3", "t4", "t5"]
    tool_calls: list[str] = []

    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(
                parts=[ToolCallPart(tool_name=name, args={}) for name in names]
            )
        return _text_response("all done")

    async with local() as r:
        agent = Agent(FunctionModel(model_logic), name=f"parallel-{mode}")

        for name in names:

            @agent.tool_plain(name=name)
            async def call(bound: str = name) -> str:
                tool_calls.append(bound)
                return bound

        durable = ResonateAgent(agent, r, parallel_execution_mode=mode)
        result = await durable.run("go", id=f"parallel-{mode}-1")

        assert result.output == "all done"
        assert set(tool_calls) == set(names)
        # Deterministic checkpoint order: tool returns follow the model's call order.
        returns = [
            part.tool_name
            for message in result.all_messages()
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        ]
        assert returns == names


@pytest.mark.asyncio
async def test_run_message_history_round_trips() -> None:
    async with local() as r:
        agent = Agent(TestModel(custom_output_text="hello again"), name="history")
        durable = ResonateAgent(agent, r)

        first = await durable.run("hello", id="history-1")
        second = await durable.run(
            "and again", id="history-2", message_history=first.all_messages()
        )

        assert second.output == "hello again"
        # The prior conversation is preserved ahead of the new exchange.
        assert len(second.all_messages()) == len(first.all_messages()) + 2


@pytest.mark.asyncio
async def test_run_deps_reach_tools() -> None:
    seen: list[str] = []

    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart(tool_name="whoami", args={})])
        return _text_response("done")

    async with local() as r:
        agent = Agent(FunctionModel(model_logic), name="deps", deps_type=dict)

        @agent.tool
        def whoami(ctx: RunContext[dict]) -> str:
            seen.append(ctx.deps["user"])
            return ctx.deps["user"]

        durable = ResonateAgent(agent, r)
        result = await durable.run("who am I?", id="deps-1", deps={"user": "ada"})

        assert result.output == "done"
        assert seen == ["ada"]


@pytest.mark.asyncio
async def test_run_without_id_generates_a_fresh_one() -> None:
    async with local() as r:
        agent = Agent(TestModel(), name="conv")
        durable = ResonateAgent(agent, r)

        # Omitting `id` mints a fresh durable id; the run works.
        result = await durable.run("hi", conversation_id="conv-42")
        assert result.conversation_id == "conv-42"

        # `conversation_id` must NOT double as the durable id: it spans many
        # runs, so keying the promise by it would make a follow-up turn replay
        # the first turn's result. No promise is created under the conv id.
        with pytest.raises(ServerError, match="not found"):
            await r.promises.get("conv-42")


@pytest.mark.asyncio
async def test_run_pins_conversation_id_across_replay() -> None:
    async with local() as r:
        agent = Agent(TestModel(), name="conv-pin")
        durable = ResonateAgent(agent, r)

        # With no explicit `conversation_id`, the id is resolved once at dispatch
        # and pinned into the durable params, so it survives a recovery replay
        # rather than being re-minted in the workflow body. Re-running with the
        # same durable `id` serves the pinned result from the journal.
        first = await durable.run("hi", id="pin-1")
        again = await durable.run("hi", id="pin-1")
        assert first.conversation_id == again.conversation_id


@pytest.mark.asyncio
async def test_run_model_failure_rejects_the_durable_run() -> None:
    def explode(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        msg = "provider unavailable"
        raise ApplicationError(msg)

    async with local() as r:
        agent = Agent(FunctionModel(explode), name="failing")
        durable = ResonateAgent(agent, r)

        with pytest.raises(ApplicationError, match="provider unavailable"):
            await durable.run("hi", id="failing-1")


@pytest.mark.asyncio
async def test_agent_remains_usable_outside_workflows() -> None:
    async with local() as r:
        model = TestModel(custom_output_text="direct")
        agent = Agent(model, name="direct")
        durable = ResonateAgent(agent, r)

        # The durable wrapper's model and toolsets fall through outside a workflow.
        async with durable.iter("hi") as agent_run:
            async for _ in agent_run:
                pass
        assert agent_run.result is not None
        assert agent_run.result.output == "direct"

        # And the wrapped agent itself is untouched.
        direct = await agent.run("hi")
        assert direct.output == "direct"


@pytest.mark.asyncio
async def test_nested_agent_runs_inline_in_outer_workflow() -> None:
    def outer_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart(tool_name="ask_expert", args={})])
        return _text_response("outer done")

    async with local() as r:
        inner_agent = Agent(TestModel(custom_output_text="inner answer"), name="inner")
        outer_agent = Agent(FunctionModel(outer_logic), name="outer")

        durable_inner = ResonateAgent(inner_agent, r)
        durable_outer = ResonateAgent(outer_agent, r)

        @outer_agent.tool_plain
        async def ask_expert() -> str:
            # Inside the outer workflow: the nested run executes inline, its model
            # request checkpointed as a step of the outer workflow.
            nested = await durable_inner.run("what do you think?")
            return nested.output

        result = await durable_outer.run("ask the expert", id="nested-1")
        assert result.output == "outer done"

        returns = [
            part
            for message in result.all_messages()
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        ]
        assert returns[0].content == "inner answer"


# =============================================================================
# Streaming
# =============================================================================


@pytest.mark.asyncio
async def test_event_stream_handler_receives_model_and_run_events() -> None:
    events: list[str] = []

    async def handler(
        ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]
    ) -> None:
        events.extend([type(event).__name__ async for event in stream])

    async with local() as r:
        agent = Agent(TestModel(custom_output_text="streamed"), name="streaming")

        @agent.tool_plain
        def noop_tool() -> str:
            return "ok"

        durable = ResonateAgent(agent, r, event_stream_handler=handler)
        result = await durable.run("stream me", id="streaming-1")

        assert result.output == "streamed"
        # Model part events (streamed inside the leaf step) and run events
        # (forwarded one at a time from the workflow body) both arrive.
        assert "PartStartEvent" in events
        assert "FunctionToolCallEvent" in events


@pytest.mark.asyncio
async def test_per_run_event_stream_handler() -> None:
    events: list[str] = []

    async def handler(
        ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]
    ) -> None:
        events.extend([type(event).__name__ async for event in stream])

    async with local() as r:
        agent = Agent(TestModel(custom_output_text="per-run"), name="per-run-handler")
        durable = ResonateAgent(agent, r)

        result = await durable.run(
            "stream me", id="per-run-1", event_stream_handler=handler
        )

        assert result.output == "per-run"
        assert "PartStartEvent" in events


@pytest.mark.asyncio
async def test_run_stream_works_outside_workflows() -> None:
    async with local() as r:
        agent = Agent(TestModel(custom_output_text="streamed out"), name="run-stream")
        durable = ResonateAgent(agent, r)

        async with durable.run_stream("hi") as response:
            assert await response.get_output() == "streamed out"


@pytest.mark.asyncio
async def test_run_stream_events_raises() -> None:
    async with local() as r:
        durable = ResonateAgent(Agent(TestModel(), name="rse"), r)
        with pytest.raises(UserError, match="run_stream_events"):
            durable.run_stream_events("hi")


@pytest.mark.asyncio
async def test_run_sync_raises() -> None:
    async with local() as r:
        durable = ResonateAgent(Agent(TestModel(), name="rs"), r)
        with pytest.raises(UserError, match="run_sync"):
            durable.run_sync("hi")


# =============================================================================
# Construction- and run-time validation
# =============================================================================


@pytest.mark.asyncio
async def test_agent_name_is_required() -> None:
    async with local() as r:
        with pytest.raises(UserError, match="unique `name`"):
            ResonateAgent(Agent(TestModel()), r)


@pytest.mark.asyncio
async def test_agent_model_is_required() -> None:
    async with local() as r:
        with pytest.raises(UserError, match="have a `model`"):
            ResonateAgent(Agent(name="modelless"), r)


@pytest.mark.asyncio
async def test_name_cannot_be_reassigned() -> None:
    async with local() as r:
        durable = ResonateAgent(Agent(TestModel(), name="frozen"), r)
        with pytest.raises(UserError, match="cannot be changed"):
            durable.name = "renamed"  # ty: ignore[invalid-assignment]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"model": TestModel()},
        {"output_type": City},
        {"toolsets": []},
        {"capabilities": []},
        {"spec": {}},
    ],
    ids=["model", "output_type", "toolsets", "capabilities", "spec"],
)
async def test_run_rejects_unserializable_arguments(kwargs: dict[str, Any]) -> None:
    async with local() as r:
        durable = ResonateAgent(
            Agent(TestModel(), name=f"reject-{next(iter(kwargs))}"), r
        )
        with pytest.raises(UserError, match="cannot be set at agent run time"):
            await durable.run("hi", **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("arg", ["instructions", "model_settings", "metadata"])
async def test_run_rejects_callable_arguments(arg: str) -> None:
    async with local() as r:
        durable = ResonateAgent(Agent(TestModel(), name=f"reject-callable-{arg}"), r)
        kwargs: dict[str, Any] = {arg: lambda _ctx: None}
        with pytest.raises(UserError, match=f"callable `{arg}`"):
            await durable.run("hi", **kwargs)


@pytest.mark.asyncio
async def test_override_rejects_non_resonate_model() -> None:
    async with local() as r:
        durable = ResonateAgent(Agent(TestModel(), name="override"), r)
        with (
            pytest.raises(UserError, match="cannot be contextually overridden"),
            durable.override(model=TestModel()),
        ):
            pass


# =============================================================================
# Replay: checkpoints are served from the journal
# =============================================================================


def _forbidden_model() -> FunctionModel:
    def explode(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        msg = "the live model must not be invoked on replay"
        raise AssertionError(msg)

    return FunctionModel(explode)


@pytest.mark.asyncio
async def test_model_request_replay_is_served_from_journal() -> None:
    stored = ModelResponseEnvelope(response=_text_response("from the journal"))
    ctx = _root([_resolved("root.1", stored)])

    model = ResonateModel(
        _forbidden_model(), retry_policy=None, get_event_stream_handler=lambda: None
    )

    token = set_workflow_context(ctx)
    try:
        response = await model.request([], None, ModelRequestParameters())
    finally:
        reset_workflow_context(token)

    assert isinstance(response, ModelResponse)
    assert response.parts == [TextPart(content="from the journal")]


@pytest.mark.asyncio
async def test_model_request_stream_replay_is_served_from_journal() -> None:
    stored = ModelResponseEnvelope(response=_text_response("streamed from the journal"))
    ctx = _root([_resolved("root.1", stored)])

    handler_calls = 0

    async def handler(
        run_ctx: RunContext[Any], stream: AsyncIterable[AgentStreamEvent]
    ) -> None:
        nonlocal handler_calls
        handler_calls += 1

    model = ResonateModel(
        _forbidden_model(), retry_policy=None, get_event_stream_handler=lambda: handler
    )
    run_context = RunContext(deps=None, model=model, usage=RunUsage())

    token = set_workflow_context(ctx)
    try:
        async with model.request_stream(
            [], None, ModelRequestParameters(), run_context
        ) as streamed:
            assert isinstance(streamed, CompletedStreamedResponse)
            response = streamed.get()
    finally:
        reset_workflow_context(token)

    assert response.parts == [TextPart(content="streamed from the journal")]
    # The handler ran inside the original leaf; a replay must not re-invoke it.
    assert handler_calls == 0


@pytest.mark.asyncio
async def test_model_request_live_checkpoints_the_response() -> None:
    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return _text_response("live answer")

    ctx = _root()
    model = ResonateModel(
        FunctionModel(model_logic),
        retry_policy=None,
        get_event_stream_handler=lambda: None,
    )

    token = set_workflow_context(ctx)
    try:
        response = await model.request([], None, ModelRequestParameters())
    finally:
        reset_workflow_context(token)

    # The live path round-trips through the same envelope the journal stores.
    assert isinstance(response, ModelResponse)
    assert response.parts == [TextPart(content="live answer")]


@pytest.mark.asyncio
async def test_replay_continues_from_last_completed_step() -> None:
    """A recovered run replays completed leaves and executes only the pending ones.

    Models the "worker did some steps, crashed, another worker resumes" case at
    the workflow-body level: the first model request (a tool call) is already
    journaled, so on replay it is served from the journal and only the still-
    pending second request hits the live model. The inline function tool re-runs,
    since function tools are replayed rather than checkpointed.
    """
    step_one = ModelResponseEnvelope(
        response=ModelResponse(
            parts=[ToolCallPart(tool_name="get_weather", args={"city": "SF"})]
        )
    )
    # The first model request is `root.1`; leaving `root.2` unsettled forces the
    # second request to run live.
    ctx = _root([_resolved("root.1", step_one)])

    model_calls = 0
    tool_calls: list[str] = []

    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        nonlocal model_calls
        model_calls += 1
        return _text_response("sunny in SF")

    async with local() as r:
        agent = Agent(FunctionModel(model_logic), name="resume")

        @agent.tool_plain
        def get_weather(city: str) -> str:
            tool_calls.append(city)
            return "sunny"

        durable = ResonateAgent(agent, r)

        # Driving `run` inside a workflow context takes the inline path, so model
        # requests route through the durable model against this preloaded `ctx`.
        token = set_workflow_context(ctx)
        try:
            result = await durable.run("weather in SF?", id="resume-1")
        finally:
            reset_workflow_context(token)

    assert result.output == "sunny in SF"
    # Step 1 served from the journal; only the pending step 2 reached the model.
    assert model_calls == 1
    # The inline tool re-ran on replay (function tools are not checkpointed).
    assert tool_calls == ["SF"]


# =============================================================================
# MCP toolsets
# =============================================================================

mcp = pytest.importorskip(
    "pydantic_ai.mcp", reason="requires the mcp optional dependency"
)

from pydantic_ai.mcp import MCPToolset  # noqa: E402
from pydantic_ai.tools import ToolDefinition  # noqa: E402

from resonate.ext.pydantic_ai.mcp_toolset import (  # noqa: E402
    ResonateMCPToolset,
    ToolResultEnvelope,
)


class StubMCPToolset(MCPToolset[Any]):
    """An `MCPToolset` with canned tools, never connecting to a server."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__("http://localhost:1/unused", **kwargs)
        self.calls: list[str] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> bool | None:
        return None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, Any]:
        self.calls.append("get_tools")
        tool_def = ToolDefinition(
            name="add",
            parameters_json_schema={
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                "required": ["a", "b"],
            },
            description="Add two integers.",
        )
        return {"add": self.tool_for_tool_def(tool_def)}

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext[Any], tool: Any
    ) -> str:
        self.calls.append(f"call_tool:{name}")
        return str(tool_args["a"] + tool_args["b"])


@pytest.mark.asyncio
async def test_mcp_toolset_is_wrapped_and_checkpointed() -> None:
    def model_logic(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if len(messages) == 1:
            return ModelResponse(
                parts=[ToolCallPart(tool_name="add", args={"a": 2, "b": 3})]
            )
        return _text_response("the sum is 5")

    async with local() as r:
        toolset = StubMCPToolset(id="calculator")
        agent = Agent(FunctionModel(model_logic), name="mcp", toolsets=[toolset])
        durable = ResonateAgent(agent, r)

        # The MCP toolset was replaced with its durable wrapper.
        assert any(isinstance(entry, ResonateMCPToolset) for entry in durable._toolsets)

        result = await durable.run("add 2 and 3", id="mcp-1")
        assert result.output == "the sum is 5"
        assert "get_tools" in toolset.calls
        assert "call_tool:add" in toolset.calls

        returns = [
            part
            for message in result.all_messages()
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        ]
        assert returns[0].content == "5"


@pytest.mark.asyncio
async def test_mcp_call_tool_replay_is_served_from_journal() -> None:
    stored = ToolResultEnvelope(result="42")
    ctx = _root([_resolved("root.1", stored)])

    toolset = StubMCPToolset(id="calculator")
    durable_toolset = ResonateMCPToolset(toolset)
    run_context = RunContext(deps=None, model=TestModel(), usage=RunUsage())

    token = set_workflow_context(ctx)
    try:
        tools = await toolset.get_tools(run_context)
        result = await durable_toolset.call_tool(
            "add", {"a": 40, "b": 2}, run_context, tools["add"]
        )
    finally:
        reset_workflow_context(token)

    assert result == "42"
    # get_tools above went straight to the stub; the call itself was replayed.
    assert toolset.calls == ["get_tools"]


@pytest.mark.asyncio
async def test_mcp_toolset_falls_through_outside_workflows() -> None:
    toolset = StubMCPToolset(id="calculator")
    durable_toolset = ResonateMCPToolset(toolset)
    run_context = RunContext(deps=None, model=TestModel(), usage=RunUsage())

    tools = await durable_toolset.get_tools(run_context)
    result = await durable_toolset.call_tool(
        "add", {"a": 1, "b": 2}, run_context, tools["add"]
    )

    assert result == "3"
    assert toolset.calls == ["get_tools", "call_tool:add"]
