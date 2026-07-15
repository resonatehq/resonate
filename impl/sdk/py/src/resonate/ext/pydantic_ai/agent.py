"""Durable wrapper for Pydantic AI agents.

``ResonateAgent`` wraps any Pydantic AI agent and registers its run loop as a
Resonate durable function. ``run()`` then executes as a durable workflow:
model requests (and MCP communication) are checkpointed as ``ctx.run`` leaves,
the workflow body itself is deterministic and replayable, and a crashed run
resumes from its last completed step.

Function tools run inline in the workflow body -- they are re-executed on
replay, not checkpointed. Tool calls are executed in ``'parallel_ordered_events'`` mode by
default so checkpoint order stays deterministic across replays.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast, overload
from uuid import uuid4

from pydantic_ai import _utils
from pydantic_ai._agent_graph import GraphAgentState, resolve_conversation_id
from pydantic_ai.agent import WrapperAgent
from pydantic_ai.durable_exec._runtime_toolsets import (
    reject_unsupported_runtime_toolsets,
)
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT
from pydantic_ai.run import AgentRunResult
from pydantic_ai.tools import AgentDepsT

from resonate.ext.pydantic_ai.context import (
    reset_workflow_context,
    set_workflow_context,
    workflow_context,
)
from resonate.ext.pydantic_ai.model import ResonateModel
from resonate.ext.pydantic_ai.types import BaseRunResult, RunParams, run_result_model
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator
    from contextlib import AbstractAsyncContextManager
    from datetime import timedelta

    from pydantic_ai import (
        AbstractToolset,
        _instructions,
        messages as _messages,
        models,
        usage as _usage,
    )
    from pydantic_ai.agent import (
        AbstractAgent,
        AgentRun,
        EventStreamHandler,
        ParallelExecutionMode,
    )
    from pydantic_ai.agent.abstract import (
        AgentMetadata,
        AgentModelSettings,
        AgentRetries,
        RunOutputDataT,
    )
    from pydantic_ai.agent.spec import AgentSpec
    from pydantic_ai.capabilities import AgentCapability
    from pydantic_ai.output import OutputSpec
    from pydantic_ai.result import StreamedRunResult
    from pydantic_ai.tools import (
        AgentNativeTool,
        DeferredToolResults,
        RunContext,
        Tool,
        ToolFuncEither,
    )

    from resonate.context import Context
    from resonate.retry import RetryPolicy
    from resonate.types import DurableRegistry

# ``None`` isn't assignable to an arbitrary ``AgentDepsT``, so mirror Pydantic
# AI's ``deps=None`` default through an ``Any`` singleton -- keeps the runtime
# default ``None`` while satisfying the type checker (and avoiding B008's ban on
# calls in argument defaults).
_NO_DEPS: Any = None

ResonateParallelExecutionMode = Literal["sequential", "parallel_ordered_events"]
"""Tool-call execution modes usable inside a durable Resonate workflow.

A subset of Pydantic AI's ``ParallelExecutionMode``: plain ``'parallel'``
cannot guarantee deterministic checkpoint ordering across replays.
"""


class ResonateAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    """Wraps an agent so its runs execute as durable Resonate workflows.

    Model requests, and MCP server communication, are checkpointed as durable
    steps; the run loop replays deterministically after a crash. Outside a
    workflow the wrapped agent remains usable as normal.

    Every value that crosses the durability boundary must survive a JSON
    round-trip, so run-time arguments that cannot be serialized (``model``,
    ``toolsets``, ``output_type``, callables) must be configured on the agent
    at construction time instead.
    """

    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        resonate: DurableRegistry,
        *,
        name: str | None = None,
        version: int = 1,
        run_timeout: timedelta | None = None,
        model_retry_policy: RetryPolicy | None = None,
        mcp_retry_policy: RetryPolicy | None = None,
        parallel_execution_mode: ResonateParallelExecutionMode = "parallel_ordered_events",
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
    ) -> None:
        """Wrap ``wrapped`` and register its run loop as a durable function.

        Args:
            wrapped: The agent to wrap.
            resonate: The Resonate instance to register the durable run
                function on and to dispatch runs through. A serverless worker
                shim (e.g. ``resonate_aws.Resonate``) is also accepted:
                it registers -- and so executes -- the durable run function,
                but cannot dispatch new runs, so ``run()`` outside a workflow
                requires the full ``resonate.resonate.Resonate`` client.
            name: Unique agent name used to register the durable function.
                Defaults to the wrapped agent's ``name``, which is then
                required.
            version: Resonate registry version for the durable run function,
                for rolling-deploy versioning.
            run_timeout: Timeout for the whole durable run (the root durable
                promise). Defaults to Resonate's top-level default (1 day).
            model_retry_policy: Retry policy for model request steps.
                Defaults to the Resonate instance's default policy.
            mcp_retry_policy: Retry policy for MCP communication steps.
                Defaults to the Resonate instance's default policy.
            parallel_execution_mode: How tool calls within a run step execute:
                ``'parallel_ordered_events'`` (default) runs them in parallel
                with deterministic event ordering; ``'sequential'`` runs them
                one at a time.
            event_stream_handler: Optional event stream handler to use instead
                of the one set on the wrapped agent. Inside a workflow it
                receives model events from within the model request step (live
                streaming, not re-invoked on replay) and the remaining run
                events from the workflow body.

        """
        super().__init__(wrapped)

        self._name = name or wrapped.name
        if self._name is None:
            msg = (
                "An agent needs to have a unique `name` in order to be used with Resonate. "
                "The name is used to register the agent's durable run function."
            )
            raise UserError(msg)

        if not isinstance(wrapped.model, Model):
            msg = (
                "An agent needs to have a `model` in order to be used with Resonate, "
                "it cannot be set at agent run time."
            )
            raise UserError(msg)

        self._resonate = resonate
        self._version = version
        self._run_timeout = run_timeout
        self._event_stream_handler = event_stream_handler
        self._run_event_stream_handler: ContextVar[
            EventStreamHandler[AgentDepsT] | None
        ] = ContextVar("resonate_pydantic_ai_run_event_stream_handler", default=None)
        self._parallel_execution_mode = cast(
            "ParallelExecutionMode", parallel_execution_mode
        )

        self._model = ResonateModel(
            wrapped.model,
            retry_policy=model_retry_policy,
            get_event_stream_handler=self._effective_event_stream_handler,
        )

        def resonatify(
            toolset: AbstractToolset[AgentDepsT],
        ) -> AbstractToolset[AgentDepsT]:
            # Replace `MCPToolset` with its Resonate-wrapped variant.
            try:
                # Deferred: `pydantic_ai.mcp` (and so our wrapper for it) needs the
                # optional `mcp` dependency.
                from pydantic_ai.mcp import MCPToolset  # noqa: PLC0415

                from resonate.ext.pydantic_ai.mcp_toolset import (  # noqa: PLC0415
                    ResonateMCPToolset,
                )
            except ImportError:
                pass
            else:
                if isinstance(toolset, MCPToolset):
                    # `isinstance` erases the toolset's deps type parameter, so
                    # restore it for the wrapper.
                    return ResonateMCPToolset(
                        cast("MCPToolset[AgentDepsT]", toolset),
                        retry_policy=mcp_retry_policy,
                    )
            return toolset

        self._toolsets = [
            toolset.visit_and_replace(resonatify) for toolset in wrapped.toolsets
        ]

        # The run-result envelope, typed with the agent's `output_type` whenever
        # Pydantic can validate it, so the output is rebuilt by the same
        # annotation-driven coercion at every durability boundary.
        self._run_result_model = run_result_model(wrapped.output_type)

        async def run_workflow(ctx: Context, params: RunParams) -> BaseRunResult:
            """Execute the wrapped agent's run loop as the durable workflow body."""
            token = set_workflow_context(ctx)
            try:
                with self._resonate_overrides():
                    result = await super(WrapperAgent, self).run(
                        params.user_prompt,
                        message_history=params.message_history,
                        deferred_tool_results=params.deferred_tool_results,
                        conversation_id=params.conversation_id,
                        instructions=params.instructions,
                        deps=cast("AgentDepsT", params.deps),
                        model_settings=cast("Any", params.model_settings),
                        usage_limits=params.usage_limits,
                        usage=params.usage,
                        metadata=params.metadata,
                        retries=cast("Any", params.retries),
                        infer_name=False,
                    )
            finally:
                reset_workflow_context(token)

            state = result._state  # noqa: SLF001 - AgentRunResult keeps its state private
            return self._run_result_model(
                output=result.output,
                all_messages=result.all_messages(),
                new_message_index=result._new_message_index,  # noqa: SLF001
                output_tool_name=result._output_tool_name,  # noqa: SLF001
                usage=result.usage,
                run_id=state.run_id,
                conversation_id=state.conversation_id,
            )

        # The registered return annotation is the agent-specific envelope, so the
        # settled workflow value is coerced back through it -- live and on recovery.
        run_workflow.__annotations__["return"] = self._run_result_model
        self._run_workflow = resonate.register(
            run_workflow, name=f"{self._name}.run", version=version
        )

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, value: str | None) -> NoReturn:
        msg = "The agent name cannot be changed after creation. If you need to change the name, create a new agent."
        raise UserError(msg)

    @property
    def model(self) -> Model:
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        with self._resonate_overrides():
            return super().toolsets

    @property
    def event_stream_handler(self) -> EventStreamHandler[AgentDepsT] | None:
        handler = self._effective_event_stream_handler()
        if handler is None:
            return None
        if workflow_context() is not None:
            # Inside a workflow, model events are streamed to the handler within the
            # model request step; the remaining (already-complete) run events arrive
            # here and are forwarded one at a time.
            return self._call_event_stream_handler_in_workflow
        return handler

    def _effective_event_stream_handler(self) -> EventStreamHandler[AgentDepsT] | None:
        # The per-run handler (stashed on the `ContextVar` by `run`) takes precedence
        # over the constructor-level handler and the wrapped agent's handler.
        return (
            self._run_event_stream_handler.get()
            or self._event_stream_handler
            or super().event_stream_handler
        )

    async def _call_event_stream_handler_in_workflow(
        self, ctx: RunContext[AgentDepsT], stream: Any
    ) -> None:
        handler = self._effective_event_stream_handler()
        assert handler is not None

        async def single_event(event: _messages.AgentStreamEvent) -> Any:
            yield event

        async for event in stream:
            await handler(ctx, single_event(event))

    @contextmanager
    def _resonate_overrides(
        self,
        *,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
    ) -> Generator[None]:
        """Route the wrapped agent through the durable model and toolsets.

        Uses the configured parallel execution mode so checkpoint order stays
        deterministic during replay. A per-run ``event_stream_handler`` is
        stashed on a ``ContextVar`` that ``ResonateModel`` reads inside its
        step; when none is given, whatever an outer call already stashed is
        kept.
        """
        token = self._run_event_stream_handler.set(
            event_stream_handler or self._run_event_stream_handler.get()
        )
        try:
            with (
                super().override(model=self._model, toolsets=self._toolsets, tools=[]),
                self.parallel_tool_call_execution_mode(self._parallel_execution_mode),
            ):
                yield
        finally:
            self._run_event_stream_handler.reset(token)

    @overload
    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        id: str | None = None,
        output_type: None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AgentRunResult[OutputDataT]: ...

    @overload
    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        id: str | None = None,
        output_type: OutputSpec[RunOutputDataT],
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AgentRunResult[RunOutputDataT]: ...

    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        id: str | None = None,
        output_type: OutputSpec[RunOutputDataT] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AgentRunResult[Any]:
        """Run the agent as a durable Resonate workflow.

        Args:
            user_prompt: User input to start/continue the conversation.
            id: Stable execution id for the durable run, used for deduplication
                and recovery. Two runs with the same id resolve to the same
                durable promise (the second call returns the first run's
                result), which is what lets you dedupe a re-submission and
                reattach to -- and recover the result of -- a run after a client
                restart. Defaults to a fresh unique id per run. It is
                deliberately not derived from
                ``conversation_id``: a conversation spans many runs, so reusing
                it would make a follow-up turn resolve to the first turn's
                promise and replay its result. Compose one when you want the
                link, e.g. ``id=f'{conversation_id}:{turn}'``.
            output_type: Custom output type for this run. Only supported when
                running inline inside an active workflow; a durable run's
                output type must be set on the agent at construction time.
            message_history: History of the conversation so far.
            deferred_tool_results: Optional results for deferred tool calls in
                the message history.
            conversation_id: ID of the conversation this run belongs to.
            model: Not supported at run time with Resonate; set the model when
                constructing the agent.
            instructions: Optional additional instructions for this run.
                Callables are only supported inline inside an active workflow.
            deps: Optional dependencies for this run. Must survive a JSON
                round-trip to be rebuilt identically on crash recovery.
            model_settings: Optional settings for the model's requests.
                Callables are only supported inline inside an active workflow.
            usage_limits: Optional limits on model request count or token usage.
            usage: Optional usage to start with.
            metadata: Optional metadata dict to attach to this run. Callables
                are only supported inline inside an active workflow.
            retries: Override the agent-level retry budgets for this run.
            infer_name: Ignored; a Resonate agent's name is fixed at
                construction time.
            toolsets: Not supported at run time with Resonate; register
                toolsets when constructing the agent.
            event_stream_handler: Optional event stream handler for this run.
                Honored on the live path; a crash-recovery replay falls back to
                the handler configured at construction time.
            capabilities: Only supported inline inside an active workflow.
            spec: Only supported inline inside an active workflow.

        Returns:
            The result of the run.

        """
        if workflow_context() is not None:
            # Nested inside an active workflow (e.g. an agent used as a tool of
            # another `ResonateAgent`): run inline, checkpointing model requests
            # into the outer workflow. No serialization boundary applies here.
            if model is not None and not isinstance(model, ResonateModel):
                msg = (
                    "A non-Resonate model cannot be set at agent run time inside a Resonate workflow, "
                    "it must be set at agent creation time."
                )
                raise UserError(msg)
            self._reject_unsupported_runtime_toolsets(toolsets)
            with self._resonate_overrides(event_stream_handler=event_stream_handler):
                return await super(WrapperAgent, self).run(
                    user_prompt,
                    output_type=output_type,
                    message_history=message_history,
                    deferred_tool_results=deferred_tool_results,
                    conversation_id=conversation_id,
                    model=model,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    retries=retries,
                    infer_name=False,
                    toolsets=toolsets,
                    capabilities=capabilities,
                    spec=spec,
                )

        # Dispatching a new durable run requires the full client. A serverless
        # worker shim satisfies construction (it registers, and so executes,
        # the run function) but cannot *start* runs -- there the run is invoked
        # by name from a client, and this process only executes pushed tasks.
        # Deferred import: the worker path never needs the full client module.

        resonate = self._resonate
        if not isinstance(resonate, Resonate):
            msg = (
                "This agent's Resonate instance can register and execute durable runs "
                "but not dispatch them (a serverless worker shim). Invoke the registered "
                f"durable function '{self._name}.run' from a client using the full "
                "`resonate.resonate.Resonate`, or call `agent.run()` inside an active workflow."
            )
            raise UserError(msg)

        self._reject_unserializable_run_arguments(
            output_type=output_type,
            model=model,
            toolsets=toolsets,
            capabilities=capabilities,
            spec=spec,
            instructions=instructions,
            model_settings=model_settings,
            metadata=metadata,
        )

        params = RunParams(
            user_prompt=user_prompt
            if isinstance(user_prompt, (str, type(None)))
            else list(user_prompt),
            message_history=list(message_history)
            if message_history is not None
            else None,
            deferred_tool_results=deferred_tool_results,
            # Resolve the conversation id once, here on the dispatch path, and
            # pin the concrete value into the durable params. The run loop's own
            # resolution (explicit arg -> `message_history` -> fresh UUID7) would
            # otherwise run inside the replayable workflow body and mint a *new*
            # id on every crash-recovery replay; pinning it keeps a run's
            # `conversation_id` stable across recovery. (`run_id` has no such
            # entry point on `Agent.run`, so it still drifts on replay.)
            conversation_id=resolve_conversation_id(conversation_id, message_history),
            # Validated non-callable above; the envelope revalidates the shape.
            instructions=cast("Any", instructions),
            deps=deps,
            model_settings=cast("Any", model_settings),
            usage_limits=usage_limits,
            usage=usage,
            metadata=cast("Any", metadata),
            retries=cast("Any", retries),
        )

        # An explicit `id` is the run's durable identity: two runs with the same
        # id resolve to the same promise, so pass one to dedupe a re-submission
        # or to reattach to (and recover) a run after a client restart. When
        # omitted we mint a fresh unique id per run. `conversation_id` is
        # deliberately *not*
        # used as the fallback: it spans many runs, so a follow-up turn would
        # collide with the first turn's promise and replay its result.
        run_id = id or f"{self._name}.run.{uuid4()}"

        if self._run_timeout is not None:
            resonate = resonate.options(timeout=self._run_timeout)

        # Stash the per-run handler before dispatch: the workflow body executes in a
        # task spawned from this context, so the `ContextVar` propagates into it on
        # the live path. (On crash recovery the workflow re-executes without it and
        # falls back to the constructor-level / wrapped agent's handler.)
        token = self._run_event_stream_handler.set(
            event_stream_handler or self._run_event_stream_handler.get()
        )
        try:
            handle = resonate.run(run_id, self._run_workflow, params)
            # Surface creation/serialization failures instead of waiting forever.
            await handle.id()
        finally:
            self._run_event_stream_handler.reset(token)

        envelope = await handle.result()

        return AgentRunResult(
            output=envelope.output,
            _output_tool_name=envelope.output_tool_name,
            _state=GraphAgentState(
                message_history=list(envelope.all_messages),
                usage=envelope.usage,
                run_id=envelope.run_id,
                conversation_id=envelope.conversation_id,
            ),
            _new_message_index=envelope.new_message_index,
        )

    def _reject_unsupported_runtime_toolsets(
        self, toolsets: Sequence[AbstractToolset[AgentDepsT]] | None
    ) -> None:
        # Inline (nested) runs execute function tools in the workflow body, so
        # `FunctionToolset` is allowed at runtime, but MCP servers need their I/O
        # wrapped in steps registered up front, and dynamic toolsets can't be
        # introspected ahead of time.
        reject_unsupported_runtime_toolsets(
            toolsets, unsupported_kinds=frozenset({"mcp", "dynamic"}), engine="Resonate"
        )

    def _reject_unserializable_run_arguments(self, **kwargs: Any) -> None:
        """Reject run-time arguments that cannot cross the durability boundary.

        The durable run's parameters are what a crashed run recovers from, so
        anything that cannot be rebuilt from JSON must be configured on the
        agent at construction time instead.
        """
        for arg in ("model", "output_type", "toolsets", "capabilities", "spec"):
            if kwargs[arg] is not None:
                msg = (
                    f"`{arg}` cannot be set at agent run time with Resonate, because it cannot be "
                    "serialized into the durable run's parameters. Set it when constructing the agent instead."
                )
                raise UserError(msg)
        for arg in ("instructions", "model_settings", "metadata"):
            value = kwargs[arg]
            if callable(value) or (
                isinstance(value, Sequence)
                and not isinstance(value, str)
                and any(callable(item) for item in value)
            ):
                msg = (
                    f"A callable `{arg}` cannot be set at agent run time with Resonate, because it cannot "
                    "be serialized into the durable run's parameters. Set it when constructing the agent instead."
                )
                raise UserError(msg)

    def run_sync(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Not supported: Resonate runs on asyncio, so use ``await agent.run(...)``."""
        msg = (
            "`agent.run_sync()` cannot be used with Resonate, which requires a running asyncio "
            "event loop. Use `await agent.run(...)` instead."
        )
        raise UserError(msg)

    @overload
    def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AbstractAsyncContextManager[StreamedRunResult[AgentDepsT, OutputDataT]]: ...

    @overload
    def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[RunOutputDataT],
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AbstractAsyncContextManager[StreamedRunResult[AgentDepsT, RunOutputDataT]]: ...

    def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[RunOutputDataT] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AbstractAsyncContextManager[StreamedRunResult[AgentDepsT, Any]]:
        """Run the agent with a streamed response, outside a durable workflow.

        Inside a durable workflow this raises ``UserError``: a stream cannot
        cross the durability boundary. Set an ``event_stream_handler`` on the
        agent and use ``agent.run()`` instead.

        Arguments are the same as for ``AbstractAgent.run_stream``.
        """
        if workflow_context() is not None:
            msg = (
                "`agent.run_stream()` cannot be used inside a durable Resonate workflow. "
                "Set an `event_stream_handler` on the agent and use `agent.run()` instead."
            )
            raise UserError(msg)
        return super().run_stream(
            user_prompt,
            output_type=output_type,
            message_history=message_history,
            deferred_tool_results=deferred_tool_results,
            conversation_id=conversation_id,
            model=model,
            instructions=instructions,
            deps=deps,
            model_settings=model_settings,
            usage_limits=usage_limits,
            usage=usage,
            metadata=metadata,
            retries=retries,
            infer_name=infer_name,
            toolsets=toolsets,
            event_stream_handler=event_stream_handler,
            capabilities=capabilities,
            spec=spec,
        )

    def run_stream_events(self, *args: Any, **kwargs: Any) -> NoReturn:
        """Not supported: events cannot stream out of a durable workflow."""
        msg = (
            "`agent.run_stream_events()` cannot be used with Resonate. "
            "Set an `event_stream_handler` on the agent and use `agent.run()` instead."
        )
        raise UserError(msg)

    @overload
    def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AbstractAsyncContextManager[AgentRun[AgentDepsT, OutputDataT]]: ...

    @overload
    def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[RunOutputDataT],
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AbstractAsyncContextManager[AgentRun[AgentDepsT, RunOutputDataT]]: ...

    @asynccontextmanager  # ty: ignore[no-matching-overload] - ty mis-resolves the decorator against the overloads
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[RunOutputDataT] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        conversation_id: str | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: _instructions.AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT = _NO_DEPS,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        retries: int | AgentRetries | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        capabilities: Sequence[AgentCapability[AgentDepsT]] | None = None,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> AsyncGenerator[AgentRun[AgentDepsT, Any]]:
        """Iterate over the agent graph with the durable model and toolsets applied.

        This is what the durable workflow body drives internally; outside a
        workflow the durable wrappers fall through to the wrapped
        implementations, so it behaves like the wrapped agent's ``iter``.

        Arguments are the same as for ``AbstractAgent.iter``.
        """
        if model is not None and not isinstance(model, ResonateModel):
            msg = (
                "A non-Resonate model cannot be set at agent run time inside a Resonate workflow, "
                "it must be set at agent creation time."
            )
            raise UserError(msg)
        self._reject_unsupported_runtime_toolsets(toolsets)
        with self._resonate_overrides():
            async with super().iter(
                user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                conversation_id=conversation_id,
                model=model,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                retries=retries,
                infer_name=infer_name,
                toolsets=toolsets,
                capabilities=capabilities,
                spec=spec,
            ) as agent_run:
                yield agent_run

    @contextmanager
    def override(
        self,
        *,
        name: str | _utils.Unset = _utils.UNSET,
        deps: AgentDepsT | _utils.Unset = _utils.UNSET,
        model: models.Model | models.KnownModelName | str | _utils.Unset = _utils.UNSET,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | _utils.Unset = _utils.UNSET,
        tools: Sequence[Tool[AgentDepsT] | ToolFuncEither[AgentDepsT, ...]]
        | _utils.Unset = _utils.UNSET,
        native_tools: Sequence[AgentNativeTool[AgentDepsT]]
        | _utils.Unset = _utils.UNSET,
        instructions: _instructions.AgentInstructions[AgentDepsT]
        | _utils.Unset = _utils.UNSET,
        model_settings: AgentModelSettings[AgentDepsT] | _utils.Unset = _utils.UNSET,
        retries: int | AgentRetries | _utils.Unset = _utils.UNSET,
        spec: dict[str, Any] | AgentSpec | None = None,
    ) -> Generator[None]:
        """Temporarily override agent configuration; the model must stay durable.

        Arguments are the same as for ``AbstractAgent.override``.
        """
        if _utils.is_set(model) and not isinstance(model, ResonateModel):
            msg = (
                "A non-Resonate model cannot be contextually overridden with Resonate, "
                "it must be set at agent creation time."
            )
            raise UserError(msg)
        with super().override(
            name=name,
            deps=deps,
            model=model,
            toolsets=toolsets,
            tools=tools,
            native_tools=native_tools,
            instructions=instructions,
            model_settings=model_settings,
            retries=retries,
            spec=spec,
        ):
            yield
