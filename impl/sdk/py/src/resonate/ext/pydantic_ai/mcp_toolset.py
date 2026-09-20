"""Durable wrapper for Pydantic AI MCP toolsets.

``ResonateMCPToolset`` turns MCP server communication -- ``get_tools``,
``get_instructions``, and ``call_tool`` -- into ``ctx.run`` leaves, so each
round-trip is checkpointed and served from the journal on replay. Only
serializable ``ToolDefinition``s and tool results cross the durability
boundary; the live ``ToolsetTool`` objects (which hold schema validators) are
rebuilt locally from the checkpointed definitions.

This module imports ``pydantic_ai.mcp`` and must only be imported when the
``mcp`` optional dependency is installed; ``ResonateAgent`` guards the import.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel
from pydantic_ai import WrapperToolset
from pydantic_ai.mcp import MCPToolset, ToolResult
from pydantic_ai.messages import InstructionPart
from pydantic_ai.tools import AgentDepsT, ToolDefinition

from resonate.ext.pydantic_ai.context import workflow_context

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import Self

    from pydantic_ai import AbstractToolset, ToolsetTool
    from pydantic_ai.tools import RunContext

    from resonate.context import Context
    from resonate.retry import RetryPolicy


class ToolDefsEnvelope(BaseModel):
    """Checkpointed result of one MCP ``get_tools`` round-trip."""

    tool_defs: dict[str, ToolDefinition]


class InstructionsEnvelope(BaseModel):
    """Checkpointed result of one MCP ``get_instructions`` round-trip."""

    instructions: str | InstructionPart | list[str | InstructionPart] | None


class ToolResultEnvelope(BaseModel):
    """Checkpointed result of one MCP ``call_tool`` round-trip."""

    result: ToolResult


async def _mcp_get_tools(
    ctx: Context,
    toolset: MCPToolset[Any],
    run_context: RunContext[Any],
) -> ToolDefsEnvelope:
    """Leaf step: list the MCP server's tools and checkpoint their definitions."""
    tools = await toolset.get_tools(run_context)
    return ToolDefsEnvelope(
        tool_defs={name: tool.tool_def for name, tool in tools.items()}
    )


async def _mcp_get_instructions(
    ctx: Context,
    toolset: MCPToolset[Any],
    run_context: RunContext[Any],
) -> InstructionsEnvelope:
    """Leaf step: fetch the MCP server's instructions and checkpoint them.

    Envelope validation coerces any sequence shape into the list form, so the
    wrapped toolset's return passes through unnormalized.
    """
    async with toolset:
        instructions = await toolset.get_instructions(run_context)
    return InstructionsEnvelope.model_validate({"instructions": instructions})


async def _mcp_call_tool(
    ctx: Context,
    toolset: MCPToolset[Any],
    name: str,
    tool_args: dict[str, Any],
    run_context: RunContext[Any],
    tool: ToolsetTool[Any],
) -> ToolResultEnvelope:
    """Leaf step: call one MCP tool and checkpoint its result."""
    result = await toolset.call_tool(name, tool_args, run_context, tool)
    return ToolResultEnvelope(result=result)


class ResonateMCPToolset(WrapperToolset[AgentDepsT]):
    """A wrapper for ``MCPToolset`` that checkpoints MCP I/O as Resonate leaf steps.

    Outside a workflow every method falls through to the wrapped toolset, so
    the agent remains usable as normal. Tool definitions are cached per run
    (on the run context) when the wrapped toolset's ``cache_tools`` is set,
    avoiding redundant MCP round-trips without shifting checkpoint order on
    recovery.
    """

    def __init__(
        self,
        wrapped: MCPToolset[AgentDepsT],
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        super().__init__(wrapped)
        self._retry_policy = retry_policy

    @property
    def _toolset(self) -> MCPToolset[AgentDepsT]:
        assert isinstance(self.wrapped, MCPToolset)
        # `isinstance` erases the deps type parameter; restore it.
        return cast("MCPToolset[AgentDepsT]", self.wrapped)

    @property
    def id(self) -> str | None:
        return self.wrapped.id

    async def __aenter__(self) -> Self:
        # The wrapped MCP toolset enters itself around listing and calling tools,
        # inside the leaf steps, so there is nothing to enter at the workflow level.
        return self

    async def __aexit__(self, *args: object) -> bool | None:
        return None

    def visit_and_replace(
        self,
        visitor: Callable[[AbstractToolset[AgentDepsT]], AbstractToolset[AgentDepsT]],
    ) -> AbstractToolset[AgentDepsT]:
        # Durable toolsets cannot be swapped out after the fact.
        return self

    async def get_tools(
        self, ctx: RunContext[AgentDepsT]
    ) -> dict[str, ToolsetTool[AgentDepsT]]:
        wctx = workflow_context()
        if wctx is None:
            return await super().get_tools(ctx)

        # The cache lives on the run context, recreated per run and rebuilt
        # identically on recovery, so whether the leaf runs depends only on the
        # workflow's own progress -- never on what earlier runs warmed.
        cache_key = self.id or ""
        cached = (
            ctx._mcp_tool_defs_cache.get(cache_key)  # noqa: SLF001
            if self._toolset.cache_tools
            else None
        )
        if cached is None:
            envelope = await wctx.options(retry_policy=self._retry_policy).run(
                _mcp_get_tools, self._toolset, ctx
            )
            cached = envelope.tool_defs
            if self._toolset.cache_tools:
                ctx._mcp_tool_defs_cache[cache_key] = cached  # noqa: SLF001
        return {
            name: self._toolset.tool_for_tool_def(tool_def)
            for name, tool_def in cached.items()
        }

    async def get_instructions(
        self, ctx: RunContext[AgentDepsT]
    ) -> str | InstructionPart | Sequence[str | InstructionPart] | None:
        wctx = workflow_context()
        if wctx is None:
            return await super().get_instructions(ctx)

        # Try locally first (fast path: returns None when disabled, or the cached
        # instructions when the server is already initialized in this process).
        result = await super().get_instructions(ctx)
        if result is not None:
            return result
        if self._toolset.include_instructions:
            envelope = await wctx.options(retry_policy=self._retry_policy).run(
                _mcp_get_instructions, self._toolset, ctx
            )
            return envelope.instructions
        return None

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
    ) -> ToolResult:
        wctx = workflow_context()
        if wctx is None:
            return await super().call_tool(name, tool_args, ctx, tool)

        envelope = await wctx.options(retry_policy=self._retry_policy).run(
            _mcp_call_tool, self._toolset, name, tool_args, ctx, tool
        )
        return envelope.result
