"""Ambient access to the active workflow :class:`~resonate.context.Context`.

Resonate passes its ``Context`` explicitly to durable functions rather than
exposing it ambiently, but Pydantic AI's ``Model`` and toolset interfaces have
no slot to thread it through. The workflow entrypoint therefore stashes the
context in a :class:`~contextvars.ContextVar` that :class:`ResonateModel
<resonate.ext.pydantic_ai.ResonateModel>` and :class:`ResonateMCPToolset
<resonate.ext.pydantic_ai.ResonateMCPToolset>` read back. ``ContextVar``
propagates into every ``asyncio`` task spawned under the workflow body, so the
agent loop's internal concurrency (e.g. parallel tool calls) still observes it.

Outside a workflow the variable is unset and the wrappers fall through to the
wrapped implementations, keeping the agent usable as normal.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from contextvars import Token

    from resonate.context import Context

_workflow_context: ContextVar[Context | None] = ContextVar(
    "resonate_pydantic_ai_workflow_context", default=None
)


def workflow_context() -> Context | None:
    """Return the active workflow context, or ``None`` outside a workflow."""
    return _workflow_context.get()


def set_workflow_context(ctx: Context) -> Token[Context | None]:
    """Stash ``ctx`` as the ambient workflow context, returning the reset token."""
    return _workflow_context.set(ctx)


def reset_workflow_context(token: Token[Context | None]) -> None:
    """Restore the ambient workflow context to its previous value."""
    _workflow_context.reset(token)
