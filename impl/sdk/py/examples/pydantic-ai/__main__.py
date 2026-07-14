"""pydantic-ai shows durable execution for Pydantic AI agents.

``ResonateAgent`` wraps any Pydantic AI agent so that ``run()`` executes as a
durable workflow: every model request (and MCP round-trip) is checkpointed in
a durable promise, function tools run inline in the workflow body, and a
crashed run replays from the journal instead of re-hitting the provider.

This example builds one agent -- a customer-support triage agent with
dependency-injected tools and a structured Pydantic output -- and uses it to
showcase, all offline:

1. Function tools + dependency injection reaching those tools.
2. Parallel tool calls checkpointed in deterministic order.
3. A structured ``BaseModel`` output rebuilt as the model across the boundary.
4. ``id``-based dedup: a second run with the same id is served from the
   journal, the model is *not* invoked again (proven by a request counter).
5. Crash recovery on a *fresh* client: a brand-new ``Resonate`` process
   pointed at the same server recovers the finished result by id, without
   recomputing -- recovery is by promise identity, not process identity.
6. A multi-turn conversation, threading ``message_history`` and composing a
   per-turn run id from the ``conversation_id``.
7. The wrapped agent staying usable as a normal agent outside any workflow.
8. A tool reaching the ambient durable ``Context`` (``workflow_context()``) to
   use Resonate primitives directly: ``ctx.sleep`` (durable timer),
   ``ctx.promise`` (human-in-the-loop approval), and ``ctx.rpc`` (dispatch to
   another durable function). All three are checkpointed, so a crash mid-tool
   replays from the journal instead of repeating the wait, the approval, or
   the dispatched work.

Requires the ``pydantic-ai`` extra::

    uv pip install -e '.[pydantic-ai]'

Uses Pydantic AI's offline ``FunctionModel`` (a scripted, deterministic model)
so no API key is needed; swap in any real model (e.g. ``'openai:gpt-5.2'``) at
agent construction for the real thing.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/pydantic-ai
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel

from resonate.ext.pydantic_ai import ResonateAgent
from resonate.ext.pydantic_ai.context import workflow_context
from resonate.resonate import Resonate
from resonate.retry import Exponential
from resonate.types import Value

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models.function import AgentInfo

    from resonate.context import Context


class Triage(BaseModel):
    """Structured result of a support triage -- rebuilt as this model, not a dict."""

    order_id: str
    status: str
    action: str


# A request counter that lives in *this* process. It only increments when the
# scripted model actually runs, so it stays flat whenever a run is served from
# the journal (dedup or crash recovery) rather than re-executed.
MODEL_CALLS = 0


def support_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Act as a scripted, deterministic stand-in for a real LLM.

    First request in a run: fan out two tool calls at once (durable execution
    checkpoints them in a deterministic order). Once tool results are in the
    history: emit the final structured output via the agent's output tool.
    """
    global MODEL_CALLS  # noqa: PLW0603
    MODEL_CALLS += 1

    have_tool_results = any(
        isinstance(part, ToolReturnPart)
        for message in messages
        for part in message.parts
    )
    if not have_tool_results:
        return ModelResponse(
            parts=[
                ToolCallPart(tool_name="lookup_order", args={"order_id": "A-1"}),
                ToolCallPart(tool_name="check_inventory", args={"sku": "sku-9"}),
            ]
        )

    output_tool = info.output_tools[0]
    return ModelResponse(
        parts=[
            ToolCallPart(
                tool_name=output_tool.name,
                args={
                    "order_id": "A-1",
                    "status": "shipped",
                    "action": "notify customer of tracking number",
                },
            )
        ]
    )


def build_agent() -> Agent[dict, str]:
    """Build a support agent with dependency-injected tools and structured output.

    ``deps_type=dict`` because deps cross the durability boundary as JSON: they
    are rebuilt from the run's serialized parameters on recovery, so a plain
    dict round-trips faithfully where an arbitrary class would not.
    """
    agent = Agent(
        FunctionModel(support_model),
        name="support-triage",
        instructions="You triage customer support tickets about orders.",
        output_type=Triage,
        deps_type=dict,
    )

    @agent.tool
    def lookup_order(ctx: RunContext[dict], order_id: str) -> str:
        # Runs inline in the workflow body; deps arrive from the durable params.
        return ctx.deps["orders"].get(order_id, "unknown")

    @agent.tool_plain
    def check_inventory(sku: str) -> str:
        return "in stock"

    return agent


# -- A tool that drives Resonate primitives directly -----------------------
#
# A refund agent whose one tool reaches the ambient durable Context and uses
# ctx.sleep + ctx.promise + ctx.rpc. Function tools run inline in the workflow
# body, so `workflow_context()` hands the tool the live Context.


class ReviewerInbox:
    """Signal channel telling a waiting reviewer which promise id to resolve.

    Injected via ``Resonate.with_dependency`` and read inside a leaf. A real
    system would publish the id to Slack/email/a dashboard instead.
    """

    def __init__(self) -> None:
        self.approval_id: asyncio.Future[str] = (
            asyncio.get_running_loop().create_future()
        )

    def publish(self, approval_id: str) -> None:
        if not self.approval_id.done():
            self.approval_id.set_result(approval_id)


async def process_refund(ctx: Context, order_id: str, amount: int) -> str:
    """Settle a refund; a tool dispatches to this via ``ctx.rpc``.

    In a real system this could run on a different worker group (a payments
    service); here it just settles the refund.
    """
    return f"refunded ${amount} for {order_id}"


async def publish_approval(ctx: Context, approval_id: str) -> str:
    """Leaf: publish the approval promise id exactly once (survives replay)."""
    ctx.get_dependency(ReviewerInbox).publish(approval_id)
    return approval_id


def refund_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Script the refund agent: call ``issue_refund`` once, then summarize."""
    returned = {
        part.tool_name
        for message in messages
        for part in message.parts
        if isinstance(part, ToolReturnPart)
    }
    if "issue_refund" not in returned:
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="issue_refund", args={"order_id": "A-1", "amount": 42}
                )
            ]
        )
    return ModelResponse(parts=[TextPart(content="refund handled")])


def build_refund_agent() -> Agent[None, str]:
    """Build an agent whose tool uses ctx.sleep, ctx.promise, and ctx.rpc."""
    agent = Agent(FunctionModel(refund_model), name="refund")

    @agent.tool_plain
    async def issue_refund(order_id: str, amount: int) -> str:
        ctx = workflow_context()
        assert ctx is not None, "the tool runs inside the durable workflow body"

        # Durable timer: a cooldown that is checkpointed, so a crash after it
        # does not restart the wait on replay.
        await ctx.sleep(timedelta(seconds=1))

        # Human-in-the-loop: open a durable promise, publish its id from a leaf
        # (once), then suspend until an external reviewer resolves it. The
        # worker holds no state while suspended -- this can be seconds or days.
        approval = ctx.promise()
        approval_id = await approval.id()
        await ctx.run(publish_approval, approval_id)
        decision = await approval
        if not decision["approved"]:
            return f"refund for {order_id} denied by reviewer"

        # Dispatch the actual refund to another durable function as its own
        # checkpointed child (could run on a different worker).
        return await ctx.rpc(process_refund, order_id, amount)

    return agent


async def simulate_reviewer(resonate: Resonate, inbox: ReviewerInbox) -> None:
    """Stand in for the external party that eventually resolves the approval."""
    approval_id = await inbox.approval_id
    await resonate.promises.resolve(approval_id, Value(data={"approved": True}))
    print(f"[reviewer] approved & resolved promise {approval_id}")


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    resonate = Resonate(url=url)

    # Retry model steps with backoff instead of relying on provider-level
    # retries -- Resonate owns the durable retry of each checkpointed step.
    durable_agent = ResonateAgent(
        build_agent(),
        resonate,
        model_retry_policy=Exponential(delay=1, factor=2, max_delay=30, max_retries=3),
    )
    deps = {"orders": {"A-1": "shipped"}}

    try:
        run_id = f"triage-A-1-{time.time_ns()}"

        # 1. A durable run: parallel tool calls + deps + structured output.
        print(f"[run] starting durable run id={run_id}")
        result = await durable_agent.run("Triage order A-1", id=run_id, deps=deps)
        assert isinstance(result.output, Triage)  # rebuilt as the model
        print(f"[run] output: {result.output!r}")
        print(f"[run] model calls so far: {MODEL_CALLS}")

        # 2. Same id -> same durable promise. The result is served from the
        # journal; the workflow body (and so the model) does not run again.
        before = MODEL_CALLS
        again = await durable_agent.run("Triage order A-1", id=run_id, deps=deps)
        assert again.output == result.output
        assert before == MODEL_CALLS, "dedup should not re-invoke the model"
        print(f"[dedup] same id served from journal; model calls still {MODEL_CALLS}")

        # 3. Crash recovery on a *fresh* client: a brand-new Resonate process
        # pointed at the same server recovers the finished result by id.
        # Recovery is by promise identity, not process identity.
        recovered_resonate = Resonate(url=url)
        try:
            recovered_agent = ResonateAgent(build_agent(), recovered_resonate)
            before = MODEL_CALLS
            recovered = await recovered_agent.run(
                "Triage order A-1", id=run_id, deps=deps
            )
            assert recovered.output == result.output
            assert before == MODEL_CALLS, "recovery should not re-invoke the model"
            print("[recover] new client recovered the result without recomputing")
        finally:
            await recovered_resonate.stop()

        # 4. A multi-turn conversation: thread `message_history` and derive a
        # stable per-turn run id from the conversation id (a conversation spans
        # many runs, so the run id must differ per turn).
        conversation_id = f"ticket-A-1-{time.time_ns()}"
        first = await durable_agent.run(
            "Open the ticket for order A-1",
            id=f"{conversation_id}:1",
            conversation_id=conversation_id,
            deps=deps,
        )
        follow_up = await durable_agent.run(
            "What should we tell the customer?",
            id=f"{conversation_id}:2",
            conversation_id=conversation_id,
            message_history=first.all_messages(),
            deps=deps,
        )
        print(f"[convo] turn 2 output: {follow_up.output!r}")

        # 5. Outside a durable run the wrapped agent stays usable as normal.
        direct = build_agent()
        direct_result = await direct.run("Triage order A-1", deps=deps)
        print(f"[direct] output: {direct_result.output!r}")

        # 6. A tool driving Resonate primitives directly: ctx.sleep (durable
        # cooldown) + ctx.promise (human approval) + ctx.rpc (dispatch the
        # refund to another durable function). A background reviewer stands in
        # for the external party that resolves the approval promise.
        inbox = ReviewerInbox()
        resonate.with_dependency(inbox)
        resonate.register(process_refund, name="process_refund", version=1)
        refund_agent = ResonateAgent(build_refund_agent(), resonate)
        reviewer = asyncio.create_task(simulate_reviewer(resonate, inbox))
        refund = await refund_agent.run(
            "Issue a refund for order A-1", id=f"refund-A-1-{time.time_ns()}"
        )
        await reviewer
        refund_result = next(
            part.content
            for message in refund.all_messages()
            for part in message.parts
            if isinstance(part, ToolReturnPart)
        )
        print(f"[refund] tool result via ctx.rpc: {refund_result!r}")
    finally:
        await resonate.stop()


if __name__ == "__main__":
    asyncio.run(main())
