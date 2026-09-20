"""Durable execution for Pydantic AI agents on Resonate.

Wrap any Pydantic AI agent in :class:`ResonateAgent` to make its runs
recoverable: the run loop is registered as a Resonate durable function, and
model requests and MCP server communication are checkpointed as durable steps
that replay from the journal after a crash.

Requires the ``pydantic-ai`` extra::

    pip install "resonate-sdk[pydantic-ai]"

Usage::

    from pydantic_ai import Agent
    from resonate import Resonate
    from resonate.ext.pydantic_ai import ResonateAgent

    resonate = Resonate(url="http://localhost:8001")

    agent = Agent(
        "openai:gpt-5.2",
        instructions="You're an expert in geography.",
        name="geography",
    )
    resonate_agent = ResonateAgent(agent, resonate)

    async def main():
        # `id` is the run's durable identity: reuse it to dedupe a re-submission
        # and to recover the result after a crash or client restart.
        result = await resonate_agent.run(
            "What is the capital of Mexico?", id="capital-of-mexico"
        )
        print(result.output)
"""

from __future__ import annotations

from resonate.ext.pydantic_ai.agent import ResonateAgent, ResonateParallelExecutionMode
from resonate.ext.pydantic_ai.model import ResonateModel

__all__ = [
    "ResonateAgent",
    "ResonateModel",
    "ResonateParallelExecutionMode",
]
