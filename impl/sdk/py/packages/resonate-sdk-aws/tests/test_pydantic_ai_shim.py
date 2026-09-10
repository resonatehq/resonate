"""The FaaS shim as a ``DurableRegistry`` for :mod:`resonate.ext.pydantic_ai`.

The shim registers -- and so executes -- an agent's durable run function, but
cannot dispatch new runs; that remains a full-client capability. Requires the
core SDK's ``pydantic-ai`` extra, installed workspace-wide in CI.
"""

from __future__ import annotations

import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import UserError
from pydantic_ai.models.test import TestModel
from resonate_aws import Resonate

from resonate.ext.pydantic_ai import ResonateAgent


@pytest.mark.asyncio
async def test_worker_shim_registers_but_cannot_dispatch() -> None:
    # A serverless worker shim satisfies `DurableRegistry`: constructing the
    # agent registers its durable run function, so pushed tasks can execute it.
    shim = Resonate()
    durable = ResonateAgent(Agent(TestModel(), name="worker-only"), shim)
    assert shim._registry.get("worker-only.run", 1) is not None

    # Dispatching a new run, however, requires the full client.
    with pytest.raises(UserError, match="but not dispatch"):
        await durable.run("hi")
