"""Serializable envelopes for the Pydantic AI durability boundary.

Pydantic AI's message types (``ModelResponse``, ``ModelMessage``, ...) are
plain dataclasses serialized by Pydantic ``TypeAdapter``s, not ``BaseModel``s,
so the Resonate codec's Pydantic hooks don't fire for them directly and
msgspec cannot reshape their untagged dataclass unions on its own. Every value
that crosses a durability boundary is therefore wrapped in a small
``BaseModel`` envelope: the codec's ``enc_hook`` serializes the whole tree via
``model_dump(mode="json")`` and its ``dec_hook`` rebuilds it via
``model_validate``, running the same Pydantic (de)serialization on the live
and the recovery path alike.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, TypeAdapter, create_model
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    UserContent,
)
from pydantic_ai.tools import DeferredToolResults
from pydantic_ai.usage import RunUsage, UsageLimits


class ModelResponseEnvelope(BaseModel):
    """Checkpointed result of one model request (or fully-consumed stream)."""

    response: ModelResponse


class RunParams(BaseModel):
    """The serializable arguments of one durable agent run.

    These are the workflow's root promise ``param``: on crash recovery a
    worker re-invokes the workflow from exactly this payload, so only values
    that survive a JSON round-trip belong here. Run-time arguments that
    cannot (callables, per-run models/toolsets/output types) are rejected by
    ``ResonateAgent.run`` before the workflow is dispatched.
    """

    user_prompt: str | list[UserContent] | None = None
    message_history: list[ModelMessage] | None = None
    deferred_tool_results: DeferredToolResults | None = None
    conversation_id: str | None = None
    instructions: str | list[str] | None = None
    deps: Any = None
    model_settings: dict[str, Any] | None = None
    usage_limits: UsageLimits | None = None
    usage: RunUsage | None = None
    metadata: dict[str, Any] | None = None
    retries: int | dict[str, int] | None = None


class BaseRunResult(BaseModel):
    """The serializable outcome of one durable agent run, minus the output.

    The ``output`` field is re-declared per agent by :func:`run_result_model`,
    typed with the agent's ``output_type`` whenever Pydantic can validate it,
    so the output is rebuilt by the same annotation-driven coercion that
    rebuilds every other value at the durability boundary.
    """

    output: Any
    all_messages: list[ModelMessage]
    new_message_index: int
    output_tool_name: str | None
    usage: RunUsage
    run_id: str
    conversation_id: str


def run_result_model(output_type: Any) -> type[BaseRunResult]:
    """Build the run-result envelope for an agent's ``output_type``.

    When ``output_type`` is something Pydantic can generate a schema for (a
    plain type, a ``BaseModel``, a dataclass, a union, ...), the envelope's
    ``output`` field is typed with it and the output round-trips faithfully.
    Output specs Pydantic cannot validate (marker objects like ``ToolOutput``,
    output functions, lists of specs) fall back to ``Any``: the output is then
    delivered as decoded JSON builtins, which is lossless for ``str`` and
    JSON-native outputs but does not rebuild custom classes.
    """
    annotation: Any
    try:
        TypeAdapter(output_type)
    except Exception:
        annotation = Any
    else:
        annotation = output_type
    return create_model("RunResult", __base__=BaseRunResult, output=(annotation, ...))
