"""Durable wrapper for Pydantic AI models.

``ResonateModel`` turns every model request into a ``ctx.run`` leaf: the
request executes once, its ``ModelResponse`` is stored in a durable promise,
and a crash-recovery replay is served from the journal instead of re-hitting
the provider. Leaf arguments are never serialized (a replayed parent re-derives
them), so the wrapped ``Model`` object itself is passed straight through; only
the response envelope crosses the durability boundary.

Streaming is consumed entirely within the leaf: the
``event_stream_handler`` is invoked there against
the live stream, and only the final ``ModelResponse`` is checkpointed. The
workflow-level caller receives a ``CompletedStreamedResponse`` built from that
checkpoint.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from pydantic_ai.models.wrapper import CompletedStreamedResponse, WrapperModel

from resonate.ext.pydantic_ai.context import workflow_context
from resonate.ext.pydantic_ai.types import ModelResponseEnvelope

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from pydantic_ai.agent import EventStreamHandler
    from pydantic_ai.messages import ModelMessage, ModelResponse
    from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
    from pydantic_ai.settings import ModelSettings
    from pydantic_ai.tools import RunContext

    from resonate.context import Context
    from resonate.retry import RetryPolicy


async def _model_request(
    ctx: Context,
    model: Model,
    messages: list[ModelMessage],
    model_settings: ModelSettings | None,
    model_request_parameters: ModelRequestParameters,
) -> ModelResponseEnvelope:
    """Leaf step: execute one model request and checkpoint its response."""
    response = await model.request(messages, model_settings, model_request_parameters)
    return ModelResponseEnvelope(response=response)


async def _model_request_stream(
    ctx: Context,
    model: Model,
    messages: list[ModelMessage],
    model_settings: ModelSettings | None,
    model_request_parameters: ModelRequestParameters,
    run_context: RunContext[Any] | None,
    event_stream_handler: EventStreamHandler[Any] | None,
) -> ModelResponseEnvelope:
    """Leaf step: consume one streamed model request and checkpoint its response.

    The user's ``event_stream_handler`` runs here, against the live stream;
    on replay the leaf is served from the journal and the handler is not
    re-invoked. If the leaf is retried by its retry policy, the handler runs
    again for the new attempt.
    """
    async with model.request_stream(
        messages, model_settings, model_request_parameters, run_context
    ) as streamed_response:
        if event_stream_handler is not None:
            if run_context is None:
                msg = (
                    "A Resonate model cannot be used with `pydantic_ai.direct.model_request_stream()` "
                    "as it requires a `run_context`. Set an `event_stream_handler` on the agent and "
                    "use `agent.run()` instead."
                )
                raise TypeError(msg)
            await event_stream_handler(run_context, streamed_response)

        async for _ in streamed_response:
            pass
    return ModelResponseEnvelope(response=streamed_response.get())


class ResonateModel(WrapperModel):
    """A wrapper for ``Model`` that checkpoints requests as Resonate leaf steps.

    Outside a workflow both ``request`` and ``request_stream`` fall through to
    the wrapped model, so the agent remains usable as normal.
    """

    def __init__(
        self,
        model: Model,
        *,
        retry_policy: RetryPolicy | None,
        get_event_stream_handler: Callable[[], EventStreamHandler[Any] | None],
    ) -> None:
        super().__init__(model)
        self._retry_policy = retry_policy
        # Resolved lazily at request time so a per-run handler (stashed on a
        # `ContextVar` by `ResonateAgent`) is honored without rebuilding the model.
        self._get_event_stream_handler = get_event_stream_handler

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        ctx = workflow_context()
        if ctx is None:
            return await super().request(
                messages, model_settings, model_request_parameters
            )

        envelope = await ctx.options(retry_policy=self._retry_policy).run(
            _model_request,
            self.wrapped,
            messages,
            model_settings,
            model_request_parameters,
        )
        return envelope.response

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncGenerator[StreamedResponse]:
        ctx = workflow_context()
        if ctx is None:
            async with super().request_stream(
                messages, model_settings, model_request_parameters, run_context
            ) as streamed_response:
                yield streamed_response
            return

        envelope = await ctx.options(retry_policy=self._retry_policy).run(
            _model_request_stream,
            self.wrapped,
            messages,
            model_settings,
            model_request_parameters,
            run_context,
            self._get_event_stream_handler(),
        )
        yield CompletedStreamedResponse(model_request_parameters, envelope.response)
