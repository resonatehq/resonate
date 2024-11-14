from __future__ import annotations

from typing import TYPE_CHECKING, final

from opentelemetry import context, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from typing_extensions import assert_never

from resonate.events import (
    ExecutionAwaited,
    ExecutionInvoked,
    ExecutionResumed,
    ExecutionTerminated,
    PromiseCompleted,
    PromiseCreated,
    SchedulerEvents,
)
from resonate.tracing import IAdapter

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer
    from opentelemetry.trace.span import Span


@final
class OpenTelemetryAdapter(IAdapter):
    def __init__(self, app_name: str, endpoint: str) -> None:
        self._spans: dict[str, tuple[Span, object]] = {}
        assert __package__ is not None

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: app_name}))
        exporter = OTLPSpanExporter(endpoint=endpoint)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        self._tracer: Tracer = trace.get_tracer(
            __package__,
        )

    def _get_span(self, id: str) -> Span:
        return self._spans[id][0]

    def process_event(self, event: SchedulerEvents) -> None:
        if isinstance(event, PromiseCreated):
            assert (
                event.id not in self._spans
            ), "There shouldn't be never another span with same name."
            self._create_span(event.id, event.parent_id, start_time=event.tick)
        elif isinstance(event, PromiseCompleted):
            assert (
                event.id in self._spans
            ), "There should always be an span with that name."
            self._close_span(event.id, end_time=event.tick)
        elif isinstance(event, ExecutionInvoked):
            span = self._get_span(event.id)
            span.add_event(
                ExecutionInvoked.__name__,
            )
        elif isinstance(event, ExecutionTerminated):
            span = self._get_span(event.id)
            span.add_event(
                ExecutionTerminated.__name__,
                timestamp=event.tick,
            )
        elif isinstance(event, ExecutionResumed):
            span = self._get_span(event.id)

            span.add_event(
                ExecutionResumed.__name__,
                timestamp=event.tick,
            )
        elif isinstance(event, ExecutionAwaited):
            span = self._get_span(event.id)
            span.add_event(
                ExecutionAwaited.__name__,
                timestamp=event.tick,
            )
        else:
            assert_never(event)

    def _create_span(self, id: str, parent_id: str | None, start_time: int) -> None:
        parent_span_and_token = (
            self._spans[parent_id] if parent_id is not None else None
        )
        parent_span: Span | None = None
        if parent_span_and_token is not None:
            parent_span = parent_span_and_token[0]

        parent_ctx = (
            trace.set_span_in_context(parent_span) if parent_span is not None else None
        )
        token = context.attach(parent_ctx) if parent_ctx is not None else None
        new_span = self._tracer.start_span(
            name=id, context=parent_ctx, start_time=start_time
        )

        assert (
            id not in self._spans
        ), "There should not be two spans with the same name at the same time."
        self._spans[id] = (new_span, token)

    def _close_span(self, id: str, end_time: int) -> None:
        assert (
            id in self._spans
        ), "There should be an span associated with the promise id."
        span, token = self._spans.pop(id)
        if token:
            context.detach(token=token)
        span.end(end_time=end_time)
