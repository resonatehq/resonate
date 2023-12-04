import * as opentelemetry from "@opentelemetry/api";
import { ILogger, ITrace } from "../logger";

export class OpenTelemetryLogger implements ILogger {
  startTrace(id: string, attrs?: Record<string, string>): ITrace {
    return new OpenTelemetrySpan(id, attrs);
  }
}

class OpenTelemetrySpan implements ITrace {
  private span: opentelemetry.Span;

  constructor(id: string, attrs?: Record<string, string>, parent?: opentelemetry.Span) {
    const tracer = opentelemetry.trace.getTracer("resonate");
    const ctx = parent ? opentelemetry.trace.setSpan(opentelemetry.context.active(), parent) : undefined;

    this.span = tracer.startSpan(id, undefined, ctx);
    if (attrs) {
      this.span.setAttributes(attrs);
    }
  }

  start(id: string, attrs?: Record<string, string>): ITrace {
    return new OpenTelemetrySpan(id, attrs, this.span);
  }

  end(): void {
    this.span.end();
  }
}
