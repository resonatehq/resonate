export interface Tracer {
  startSpan(id: string, startTime: number): Span;
  decode(headers: Record<string, string>): Span;
}

export interface Span {
  startSpan(id: string, startTime: number): Span;
  encode(): Record<string, string>;
  setAttribute(key: string, value: string | number | boolean): void;
  end(endTime: number): void;
}

export class NoopTracer {
  startSpan(id: string, startTime: number): NoopSpan {
    return new NoopSpan();
  }
  decode(headers: Record<string, string>): Span {
    return new NoopSpan();
  }
}

export class NoopSpan {
  startSpan(id: string, startTime: number): Span {
    return new NoopSpan();
  }
  setAttribute(key: string, value: string | number | boolean): void {}
  encode(): Record<string, string> {
    return {};
  }
  end(endTime: number): void {}
}
