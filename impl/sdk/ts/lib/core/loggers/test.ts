import { Logger } from "./logger";
import { ILogger, ITrace } from "../logger";

export class TestLogger extends Logger implements ILogger {
  public traces: any[] = [];

  startTrace(id: string, attrs?: Record<string, string>): ITrace {
    return new TestTrace(id, this.traces, attrs);
  }
}

class TestTrace implements ITrace {
  private timestamp: number = Date.now();

  constructor(
    private id: string,
    private traces: any[],
    private attrs?: Record<string, string>,
    private parent?: string,
  ) {}

  start(id: string, attrs?: Record<string, string>): ITrace {
    return new TestTrace(id, this.traces, attrs, this.id);
  }

  end(): void {
    this.traces.push({
      id: this.id,
      parentId: this.parent,
      timestamp: this.timestamp,
      duration: Date.now() - this.timestamp,
      attrs: this.attrs,
    });
  }
}
