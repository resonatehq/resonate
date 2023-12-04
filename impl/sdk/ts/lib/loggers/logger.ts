import { ILogger, ITrace } from "../logger";

export class Logger implements ILogger {
  startTrace(id: string, attrs?: Record<string, string>): ITrace {
    return new Trace(this, id, attrs);
  }

  debug = console.debug;

  info = console.info;

  warn = console.warn;

  error = console.error;
}

class Trace implements ITrace {
  private timestamp: number = Date.now();

  constructor(
    private logger: Logger,
    private id: string,
    private attrs?: Record<string, string>,
    private parent?: string,
  ) {}

  start(id: string, attrs?: Record<string, string>): ITrace {
    return new Trace(this.logger, id, attrs, this.id);
  }

  end(): void {
    this.logger.info({
      id: this.id,
      parentId: this.parent,
      timestamp: this.timestamp,
      duration: Date.now() - this.timestamp,
      attrs: this.attrs,
    });
  }
}
