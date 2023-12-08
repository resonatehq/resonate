import { ILogger, ITrace } from "../logger";

type LogLevel = "debug" | "info" | "warn" | "error";

export class Logger implements ILogger {
  private _level: number;

  constructor(public level: LogLevel = "info") {
    switch (level) {
      case "debug":
        this._level = 0;
        break;
      case "info":
        this._level = 1;
        break;
      case "warn":
        this._level = 2;
        break;
      case "error":
        this._level = 3;
        break;
    }
  }

  startTrace(id: string, attrs?: Record<string, string>): ITrace {
    return new Trace(this, id, attrs);
  }

  private log(level: number, args: any[]): void {
    if (this._level <= level) {
      console.log(...args);
    }
  }

  debug(...args: any[]): void {
    this.log(0, args);
  }

  info(...args: any[]): void {
    this.log(1, args);
  }

  warn(...args: any[]): void {
    this.log(2, args);
  }

  error(...args: any[]): void {
    this.log(3, args);
  }
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
    this.logger.debug({
      id: this.id,
      parentId: this.parent,
      timestamp: this.timestamp,
      duration: Date.now() - this.timestamp,
      attrs: this.attrs,
    });
  }
}
