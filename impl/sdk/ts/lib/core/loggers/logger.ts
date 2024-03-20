import { ILogger, LogLevel } from "../logger";

export class Logger implements ILogger {
  public level: LogLevel;
  private _level: number;

  constructor(level?: LogLevel) {
    this.level = level ?? (process.env.LOG_LEVEL as LogLevel) ?? "info";

    switch (this.level) {
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

  table(...args: any[]): void {
    if (this._level <= 0) {
      console.table(...args);
    }
  }

  private log(level: number, args: any[]): void {
    if (this._level <= level) {
      console.log(...args);
    }
  }
}
