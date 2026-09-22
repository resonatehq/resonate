/**
 * Structured logging interface for the Resonate SDK.
 *
 * Users can inject any logger that satisfies this interface (e.g., pino, winston)
 * via the Resonate constructor. The default implementation is {@link ConsoleLogger}.
 */
export interface Logger {
  debug(fields: Record<string, any>, msg: string): void;
  info(fields: Record<string, any>, msg: string): void;
  warn(fields: Record<string, any>, msg: string): void;
  error(fields: Record<string, any>, msg: string): void;
}

export type LogLevel = "debug" | "info" | "warn" | "error";

const LOG_LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/**
 * Default structured logger that writes JSON lines to stdout/stderr.
 *
 * - debug and info go to `console.log` (stdout)
 * - warn goes to `console.warn` (stderr)
 * - error goes to `console.error` (stderr)
 *
 * Messages at levels below the configured `level` are suppressed.
 */
export class ConsoleLogger implements Logger {
  private level: LogLevel;

  constructor(level: LogLevel = "warn") {
    this.level = level;
  }

  debug(fields: Record<string, any>, msg: string): void {
    if (!this.shouldLog("debug")) return;
    this.emit("debug", fields, msg, console.log);
  }

  info(fields: Record<string, any>, msg: string): void {
    if (!this.shouldLog("info")) return;
    this.emit("info", fields, msg, console.log);
  }

  warn(fields: Record<string, any>, msg: string): void {
    if (!this.shouldLog("warn")) return;
    this.emit("warn", fields, msg, console.warn);
  }

  error(fields: Record<string, any>, msg: string): void {
    if (!this.shouldLog("error")) return;
    this.emit("error", fields, msg, console.error);
  }

  private shouldLog(level: LogLevel): boolean {
    return LOG_LEVEL_ORDER[level] >= LOG_LEVEL_ORDER[this.level];
  }

  private emit(level: string, fields: Record<string, any>, msg: string, writer: (...args: any[]) => void): void {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      msg,
      ...fields,
    };
    writer(JSON.stringify(entry));
  }
}
