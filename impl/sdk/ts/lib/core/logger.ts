export type LogLevel = "debug" | "info" | "warn" | "error";

export interface ILogger {
  level: LogLevel;
  debug(...args: any[]): void;
  info(...args: any[]): void;
  warn(...args: any[]): void;
  error(...args: any[]): void;
  table(...args: any[]): void;
}
