export interface ILogger {
  debug(...args: any[]): void;
  info(...args: any[]): void;
  warn(...args: any[]): void;
  error(...args: any[]): void;
  startTrace(id: string, attrs?: Record<string, string>): ITrace;
}

export interface ITrace {
  start(id: string, attrs?: Record<string, string>): ITrace;
  end(): void;
}
