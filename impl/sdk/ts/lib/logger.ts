export interface ILogger {
  startTrace(id: string, attrs?: Record<string, string>): ITrace;
}

export interface ITrace {
  start(id: string, attrs?: Record<string, string>): ITrace;
  end(): void;
}
