import {
  Codec,
  ConsoleLogger,
  Core,
  type Encryptor,
  type Func,
  HttpNetwork,
  isExecuteMsg,
  type Logger,
  type LogLevel,
  NoopEncryptor,
  NoopHeartbeat,
  OptionsBuilder,
  Registry,
  WallClock,
} from "@resonatehq/sdk";
import type { LambdaFunctionURLHandler, LambdaFunctionURLResult } from "aws-lambda";

function isUrl(str: string): boolean {
  try {
    new URL(str);
    return true;
  } catch {
    return false;
  }
}

export class Resonate {
  private registry: Registry;
  private codec: Codec;
  private idPrefix: string;
  private logger: Logger;
  private pid: string;
  private dependencies: Map<string, any>;
  private token?: string;
  private timeout?: number;
  private ttl: number;

  /**
   * Creates a new Resonate client instance.
   *
   * @param options - Configuration options for the client.
   * @param options.pid - Process identifier for the client. Defaults to a random UUID.
   * @param options.ttl - Time-to-live (in ms) for acquired tasks. The server will release
   *   a task if no heartbeat is received within this window. Defaults to `5 * 60 * 1000`
   *   (5 minutes). Set this to at least the maximum expected function execution time.
   *   Because serverless functions cannot send async heartbeats, choose a value safely
   *   above your function's configured timeout.
   * @param options.token - Bearer token for authentication. Passed through to HttpNetwork
   *   which falls back to `RESONATE_TOKEN` env var.
   * @param options.timeout - Network request timeout. Passed through to HttpNetwork
   *   which falls back to `RESONATE_TIMEOUT` env var (default: 10s).
   * @param options.verbose - Enables verbose logging (shorthand for `logLevel: "debug"`). Defaults to `false`.
   * @param options.logLevel - Log level for the default ConsoleLogger. Defaults to `"warn"`. Takes precedence over `verbose`.
   * @param options.logger - Custom logger implementation. Defaults to {@link ConsoleLogger}.
   * @param options.encryptor - Payload encryptor. Defaults to {@link NoopEncryptor}.
   * @param options.prefix - ID prefix applied to generated IDs. Defaults to
   *   `process.env.RESONATE_PREFIX` when set.
   */
  constructor({
    pid = undefined,
    ttl = 5 * 60 * 1000,
    token = undefined,
    timeout = undefined,
    verbose = false,
    logLevel = undefined,
    logger = undefined,
    encryptor = undefined,
    prefix = undefined,
  }: {
    pid?: string;
    ttl?: number;
    token?: string;
    timeout?: number;
    verbose?: boolean;
    logLevel?: LogLevel;
    logger?: Logger;
    encryptor?: Encryptor;
    prefix?: string;
  } = {}) {
    this.codec = new Codec(encryptor ?? new NoopEncryptor());
    const resolvedPrefix = prefix ?? process.env.RESONATE_PREFIX;
    this.idPrefix = resolvedPrefix ? `${resolvedPrefix}:` : "";
    const resolvedLogLevel: LogLevel = logLevel ?? (verbose ? "debug" : "warn");
    this.logger = logger ?? new ConsoleLogger(resolvedLogLevel);
    this.pid = pid ?? crypto.randomUUID().replace(/-/g, "");

    this.registry = new Registry();
    this.dependencies = new Map();
    this.token = token;
    this.timeout = timeout;
    this.ttl = ttl;
  }

  /**
   * Registers a function with Resonate for execution and version control.
   */
  public register<F extends Func>(name: string, func: F, options?: { version?: number }): void;
  public register<F extends Func>(func: F, options?: { version?: number }): void;
  public register<F extends Func>(
    nameOrFunc: string | F,
    funcOrOptions?:
      | F
      | {
          version?: number;
        },
    maybeOptions: {
      version?: number;
    } = {},
  ): void {
    const { version = 1 } = (typeof funcOrOptions === "object" ? funcOrOptions : maybeOptions) ?? {};
    const func = typeof nameOrFunc === "function" ? nameOrFunc : (funcOrOptions as F);
    const name = typeof nameOrFunc === "string" ? nameOrFunc : func.name;

    this.registry.add(func, name, version);
  }

  /**
   * Registers a named dependency that will be available to all Resonate functions
   * via `context.getDependency(name)`.
   *
   * Use this to inject services, clients, or configuration objects into Resonate
   * functions without tight coupling.
   *
   * @param name - A unique key identifying the dependency.
   * @param obj - The dependency value (any object or primitive).
   */
  public setDependency(name: string, obj: any): void {
    this.dependencies.set(name, obj);
  }

  public handlerHttp(): LambdaFunctionURLHandler {
    return async (event): Promise<LambdaFunctionURLResult> => {
      const method = event.requestContext.http.method;
      const path = event.requestContext.http.path ?? "/";

      try {
        if (method !== "POST") {
          return {
            statusCode: 405,
            body: JSON.stringify({ error: "Method not allowed. Use POST." }),
          };
        }

        if (!event.body) {
          return {
            statusCode: 400,
            body: JSON.stringify({ error: "Request body missing." }),
          };
        }

        const body = JSON.parse(event.body);

        if (!isExecuteMsg(body)) {
          return {
            statusCode: 400,
            body: JSON.stringify({
              error: "Request body must be a valid execute message.",
            }),
          };
        }

        // The Resonate server URL: prefer the message head, fall back to
        // RESONATE_URL env var (HttpNetwork handles that fallback internally).
        const resonateServerUrl = body.head.serverUrl;

        const network = new HttpNetwork({
          url: resonateServerUrl,
          timeout: this.timeout,
          headers: {},
          token: this.token,
          logger: this.logger,
        });

        // The function's own public URL — used as the anycast address so
        // sub-tasks are routed back to this same function invocation.
        // Prefer reconstructing from headers (works in Lambda + API GW).
        // Fall back to FUNCTION_URL env var (useful for SAM local and Lambda
        // Function URLs where forwarded headers may be absent).
        const proto = event.headers["x-forwarded-proto"];
        const host = event.headers.host;
        const functionUrl = proto && host ? `${proto}://${host}${path}` : (process.env.FUNCTION_URL ?? null);

        if (!functionUrl) {
          return {
            statusCode: 500,
            body: JSON.stringify({
              error: "Cannot determine function URL: missing x-forwarded-proto or host header.",
            }),
          };
        }

        const core = new Core({
          pid: this.pid,
          ttl: this.ttl,
          clock: new WallClock(),
          send: network.send,
          codec: this.codec,
          registry: this.registry,
          heartbeat: new NoopHeartbeat(),
          dependencies: this.dependencies,
          optsBuilder: new OptionsBuilder({
            match: (target: string): string => {
              if (isUrl(target)) return target;
              return functionUrl;
            },
            idPrefix: this.idPrefix,
          }),
          logger: this.logger,
        });

        const status = await core.onMessage(body);

        if (status?.kind === "done") {
          return {
            statusCode: 200,
            body: JSON.stringify({ status: "completed" }),
          };
        }
        return {
          statusCode: 200,
          body: JSON.stringify({ status: "suspended" }),
        };
      } catch (error) {
        return {
          statusCode: 500,
          body: JSON.stringify({
            error: `Handler failed: ${error}`,
          }),
        };
      }
    };
  }
}
