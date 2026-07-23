import type { ExecutionContext } from "@cloudflare/workers-types";
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
  private pid: string | undefined;
  private dependencies: Map<string, any>;
  private token?: string;
  private timeout?: number;
  private ttl: number;
  private initializer?: (env: Record<string, string>) => Promise<void>;

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
   * @param options.prefix - ID prefix applied to generated IDs.
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
    this.idPrefix = prefix ? `${prefix}:` : "";
    const resolvedLogLevel: LogLevel = logLevel ?? (verbose ? "debug" : "warn");
    this.logger = logger ?? new ConsoleLogger(resolvedLogLevel);
    this.pid = pid;

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
  public onInitialize(fn: (env: Record<string, string>) => Promise<void>): void {
    this.initializer = fn;
  }

  public setDependency(name: string, obj: any): void {
    this.dependencies.set(name, obj);
  }

  public handlerHttp(): {
    fetch: (request: Request, env: Record<string, string>, ctx: ExecutionContext) => Promise<Response>;
  } {
    return {
      fetch: async (request: Request, env: Record<string, string>, _ctx: ExecutionContext): Promise<Response> => {
        if (this.initializer !== undefined) {
          await this.initializer(env);
        }

        try {
          if (request.method !== "POST") {
            return new Response(JSON.stringify({ error: "Method not allowed. Use POST." }), { status: 405 });
          }

          if (!request.body) {
            return new Response(JSON.stringify({ error: "Request body missing." }), { status: 400 });
          }

          const body: any = await request.json();

          if (!isExecuteMsg(body)) {
            return new Response(
              JSON.stringify({
                error: "Request body must be a valid execute message.",
              }),
              { status: 400 },
            );
          }

          const resonateServerUrl = body.head.serverUrl;

          const network = new HttpNetwork({
            url: resonateServerUrl,
            timeout: this.timeout,
            headers: {},
            token: this.token,
            logger: this.logger,
          });

          const { protocol, host, pathname } = new URL(request.url);
          const functionUrl = `${protocol}//${host}${pathname}`;

          const core = new Core({
            pid: this.pid ?? crypto.randomUUID().replace(/-/g, ""),
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
            return new Response(JSON.stringify({ status: "completed" }), {
              status: 200,
            });
          }
          return new Response(JSON.stringify({ status: "suspended" }), {
            status: 200,
          });
        } catch (error) {
          return new Response(JSON.stringify({ error: `Handler failed: ${error}` }), { status: 500 });
        }
      },
    };
  }
}
