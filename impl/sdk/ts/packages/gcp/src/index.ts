import type { HttpFunction, Request, Response } from "@google-cloud/functions-framework";
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
import { type AuthOptions, resolveAuth } from "./auth.js";

export type { AuthMode, AuthOptions } from "./auth.js";

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
  private auth: AuthOptions;
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
   * @param options.auth - Outbound auth mode for calls to the Resonate server.
   *   Defaults to `{ mode: "auto" }`:
   *   - `auto`        — mint a Google OIDC ID token for HTTPS server URLs; no auth for HTTP.
   *   - `none`        — no auth header.
   *   - `bearer`      — static bearer token (`token` field, then `RESONATE_TOKEN` env var).
   *   - `oidcIdToken` — always mint a Google OIDC ID token for `audience ?? serverUrl`.
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
    auth = { mode: "auto" },
    timeout = undefined,
    verbose = false,
    logLevel = undefined,
    logger = undefined,
    encryptor = undefined,
    prefix = undefined,
  }: {
    pid?: string;
    ttl?: number;
    auth?: AuthOptions;
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
    this.timeout = timeout;
    this.ttl = ttl;
    this.auth = auth;
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

  public handlerHttp(): HttpFunction {
    return async (req: Request, res: Response) => {
      try {
        if (req.method !== "POST") {
          return res.status(405).json({ error: "Method not allowed. Use POST." });
        }

        if (!req.body) {
          return res.status(400).json({ error: "Request body missing." });
        }

        if (!isExecuteMsg(req.body)) {
          return res.status(400).json({
            error: "Request body must be a valid execute message.",
          });
        }

        const body = req.body;

        // The Resonate server URL: prefer the message head, fall back to
        // RESONATE_URL env var (HttpNetwork handles that fallback internally).
        const resonateServerUrl = body.head.serverUrl;

        // Resolve outbound auth for the call back to the Resonate server.
        // resonateServerUrl may be undefined; resolveAuth mirrors HttpNetwork's
        // RESONATE_URL fallback so auto-HTTPS detection still works.
        const resolved = await resolveAuth(resonateServerUrl, this.auth);

        const network = new HttpNetwork({
          url: resonateServerUrl,
          timeout: this.timeout,
          headers: resolved.headers,
          token: resolved.token,
          logger: this.logger,
        });

        // The function's own public URL — used as the anycast address so
        // sub-tasks are routed back to this same function invocation.
        const proto = req.get("x-forwarded-proto") || req.protocol;
        const host = req.get("host");
        if (!proto || !host) {
          return res.status(500).json({
            error: "Cannot determine function URL: missing x-forwarded-proto or host header.",
          });
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
              return `${proto}://${host}${req.originalUrl || ""}`;
            },
            idPrefix: this.idPrefix,
          }),
          logger: this.logger,
        });

        const status = await core.onMessage(body);

        if (status?.kind === "done") {
          return res.status(200).json({ status: "completed" });
        }
        return res.status(200).json({ status: "suspended" });
      } catch (error) {
        return res.status(500).json({
          error: `Handler failed: ${error}`,
        });
      }
    };
  }
}
