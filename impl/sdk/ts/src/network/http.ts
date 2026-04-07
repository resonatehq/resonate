import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { EventSource } from "eventsource";
import { ResonateTimeoutException } from "../exceptions.js";
import type { Logger } from "../logger.js";
import * as util from "../util.js";
import type { Network } from "./network.js";
import { isMessage, isResponse, type Message, type Request, type Response } from "./types.js";

// TODO: Prometheus-style metrics (counters, histograms, gauges) are deferred.
// The spec calls for: resonate_network_requests_total, resonate_network_platform_failures_total,
// resonate_network_timeouts_total, resonate_network_messages_received_total,
// resonate_network_request_duration_ms (histogram), resonate_network_inflight_requests (gauge).
// See 04-networking.md Observability section for full details.

// =============================================================================
// HttpAdapter Interface
// =============================================================================

export interface HttpAdapter {
  readonly unicast: string;
  readonly anycast: string;
  init(): Promise<void>;
  stop(): Promise<void>;
  onReceive(callback: (msg: Message) => void): void;
}

// =============================================================================
// HttpNetwork
// =============================================================================

export interface HttpNetworkConfig {
  url?: string;
  timeout?: number;
  headers?: { [key: string]: string };
  token?: string;
  logger?: Logger;
  adapter?: HttpAdapter;
}

export class HttpNetwork implements Network {
  private url: string;
  private timeout: number;
  private headers: { [key: string]: string };
  private token?: string;
  private adapter?: HttpAdapter;
  private logger?: Logger;

  constructor({
    url = undefined,
    timeout = undefined,
    headers = {},
    token = undefined,
    logger = undefined,
    adapter = undefined,
  }: HttpNetworkConfig) {
    // Priority: programmatic config > RESONATE_URL env var > default
    this.url = url ?? process.env.RESONATE_URL ?? "http://localhost:8001";

    // Priority: programmatic config > RESONATE_TIMEOUT env var > default (10s)
    const envTimeout = process.env.RESONATE_TIMEOUT ? Number.parseInt(process.env.RESONATE_TIMEOUT, 10) : undefined;
    this.timeout = timeout ?? (envTimeout && !Number.isNaN(envTimeout) ? envTimeout : 10 * util.SEC);
    this.logger = logger;
    this.adapter = adapter;

    // Priority: programmatic token > env var
    const resolvedToken = token ?? process.env.RESONATE_TOKEN;

    this.headers = { "Content-Type": "application/json", ...headers };
    if (resolvedToken) {
      this.headers.Authorization = `Bearer ${resolvedToken}`;
      this.token = resolvedToken;
    }
  }

  get unicast(): string {
    util.assert(this.adapter !== undefined);
    return this.adapter.unicast;
  }

  get anycast(): string {
    util.assert(this.adapter !== undefined);
    return this.adapter.anycast;
  }

  async init(): Promise<void> {
    util.assert(this.adapter !== undefined);
    await this.adapter.init();
    this.logger?.info(
      { component: "network", url: this.url, adapter_type: this.adapter.constructor.name },
      "network initialized",
    );
  }

  async stop(): Promise<void> {
    util.assert(this.adapter !== undefined);
    await this.adapter.stop();
    this.logger?.info({ component: "network" }, "network stopped");
  }

  recv(callback: (msg: Message) => void): void {
    this.adapter?.onReceive(callback);
  }

  // Valid protocol status codes — responses with these statuses are returned as-is.
  // Everything else is a platform failure that throws ResonateTimeoutException.
  private static readonly PROTOCOL_STATUSES = new Set([200, 300, 404, 409, 422, 501]);

  send = async <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): Promise<Extract<Response, { kind: K }>> => {
    const startTime = Date.now();
    this.logger?.debug(
      { component: "network", url: `${this.url}`, kind: req.kind, corr_id: req.head.corrId },
      "request sent",
    );

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    if (this.token) {
      req = { ...req, head: { ...req.head, auth: this.token } };
    }

    let httpResponse: globalThis.Response;
    try {
      httpResponse = await fetch(`${this.url}`, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify(req),
        signal: controller.signal,
      });
    } catch (e) {
      const cause = e instanceof Error ? e.message : String(e);
      const errorType = cause.includes("abort") ? "http_timeout" : "connection_error";
      this.logger?.warn(
        { component: "network", kind: req.kind, corr_id: req.head.corrId, error_type: errorType, error: cause },
        "platform failure",
      );
      throw new ResonateTimeoutException(cause);
    } finally {
      clearTimeout(timeoutId);
    }

    // Check HTTP status — non-protocol statuses are platform failures
    if (!HttpNetwork.PROTOCOL_STATUSES.has(httpResponse.status)) {
      const cause = `HTTP ${httpResponse.status}`;
      this.logger?.warn(
        {
          component: "network",
          kind: req.kind,
          corr_id: req.head.corrId,
          error_type: `server_error_${httpResponse.status}`,
          error: cause,
        },
        "platform failure",
      );
      throw new ResonateTimeoutException(cause);
    }

    let resStr: string;
    try {
      resStr = await httpResponse.text();
    } catch (e) {
      const cause = e instanceof Error ? e.message : String(e);
      this.logger?.warn(
        {
          component: "network",
          kind: req.kind,
          corr_id: req.head.corrId,
          error_type: "malformed_response",
          error: cause,
        },
        "platform failure",
      );
      throw new ResonateTimeoutException(cause);
    }

    if (httpResponse.status === 404 && !httpResponse.headers.get("content-type")?.includes("application/json")) {
      this.logger?.warn(
        {
          component: "network",
          kind: req.kind,
          corr_id: req.head.corrId,
          error_type: "outdated_server",
        },
        "Legacy server detected. Please upgrade to the latest server: https://github.com/resonatehq/resonate",
      );
      throw new ResonateTimeoutException("legacy server detected");
    }

    let res: unknown;
    try {
      res = JSON.parse(resStr);
    } catch {
      this.logger?.warn(
        {
          component: "network",
          kind: req.kind,
          corr_id: req.head.corrId,
          error_type: "malformed_response",
          error: "failed to parse response JSON",
        },
        "platform failure",
      );
      throw new ResonateTimeoutException("failed to parse response JSON");
    }

    if (!isResponse(res) || res.kind !== req.kind || res.head.corrId !== req.head.corrId) {
      this.logger?.warn(
        {
          component: "network",
          kind: req.kind,
          corr_id: req.head.corrId,
          error_type: "malformed_response",
          error: "response did not match request",
        },
        "platform failure",
      );
      throw new ResonateTimeoutException("response did not match request");
    }

    const durationMs = Date.now() - startTime;
    this.logger?.debug(
      {
        component: "network",
        kind: res.kind,
        corr_id: res.head.corrId,
        status: res.head.status,
        duration_ms: durationMs,
      },
      "protocol response",
    );

    return res as Extract<Response, { kind: K }>;
  };
}

// =============================================================================
// PollMessageSource — implements HttpAdapter
// =============================================================================

export class PollMessageSource implements HttpAdapter {
  readonly unicast: string;
  readonly anycast: string;

  private pollUrl: string;
  private headers: { [key: string]: string };
  private eventSource: EventSource;
  private callbacks: Array<(msg: Message) => void> = [];
  private logger?: Logger;

  // Exponential backoff state
  private reconnectAttempt = 0;
  private static readonly INITIAL_BACKOFF_MS = 1000;
  private static readonly MAX_BACKOFF_MS = 30000;

  constructor({
    url,
    token = undefined,
    logger = undefined,
  }: {
    url: string;
    token?: string;
    logger?: Logger;
  }) {
    this.pollUrl = url;
    this.logger = logger;

    // Derive unicast/anycast from the URL
    // URL format: ${baseUrl}/poll/${group}/${pid}
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/").filter(Boolean);
    // segments: ["poll", group, pid]
    const group = segments.length >= 2 ? decodeURIComponent(segments[1]) : "default";
    const pid = segments.length >= 3 ? decodeURIComponent(segments[2]) : "";
    this.unicast = `poll://uni@${group}/${pid}`;
    this.anycast = `poll://any@${group}/${pid}`;

    this.headers = {};
    if (token) {
      this.headers.Authorization = `Bearer ${token}`;
    }

    this.eventSource = this.connect();
  }

  private connect() {
    this.eventSource = new EventSource(this.pollUrl, {
      fetch: (url, init) =>
        fetch(url, {
          ...init,
          headers: {
            ...init.headers,
            ...this.headers,
          },
        }),
    });

    this.eventSource.addEventListener("open", () => {
      if (this.reconnectAttempt > 0) {
        this.logger?.info(
          { component: "network", adapter_type: "HttpPoll", address: this.pollUrl, attempt: this.reconnectAttempt },
          "adapter reconnected",
        );
      } else {
        this.logger?.info(
          { component: "network", adapter_type: "HttpPoll", address: this.pollUrl },
          "adapter connected",
        );
      }
      this.reconnectAttempt = 0;
    });

    this.eventSource.addEventListener("message", (event) => {
      this.deliver(event.data);
    });

    this.eventSource.addEventListener("error", () => {
      this.eventSource.close();

      this.reconnectAttempt++;
      const delay = Math.min(
        PollMessageSource.INITIAL_BACKOFF_MS * 2 ** (this.reconnectAttempt - 1),
        PollMessageSource.MAX_BACKOFF_MS,
      );

      this.logger?.warn(
        {
          component: "network",
          adapter_type: "HttpPoll",
          error: "SSE connection error",
          attempt: this.reconnectAttempt,
        },
        "adapter reconnecting",
      );

      setTimeout(() => this.connect(), delay);
    });

    return this.eventSource;
  }

  private deliver(msgStr: string): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(msgStr);
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      this.logger?.warn({ component: "network", error, raw_data: msgStr.slice(0, 200) }, "message parse error");
      return;
    }
    if (!isMessage(parsed)) {
      this.logger?.warn(
        { component: "network", error: "invalid message structure", raw_data: msgStr.slice(0, 200) },
        "message parse error",
      );
      return;
    }

    const msgKind = parsed.kind;
    const idField =
      msgKind === "execute" ? { task_id: parsed.data?.task?.id } : { promise_id: parsed.data?.promise?.id };
    this.logger?.debug({ component: "network", msg_kind: msgKind, ...idField }, "message received");

    for (const callback of this.callbacks) {
      callback(parsed);
    }
  }

  public async init(): Promise<void> {}

  public async stop(): Promise<void> {
    this.eventSource.close();
  }

  public onReceive(callback: (msg: Message) => void): void {
    this.callbacks.push(callback);
  }
}

// =============================================================================
// PushMessageSource — implements HttpAdapter
// =============================================================================

export class PushMessageSource implements HttpAdapter {
  unicast: string;
  anycast: string;

  private host: string;
  private port: number;
  private server: Server;
  private callbacks: Array<(msg: Message) => void> = [];
  private logger?: Logger;

  constructor({
    host = "0.0.0.0",
    port = 0,
    logger = undefined,
  }: {
    host?: string;
    port?: number;
    logger?: Logger;
  } = {}) {
    this.host = host;
    this.port = port;
    this.logger = logger;

    // addresses are set after init() resolves the actual port
    this.unicast = "";
    this.anycast = "";

    this.server = createServer((req, res) => this.handleRequest(req, res));
  }

  public init(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.listen(this.port, this.host, () => {
        const addr = this.server.address();
        if (addr && typeof addr === "object") {
          this.port = addr.port;
        }

        // set addresses now that the server is listening and port is known
        this.unicast = `http://${this.host}:${this.port}`;
        this.anycast = `http://${this.host}:${this.port}`;

        this.logger?.info(
          { component: "network", adapter_type: "HttpPush", address: `${this.host}:${this.port}` },
          "adapter connected",
        );
        resolve();
      });
      this.server.once("error", reject);
    });
  }

  public stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.close((err) => (err ? reject(err) : resolve()));
    });
  }

  private handleRequest(req: IncomingMessage, res: ServerResponse): void {
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
      });
      res.end();
      return;
    }

    if (req.method !== "POST") {
      res.writeHead(405, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Method not allowed" }));
      return;
    }

    let body = "";
    req.on("data", (chunk: Buffer) => {
      body += chunk;
    });

    req.on("end", () => {
      this.deliver(body);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok" }));
    });
  }

  private deliver(msgStr: string): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(msgStr);
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      this.logger?.warn({ component: "network", error, raw_data: msgStr.slice(0, 200) }, "message parse error");
      return;
    }
    if (!isMessage(parsed)) {
      this.logger?.warn(
        { component: "network", error: "invalid message structure", raw_data: msgStr.slice(0, 200) },
        "message parse error",
      );
      return;
    }

    const msgKind = parsed.kind;
    const idField =
      msgKind === "execute" ? { task_id: parsed.data?.task?.id } : { promise_id: parsed.data?.promise?.id };
    this.logger?.debug({ component: "network", msg_kind: msgKind, ...idField }, "message received");

    for (const callback of this.callbacks) {
      callback(parsed);
    }
  }

  public onReceive(callback: (msg: Message) => void): void {
    this.callbacks.push(callback);
  }
}
