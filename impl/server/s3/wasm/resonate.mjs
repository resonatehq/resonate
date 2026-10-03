// The server as a JS object: the wasm build of `src/wasm.zig`, instantiated and
// wired to `fetch` and a timer.
//
//   import { createResonate } from "./resonate.mjs";
//
//   const resonate = await createResonate({
//     wasm: await readFile("zig-out/bin/resonate.wasm"),
//     store: "s3",
//     endpoint: "http://127.0.0.1:9000",
//     bucket: "resonate",
//   });
//
//   const res = await resonate.rpc({
//     kind: "promise.create",
//     head: { corrId: "1", version: "2026-04-01" },
//     data: { id: "p1", timeoutAt: Date.now() + 60_000, param: {}, tags: {} },
//   });
//
//   // Or serve it: a WHATWG fetch handler, for Deno, Bun, workers, Node adapters.
//   Deno.serve(resonate.fetch);
//
// No Node APIs in this file: it runs anywhere with WebAssembly, `fetch` and
// `setTimeout`. Each call to `createResonate` is a separate server with its
// own memory, clock and timers; compile the module once and pass the
// `WebAssembly.Module` to make more of them cheaply.

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * @param {object} options
 * @param {BufferSource | WebAssembly.Module | Response | Promise<Response>} options.wasm
 *   The module: its bytes, a compiled module, or a `fetch` of it.
 * @param {"memory" | "s3"} [options.store] Where the state lives. Default: memory.
 * @param {string} [options.endpoint] The S3 endpoint. Default: http://127.0.0.1:9000.
 * @param {string} [options.bucket] The bucket. Default: resonate.
 * @param {string} [options.prefix] A key prefix inside the bucket.
 * @param {string} [options.serverUrl] The URL workers answer. Default: http://localhost:8001.
 * @param {boolean} [options.debug] The clock belongs to the caller (`resonate:debug_time`).
 * @param {number} [options.requestTimeout] How long one store request may take, in ms.
 *   Past it the module stops waiting and treats the request as "may or may not
 *   have happened". Default: 10000.
 * @param {typeof fetch} [options.fetch] Carries every S3 and worker request. Sign
 *   them here: the module sends them unsigned.
 * @param {(line: string) => void} [options.log] Default: console.error.
 */
export async function createResonate(options) {
  const {
    wasm: source,
    store = "memory",
    endpoint = "http://127.0.0.1:9000",
    bucket = "resonate",
    prefix = "",
    serverUrl = "http://localhost:8001",
    debug = false,
    requestTimeout = 10_000,
    fetch: fetchImpl = globalThis.fetch.bind(globalThis),
    log = (line) => console.error(line),
  } = options ?? {};
  if (store !== "memory" && store !== "s3") throw new TypeError('store is "memory" or "s3"');

  let exports; // the instance's, once it exists
  let closed = false;
  let timer = null;
  /** Requests waiting on `host_respond`, by id. */
  const waiting = new Map();
  let nextId = 1;

  // The module's memory can grow during any call, which detaches every view of
  // the old buffer — so a view is made fresh for every read and write.
  const bytes = () => new Uint8Array(exports.memory.buffer);
  const read = (ptr, len) => bytes().slice(ptr, ptr + len);
  const readText = (ptr, len) => decoder.decode(read(ptr, len));

  /** Copy `data` into the module, call `fn(ptr, len)`, and free it after. */
  function withBuffer(data, fn) {
    const buf = typeof data === "string" ? encoder.encode(data) : data;
    // Even an empty buffer is a real allocation: a pointer the module receives
    // is never null, and `alloc`/`free` round a length of zero up to one alike.
    const ptr = exports.alloc(buf.length);
    if (ptr === 0) throw new Error("the module is out of memory");
    bytes().set(buf, ptr);
    try {
      return fn(ptr, buf.length);
    } finally {
      exports.free(ptr, buf.length);
    }
  }

  /** Run `start(id)` and resolve with what the module answers for that id. */
  function expect(start) {
    if (closed) return Promise.reject(new Error("this server is closed"));
    return new Promise((resolve) => {
      const id = nextId++;
      waiting.set(id, resolve);
      start(id);
    });
  }

  function deliver(id, status, head, payload) {
    if (closed) return;
    withBuffer(head, (hPtr, hLen) =>
      withBuffer(payload, (bPtr, bLen) => exports.on_fetch(id, status, hPtr, hLen, bPtr, bLen)),
    );
  }

  const imports = {
    env: {
      host_now_ms: () => Date.now(),

      host_set_timer(atMs) {
        if (timer !== null) clearTimeout(timer);
        timer = null;
        if (atMs < 0 || closed) return;
        timer = setTimeout(() => {
          timer = null;
          if (!closed) exports.on_timer();
        }, Math.max(0, atMs - Date.now()));
      },

      host_respond(id, status, ctPtr, ctLen, bodyPtr, bodyLen) {
        const resolve = waiting.get(id);
        if (!resolve) return;
        waiting.delete(id);
        // Copied now: the buffer is the module's and may be gone on return.
        resolve({ status, contentType: readText(ctPtr, ctLen), body: read(bodyPtr, bodyLen) });
      },

      host_log(ptr, len) {
        log(readText(ptr, len));
      },

      host_fetch(id, mPtr, mLen, uPtr, uLen, hPtr, hLen, bPtr, bLen) {
        // Everything is copied before the first await: the module reuses these.
        const method = readText(mPtr, mLen);
        const url = readText(uPtr, uLen);
        const headers = {};
        for (const line of readText(hPtr, hLen).split("\n")) {
          const colon = line.indexOf(":");
          if (colon > 0) headers[line.slice(0, colon).trim()] = line.slice(colon + 1).trim();
        }
        const body = method === "GET" || method === "DELETE" ? undefined : read(bPtr, bLen);
        // Always answered on a later turn, never inside this call.
        (async () => {
          let status, head, payload;
          try {
            // The module keeps its own deadline and stops waiting at
            // `requestTimeout`; this one is only so the socket is not held on to
            // forever after it has.
            const signal = AbortSignal.timeout(requestTimeout + 1_000);
            const res = await fetchImpl(url, { method, headers, body, signal });
            // Inside the `try`: a body cut off after the headers arrived fails
            // *here*, not at `fetch`, and a truncated answer must never be read
            // as a short one — nor escape as an unhandled rejection.
            payload = new Uint8Array(await res.arrayBuffer());
            status = res.status;
            // The ETag first: the module keeps a bounded number of headers, and
            // that is the one the whole design rests on.
            head = "";
            const etag = res.headers.get("etag");
            if (etag !== null) head += `etag: ${etag}\n`;
            for (const [name, value] of res.headers) if (name !== "etag") head += `${name}: ${value}\n`;
          } catch (err) {
            deliver(id, 0, "", encoder.encode(String(err?.cause?.message ?? err?.message ?? err)));
            return;
          }
          // Outside it: a fault in the module is not a fault in the exchange,
          // and must not be delivered a second time as one.
          deliver(id, status, head, payload);
        })();
      },
    },
  };

  const module = await compile(source);
  exports = (await WebAssembly.instantiate(module, imports)).exports;

  const started = withBuffer(endpoint, (eP, eL) =>
    withBuffer(bucket, (bP, bL) =>
      withBuffer(prefix, (pP, pL) =>
        withBuffer(serverUrl, (uP, uL) =>
          exports.init(eP, eL, bP, bL, pP, pL, uP, uL, (debug ? 1 : 0) | (store === "memory" ? 2 : 0), requestTimeout),
        ),
      ),
    ),
  );
  if (started !== 0) throw new Error("the server did not start; see the log");

  /** A raw protocol request: the body of `POST /`, as bytes or a string. */
  function handle(body) {
    const buf = typeof body === "string" ? encoder.encode(body) : body;
    return expect((id) => withBuffer(buf, (ptr, len) => exports.rpc(id, ptr, len)));
  }

  const resonate = {
    /**
     * One protocol envelope in, its answer out. The protocol's status is in
     * the answer's `head.status`; this does not throw on a 4xx.
     */
    async rpc(envelope) {
      const res = await handle(typeof envelope === "string" ? envelope : JSON.stringify(envelope));
      const text = decoder.decode(res.body);
      if (!res.contentType.startsWith("application/json")) throw new Error(`${res.status}: ${text.trim()}`);
      return JSON.parse(text);
    },

    /** `{status, contentType, body}` for one `POST /` body, untouched. */
    handle,

    /** Whether the store answers. */
    async ready() {
      return (await expect((id) => exports.ready(id))).status === 200;
    },

    /** The Prometheus text `GET /metrics` serves. */
    async metrics() {
      return decoder.decode((await expect((id) => exports.metrics(id))).body);
    },

    /** The whole HTTP surface, as a WHATWG fetch handler. */
    async fetch(request) {
      const path = new URL(request.url).pathname;
      const reply = ({ status, contentType, body }) =>
        new Response(body, { status, headers: { "content-type": contentType } });
      const text = (status, body, type = "text/plain") =>
        new Response(body, { status, headers: { "content-type": type } });
      if (path === "/" && request.method === "POST") {
        return reply(await handle(new Uint8Array(await request.arrayBuffer())));
      }
      if (path === "/ready" && request.method === "GET") return reply(await expect((id) => exports.ready(id)));
      if (path === "/metrics" && request.method === "GET") return reply(await expect((id) => exports.metrics(id)));
      for (const legacy of ["/promises", "/schedules", "/tasks"]) {
        if (path === legacy || path.startsWith(legacy + "/")) {
          return text(410,
            '{"error":"This endpoint is no longer supported. Please update to the latest SDK."}',
            "application/json");
        }
      }
      if (path === "/") return text(405, "the protocol endpoint takes POST\n");
      return text(404, "not found\n");
    },

    /**
     * Stop: no more timers, no more answers. Requests still waiting are
     * dropped. Everything committed is in the store, so nothing is lost —
     * the same as a native server that is killed.
     */
    close() {
      closed = true;
      if (timer !== null) clearTimeout(timer);
      timer = null;
      waiting.clear();
    },
  };
  resonate.fetch = resonate.fetch.bind(resonate);
  return resonate;
}

async function compile(source) {
  if (source instanceof WebAssembly.Module) return source;
  const resolved = await source;
  if (typeof Response !== "undefined" && resolved instanceof Response) {
    return WebAssembly.compileStreaming
      ? WebAssembly.compileStreaming(resolved)
      : WebAssembly.compile(await resolved.arrayBuffer());
  }
  if (resolved == null) throw new TypeError("options.wasm is required: the module's bytes, a Module, or a Response");
  return WebAssembly.compile(resolved);
}
