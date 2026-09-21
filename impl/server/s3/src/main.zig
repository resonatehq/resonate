//! The binary: parse the arguments, build the environment, serve.
//!
//! This is the composition root and nothing else. Every decision it makes is
//! which implementation of a port to hand to `Runtime` — an object store in a
//! bucket or in memory, a clock and a timer from the ring, a message bus that
//! posts to workers or one that routes nothing. Nothing below it knows which it
//! got, which is the same property the simulator relies on.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const env = @import("env.zig");
const io_mod = @import("io.zig");
const net = @import("net.zig");
const http = @import("http.zig");
const s3_mod = @import("s3.zig");
const bus_mod = @import("bus.zig");
const server_mod = @import("server.zig");

const assert = stdx.assert;

const usage =
    \\resonate — durable promises on an object store
    \\
    \\Usage:
    \\  resonate serve [options]
    \\  resonate --help
    \\
    \\Where the state lives:
    \\  --store <kind>           memory | s3            [default: memory]
    \\  --endpoint <url>         the object store's endpoint, for --store s3
    \\  --bucket <name>          the bucket, for --store s3
    \\  --prefix <prefix>        key prefix inside the bucket   [default: none]
    \\  --timer-shards <n>       how many prefixes deadlines spread across [default: 4]
    \\
    \\Where it listens:
    \\  --bind <address>         [default: 0.0.0.0]
    \\  --port <port>            [default: 8001]
    \\  --server-url <url>       what workers are told to answer
    \\                           [default: http://<bind>:<port>]
    \\  --max-connections <n>    [default: 1024]
    \\
    \\Behaviour:
    \\  --debug                  the clock belongs to the caller: honour
    \\                           resonate:debug_time, answer debug.*, and run
    \\                           nothing on wall time
    \\  --preload-limit <n>      branch siblings carried in a task response [default: 10]
    \\  --cas-retries <n>        re-decides before a contended origin gives up [default: 8]
    \\  --cache-entries <n>      documents held in memory                     [default: 4096]
    \\  --cache-bytes <n>        what those documents may weigh, in bytes      [default: 67108864]
    \\  --sole-writer            no other process writes this bucket, so a cached
    \\                           document is answered from without revalidating it.
    \\                           Wrong if anything else writes the bucket, and
    \\                           nothing can check that for you
    \\  --shutdown-timeout <ms>  how long SIGTERM waits for work in flight
    \\                           [default: 2000]
    \\
    \\There is no TLS and no authentication here on purpose. Put a proxy in front:
    \\it terminates TLS, authenticates, authorizes, and forwards what is left.
    \\
;

const Args = struct {
    store_kind: enum { memory, s3 } = .memory,
    endpoint: []const u8 = "http://127.0.0.1:9000",
    bucket: []const u8 = "resonate",
    prefix: []const u8 = "",
    timer_shards: u32 = store_mod.KeySpace.default_timer_shards,
    bind: []const u8 = "0.0.0.0",
    port: u16 = 8001,
    server_url: ?[]const u8 = null,
    max_connections: u32 = 1024,
    debug: bool = false,
    preload_limit: u32 = protocol.preload_limit_default,
    cas_retries: u32 = 8,
    cache_entries: u32 = 4096,
    cache_bytes: u64 = 64 << 20,
    sole_writer: bool = false,
    shutdown_timeout: i64 = 2_000,
};

pub fn main() u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = std.process.argsAlloc(allocator) catch return fail("out of memory reading the arguments");
    defer std.process.argsFree(allocator, argv);

    if (argv.len < 2) {
        std.io.getStdErr().writeAll(usage) catch {};
        return 1;
    }
    if (std.mem.eql(u8, argv[1], "--help") or std.mem.eql(u8, argv[1], "-h")) {
        std.io.getStdOut().writeAll(usage) catch {};
        return 0;
    }
    if (!std.mem.eql(u8, argv[1], "serve")) {
        return fail("the only command is `serve`");
    }

    var args = Args{};
    var i: usize = 2;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        const value = blk: {
            if (i + 1 < argv.len) break :blk argv[i + 1];
            break :blk null;
        };
        if (std.mem.eql(u8, arg, "--sole-writer")) {
            args.sole_writer = true;
        } else if (std.mem.eql(u8, arg, "--debug")) {
            args.debug = true;
        } else if (std.mem.eql(u8, arg, "--store")) {
            const v = value orelse return fail("--store needs a value");
            i += 1;
            if (std.mem.eql(u8, v, "memory")) {
                args.store_kind = .memory;
            } else if (std.mem.eql(u8, v, "s3")) {
                args.store_kind = .s3;
            } else return fail("--store is memory or s3");
        } else if (std.mem.eql(u8, arg, "--endpoint")) {
            args.endpoint = value orelse return fail("--endpoint needs a value");
            i += 1;
        } else if (std.mem.eql(u8, arg, "--bucket")) {
            args.bucket = value orelse return fail("--bucket needs a value");
            i += 1;
        } else if (std.mem.eql(u8, arg, "--prefix")) {
            args.prefix = value orelse return fail("--prefix needs a value");
            i += 1;
        } else if (std.mem.eql(u8, arg, "--bind")) {
            args.bind = value orelse return fail("--bind needs a value");
            i += 1;
        } else if (std.mem.eql(u8, arg, "--server-url")) {
            args.server_url = value orelse return fail("--server-url needs a value");
            i += 1;
        } else if (std.mem.eql(u8, arg, "--port")) {
            const v = value orelse return fail("--port needs a value");
            i += 1;
            args.port = std.fmt.parseInt(u16, v, 10) catch return fail("--port is not a port");
        } else if (std.mem.eql(u8, arg, "--timer-shards")) {
            const v = value orelse return fail("--timer-shards needs a value");
            i += 1;
            args.timer_shards = std.fmt.parseInt(u32, v, 10) catch return fail("--timer-shards is not a number");
            if (args.timer_shards == 0) return fail("--timer-shards must be at least 1");
        } else if (std.mem.eql(u8, arg, "--max-connections")) {
            const v = value orelse return fail("--max-connections needs a value");
            i += 1;
            args.max_connections = std.fmt.parseInt(u32, v, 10) catch return fail("--max-connections is not a number");
            if (args.max_connections == 0) return fail("--max-connections must be at least 1");
        } else if (std.mem.eql(u8, arg, "--preload-limit")) {
            const v = value orelse return fail("--preload-limit needs a value");
            i += 1;
            args.preload_limit = std.fmt.parseInt(u32, v, 10) catch return fail("--preload-limit is not a number");
        } else if (std.mem.eql(u8, arg, "--cas-retries")) {
            const v = value orelse return fail("--cas-retries needs a value");
            i += 1;
            args.cas_retries = std.fmt.parseInt(u32, v, 10) catch return fail("--cas-retries is not a number");
        } else if (std.mem.eql(u8, arg, "--cache-entries")) {
            const v = value orelse return fail("--cache-entries needs a value");
            i += 1;
            args.cache_entries = std.fmt.parseInt(u32, v, 10) catch return fail("--cache-entries is not a number");
            if (args.cache_entries == 0) return fail("--cache-entries must be at least 1");
        } else if (std.mem.eql(u8, arg, "--cache-bytes")) {
            const v = value orelse return fail("--cache-bytes needs a value");
            i += 1;
            args.cache_bytes = std.fmt.parseInt(u64, v, 10) catch return fail("--cache-bytes is not a number");
            if (args.cache_bytes == 0) return fail("--cache-bytes must be at least 1");
        } else if (std.mem.eql(u8, arg, "--shutdown-timeout")) {
            const v = value orelse return fail("--shutdown-timeout needs a value");
            i += 1;
            args.shutdown_timeout = std.fmt.parseInt(i64, v, 10) catch
                return fail("--shutdown-timeout is not a number");
            if (args.shutdown_timeout < 0) return fail("--shutdown-timeout must not be negative");
        } else {
            std.debug.print("unknown option: {s}\n\n{s}", .{ arg, usage });
            return 1;
        }
    }

    run(allocator, args) catch |e| {
        std.debug.print("resonate: {s}\n", .{@errorName(e)});
        return 1;
    };
    return 0;
}

fn fail(message: []const u8) u8 {
    std.debug.print("resonate: {s}\n\n{s}", .{ message, usage });
    return 1;
}

/// Everything the process owns, so the handler can reach it from a callback.
const Process = struct {
    allocator: std.mem.Allocator,
    loop: *io_mod.Loop,
    runtime: *server_mod.Runtime,
    http_server: *net.Server,
    client: *net.Client,
    push: *bus_mod.HttpPush,
    memory_store: ?*store_mod.MemoryStore,
    s3: ?*s3_mod.S3,
    seeded: bool = false,
    seed_timeout: env.Timeout = undefined,
    /// How long a shutdown waits for what is in flight, and when it began.
    shutdown_timeout: i64 = 2_000,
    stopping_since: ?i64 = null,

    fn handler(self: *Process) net.Handler {
        return .{ .ptr = self, .handle = handle };
    }

    /// The whole HTTP surface.
    fn handle(ptr: ?*anyopaque, exchange: *net.Exchange) void {
        const self: *Process = @ptrCast(@alignCast(ptr.?));
        const target = path_of(exchange.target);

        if (std.mem.eql(u8, target, "/") and std.mem.eql(u8, exchange.method, "POST")) {
            return self.handle_rpc(exchange);
        }
        if (std.mem.eql(u8, target, "/ready") and std.mem.eql(u8, exchange.method, "GET")) {
            return self.handle_ready(exchange);
        }
        if (std.mem.eql(u8, target, "/metrics") and std.mem.eql(u8, exchange.method, "GET")) {
            return self.handle_metrics(exchange);
        }
        // The REST surface these paths used to serve is gone; an SDK still
        // calling it needs to be told, not 404'd into a guess.
        for ([_][]const u8{ "/promises", "/schedules", "/tasks" }) |legacy| {
            if (std.mem.eql(u8, target, legacy) or
                (std.mem.startsWith(u8, target, legacy) and target.len > legacy.len and target[legacy.len] == '/'))
            {
                return exchange.respond(
                    410,
                    "application/json",
                    "{\"error\":\"This endpoint is no longer supported. Please update to the latest SDK.\"}",
                );
            }
        }
        if (std.mem.eql(u8, target, "/")) {
            return exchange.respond(405, "text/plain", "the protocol endpoint takes POST\n");
        }
        exchange.respond(404, "text/plain", "not found\n");
    }

    fn path_of(target: []const u8) []const u8 {
        const query = std.mem.indexOfScalar(u8, target, '?') orelse return target;
        return target[0..query];
    }

    const Rpc = struct {
        exchange: *net.Exchange,
        arena: std.heap.ArenaAllocator,
        request: server_mod.Request,

        fn on_answered(request: *server_mod.Request) void {
            const self: *Rpc = @ptrCast(@alignCast(request.context.?));
            const status: u16 = if (request.status >= 100 and request.status <= 599)
                @intCast(request.status)
            else
                500;
            // Copied out before answering, because answering may be the last thing
            // that happens to this connection: a write that fails closes it, and
            // `self` lives in the arena that goes with it.
            var arena = self.arena;
            const exchange = self.exchange;
            // The status travels twice: in the envelope, because that is the
            // protocol, and as the HTTP status, because that is the transport.
            // A client reads whichever it has.
            exchange.respond(status, "application/json", request.response);
            // The answer has been copied into the connection's own buffer, so
            // everything this request allocated can go. Without this the arena
            // outlived the request that made it, which is a few kilobytes per
            // request and a server that dies of them in a day.
            arena.deinit();
        }
    };

    fn handle_rpc(self: *Process, exchange: *net.Exchange) void {
        // Two arenas, and they are not the same one. The exchange's holds what the
        // *transport* needs and is reset when the response has been written; this
        // request's holds what the *protocol* needs — the parsed envelope, the
        // decided answer — and `Rpc.on_answered` frees it once the answer has been
        // copied out.
        const rpc = exchange.arena.create(Rpc) catch {
            return exchange.respond(503, "text/plain", "out of memory\n");
        };
        rpc.* = .{
            .exchange = exchange,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .request = undefined,
        };
        rpc.request = .{
            .body = exchange.body,
            .arena = &rpc.arena,
            .callback = Rpc.on_answered,
            .context = rpc,
        };
        self.runtime.server.process(&rpc.request);
    }

    const Probe = struct {
        exchange: *net.Exchange,
        readiness: server_mod.Server.Readiness,

        fn on_probed(readiness: *server_mod.Server.Readiness) void {
            const self: *Probe = @ptrCast(@alignCast(readiness.context.?));
            if (readiness.ok) {
                self.exchange.respond(200, "text/plain", "ready\n");
            } else {
                self.exchange.respond(503, "text/plain", "the store did not answer\n");
            }
        }
    };

    fn handle_ready(self: *Process, exchange: *net.Exchange) void {
        const probe = exchange.arena.create(Probe) catch {
            return exchange.respond(503, "text/plain", "out of memory\n");
        };
        probe.* = .{
            .exchange = exchange,
            .readiness = .{
                .arena = exchange.arena,
                .key_buf = std.ArrayList(u8).init(exchange.arena),
                .callback = Probe.on_probed,
                .context = probe,
            },
        };
        self.runtime.server.ready(&probe.readiness);
    }

    /// Prometheus text exposition, hand written. Ten counters do not need a
    /// registry.
    fn handle_metrics(self: *Process, exchange: *net.Exchange) void {
        var out = std.ArrayList(u8).init(exchange.arena);
        const w = out.writer();
        const runtime = self.runtime;
        const rows = .{
            .{ "resonate_requests_total", "Protocol requests answered", runtime.server.requests },
            .{ "resonate_commits_total", "Documents committed", runtime.applier.commits },
            .{ "resonate_contentions_total", "Commits that lost a race and were re-decided", runtime.applier.contentions },
            .{ "resonate_conflicts_total", "Conditional writes the store could not order", runtime.applier.conflicts },
            .{ "resonate_cache_hits_total", "Documents served from memory", runtime.applier.cache.hits },
            .{ "resonate_cache_misses_total", "Documents read from the store", runtime.applier.cache.misses },
            .{ "resonate_messages_sent_total", "Messages handed to a transport", runtime.sender.sent },
            .{ "resonate_messages_delivered_total", "Messages a worker accepted", runtime.sender.delivered },
            .{ "resonate_messages_failed_total", "Messages that did not arrive", runtime.sender.failed },
            .{ "resonate_deadlines_fired_total", "Deadlines swept", runtime.timerd.fired },
            .{ "resonate_deadlines_collected_total", "Spent deadline objects removed", runtime.timerd.collected },
            .{ "resonate_deadlines_failed_total", "Deadlines whose sweep did not commit", runtime.timerd.failed },
            .{ "resonate_connections_accepted_total", "Inbound connections accepted", self.http_server.accepted },
            .{ "resonate_client_failures_total", "Outbound requests that did not complete", self.client.failures },
        };
        inline for (rows) |row| {
            w.print("# HELP {s} {s}\n# TYPE {s} counter\n{s} {d}\n", .{
                row[0], row[1], row[0], row[0], row[2],
            }) catch {};
        }
        w.print("# HELP resonate_deadlines_armed Deadlines held in memory\n# TYPE resonate_deadlines_armed gauge\nresonate_deadlines_armed {d}\n", .{runtime.timerd.armed_count()}) catch {};
        w.print("# TYPE resonate_messages_pending gauge\nresonate_messages_pending {d}\n", .{runtime.sender.pending()}) catch {};
        w.print("# HELP resonate_cache_bytes What the documents in memory weigh\n# TYPE resonate_cache_bytes gauge\nresonate_cache_bytes {d}\n", .{runtime.applier.cache.bytes}) catch {};
        exchange.respond(200, "text/plain; version=0.0.4", out.items);
    }

    /// Rebuild the deadline queue before anything fires.
    ///
    /// Retried rather than skipped: the last process's queue died with it and the
    /// keys did not, so firing before this succeeds would fire only what this
    /// process happened to arm itself.
    fn seed(self: *Process) void {
        self.runtime.timerd.seed(on_seeded, self);
    }

    fn on_seeded(context: ?*anyopaque, armed: ?usize) void {
        const self: *Process = @ptrCast(@alignCast(context.?));
        if (armed) |count| {
            self.seeded = true;
            log("deadline queue seeded from the store: {d} armed", .{count});
            return;
        }
        log("the store did not answer the deadline listing; retrying in a second", .{});
        self.seed_timeout = .{
            .at_ms = 0,
        };
        self.seed_timeout.listen(*Process, self, on_seed_retry);
        self.loop.timer().arm(&self.seed_timeout, self.loop.clock().now_ms() + 1_000);
    }

    fn on_seed_retry(self: *Process, _: *env.Timeout) void {
        self.seed();
    }

    fn on_iteration(context: ?*anyopaque) void {
        const self: *Process = @ptrCast(@alignCast(context.?));
        // Everything that arrived in this poll gets to commit together.
        self.runtime.drain();
        if (stop_requested.load(.monotonic)) self.step_shutdown();
    }

    /// Stop taking work, let what is in flight finish, and then leave.
    ///
    /// Nothing here is needed for *safety*: every transition is committed before
    /// it is answered and every operation is idempotent, so a server that is
    /// killed outright loses nothing and a caller that was told nothing retries.
    /// What it is for is the caller that was about to be told something: a
    /// shutdown that drops a connection mid-answer turns a completed transition
    /// into a 503, and during a rolling restart that is every request in flight.
    fn step_shutdown(self: *Process) void {
        const now = self.loop.clock().now_ms();
        if (self.stopping_since == null) {
            self.stopping_since = now;
            self.http_server.close_to_new();
            log("stopping: no new connections, {d}ms for what is in flight", .{self.shutdown_timeout});
        }
        const quiet = self.runtime.applier.idle() and self.runtime.sender.pending() == 0 and
            self.http_server.gave_the_port_back();
        const out_of_time = now - self.stopping_since.? >= self.shutdown_timeout;
        if (quiet or out_of_time) {
            if (!quiet) log("stopping: time is up, with work still in flight", .{});
            log("stopped", .{});
            self.loop.stop();
        }
    }
};

/// Set by a signal handler and read by the loop. A handler may do nothing else:
/// it runs between two instructions of whatever was running, so the only safe
/// thing is to store a word somebody else reads.
var stop_requested = std.atomic.Value(bool).init(false);

fn on_stop_signal(_: i32) callconv(.C) void {
    stop_requested.store(true, .monotonic);
}

/// Take SIGTERM and SIGINT as "stop", and ignore SIGPIPE.
///
/// SIGPIPE is the one that matters even without a shutdown: a peer that hangs up
/// mid-response would otherwise end the process, and a client closing its
/// connection is not something a server dies of. The sends ask for
/// `MSG_NOSIGNAL` as well, and this is the belt to that pair of braces.
fn install_signal_handlers() void {
    var stop = posix.Sigaction{
        .handler = .{ .handler = on_stop_signal },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &stop, null);
    posix.sigaction(posix.SIG.INT, &stop, null);
    var ignore = posix.Sigaction{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &ignore, null);
}

fn log(comptime fmt: []const u8, args: anytype) void {
    const stderr = std.io.getStdErr().writer();
    stderr.print("resonate: " ++ fmt ++ "\n", args) catch {};
}

fn run(allocator: std.mem.Allocator, args: Args) !void {
    var loop = io_mod.Loop.init(allocator) catch |e| {
        log("io_uring is not available: {s}", .{@errorName(e)});
        return e;
    };
    defer loop.deinit();

    var client = net.Client.init(allocator, &loop);
    defer client.deinit();

    var push = bus_mod.HttpPush.init(allocator, &client);

    // The store: a bucket, or memory.
    var memory_store: ?*store_mod.MemoryStore = null;
    var s3: ?*s3_mod.S3 = null;
    defer {
        if (memory_store) |m| {
            m.deinit();
            allocator.destroy(m);
        }
        if (s3) |s| {
            s.deinit();
            allocator.destroy(s);
        }
    }
    const store: store_mod.Store = switch (args.store_kind) {
        .memory => blk: {
            const m = try allocator.create(store_mod.MemoryStore);
            m.* = store_mod.MemoryStore.init(allocator);
            memory_store = m;
            log("state is in memory: it goes when this process does", .{});
            break :blk m.store();
        },
        .s3 => blk: {
            const s = try allocator.create(s3_mod.S3);
            s.* = s3_mod.S3.init(allocator, &client, args.endpoint, args.bucket);
            s3 = s;
            log("state is in {s}/{s}", .{ args.endpoint, args.bucket });
            log("conditional writes are required: S3, R2, GCS and Azure have them, " ++
                "and a store that ignores them loses writes silently", .{});
            break :blk s.store();
        },
    };

    const address = try std.net.Address.parseIp(args.bind, args.port);
    const listener = try io_mod.listen(address, 512);
    // Closed by the `net.Server` built from it below: on a graceful stop by
    // `close_to_new`, which gives the port back while a loop is still running to
    // reap the accept parked on it, and otherwise by its `deinit`.
    const port = try io_mod.bound_port(listener);

    const server_url = args.server_url orelse try std.fmt.allocPrint(
        allocator,
        "http://{s}:{d}",
        .{ args.bind, port },
    );
    defer if (args.server_url == null) allocator.free(server_url);

    const runtime = try server_mod.Runtime.create(
        allocator,
        .{
            .prefix = args.prefix,
            .timer_shards = args.timer_shards,
            .server_url = server_url,
            .debug = args.debug,
            .applier = .{
                .machine = .{ .preload_limit = args.preload_limit },
                .max_cas_retries = args.cas_retries,
                .cache_entries = args.cache_entries,
                .cache_bytes = args.cache_bytes,
                .linearizable_reads = !args.sole_writer,
            },
        },
        store,
        loop.clock(),
        loop.timer(),
        push.message_bus(),
    );
    defer runtime.destroy();

    var rng = stdx.Random.init(@bitCast(std.time.milliTimestamp()));
    runtime.applier.random = &rng;

    install_signal_handlers();

    var process = Process{
        .allocator = allocator,
        .shutdown_timeout = args.shutdown_timeout,
        .loop = &loop,
        .runtime = runtime,
        .http_server = undefined,
        .client = &client,
        .push = &push,
        .memory_store = memory_store,
        .s3 = s3,
    };

    var http_server = try net.Server.init(allocator, &loop, listener, process.handler(), args.max_connections);
    defer http_server.deinit();
    process.http_server = &http_server;
    http_server.handler = process.handler();
    http_server.accept_more();

    loop.on_iteration = Process.on_iteration;
    loop.on_iteration_context = &process;

    if (args.debug) {
        log("debug mode: the clock belongs to the caller, nothing runs on wall time", .{});
    } else {
        process.seed();
    }

    if (args.sole_writer) {
        log("sole writer: cached documents are answered from without revalidating. " ++
            "Reads are stale if anything else writes this bucket", .{});
    }
    log("listening on {s}:{d}, workers answer {s}", .{ args.bind, port, server_url });
    try loop.run();
}
