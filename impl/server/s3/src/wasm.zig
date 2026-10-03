//! The server as a wasm module: `main.zig` with the event loop turned inside out.
//!
//! Natively the program owns the loop and the world calls back into it through
//! `io_uring`. Here the host owns the loop — a JS event loop, a worker runtime —
//! and calls the exports below; the program calls the host back through four
//! imports. The server, the store and the bus are the same code either way:
//! the clock and the timer are `env.Simulated` driven from the host's clock, and
//! the HTTP client is `wasm_net.zig`, which hands each request to `fetch`.
//!
//! Every export is one turn of the loop. It moves the clock to the host's now
//! (firing whatever came due), does its work, drains what that queued — which
//! is what one poll of the native loop does — and tells the host when the next
//! deadline is. The host does nothing between turns but wait.
//!
//! Memory crosses one way: the host asks for a buffer with `alloc`, writes into
//! it, passes it to an export, and frees it with `free` when that returns.
//! Nothing here keeps a host buffer past the call it was passed to.

const std = @import("std");
const stdx = @import("stdx.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const env = @import("env.zig");
const server_mod = @import("server.zig");
const s3_mod = @import("s3.zig");
const bus_mod = @import("bus.zig");
const net = @import("wasm_net.zig");

// ── Imports ───────────────────────────────────────────────────────────────────

/// Wall time, in Unix milliseconds. A float because that is what a JS number is.
extern "env" fn host_now_ms() f64;
/// Call `on_timer` at or after `at_ms`. Replaces any earlier request; a negative
/// `at_ms` cancels it. One timer is enough because the program keeps its own.
extern "env" fn host_set_timer(at_ms: f64) void;
/// Answer the request the host gave `id`. Called once per request, possibly
/// inside the export that received it, possibly turns later.
extern "env" fn host_respond(
    id: u32,
    status: u32,
    content_type_ptr: [*]const u8,
    content_type_len: usize,
    body_ptr: [*]const u8,
    body_len: usize,
) void;
extern "env" fn host_log(ptr: [*]const u8, len: usize) void;

// ── Panics ────────────────────────────────────────────────────────────────────

/// Say why, then trap. Every assertion in this program is an `unreachable`, and
/// a wasm trap on its own says nothing about which one.
pub const panic = std.debug.FullPanic(on_panic);

fn on_panic(message: []const u8, _: ?usize) noreturn {
    log("panic: {s}", .{message});
    @trap();
}

fn log(comptime fmt: []const u8, args: anytype) void {
    var buf: [1024]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, "resonate: " ++ fmt, args) catch buf[0..];
    host_log(line.ptr, line.len);
}

// ── Memory ────────────────────────────────────────────────────────────────────

const allocator = std.heap.wasm_allocator;

export fn alloc(len: usize) ?[*]u8 {
    const buf = allocator.alloc(u8, @max(len, 1)) catch return null;
    return buf.ptr;
}

export fn free(ptr: [*]u8, len: usize) void {
    allocator.free(ptr[0..@max(len, 1)]);
}

// ── The process ───────────────────────────────────────────────────────────────

const Process = struct {
    sim: env.Simulated,
    client: net.Client,
    push: bus_mod.HttpPush,
    memory_store: ?*store_mod.MemoryStore = null,
    s3: ?*s3_mod.S3 = null,
    runtime: *server_mod.Runtime,
    rng: stdx.Random,
    /// Strings the runtime and the store hold for the life of the process.
    strings: std.heap.ArenaAllocator,
    seed_timeout: env.Timeout = .{ .at_ms = 0 },
    seeded: bool = false,
};

var process: ?*Process = null;

fn now() i64 {
    return @intFromFloat(host_now_ms());
}

/// The start of a turn: the clock is the host's, and what came due fires.
fn enter() *Process {
    const p = process orelse @panic("called before init");
    _ = p.sim.advance_to(@max(now(), p.sim.now));
    return p;
}

/// The end of a turn: commit what the turn queued, fire what that made due,
/// and tell the host when to come back.
fn leave(p: *Process) void {
    p.runtime.drain();
    var guard: u32 = 0;
    while (p.sim.tick() > 0) : (guard += 1) {
        p.runtime.drain();
        if (guard == 1000) break;
    }
    const next = p.sim.next_deadline() orelse return host_set_timer(-1);
    host_set_timer(@floatFromInt(next));
}

/// Start the server. `flags`: bit 0 is debug mode, bit 1 keeps state in memory
/// instead of the bucket. `request_timeout_ms` bounds every store request; 0
/// takes the default. Returns 0, or 1 if it could not start.
export fn init(
    endpoint_ptr: [*]const u8,
    endpoint_len: usize,
    bucket_ptr: [*]const u8,
    bucket_len: usize,
    prefix_ptr: [*]const u8,
    prefix_len: usize,
    server_url_ptr: [*]const u8,
    server_url_len: usize,
    flags: u32,
    request_timeout_ms: u32,
) u32 {
    if (process != null) return 1;
    start(
        endpoint_ptr[0..endpoint_len],
        bucket_ptr[0..bucket_len],
        prefix_ptr[0..prefix_len],
        server_url_ptr[0..server_url_len],
        flags & 1 != 0,
        flags & 2 != 0,
        request_timeout_ms,
    ) catch |e| {
        log("could not start: {s}", .{@errorName(e)});
        return 1;
    };
    return 0;
}

fn start(
    endpoint_in: []const u8,
    bucket_in: []const u8,
    prefix_in: []const u8,
    server_url_in: []const u8,
    debug: bool,
    in_memory: bool,
    request_timeout_ms: u32,
) !void {
    const p = try allocator.create(Process);
    p.* = .{
        .sim = env.Simulated.init(allocator, now()),
        .client = undefined,
        .push = undefined,
        .runtime = undefined,
        .rng = stdx.Random.init(@bitCast(now())),
        .strings = std.heap.ArenaAllocator.init(allocator),
    };
    // After `p` has its address: the client keeps pointers to the clock and the
    // timer, which live in `p.sim`.
    p.client = net.Client.init(allocator, p.sim.clock(), p.sim.timer());
    if (request_timeout_ms > 0) p.client.request_timeout_ms = request_timeout_ms;
    p.push = bus_mod.HttpPush.init(allocator, &p.client);
    const a = p.strings.allocator();
    const endpoint = try a.dupe(u8, endpoint_in);
    const bucket = try a.dupe(u8, bucket_in);
    const server_url = try a.dupe(u8, server_url_in);

    const store: store_mod.Store = if (in_memory) blk: {
        const m = try allocator.create(store_mod.MemoryStore);
        m.* = store_mod.MemoryStore.init(allocator);
        p.memory_store = m;
        log("state is in memory: it goes when this module does", .{});
        break :blk m.store();
    } else blk: {
        const s = try allocator.create(s3_mod.S3);
        s.* = s3_mod.S3.init(allocator, &p.client, endpoint, bucket);
        p.s3 = s;
        log("state is in {s}/{s}", .{ endpoint, bucket });
        break :blk s.store();
    };

    p.runtime = try server_mod.Runtime.create(
        allocator,
        .{
            .prefix = try a.dupe(u8, prefix_in),
            .timer_shards = store_mod.KeySpace.default_timer_shards,
            .server_url = server_url,
            .debug = debug,
            .applier = .{
                .machine = .{ .preload_limit = protocol.preload_limit_default },
                .max_cas_retries = 8,
                .cache_entries = 4096,
                .cache_bytes = 64 << 20,
                .linearizable_reads = true,
            },
        },
        store,
        p.sim.clock(),
        p.sim.timer(),
        p.push.message_bus(),
    );
    p.runtime.applier.random = &p.rng;
    process = p;

    if (debug) {
        log("debug mode: the clock belongs to the caller, nothing runs on wall time", .{});
    } else {
        seed(p);
    }
    log("ready, workers answer {s}", .{server_url});
    leave(p);
}

/// Rebuild the deadline queue before anything fires; retried, as in `main.zig`.
fn seed(p: *Process) void {
    p.runtime.timerd.seed(on_seeded, p);
}

fn on_seeded(context: ?*anyopaque, armed: ?usize) void {
    const p: *Process = @ptrCast(@alignCast(context.?));
    if (armed) |count| {
        p.seeded = true;
        log("deadline queue seeded from the store: {d} armed", .{count});
        return;
    }
    log("the store did not answer the deadline listing; retrying in a second", .{});
    p.seed_timeout = .{ .at_ms = 0 };
    p.seed_timeout.listen(*Process, p, on_seed_retry);
    p.sim.timer().arm(&p.seed_timeout, p.sim.now + 1_000);
}

fn on_seed_retry(p: *Process, _: *env.Timeout) void {
    seed(p);
}

// ── Turns ─────────────────────────────────────────────────────────────────────

/// The host's timer fired.
export fn on_timer() void {
    leave(enter());
}

/// The host finished a `host_fetch`.
export fn on_fetch(
    id: u32,
    status: u32,
    headers_ptr: [*]const u8,
    headers_len: usize,
    body_ptr: [*]const u8,
    body_len: usize,
) void {
    const p = enter();
    p.client.complete(id, @intCast(@min(status, 999)), headers_ptr[0..headers_len], body_ptr[0..body_len]);
    leave(p);
}

const Rpc = struct {
    id: u32,
    arena: std.heap.ArenaAllocator,
    request: server_mod.Request,

    fn on_answered(request: *server_mod.Request) void {
        const self: *Rpc = @ptrCast(@alignCast(request.context.?));
        const status: u32 = if (request.status >= 100 and request.status <= 599)
            @intCast(request.status)
        else
            500;
        const content_type = "application/json";
        host_respond(self.id, status, content_type, content_type.len, request.response.ptr, request.response.len);
        self.arena.deinit();
        allocator.destroy(self);
    }
};

/// `POST /`: one protocol envelope. Answered through `host_respond(id, …)`.
export fn rpc(id: u32, body_ptr: [*]const u8, body_len: usize) void {
    const p = enter();
    defer leave(p);
    const r = allocator.create(Rpc) catch return respond_text(id, 503, "out of memory\n");
    r.* = .{ .id = id, .arena = std.heap.ArenaAllocator.init(allocator), .request = undefined };
    // The host's buffer goes when this returns; the request may outlive it.
    const body = r.arena.allocator().dupe(u8, body_ptr[0..body_len]) catch {
        r.arena.deinit();
        allocator.destroy(r);
        return respond_text(id, 503, "out of memory\n");
    };
    r.request = .{ .body = body, .arena = &r.arena, .callback = Rpc.on_answered, .context = r };
    p.runtime.server.process(&r.request);
}

const Probe = struct {
    id: u32,
    arena: std.heap.ArenaAllocator,
    readiness: server_mod.Server.Readiness,

    fn on_probed(readiness: *server_mod.Server.Readiness) void {
        const self: *Probe = @ptrCast(@alignCast(readiness.context.?));
        if (readiness.ok) {
            respond_text(self.id, 200, "ready\n");
        } else {
            respond_text(self.id, 503, "the store did not answer\n");
        }
        self.arena.deinit();
        allocator.destroy(self);
    }
};

/// `GET /ready`.
export fn ready(id: u32) void {
    const p = enter();
    defer leave(p);
    const probe = allocator.create(Probe) catch return respond_text(id, 503, "out of memory\n");
    probe.* = .{ .id = id, .arena = std.heap.ArenaAllocator.init(allocator), .readiness = undefined };
    const a = probe.arena.allocator();
    probe.readiness = .{
        .arena = a,
        .key_buf = std.ArrayList(u8).init(a),
        .callback = Probe.on_probed,
        .context = probe,
    };
    p.runtime.server.ready(&probe.readiness);
}

/// `GET /metrics`: the native server's counters, less the ones about sockets.
export fn metrics(id: u32) void {
    const p = enter();
    defer leave(p);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var out = std.ArrayList(u8).init(arena.allocator());
    const w = out.writer();
    const runtime = p.runtime;
    const rows = .{
        .{ "resonate_requests_total", "Protocol requests answered", runtime.server.requests },
        .{ "resonate_commits_total", "Documents committed", runtime.applier.commits },
        .{ "resonate_contentions_total", "Commits that lost a race and were re-decided", runtime.applier.contentions },
        .{ "resonate_timeouts_total", "Commits the store did not answer, which were re-decided", runtime.applier.timeouts },
        .{ "resonate_cache_hits_total", "Documents served from memory", runtime.applier.cache.hits },
        .{ "resonate_cache_misses_total", "Documents read from the store", runtime.applier.cache.misses },
        .{ "resonate_messages_sent_total", "Messages handed to a transport", runtime.sender.sent },
        .{ "resonate_messages_delivered_total", "Messages a worker accepted", runtime.sender.delivered },
        .{ "resonate_messages_failed_total", "Messages that did not arrive", runtime.sender.failed },
        .{ "resonate_deadlines_fired_total", "Deadlines swept", runtime.timerd.fired },
        .{ "resonate_deadlines_collected_total", "Spent deadline objects removed", runtime.timerd.collected },
        .{ "resonate_deadlines_failed_total", "Deadlines whose sweep did not commit", runtime.timerd.failed },
        .{ "resonate_client_failures_total", "Outbound requests that did not complete", p.client.failures },
    };
    inline for (rows) |row| {
        w.print("# HELP {s} {s}\n# TYPE {s} counter\n{s} {d}\n", .{
            row[0], row[1], row[0], row[0], row[2],
        }) catch {};
    }
    w.print("# TYPE resonate_deadlines_armed gauge\nresonate_deadlines_armed {d}\n", .{runtime.timerd.armed_count()}) catch {};
    w.print("# TYPE resonate_messages_pending gauge\nresonate_messages_pending {d}\n", .{runtime.sender.pending()}) catch {};
    const content_type = "text/plain; version=0.0.4";
    host_respond(id, 200, content_type, content_type.len, out.items.ptr, out.items.len);
}

fn respond_text(id: u32, status: u32, body: []const u8) void {
    const content_type = "text/plain";
    host_respond(id, status, content_type, content_type.len, body.ptr, body.len);
}
