//! The server: one envelope in, one envelope out.
//!
//! Everything below this file is a subsystem that answers one shape of request.
//! This is where an envelope is admitted, where `now` is resolved, and where a
//! request kind is turned into the subsystem that owns it. No decision about
//! *what* an operation does is made here, and none about how it is carried —
//! that is the transport's, and this file has no idea whether it is being spoken
//! to over a socket or by a simulator.
//!
//! ## The clock is the caller's, or it is not
//!
//! Under the debug flag, `head["resonate:debug_time"]` is honoured, the `debug.*`
//! operations are answered, and nothing in the process runs on wall time. The
//! gate is here rather than at the HTTP edge so that every caller of the server
//! is subject to it: a client must not be able to move a production server's
//! clock, and a test must be able to move a test server's.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const env = @import("env.zig");
const handle = @import("handle.zig");
const applier_mod = @import("applier.zig");
const scan = @import("scan.zig");
const schedules = @import("schedules.zig");
const timerd_mod = @import("timerd.zig");
const sender_mod = @import("sender.zig");

const assert = stdx.assert;
const KeySpace = store_mod.KeySpace;

pub const Options = struct {
    /// Key prefix inside the bucket. Normalized to end in `/`, or empty.
    prefix: []const u8 = "",
    timer_shards: u32 = KeySpace.default_timer_shards,
    /// Where workers answer. Stamped into every offer.
    server_url: []const u8 = "",
    /// The clock belongs to the caller. See the module comment.
    debug: bool = false,
    applier: applier_mod.Config = .{},
};

/// One request through the server, from bytes to bytes.
///
/// Allocated by the caller, in an arena the caller frees once it has the
/// response. The sub-request it routes to lives inside it, so a request costs
/// one allocation however many subsystems it passes through.
pub const Request = struct {
    body: []const u8,
    arena: *std.heap.ArenaAllocator,
    callback: *const fn (*Request) void,
    context: ?*anyopaque = null,

    /// The status, which is also the HTTP status the transport should use.
    status: i32 = 0,
    /// The whole response envelope, as JSON.
    response: []const u8 = "",

    server: *Server = undefined,
    kind: []const u8 = "unknown",
    corr_id: []const u8 = "0",
    inner: union {
        none: void,
        work: applier_mod.Work,
        read: scan.Request,
        schedule: schedules.Request,
        tick: timerd_mod.Timerd.Tick,
    } = .{ .none = {} },
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    options: Options,
    keys: KeySpace,
    clock: env.Clock,

    sender: *sender_mod.Sender,
    applier: *applier_mod.Applier,
    scanner: *scan.Scanner,
    schedule_service: *schedules.Service,
    timerd: *timerd_mod.Timerd,

    /// Wall time never goes backwards as far as this server is concerned. A
    /// clock that stepped back would let a later request decide at an earlier
    /// instant than an earlier one, which no amount of care below could repair.
    monotonic: stdx.MonotonicMillis = .{},

    requests: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        options: Options,
        keys: KeySpace,
        clock: env.Clock,
        sender: *sender_mod.Sender,
        applier: *applier_mod.Applier,
        scanner: *scan.Scanner,
        schedule_service: *schedules.Service,
        timerd: *timerd_mod.Timerd,
    ) Server {
        return .{
            .allocator = allocator,
            .options = options,
            .keys = keys,
            .clock = clock,
            .sender = sender,
            .applier = applier,
            .scanner = scanner,
            .schedule_service = schedule_service,
            .timerd = timerd,
        };
    }

    /// Answer one request.
    ///
    /// The callback may run before this returns — an in-memory store completes
    /// inline — or long after, from a completion. A caller must handle both, and
    /// must not touch the request between the two.
    pub fn process(self: *Server, req: *Request) void {
        req.server = self;
        self.requests += 1;
        const a = req.arena.allocator();

        const parsed = protocol.parse_envelope(a, req.body);
        const envelope = switch (parsed) {
            .ok => |e| e,
            .invalid => |invalid| {
                // A body that is not a request never reaches a subsystem. The
                // kind and correlation id are salvaged so that even this answer
                // can be matched to what caused it.
                const salvaged = protocol.salvage_context(a, req.body);
                req.kind = salvaged.kind;
                req.corr_id = salvaged.corr_id;
                var buf = std.ArrayList(u8).init(a);
                const message = protocol.invalid_message(&buf, invalid) catch "Invalid request envelope";
                return self.reply_error(req, 400, message);
            },
        };
        req.kind = envelope.kind;
        req.corr_id = envelope.corr_id;

        // The console's namespace is not served here. It is a read model shaped
        // for one screen at a time, and refusing it at the worker endpoint is
        // what keeps an SDK from coming to depend on a request that exists to
        // draw a table.
        if (std.mem.startsWith(u8, envelope.kind, "ui.")) {
            return self.reply_error(
                req,
                404,
                "Console requests ('ui.*') are served on the console's own endpoint, not here",
            );
        }

        const now = self.resolve_time(envelope.debug_time);

        if (handle.Op.parse(envelope.kind)) |op| {
            req.inner = .{ .work = .{
                .kind = .{ .request = op },
                .corr_id = envelope.corr_id,
                .data = envelope.data,
                .now = now,
                .arena = a,
                .callback = on_work,
                .context = req,
            } };
            const origin = handle.origin_of_request(op, envelope.data) orelse "";
            self.applier.submit(origin, &req.inner.work);
            self.applier.drain();
            return;
        }

        if (schedules.Kind.parse(envelope.kind)) |kind| {
            req.inner = .{ .schedule = .{
                .kind = kind,
                .corr_id = envelope.corr_id,
                .data = envelope.data,
                .now = now,
                .arena = req.arena,
                .callback = on_schedule,
                .context = req,
            } };
            self.schedule_service.submit(&req.inner.schedule);
            self.applier.drain();
            return;
        }

        const is_debug_kind = std.mem.startsWith(u8, envelope.kind, "debug.");
        if (is_debug_kind and !self.options.debug) {
            return self.reply_error(req, 403, "Debug operations are disabled");
        }

        if (std.mem.eql(u8, envelope.kind, "debug.tick")) {
            const time = blk: {
                const v = envelope.data.get("time") orelse
                    return self.reply_error(req, 400, "Missing or invalid 'time' field");
                break :blk v.as_i64() orelse
                    return self.reply_error(req, 400, "Missing or invalid 'time' field");
            };
            if (envelope.debug_time) |dt| {
                // Two ways of saying the same thing, and they have to agree — a
                // request that says one instant in its head and another in its
                // body is a request nobody meant.
                if (dt != time) {
                    return self.reply_error(req, 400, "resonate:debug_time must equal data.time");
                }
            }
            req.inner = .{ .tick = .{
                .timerd = self.timerd,
                .time = time,
                .arena = req.arena,
                .callback = on_tick,
                .context = req,
            } };
            self.timerd.tick(&req.inner.tick);
            self.applier.drain();
            return;
        }

        if (scan.Kind.parse(envelope.kind)) |kind| {
            req.inner = .{ .read = .{
                .kind = kind,
                .corr_id = envelope.corr_id,
                .data = envelope.data,
                .now = now,
                .arena = req.arena,
                .callback = on_read,
                .context = req,
            } };
            self.scanner.submit(&req.inner.read);
            self.applier.drain();
            return;
        }

        // Whether a kind names an operation is the server's to answer: it is the
        // only party that knows what it implements.
        const message = std.fmt.allocPrint(a, "Unknown operation: {s}", .{envelope.kind}) catch
            "Unknown operation";
        self.reply_error(req, 400, message);
    }

    /// The instant an operation decides at.
    fn resolve_time(self: *Server, debug_time: ?i64) i64 {
        if (self.options.debug) {
            if (debug_time) |t| return t;
        }
        return self.monotonic.advance(self.clock.now_ms());
    }

    fn on_work(work: *applier_mod.Work) void {
        const req: *Request = @ptrCast(@alignCast(work.context.?));
        req.server.reply(req, work.status, work.reply_data);
    }

    fn on_read(read: *scan.Request) void {
        const req: *Request = @ptrCast(@alignCast(read.context.?));
        req.server.reply(req, read.status, read.reply_data);
    }

    fn on_schedule(sched: *schedules.Request) void {
        const req: *Request = @ptrCast(@alignCast(sched.context.?));
        req.server.reply(req, sched.status, sched.reply_data);
    }

    fn on_tick(t: *timerd_mod.Timerd.Tick) void {
        const req: *Request = @ptrCast(@alignCast(t.context.?));
        req.server.reply(req, t.status, t.reply_data);
    }

    fn reply_error(self: *Server, req: *Request, status: i32, message: []const u8) void {
        const a = req.arena.allocator();
        var buf = std.ArrayList(u8).init(a);
        json.write_string(&buf, message) catch {};
        self.reply(req, status, buf.items);
    }

    /// Wrap a subsystem's answer in the response envelope.
    fn reply(self: *Server, req: *Request, status: i32, data: []const u8) void {
        _ = self;
        const a = req.arena.allocator();
        var out = std.ArrayList(u8).init(a);
        var w = json.Writer.init(&out);
        w.object_begin() catch {};
        w.field_string("kind", req.kind) catch {};
        w.key("head") catch {};
        w.object_begin() catch {};
        w.field_string("corrId", req.corr_id) catch {};
        w.field_int("status", status) catch {};
        w.field_string("version", protocol.protocol_version) catch {};
        w.object_end() catch {};
        w.key("data") catch {};
        w.raw(if (data.len > 0) data else "null") catch {};
        w.object_end() catch {};

        req.status = status;
        req.response = out.items;
        req.callback(req);
    }

    /// Can this server serve right now?
    ///
    /// It asks the bucket. A process that is up but whose storage has gone away
    /// should report unready rather than take traffic it cannot serve.
    pub fn ready(self: *Server, probe: *Readiness) void {
        probe.server = self;
        const prefix = self.keys.doc_prefix(&probe.key_buf) catch {
            probe.ok = false;
            probe.callback(probe);
            return;
        };
        probe.op = .{
            .kind = .list,
            .key = prefix,
            .max_keys = 1,
            .arena = probe.arena,
            .callback = Readiness.on_listed,
            .context = probe,
        };
        self.applier.store.submit(&probe.op);
    }

    pub const Readiness = struct {
        arena: std.mem.Allocator,
        key_buf: std.ArrayList(u8),
        callback: *const fn (*Readiness) void,
        context: ?*anyopaque = null,
        ok: bool = false,
        server: *Server = undefined,
        op: store_mod.Operation = undefined,

        fn on_listed(op: *store_mod.Operation) void {
            const self: *Readiness = @ptrCast(@alignCast(op.context.?));
            self.ok = op.result == .keys;
            self.callback(self);
        }
    };
};

// ── Wiring ────────────────────────────────────────────────────────────────────

/// Every subsystem, owned together.
///
/// The composition root. A caller supplies the environment — a store, a clock, a
/// timer, a message bus — and gets a server; what the environment is made of is
/// none of this file's business, which is what lets the simulator build one out
/// of nothing but memory and a seed.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    options: Options,
    keys: KeySpace,
    prefix_owned: []const u8,

    sender: sender_mod.Sender,
    applier: applier_mod.Applier,
    scanner: scan.Scanner,
    schedule_service: schedules.Service,
    timerd: timerd_mod.Timerd,
    server: Server,

    pub fn create(
        allocator: std.mem.Allocator,
        options: Options,
        store: store_mod.Store,
        clock: env.Clock,
        timer: env.Timer,
        bus: env.MessageBus,
    ) !*Runtime {
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);

        const prefix = try KeySpace.normalize_prefix(allocator, options.prefix);
        errdefer if (prefix.len > 0) allocator.free(prefix);
        const keys = KeySpace.init(prefix, options.timer_shards);

        self.* = .{
            .allocator = allocator,
            .options = options,
            .keys = keys,
            .prefix_owned = prefix,
            .sender = sender_mod.Sender.init(allocator, bus, options.server_url),
            .applier = undefined,
            .scanner = undefined,
            .schedule_service = undefined,
            .timerd = undefined,
            .server = undefined,
        };
        // Under the debug flag the outbox holds rather than delivers, so that
        // `debug.snap` reports exactly what the server decided to send and no
        // background delivery settles anything a recorded trace did not ask for.
        self.sender.hold = options.debug;

        self.applier = applier_mod.Applier.init(
            allocator,
            store,
            keys,
            clock,
            timer,
            &self.sender,
            options.applier,
        );
        self.scanner = scan.Scanner.init(allocator, store, keys, &self.sender);
        self.schedule_service = schedules.Service.init(allocator, store, keys, &self.applier);
        self.timerd = timerd_mod.Timerd.init(
            allocator,
            store,
            keys,
            clock,
            timer,
            &self.applier,
            &self.schedule_service,
        );
        self.timerd.paused = options.debug;

        // The two writers tell the daemon what they armed, so the firing loop
        // never has to list for it.
        self.applier.deadline_hook = on_origin_deadline;
        self.applier.deadline_context = self;
        self.schedule_service.deadline_hook = on_schedule_deadline;
        self.schedule_service.deadline_context = self;
        // A reset empties the bucket, so everything held in memory is a ghost.
        self.scanner.on_reset = on_reset;
        self.scanner.on_reset_context = self;

        self.server = Server.init(
            allocator,
            options,
            keys,
            clock,
            &self.sender,
            &self.applier,
            &self.scanner,
            &self.schedule_service,
            &self.timerd,
        );
        return self;
    }

    pub fn destroy(self: *Runtime) void {
        self.timerd.deinit();
        self.applier.deinit();
        self.sender.deinit();
        if (self.prefix_owned.len > 0) self.allocator.free(self.prefix_owned);
        self.allocator.destroy(self);
    }

    fn on_origin_deadline(context: ?*anyopaque, origin: []const u8, at: i64) void {
        const self: *Runtime = @ptrCast(@alignCast(context.?));
        self.timerd.arm_origin(origin, at);
    }

    fn on_schedule_deadline(context: ?*anyopaque, id: []const u8, at: i64) void {
        const self: *Runtime = @ptrCast(@alignCast(context.?));
        self.timerd.arm_schedule(id, at);
    }

    fn on_reset(context: ?*anyopaque) void {
        const self: *Runtime = @ptrCast(@alignCast(context.?));
        self.applier.reset();
        self.timerd.reset();
    }

    /// Run whatever is runnable. The transport calls this once per poll of its
    /// event loop, after submitting everything the poll produced.
    pub fn drain(self: *Runtime) void {
        self.applier.drain();
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

const Harness = struct {
    allocator: std.mem.Allocator,
    sim: env.Simulated,
    mem: store_mod.MemoryStore,
    delivered: std.ArrayList([]u8),
    runtime: *Runtime,

    fn create(allocator: std.mem.Allocator, debug: bool) !*Harness {
        const self = try allocator.create(Harness);
        self.* = .{
            .allocator = allocator,
            .sim = env.Simulated.init(allocator, 1_000_000_000),
            .mem = store_mod.MemoryStore.init(allocator),
            .delivered = std.ArrayList([]u8).init(allocator),
            .runtime = undefined,
        };
        self.runtime = try Runtime.create(
            allocator,
            .{ .prefix = "res", .server_url = "http://server:8001", .debug = debug },
            self.mem.store(),
            self.sim.clock(),
            self.sim.timer(),
            self.bus(),
        );
        return self;
    }

    fn destroy(self: *Harness) void {
        self.runtime.destroy();
        for (self.delivered.items) |d| self.allocator.free(d);
        self.delivered.deinit();
        self.mem.deinit();
        self.sim.deinit();
        self.allocator.destroy(self);
    }

    fn bus(self: *Harness) env.MessageBus {
        return .{ .ptr = self, .vtable = &.{ .send = send, .serves = serves } };
    }
    fn serves(_: *anyopaque, _: []const u8) bool {
        return true;
    }
    fn send(ptr: *anyopaque, d: *env.Delivery) void {
        const self: *Harness = @ptrCast(@alignCast(ptr));
        self.delivered.append(self.allocator.dupe(u8, d.body) catch unreachable) catch unreachable;
        d.complete(.delivered, "");
    }

    const Answer = struct {
        status: i32 = 0,
        body: []const u8 = "",
        done: bool = false,
        fn callback(req: *Request) void {
            const self: *Answer = @ptrCast(@alignCast(req.context.?));
            self.status = req.status;
            self.body = req.response;
            self.done = true;
        }
    };

    /// Send one envelope, exactly as a transport would.
    fn post(self: *Harness, arena: *std.heap.ArenaAllocator, body: []const u8) !Answer {
        var answer = Answer{};
        var req = Request{
            .body = body,
            .arena = arena,
            .callback = Answer.callback,
            .context = &answer,
        };
        self.runtime.server.process(&req);
        var guard: usize = 0;
        while (!answer.done) {
            guard += 1;
            if (guard > 1000) return error.NeverAnswered;
            self.runtime.drain();
            _ = self.sim.tick();
        }
        return answer;
    }

    fn envelope(
        arena: *std.heap.ArenaAllocator,
        kind: []const u8,
        data: []const u8,
        debug_time: ?i64,
    ) ![]const u8 {
        const a = arena.allocator();
        if (debug_time) |t| {
            return std.fmt.allocPrint(
                a,
                "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"c1\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
                .{ kind, protocol.protocol_version, t, data },
            );
        }
        return std.fmt.allocPrint(
            a,
            "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"c1\",\"version\":\"{s}\"}},\"data\":{s}}}",
            .{ kind, protocol.protocol_version, data },
        );
    }
};

test "a request goes in as an envelope and comes back as one" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const answer = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}",
        null,
    ));
    try testing.expectEqual(@as(i32, 200), answer.status);
    try testing.expect(std.mem.startsWith(u8, answer.body, "{\"kind\":\"promise.create\",\"head\":{\"corrId\":\"c1\",\"status\":200,\"version\":\"2026-04-01\"},\"data\":{"));

    // And the reply parses back into something a client can read.
    const parsed = try json.parse(arena.allocator(), answer.body);
    try testing.expectEqualStrings("promise.create", parsed.get_string("kind").?);
    try testing.expectEqual(@as(i64, 200), parsed.get("head").?.get_i64("status").?);
    try testing.expectEqualStrings("o:a", parsed.get("data").?.get("promise").?.get_string("id").?);
}

test "an envelope the protocol does not admit is refused at the edge" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const cases = [_]struct { body: []const u8, kind: []const u8, corr_id: []const u8, needle: []const u8 }{
        .{ .body = "not json", .kind = "unknown", .corr_id = "0", .needle = "Invalid request envelope" },
        .{
            .body = "{\"kind\":\"\",\"head\":{\"corrId\":\"c9\",\"version\":\"2026-04-01\"},\"data\":{}}",
            .kind = "",
            .corr_id = "c9",
            .needle = "Missing or invalid 'kind' field",
        },
        .{
            .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"c9\",\"version\":\"2026-04-01\"},\"data\":[]}",
            .kind = "promise.get",
            .corr_id = "c9",
            .needle = "Invalid 'data' field",
        },
        .{
            .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"c9\",\"version\":\"1999-01-01\"},\"data\":{}}",
            .kind = "promise.get",
            .corr_id = "c9",
            .needle = "Unsupported protocol version '1999-01-01'",
        },
    };
    for (cases) |c| {
        const answer = try h.post(&arena, c.body);
        try testing.expectEqual(@as(i32, 400), answer.status);
        const parsed = try json.parse(arena.allocator(), answer.body);
        // Even a rejection can be correlated to what caused it.
        try testing.expectEqualStrings(c.kind, parsed.get_string("kind").?);
        try testing.expectEqualStrings(c.corr_id, parsed.get("head").?.get_string("corrId").?);
        try testing.expect(std.mem.indexOf(u8, parsed.get("data").?.as_string().?, c.needle) != null);
    }
}

test "an unknown operation says so, and the console's namespace is not served here" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const unknown = try h.post(&arena, try Harness.envelope(&arena, "promise.explode", "{}", null));
    try testing.expectEqual(@as(i32, 400), unknown.status);
    try testing.expect(std.mem.indexOf(u8, unknown.body, "Unknown operation: promise.explode") != null);

    const ui = try h.post(&arena, try Harness.envelope(&arena, "ui.executions.search", "{}", null));
    try testing.expectEqual(@as(i32, 404), ui.status);
    try testing.expect(std.mem.indexOf(u8, ui.body, "console's own endpoint") != null);
}

test "the debug namespace is closed unless the clock belongs to the caller" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    for ([_][]const u8{ "debug.snap", "debug.reset", "debug.tick" }) |kind| {
        const answer = try h.post(&arena, try Harness.envelope(&arena, kind, "{\"time\":1}", null));
        try testing.expectEqual(@as(i32, 403), answer.status);
        try testing.expect(std.mem.indexOf(u8, answer.body, "Debug operations are disabled") != null);
    }

    // And a client cannot move a production server's clock.
    const answer = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}",
        5, // long in the past: would make the promise be born timed out
    ));
    try testing.expect(std.mem.indexOf(u8, answer.body, "\"state\":\"pending\"") != null);
}

test "under the debug flag the caller owns the clock and the debug namespace answers" {
    const h = try Harness.create(testing.allocator, true);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const created = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":2000000,\"tags\":{\"resonate:target\":\"http://w\"}}",
        1_000_000,
    ));
    try testing.expectEqual(@as(i32, 200), created.status);
    try testing.expect(std.mem.indexOf(u8, created.body, "\"createdAt\":1000000") != null);

    // Nothing was delivered: the outbox holds so that the snapshot can report it.
    try testing.expectEqual(@as(usize, 0), h.delivered.items.len);

    const snap = try h.post(&arena, try Harness.envelope(&arena, "debug.snap", "{}", 1_000_000));
    try testing.expectEqual(@as(i32, 200), snap.status);
    const data = (try json.parse(arena.allocator(), snap.body)).get("data").?;
    try testing.expectEqual(@as(usize, 1), data.get("promises").?.as_array().?.len);
    try testing.expectEqual(@as(usize, 1), data.get("tasks").?.as_array().?.len);
    try testing.expectEqual(@as(usize, 1), data.get("messages").?.as_array().?.len);
    try testing.expectEqual(@as(usize, 1), data.get("promiseTimeouts").?.as_array().?.len);

    // The tick is the only thing that moves time, and it must agree with itself.
    const disagree = try h.post(&arena, try Harness.envelope(&arena, "debug.tick", "{\"time\":9}", 1_000_000));
    try testing.expectEqual(@as(i32, 400), disagree.status);
    try testing.expect(std.mem.indexOf(u8, disagree.body, "must equal data.time") != null);

    const missing = try h.post(&arena, try Harness.envelope(&arena, "debug.tick", "{}", null));
    try testing.expectEqual(@as(i32, 400), missing.status);
    try testing.expect(std.mem.indexOf(u8, missing.body, "Missing or invalid 'time' field") != null);

    // Past the promise's deadline.
    const tick = try h.post(&arena, try Harness.envelope(&arena, "debug.tick", "{\"time\":2000000}", 2_000_000));
    try testing.expectEqual(@as(i32, 200), tick.status);
    // The tick has always answered with an empty array, not an object.
    try testing.expectEqual(
        @as(usize, 0),
        (try json.parse(arena.allocator(), tick.body)).get("data").?.as_array().?.len,
    );

    const got = try h.post(&arena, try Harness.envelope(&arena, "promise.get", "{\"id\":\"o:a\"}", 2_000_000));
    try testing.expect(std.mem.indexOf(u8, got.body, "rejected_timedout") != null);

    // And reset empties everything.
    const reset = try h.post(&arena, try Harness.envelope(&arena, "debug.reset", "{}", 2_000_000));
    try testing.expectEqual(@as(i32, 200), reset.status);
    try testing.expectEqual(@as(usize, 0), h.mem.count());
    const after = try h.post(&arena, try Harness.envelope(&arena, "promise.get", "{\"id\":\"o:a\"}", 2_000_000));
    try testing.expectEqual(@as(i32, 404), after.status);
}

test "every namespace routes to the subsystem that owns it" {
    const h = try Harness.create(testing.allocator, true);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const t: i64 = 1_000_000_000;

    // A promise with a task, so the task namespace has something to talk about.
    _ = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"http://w\"}}",
        t,
    ));

    const cases = [_]struct { kind: []const u8, data: []const u8, status: i32 }{
        .{ .kind = "promise.get", .data = "{\"id\":\"o:a\"}", .status = 200 },
        .{ .kind = "promise.settle", .data = "{\"id\":\"o:a\",\"state\":\"resolved\"}", .status = 200 },
        .{ .kind = "promise.register_listener", .data = "{\"awaited\":\"o:a\",\"address\":\"http://l\"}", .status = 200 },
        .{ .kind = "promise.search", .data = "{}", .status = 200 },
        .{ .kind = "task.get", .data = "{\"id\":\"o:a\"}", .status = 200 },
        .{ .kind = "task.search", .data = "{}", .status = 200 },
        .{ .kind = "task.acquire", .data = "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w\",\"ttl\":1}", .status = 409 },
        .{ .kind = "task.heartbeat", .data = "{\"pid\":\"w\",\"tasks\":[{\"id\":\"o:a\",\"version\":1}]}", .status = 200 },
        .{ .kind = "task.halt", .data = "{\"id\":\"o:a\"}", .status = 409 },
        .{ .kind = "task.continue", .data = "{\"id\":\"o:a\"}", .status = 409 },
        .{ .kind = "schedule.create", .data = "{\"id\":\"s0\",\"cron\":\"* * * * *\",\"promiseId\":\"{{.id}}.{{.timestamp}}\",\"promiseTimeout\":1,\"promiseTags\":{\"resonate:target\":\"http://w\"}}", .status = 200 },
        .{ .kind = "schedule.get", .data = "{\"id\":\"s0\"}", .status = 200 },
        .{ .kind = "schedule.search", .data = "{}", .status = 200 },
        .{ .kind = "schedule.delete", .data = "{\"id\":\"s0\"}", .status = 200 },
        .{ .kind = "debug.snap", .data = "{}", .status = 200 },
    };
    for (cases) |c| {
        const answer = try h.post(&arena, try Harness.envelope(&arena, c.kind, c.data, t));
        try testing.expectEqual(c.status, answer.status);
        const parsed = try json.parse(arena.allocator(), answer.body);
        try testing.expectEqualStrings(c.kind, parsed.get_string("kind").?);
        try testing.expectEqual(@as(i64, c.status), parsed.get("head").?.get_i64("status").?);
    }
}

test "readiness asks the bucket" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const Probe = struct {
        var ok: ?bool = null;
        fn cb(p: *Server.Readiness) void {
            ok = p.ok;
        }
    };
    Probe.ok = null;
    var probe = Server.Readiness{
        .arena = arena.allocator(),
        .key_buf = std.ArrayList(u8).init(arena.allocator()),
        .callback = Probe.cb,
    };
    h.runtime.server.ready(&probe);
    try testing.expectEqual(true, Probe.ok.?);

    var rng = stdx.Random.init(1);
    h.mem.random = &rng;
    h.mem.faults.unavailable_percent = 100;
    Probe.ok = null;
    var probe2 = Server.Readiness{
        .arena = arena.allocator(),
        .key_buf = std.ArrayList(u8).init(arena.allocator()),
        .callback = Probe.cb,
    };
    h.runtime.server.ready(&probe2);
    try testing.expectEqual(false, Probe.ok.?);
}

test "a prefix is normalized once and used everywhere" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"http://w\"}}",
        null,
    ));
    var it = h.mem.objects.keyIterator();
    var seen: usize = 0;
    while (it.next()) |k| {
        try testing.expect(std.mem.startsWith(u8, k.*, "res/"));
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 2), seen);
}

test "a worker is offered the task once the write has landed" {
    const h = try Harness.create(testing.allocator, false);
    defer h.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try h.post(&arena, try Harness.envelope(
        &arena,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"http://w:9/x\"}}",
        null,
    ));
    try testing.expectEqual(@as(usize, 1), h.delivered.items.len);
    try testing.expectEqualStrings(
        "{\"data\":{\"task\":{\"id\":\"o:a\",\"version\":0}},\"head\":{\"serverUrl\":\"http://server:8001\"},\"kind\":\"execute\"}",
        h.delivered.items[0],
    );
}
