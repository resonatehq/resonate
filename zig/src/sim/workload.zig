//! The workload: requests that actually land.
//!
//! A generator that picks ids at random from an unbounded space produces a
//! history of 404s, which proves nothing. Two things make this one produce the
//! states worth checking:
//!
//! * **A tiny id space.** Two origins, a handful of promises and tasks in each.
//!   Collisions are the point: they are what produce version conflicts, double
//!   creates, settling something already settled, and suspending on a promise
//!   that settled a moment ago.
//! * **A shadow of what it has seen.** Task versions and promise states are
//!   remembered from the answers, so an acquire names the version the task
//!   probably has rather than a number nobody would accept. It is deliberately a
//!   guess and not a query: the guess is right often enough to reach the
//!   interesting paths and wrong often enough to reach the conflict paths.
//!
//! Each operation also says whether it belongs in a linearizability check. The
//! ones that do not are the cross-origin reads — the searches and the snapshot —
//! which are surveys of many objects read one at a time and were never atomic.
//! Excluding them is sound because they change nothing; including them would
//! refute a correct server.

const std = @import("std");
const stdx = @import("../stdx.zig");
const protocol = @import("../protocol.zig");
const json = @import("../json.zig");

const assert = stdx.assert;

pub const Kind = enum {
    promise_create,
    promise_create_task,
    promise_create_timer,
    promise_get,
    promise_settle,
    promise_register_callback,
    promise_register_listener,
    promise_search,
    task_create,
    task_get,
    task_acquire,
    task_release,
    task_fulfill,
    task_suspend,
    task_fence,
    task_heartbeat,
    task_halt,
    task_continue,
    task_search,
    schedule_create,
    schedule_get,
    schedule_delete,
    schedule_search,
    debug_snap,
    debug_tick,

    pub fn wire(self: Kind) []const u8 {
        return switch (self) {
            .promise_create, .promise_create_task, .promise_create_timer => "promise.create",
            .promise_get => "promise.get",
            .promise_settle => "promise.settle",
            .promise_register_callback => "promise.register_callback",
            .promise_register_listener => "promise.register_listener",
            .promise_search => "promise.search",
            .task_create => "task.create",
            .task_get => "task.get",
            .task_acquire => "task.acquire",
            .task_release => "task.release",
            .task_fulfill => "task.fulfill",
            .task_suspend => "task.suspend",
            .task_fence => "task.fence",
            .task_heartbeat => "task.heartbeat",
            .task_halt => "task.halt",
            .task_continue => "task.continue",
            .task_search => "task.search",
            .schedule_create => "schedule.create",
            .schedule_get => "schedule.get",
            .schedule_delete => "schedule.delete",
            .schedule_search => "schedule.search",
            .debug_snap => "debug.snap",
            .debug_tick => "debug.tick",
        };
    }

    /// Does a linearizability check admit this operation?
    ///
    /// A cross-origin read does not: it is a listing followed by a read of each
    /// object, each at its own instant, and the protocol never promises it is a
    /// consistent snapshot. Every operation that *decides* anything is
    /// single-origin, which is what makes the rest of the history checkable.
    pub fn checkable(self: Kind) bool {
        return switch (self) {
            .promise_search, .task_search, .schedule_search, .debug_snap => false,
            else => true,
        };
    }

    /// Must this operation be the only one in flight?
    ///
    /// The tick sweeps every origin, one object at a time. As a debug operation
    /// that exists to move a test's clock, it is only meaningful when nothing else
    /// is racing it — and issuing it alone is also what makes it atomic in the
    /// recorded history, so a checker can hold it to that.
    pub fn exclusive(self: Kind) bool {
        return self == .debug_tick;
    }
};

pub const Request = struct {
    kind: Kind,
    /// The whole envelope, allocated in the caller's arena.
    envelope: []const u8,
    /// The instant it carries.
    now: i64,
    /// Which origin it belongs to, for diagnostics.
    origin: []const u8,
};

/// How many of each id there are. Small on purpose.
pub const origin_count = 2;
pub const promises_per_origin = 3;
pub const tasks_per_origin = 3;
pub const schedule_count = 2;

/// How many operations an epoch lasts.
///
/// A settled promise stays settled, so a fixed id space saturates: after a while
/// every promise is settled and every task is finished, and the operations that
/// need a live task stop reaching anything. Advancing an epoch mints a fresh set
/// of origins, which keeps the space small enough for collisions and fresh enough
/// for the interesting states to keep occurring.
pub const epoch_length: u64 = 150;

pub const Workload = struct {
    random: *stdx.Random,
    /// The instant the next request carries. Only a tick moves it.
    now: i64,
    /// What the answers have said so far. A guess, not a query.
    task_versions: std.StringHashMapUnmanaged(i64) = .{},
    promise_settled: std.StringHashMapUnmanaged(void) = .{},
    allocator: std.mem.Allocator,
    /// Which kinds have produced a 2xx. Reported at the end, because a run that
    /// only ever reached failure paths has checked nothing.
    covered: std.EnumSet(Kind) = std.EnumSet(Kind).initEmpty(),
    /// How many requests have been built, and which generation of ids they use.
    built: u64 = 0,
    epoch: u32 = 0,

    pub fn init(allocator: std.mem.Allocator, random: *stdx.Random, start_ms: i64) Workload {
        return .{ .random = random, .now = start_ms, .allocator = allocator };
    }

    pub fn deinit(self: *Workload) void {
        var it = self.task_versions.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.task_versions.deinit(self.allocator);
        var it2 = self.promise_settled.keyIterator();
        while (it2.next()) |k| self.allocator.free(k.*);
        self.promise_settled.deinit(self.allocator);
    }

    fn origin(self: *Workload, arena: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(arena, "c{d}e{d}", .{ self.random.below(origin_count), self.epoch });
    }

    /// The lineage every id in an origin hangs off.
    ///
    /// The branch tag has to be a prefix of the id it is put on — that is what
    /// makes an id say where it came from — so the ids are all segments below one
    /// root rather than siblings of it.
    fn branch(arena: std.mem.Allocator, og: []const u8) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}:root", .{og});
    }

    fn promise_id(self: *Workload, arena: std.mem.Allocator, og: []const u8) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}:root.p{d}", .{ og, self.random.below(promises_per_origin) });
    }

    fn task_id(self: *Workload, arena: std.mem.Allocator, og: []const u8) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}:root.t{d}", .{ og, self.random.below(tasks_per_origin) });
    }

    fn version_of(self: *Workload, id: []const u8) i64 {
        const known = self.task_versions.get(id) orelse 0;
        // Mostly the version last seen, sometimes a wrong one: both paths matter,
        // and only one of them is the happy one.
        if (self.random.chance(15)) return known + @as(i64, @intCast(self.random.below(3)));
        return known;
    }

    /// Learn from an answer, so the next request is a better guess.
    pub fn observe(self: *Workload, kind: Kind, status: i32, data: []const u8, arena: std.mem.Allocator) void {
        if (status >= 200 and status < 400) self.covered.insert(kind);
        if (status != 200) return;
        const parsed = json.parse(arena, data) catch return;
        if (parsed.get("task")) |task| {
            const id = task.get_string("id") orelse return;
            const version = task.get_i64("version") orelse return;
            self.remember_version(id, version);
        }
        if (parsed.get("promise")) |promise| {
            const id = promise.get_string("id") orelse return;
            const state = promise.get_string("state") orelse return;
            if (!std.mem.eql(u8, state, "pending")) self.remember_settled(id);
        }
    }

    fn remember_version(self: *Workload, id: []const u8, version: i64) void {
        if (self.task_versions.getPtr(id)) |slot| {
            slot.* = version;
            return;
        }
        const key = self.allocator.dupe(u8, id) catch return;
        self.task_versions.put(self.allocator, key, version) catch self.allocator.free(key);
    }

    fn remember_settled(self: *Workload, id: []const u8) void {
        if (self.promise_settled.contains(id)) return;
        const key = self.allocator.dupe(u8, id) catch return;
        self.promise_settled.put(self.allocator, key, {}) catch self.allocator.free(key);
    }

    /// Pick the next kind.
    ///
    /// Weighted so that the operations that build state run more often than the
    /// ones that read it, and so the clock moves often enough for deadlines to
    /// fall but not so often that nothing survives to be contended.
    pub fn pick(self: *Workload) Kind {
        const roll = self.random.below(100);
        var seen: u64 = 0;
        const table = [_]struct { kind: Kind, weight: u64 }{
            .{ .kind = .promise_create, .weight = 6 },
            .{ .kind = .promise_create_task, .weight = 8 },
            .{ .kind = .promise_create_timer, .weight = 2 },
            .{ .kind = .promise_get, .weight = 6 },
            .{ .kind = .promise_settle, .weight = 7 },
            .{ .kind = .promise_register_callback, .weight = 4 },
            .{ .kind = .promise_register_listener, .weight = 3 },
            .{ .kind = .promise_search, .weight = 2 },
            .{ .kind = .task_create, .weight = 7 },
            .{ .kind = .task_get, .weight = 4 },
            .{ .kind = .task_acquire, .weight = 7 },
            .{ .kind = .task_release, .weight = 4 },
            .{ .kind = .task_fulfill, .weight = 6 },
            .{ .kind = .task_suspend, .weight = 5 },
            .{ .kind = .task_fence, .weight = 4 },
            .{ .kind = .task_heartbeat, .weight = 3 },
            .{ .kind = .task_halt, .weight = 3 },
            .{ .kind = .task_continue, .weight = 3 },
            .{ .kind = .task_search, .weight = 1 },
            .{ .kind = .schedule_create, .weight = 3 },
            .{ .kind = .schedule_get, .weight = 1 },
            .{ .kind = .schedule_delete, .weight = 1 },
            .{ .kind = .schedule_search, .weight = 1 },
            .{ .kind = .debug_snap, .weight = 1 },
            .{ .kind = .debug_tick, .weight = 8 },
        };
        for (table) |row| {
            seen += row.weight;
            if (roll < seen) return row.kind;
        }
        return .debug_tick;
    }

    /// Build one request of the given kind.
    pub fn build(
        self: *Workload,
        arena: std.mem.Allocator,
        kind: Kind,
        corr_id: []const u8,
        pid: []const u8,
    ) !Request {
        self.built += 1;
        if (self.built % epoch_length == 0) self.epoch += 1;
        const og = try self.origin(arena);
        var data = std.ArrayList(u8).init(arena);
        const w = data.writer();
        var request_now = self.now;

        switch (kind) {
            .promise_create => {
                const id = try self.promise_id(arena, og);
                // A global scope makes it awaitable without giving it a task,
                // which is the shape a child promise has.
                try w.print(
                    "{{\"id\":\"{s}\",\"timeoutAt\":{d},\"param\":{{\"data\":\"p\"}},\"tags\":{{\"resonate:scope\":\"global\",\"resonate:branch\":\"{s}:root\"}}}}",
                    .{ id, self.now + @as(i64, @intCast(self.random.between(1, 200_000))), og },
                );
            },
            .promise_create_task => {
                const id = try self.task_id(arena, og);
                try w.print(
                    "{{\"id\":\"{s}\",\"timeoutAt\":{d},\"tags\":{{\"resonate:target\":\"http://w/{s}\",\"resonate:branch\":\"{s}:root\"}}}}",
                    .{ id, self.now + @as(i64, @intCast(self.random.between(1, 200_000))), og, og },
                );
            },
            .promise_create_timer => {
                const id = try std.fmt.allocPrint(arena, "{s}:root.sleep{d}", .{ og, self.random.below(2) });
                try w.print(
                    "{{\"id\":\"{s}\",\"timeoutAt\":{d},\"tags\":{{\"resonate:timer\":\"true\"}}}}",
                    .{ id, self.now + @as(i64, @intCast(self.random.between(1, 60_000))) },
                );
            },
            .promise_get => {
                const id = if (self.random.chance(50))
                    try self.promise_id(arena, og)
                else
                    try self.task_id(arena, og);
                try w.print("{{\"id\":\"{s}\"}}", .{id});
            },
            .promise_settle => {
                const id = if (self.random.chance(60))
                    try self.promise_id(arena, og)
                else
                    try self.task_id(arena, og);
                const states = [_][]const u8{ "resolved", "rejected", "rejected_canceled" };
                try w.print(
                    "{{\"id\":\"{s}\",\"state\":\"{s}\",\"value\":{{\"data\":\"v{d}\"}}}}",
                    .{ id, states[self.random.below(states.len)], self.random.below(4) },
                );
            },
            .promise_register_callback => {
                const awaited = try self.promise_id(arena, og);
                const awaiter = try self.task_id(arena, og);
                try w.print("{{\"awaited\":\"{s}\",\"awaiter\":\"{s}\"}}", .{ awaited, awaiter });
            },
            .promise_register_listener => {
                const awaited = if (self.random.chance(50))
                    try self.promise_id(arena, og)
                else
                    try self.task_id(arena, og);
                try w.print(
                    "{{\"awaited\":\"{s}\",\"address\":\"http://listener/{d}\"}}",
                    .{ awaited, self.random.below(2) },
                );
            },
            .promise_search => {
                if (self.random.chance(50)) {
                    try w.print("{{\"limit\":{d}}}", .{self.random.between(1, 5)});
                } else {
                    try w.print("{{\"state\":\"pending\"}}", .{});
                }
            },
            .task_create => {
                const id = try self.task_id(arena, og);
                try w.print(
                    "{{\"pid\":\"{s}\",\"ttl\":{d},\"action\":{{\"kind\":\"promise.create\",\"head\":{{}},\"data\":{{\"id\":\"{s}\",\"timeoutAt\":{d},\"tags\":{{\"resonate:target\":\"http://w/{s}\",\"resonate:branch\":\"{s}:root\"}}}}}}}}",
                    .{
                        pid,
                        self.random.between(1_000, 60_000),
                        id,
                        self.now + @as(i64, @intCast(self.random.between(1, 200_000))),
                        og,
                        og,
                    },
                );
            },
            .task_get => {
                const id = try self.task_id(arena, og);
                try w.print("{{\"id\":\"{s}\"}}", .{id});
            },
            .task_acquire => {
                const id = try self.task_id(arena, og);
                try w.print(
                    "{{\"id\":\"{s}\",\"version\":{d},\"pid\":\"{s}\",\"ttl\":{d}}}",
                    .{ id, self.version_of(id), pid, self.random.between(1_000, 60_000) },
                );
            },
            .task_release => {
                const id = try self.task_id(arena, og);
                try w.print("{{\"id\":\"{s}\",\"version\":{d}}}", .{ id, self.version_of(id) });
            },
            .task_fulfill => {
                const id = try self.task_id(arena, og);
                const states = [_][]const u8{ "resolved", "rejected" };
                try w.print(
                    "{{\"id\":\"{s}\",\"version\":{d},\"action\":{{\"kind\":\"promise.settle\",\"head\":{{}},\"data\":{{\"id\":\"{s}\",\"state\":\"{s}\",\"value\":{{\"data\":\"f\"}}}}}}}}",
                    .{ id, self.version_of(id), id, states[self.random.below(states.len)] },
                );
            },
            .task_suspend => {
                const id = try self.task_id(arena, og);
                const awaited = try self.promise_id(arena, og);
                try w.print(
                    "{{\"id\":\"{s}\",\"version\":{d},\"actions\":[{{\"kind\":\"promise.register_callback\",\"head\":{{}},\"data\":{{\"awaited\":\"{s}\",\"awaiter\":\"{s}\"}}}}]}}",
                    .{ id, self.version_of(id), awaited, id },
                );
            },
            .task_fence => {
                const id = try self.task_id(arena, og);
                const other = try self.promise_id(arena, og);
                if (self.random.chance(50)) {
                    try w.print(
                        "{{\"id\":\"{s}\",\"version\":{d},\"action\":{{\"kind\":\"promise.create\",\"head\":{{}},\"data\":{{\"id\":\"{s}\",\"timeoutAt\":{d},\"tags\":{{\"resonate:scope\":\"global\"}}}}}}}}",
                        .{ id, self.version_of(id), other, self.now + 100_000 },
                    );
                } else {
                    try w.print(
                        "{{\"id\":\"{s}\",\"version\":{d},\"action\":{{\"kind\":\"promise.settle\",\"head\":{{}},\"data\":{{\"id\":\"{s}\",\"state\":\"resolved\"}}}}}}",
                        .{ id, self.version_of(id), other },
                    );
                }
            },
            .task_heartbeat => {
                const id = try self.task_id(arena, og);
                try w.print(
                    "{{\"pid\":\"{s}\",\"tasks\":[{{\"id\":\"{s}\",\"version\":{d}}}]}}",
                    .{ pid, id, self.version_of(id) },
                );
            },
            .task_halt, .task_continue => {
                const id = try self.task_id(arena, og);
                try w.print("{{\"id\":\"{s}\"}}", .{id});
            },
            .task_search => {
                try w.print("{{\"limit\":{d}}}", .{self.random.between(1, 5)});
            },
            .schedule_create => {
                const id = try std.fmt.allocPrint(arena, "s{d}", .{self.random.below(schedule_count)});
                const crons = [_][]const u8{ "* * * * *", "*/30 * * * * *" };
                try w.print(
                    "{{\"id\":\"{s}\",\"cron\":\"{s}\",\"promiseId\":\"{{{{.id}}}}.{{{{.timestamp}}}}\",\"promiseTimeout\":{d},\"promiseTags\":{{\"resonate:target\":\"http://w/s\"}}}}",
                    .{ id, crons[self.random.below(crons.len)], self.random.between(1_000, 120_000) },
                );
            },
            .schedule_get, .schedule_delete => {
                const id = try std.fmt.allocPrint(arena, "s{d}", .{self.random.below(schedule_count)});
                try w.print("{{\"id\":\"{s}\"}}", .{id});
            },
            .schedule_search => {
                try w.print("{{\"limit\":{d}}}", .{self.random.between(1, 3)});
            },
            .debug_snap => {
                try w.print("{{}}", .{});
            },
            .debug_tick => {
                // Forward, never back: the clock the server sees is monotone, so a
                // trace that moved it backwards would be a trace no server could
                // have produced.
                self.now += @intCast(self.random.between(0, 40_000));
                request_now = self.now;
                try w.print("{{\"time\":{d}}}", .{self.now});
            },
        }

        const envelope = try std.fmt.allocPrint(
            arena,
            "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"{s}\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
            .{ kind.wire(), corr_id, protocol.protocol_version, request_now, data.items },
        );
        return .{ .kind = kind, .envelope = envelope, .now = request_now, .origin = og };
    }

    /// Which kinds never produced a success.
    pub fn uncovered(self: *const Workload, out: *std.ArrayList(Kind)) !void {
        var it = std.EnumSet(Kind).initFull().iterator();
        while (it.next()) |kind| {
            if (!self.covered.contains(kind)) try out.append(kind);
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const Model = @import("model.zig").Model;

test "every kind builds an envelope the protocol admits" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var rng = stdx.Random.init(1);
    var workload = Workload.init(testing.allocator, &rng, 1_000_000);
    defer workload.deinit();

    var it = std.EnumSet(Kind).initFull().iterator();
    while (it.next()) |kind| {
        const request = try workload.build(a, kind, "c1", "w1");
        const parsed = try json.parse(a, request.envelope);
        try testing.expectEqualStrings(kind.wire(), parsed.get_string("kind").?);
        try testing.expect(parsed.get("data").?.is_object());
        try testing.expectEqual(
            request.now,
            parsed.get("head").?.get_i64("resonate:debug_time").?,
        );
    }
}

test "the clock only ever moves forward" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var rng = stdx.Random.init(2);
    var workload = Workload.init(testing.allocator, &rng, 0);
    defer workload.deinit();
    var last: i64 = -1;
    for (0..200) |_| {
        const request = try workload.build(arena.allocator(), .debug_tick, "c", "w");
        try testing.expect(request.now >= last);
        last = request.now;
    }
}

test "a run of the workload reaches every operation" {
    // Against the model, which is the server: if the generator cannot make these
    // operations succeed here, it cannot make them succeed anywhere.
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var rng = stdx.Random.init(20260919);
    var workload = Workload.init(testing.allocator, &rng, 1_000_000_000);
    defer workload.deinit();

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for (0..6_000) |i| {
        _ = arena.reset(.retain_capacity);
        const a = arena.allocator();
        const kind = workload.pick();
        const corr_id = try std.fmt.allocPrint(a, "c{d}", .{i});
        const request = try workload.build(a, kind, corr_id, "w0");
        const reply = model.apply(request.envelope);
        defer model.free_reply(reply);
        // Nothing the generator produces may be a server error.
        if (reply.status >= 500) {
            std.debug.print("\n{s} -> {d} {s}\n  {s}\n", .{ kind.wire(), reply.status, reply.data, request.envelope });
            return error.ServerError;
        }
        workload.observe(kind, reply.status, reply.data, a);
    }

    var missing = std.ArrayList(Kind).init(testing.allocator);
    defer missing.deinit();
    try workload.uncovered(&missing);
    if (missing.items.len > 0) {
        std.debug.print("\nnever succeeded:", .{});
        for (missing.items) |kind| std.debug.print(" {s}", .{@tagName(kind)});
        std.debug.print("\n", .{});
        return error.CoverageIncomplete;
    }
}
