//! The reads that span origins: the searches, the snapshot, and the reset.
//!
//! Everything here is a listing of a prefix followed by a read of each object,
//! filtered and projected in memory. That is O(origins) reads per query, which
//! is honest for a small deployment and for the test suites and plainly wrong
//! for a large one. It is stated rather than hidden, and this module is the seam
//! a secondary index would go behind: adding one would not touch the state
//! machine or the commit loop.
//!
//! Two properties of these reads are deliberate and worth stating, because they
//! look like bugs and are not:
//!
//! * **They see stored state, not effective state.** A promise whose deadline has
//!   passed but which nothing has named yet still reads as pending here, exactly
//!   as it does from a `SELECT` in a relational backend. It is a read; making it
//!   settle promises would make a search a write.
//! * **They are not a consistent snapshot across origins.** Each document is read
//!   at its own instant. Within an origin every answer is linearizable; a
//!   listing of many is a survey, and the protocol never asks a caller to rely on
//!   more than that — which is why every operation that *decides* anything is
//!   single-origin.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const doc_mod = @import("doc.zig");
const handle = @import("handle.zig");
const sender_mod = @import("sender.zig");

const assert = stdx.assert;
const Doc = doc_mod.Doc;
const ScheduleDoc = doc_mod.ScheduleDoc;
const Store = store_mod.Store;
const KeySpace = store_mod.KeySpace;

/// How many keys one listing asks for. Large enough that the suites never
/// paginate, small enough that a runaway bucket does not allocate without bound.
const list_batch = 10_000;

pub const Kind = enum {
    promise_search,
    task_search,
    schedule_search,
    snapshot,
    reset,

    pub fn parse(kind: []const u8) ?Kind {
        const table = .{
            .{ "promise.search", Kind.promise_search },
            .{ "task.search", Kind.task_search },
            .{ "schedule.search", Kind.schedule_search },
            .{ "debug.snap", Kind.snapshot },
            .{ "debug.reset", Kind.reset },
        };
        inline for (table) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Request = struct {
    kind: Kind,
    corr_id: []const u8 = "",
    data: json.Value = json.Value.null_value,
    now: i64 = 0,

    /// Where the reply is allocated, and the working memory of the scan itself.
    arena: *std.heap.ArenaAllocator,
    callback: *const fn (*Request) void,
    context: ?*anyopaque = null,

    status: i32 = 0,
    reply_data: []const u8 = "",

    // ── Scan state ────────────────────────────────────────────────────────────
    phase: enum { start, listing, reading, deleting, done } = .start,
    /// Which prefix the reset is working through.
    reset_stage: u8 = 0,
    keys: []const []const u8 = &.{},
    index: usize = 0,
    docs: std.ArrayListUnmanaged(Doc) = .{},
    schedules: std.ArrayListUnmanaged(ScheduleDoc) = .{},
    op: store_mod.Operation = undefined,
    scanner: *Scanner = undefined,
    key_buf: std.ArrayList(u8) = undefined,
};

pub const Scanner = struct {
    allocator: std.mem.Allocator,
    store: Store,
    keys: KeySpace,
    sender: *sender_mod.Sender,
    /// Called when a reset has emptied the bucket, so the caches and the timer
    /// queue can forget what they are holding: the objects are gone, and
    /// anything still cached is a document about state that no longer exists.
    on_reset: ?*const fn (context: ?*anyopaque) void = null,
    on_reset_context: ?*anyopaque = null,

    pub fn init(
        allocator: std.mem.Allocator,
        store: Store,
        keys: KeySpace,
        sender: *sender_mod.Sender,
    ) Scanner {
        return .{ .allocator = allocator, .store = store, .keys = keys, .sender = sender };
    }

    pub fn submit(self: *Scanner, req: *Request) void {
        req.scanner = self;
        req.key_buf = std.ArrayList(u8).init(req.arena.allocator());
        switch (req.kind) {
            .reset => self.begin_reset(req),
            .schedule_search => self.begin_list(req, self.sched_prefix(req)),
            else => self.begin_list(req, self.doc_prefix(req)),
        }
    }

    fn doc_prefix(self: *Scanner, req: *Request) []const u8 {
        return self.keys.doc_prefix(&req.key_buf) catch "";
    }

    fn sched_prefix(self: *Scanner, req: *Request) []const u8 {
        return self.keys.sched_prefix(&req.key_buf) catch "";
    }

    fn begin_list(self: *Scanner, req: *Request, prefix: []const u8) void {
        req.phase = .listing;
        req.op = .{
            .kind = .list,
            .key = prefix,
            .max_keys = list_batch,
            .arena = req.arena.allocator(),
        };
        req.op.listen(*Request, req, on_complete);
        self.store.submit(&req.op);
    }

    fn on_complete(req: *Request, op: *store_mod.Operation) void {
        const self = req.scanner;
        switch (req.phase) {
            .listing => self.on_listed(req, op.result),
            .reading => self.on_read(req, op.result),
            .deleting => self.on_deleted(req, op.result),
            else => unreachable,
        }
    }

    fn on_listed(self: *Scanner, req: *Request, result: store_mod.Result) void {
        switch (result) {
            .keys => |keys| req.keys = keys,
            .unavailable => |detail| return fail(req, 503, detail),
            else => return fail(req, 500, "the store answered a listing with something else"),
        }
        req.index = 0;
        self.read_next(req);
    }

    fn read_next(self: *Scanner, req: *Request) void {
        if (req.index >= req.keys.len) return self.project(req);
        req.phase = .reading;
        req.op = .{
            .kind = .get,
            .key = req.keys[req.index],
            .arena = req.arena.allocator(),
        };
        req.op.listen(*Request, req, on_complete);
        self.store.submit(&req.op);
    }

    fn on_read(self: *Scanner, req: *Request, result: store_mod.Result) void {
        const a = req.arena.allocator();
        const key = req.keys[req.index];
        switch (result) {
            .found => |f| {
                if (req.kind == .schedule_search) {
                    const id = (self.keys.id_of_sched_key(a, key) catch null) orelse {
                        req.index += 1;
                        return self.read_next(req);
                    };
                    if (ScheduleDoc.decode(a, f.body, id)) |sched| {
                        req.schedules.append(a, sched) catch return fail(req, 503, "out of memory");
                    } else |_| {
                        // A schedule this build cannot read is skipped rather
                        // than fatal: one bad object must not take a search down.
                    }
                } else {
                    const origin = (self.keys.origin_of_doc_key(a, key) catch null) orelse {
                        req.index += 1;
                        return self.read_next(req);
                    };
                    if (Doc.decode(a, f.body, origin)) |document| {
                        req.docs.append(a, document) catch return fail(req, 503, "out of memory");
                    } else |_| {}
                }
            },
            // Deleted between the listing and the read. Not an error: a listing
            // is a survey.
            .not_found => {},
            .unavailable => |detail| return fail(req, 503, detail),
            else => return fail(req, 500, "the store answered a read with something else"),
        }
        req.index += 1;
        self.read_next(req);
    }

    // ── Reset ─────────────────────────────────────────────────────────────────

    fn begin_reset(self: *Scanner, req: *Request) void {
        req.reset_stage = 0;
        self.reset_stage(req);
    }

    fn reset_stage(self: *Scanner, req: *Request) void {
        const prefix = switch (req.reset_stage) {
            0 => self.keys.doc_prefix(&req.key_buf) catch "",
            1 => self.keys.sched_prefix(&req.key_buf) catch "",
            2 => self.keys.timer_prefix(&req.key_buf) catch "",
            else => {
                // The objects are gone, so anything still held in memory is a
                // ghost. Order matters: this happens after the deletes, never
                // before.
                if (self.on_reset) |hook| hook(self.on_reset_context);
                self.sender.clear();
                req.status = 200;
                req.reply_data = "{}";
                req.phase = .done;
                req.callback(req);
                return;
            },
        };
        req.phase = .listing;
        req.op = .{
            .kind = .list,
            .key = prefix,
            .max_keys = list_batch,
            .arena = req.arena.allocator(),
        };
        req.op.listen(*Request, req, on_reset_listed);
        self.store.submit(&req.op);
    }

    fn on_reset_listed(req: *Request, op: *store_mod.Operation) void {
        const self = req.scanner;
        switch (op.result) {
            .keys => |keys| req.keys = keys,
            .unavailable => |detail| return fail(req, 503, detail),
            else => return fail(req, 500, "the store answered a listing with something else"),
        }
        if (req.keys.len == 0) {
            req.reset_stage += 1;
            return self.reset_stage(req);
        }
        req.index = 0;
        self.delete_next(req);
    }

    fn delete_next(self: *Scanner, req: *Request) void {
        if (req.index >= req.keys.len) {
            // A listing is capped, so there may be more. Ask again until it is
            // empty.
            return self.reset_stage(req);
        }
        req.phase = .deleting;
        req.op = .{
            .kind = .delete,
            .key = req.keys[req.index],
            .arena = req.arena.allocator(),
        };
        req.op.listen(*Request, req, on_complete);
        self.store.submit(&req.op);
    }

    fn on_deleted(self: *Scanner, req: *Request, result: store_mod.Result) void {
        switch (result) {
            .deleted => {},
            .unavailable => |detail| return fail(req, 503, detail),
            else => return fail(req, 500, "the store answered a delete with something else"),
        }
        req.index += 1;
        self.delete_next(req);
    }

    // ── Projection ────────────────────────────────────────────────────────────

    fn project(self: *Scanner, req: *Request) void {
        const a = req.arena.allocator();
        var out = std.ArrayList(u8).init(a);
        var w = json.Writer.init(&out);
        const status: i32 = switch (req.kind) {
            .promise_search => self.project_promise_search(req, a, &w, &out) catch |e| return on_error(req, e),
            .task_search => self.project_task_search(req, a, &w, &out) catch |e| return on_error(req, e),
            .schedule_search => self.project_schedule_search(req, a, &w, &out) catch |e| return on_error(req, e),
            .snapshot => self.project_snapshot(req, a, &w) catch |e| return on_error(req, e),
            .reset => unreachable,
        };
        req.status = status;
        req.reply_data = out.items;
        req.phase = .done;
        req.callback(req);
    }

    /// A projection that failed.
    ///
    /// `error.Answered` means the projection already sent a reply of its own —
    /// a rejected `limit`, an unknown state — and there is nothing more to say.
    /// Anything else is this layer running out of memory.
    fn on_error(req: *Request, e: anyerror) void {
        if (e == error.Answered) return;
        fail(req, 503, @errorName(e));
    }

    /// The page size a search asks for, or the rejection for asking too much.
    fn resolve_limit(data: json.Value, default: i64) !i64 {
        const v = data.get("limit") orelse return default;
        if (v.is_null()) return default;
        const n = v.as_i64() orelse return error.LimitNotAnInteger;
        if (n < 1) return error.LimitNotPositive;
        if (n > protocol.search_limit_max) return error.LimitTooLarge;
        return n;
    }

    fn limit_failure(req: *Request, e: anyerror) bool {
        const message: []const u8 = switch (e) {
            error.LimitNotPositive => "Limit must be a positive integer",
            error.LimitTooLarge => "Invalid 'limit' — must be between 1 and 1000",
            error.LimitNotAnInteger => "Invalid request: invalid type for field `limit`, expected an integer",
            else => return false,
        };
        fail(req, 400, message);
        return true;
    }

    fn project_promise_search(
        self: *Scanner,
        req: *Request,
        a: std.mem.Allocator,
        w: *json.Writer,
        out: *std.ArrayList(u8),
    ) !i32 {
        _ = self;
        const limit = resolve_limit(req.data, protocol.search_limit_default) catch |e| {
            if (limit_failure(req, e)) return error.Answered;
            return e;
        };
        const state_filter: ?protocol.PromiseState = blk: {
            const v = req.data.get("state") orelse break :blk null;
            if (v.is_null()) break :blk null;
            const s = v.as_string() orelse {
                fail(req, 400, "Invalid request: invalid type for field `state`, expected a string");
                return error.Answered;
            };
            break :blk protocol.PromiseState.parse(s) orelse {
                const message = try std.fmt.allocPrint(
                    a,
                    "Invalid request: unknown variant `{s}`, expected one of `pending`, `resolved`, `rejected`, `rejected_canceled`, `rejected_timedout`",
                    .{s},
                );
                fail(req, 400, message);
                return error.Answered;
            };
        };
        const tag_filter: protocol.StringMap = blk: {
            const v = req.data.get("tags") orelse break :blk .empty;
            if (v.is_null()) break :blk .empty;
            break :blk protocol.StringMap.from_json(a, v) catch {
                fail(req, 400, "Invalid request: invalid type for field `tags`, expected an object of strings");
                return error.Answered;
            };
        };
        const cursor: ?[]const u8 = blk: {
            const v = req.data.get("cursor") orelse break :blk null;
            break :blk v.as_string();
        };

        var matches = std.ArrayList(*const doc_mod.Promise).init(a);
        for (req.docs.items) |*d| {
            for (d.promises.items) |*p| {
                if (state_filter) |s| {
                    if (p.state != s) continue;
                }
                if (!p.tags.contains_all(tag_filter)) continue;
                if (cursor) |c| {
                    if (!std.mem.lessThan(u8, c, p.id)) continue;
                }
                try matches.append(p);
            }
        }
        std.mem.sort(*const doc_mod.Promise, matches.items, {}, less_promise);

        const page = @min(matches.items.len, @as(usize, @intCast(limit)));
        const has_more = matches.items.len > page;
        try w.object_begin();
        try w.key("promises");
        try w.array_begin();
        for (matches.items[0..page]) |p| {
            // `now = 0`: a search reports stored state.
            try handle.write_promise_record(w, 0, p);
        }
        try w.array_end();
        if (has_more and page > 0) try w.field_string("cursor", matches.items[page - 1].id);
        try w.object_end();
        _ = out;
        return 200;
    }

    fn project_task_search(
        self: *Scanner,
        req: *Request,
        a: std.mem.Allocator,
        w: *json.Writer,
        out: *std.ArrayList(u8),
    ) !i32 {
        _ = self;
        _ = out;
        const limit = resolve_limit(req.data, protocol.search_limit_default) catch |e| {
            if (limit_failure(req, e)) return error.Answered;
            return e;
        };
        const state_filter: ?protocol.TaskState = blk: {
            const v = req.data.get("state") orelse break :blk null;
            if (v.is_null()) break :blk null;
            const s = v.as_string() orelse {
                fail(req, 400, "Invalid request: invalid type for field `state`, expected a string");
                return error.Answered;
            };
            break :blk protocol.TaskState.parse(s) orelse {
                const message = try std.fmt.allocPrint(
                    a,
                    "Invalid request: unknown variant `{s}`, expected one of `pending`, `acquired`, `suspended`, `halted`, `fulfilled`",
                    .{s},
                );
                fail(req, 400, message);
                return error.Answered;
            };
        };
        const cursor: ?[]const u8 = blk: {
            const v = req.data.get("cursor") orelse break :blk null;
            break :blk v.as_string();
        };

        var matches = std.ArrayList(*const doc_mod.Task).init(a);
        for (req.docs.items) |*d| {
            for (d.tasks.items) |*t| {
                if (state_filter) |s| {
                    if (t.state != s) continue;
                }
                if (cursor) |c| {
                    if (!std.mem.lessThan(u8, c, t.id)) continue;
                }
                try matches.append(t);
            }
        }
        std.mem.sort(*const doc_mod.Task, matches.items, {}, less_task);

        const page = @min(matches.items.len, @as(usize, @intCast(limit)));
        const has_more = matches.items.len > page;
        try w.object_begin();
        try w.key("tasks");
        try w.array_begin();
        for (matches.items[0..page]) |t| {
            try handle.write_task_record(w, t.id, t.state, t.version, @intCast(t.resumes.len()), t.ttl, t.pid);
        }
        try w.array_end();
        if (has_more and page > 0) try w.field_string("cursor", matches.items[page - 1].id);
        try w.object_end();
        return 200;
    }

    fn project_schedule_search(
        self: *Scanner,
        req: *Request,
        a: std.mem.Allocator,
        w: *json.Writer,
        out: *std.ArrayList(u8),
    ) !i32 {
        _ = self;
        _ = out;
        const limit = resolve_limit(req.data, protocol.schedule_search_limit_default) catch |e| {
            if (limit_failure(req, e)) return error.Answered;
            return e;
        };
        const tag_filter: protocol.StringMap = blk: {
            const v = req.data.get("tags") orelse break :blk .empty;
            if (v.is_null()) break :blk .empty;
            break :blk protocol.StringMap.from_json(a, v) catch {
                fail(req, 400, "Invalid request: invalid type for field `tags`, expected an object of strings");
                return error.Answered;
            };
        };
        const cursor: ?[]const u8 = blk: {
            const v = req.data.get("cursor") orelse break :blk null;
            break :blk v.as_string();
        };

        var matches = std.ArrayList(*const ScheduleDoc).init(a);
        for (req.schedules.items) |*s| {
            // A tombstone is a schedule that has been deleted; its object has
            // simply not gone yet.
            if (s.deleted) continue;
            if (!s.promise_tags.contains_all(tag_filter)) continue;
            if (cursor) |c| {
                if (!std.mem.lessThan(u8, c, s.id)) continue;
            }
            try matches.append(s);
        }
        std.mem.sort(*const ScheduleDoc, matches.items, {}, less_schedule);

        const page = @min(matches.items.len, @as(usize, @intCast(limit)));
        const has_more = matches.items.len > page;
        try w.object_begin();
        try w.key("schedules");
        try w.array_begin();
        for (matches.items[0..page]) |s| try write_schedule_record(w, s);
        try w.array_end();
        if (has_more and page > 0) try w.field_string("cursor", matches.items[page - 1].id);
        try w.object_end();
        return 200;
    }

    /// `debug.snap`: everything, in one shape two implementations can be
    /// compared on.
    ///
    /// Each field is the projection its relational counterpart makes, including
    /// the ones that are not simply "all of them": `promiseTimeouts` holds only
    /// deadlines that are actually armed, and a task's `ttl` and `pid` appear
    /// only while it holds a lease.
    fn project_snapshot(
        self: *Scanner,
        req: *Request,
        a: std.mem.Allocator,
        w: *json.Writer,
    ) !i32 {
        var promises = std.ArrayList(*const doc_mod.Promise).init(a);
        var tasks = std.ArrayList(*const doc_mod.Task).init(a);
        for (req.docs.items) |*d| {
            for (d.promises.items) |*p| try promises.append(p);
            for (d.tasks.items) |*t| try tasks.append(t);
        }
        std.mem.sort(*const doc_mod.Promise, promises.items, {}, less_promise);
        std.mem.sort(*const doc_mod.Task, tasks.items, {}, less_task);

        try w.object_begin();

        try w.key("promises");
        try w.array_begin();
        for (promises.items) |p| try handle.write_promise_record(w, 0, p);
        try w.array_end();

        try w.key("promiseTimeouts");
        try w.array_begin();
        for (promises.items) |p| {
            if (!p.timeout_armed) continue;
            try w.object_begin();
            try w.field_string("id", p.id);
            try w.field_int("timeout", p.timeout_at);
            try w.object_end();
        }
        try w.array_end();

        // Sorted by awaited then awaiter. The promises are already in id order,
        // and each promise's registrations are a sorted set, so this is sorted
        // by construction.
        try w.key("callbacks");
        try w.array_begin();
        for (promises.items) |p| {
            for (p.callbacks.items) |awaiter| {
                try w.object_begin();
                try w.field_string("awaiter", awaiter);
                try w.field_string("awaited", p.id);
                try w.object_end();
            }
        }
        try w.array_end();

        try w.key("listeners");
        try w.array_begin();
        for (promises.items) |p| {
            for (p.listeners.items) |address| {
                try w.object_begin();
                try w.field_string("id", p.id);
                try w.field_string("address", address);
                try w.object_end();
            }
        }
        try w.array_end();

        try w.key("tasks");
        try w.array_begin();
        for (tasks.items) |t| {
            try handle.write_task_record(w, t.id, t.state, t.version, @intCast(t.resumes.len()), t.ttl, t.pid);
        }
        try w.array_end();

        try w.key("taskTimeouts");
        try w.array_begin();
        for (tasks.items) |t| {
            const to = t.timeout orelse continue;
            try w.object_begin();
            try w.field_string("id", t.id);
            try w.field_int("type", @intFromEnum(to.kind));
            try w.field_int("timeout", to.at);
            try w.object_end();
        }
        try w.array_end();

        try w.key("messages");
        try self.sender.write_snapshot(w);

        try w.object_end();
        return 200;
    }

    fn fail(req: *Request, status: i32, message: []const u8) void {
        var buf = std.ArrayList(u8).init(req.arena.allocator());
        json.write_string(&buf, message) catch {};
        req.status = status;
        req.reply_data = buf.items;
        req.phase = .done;
        req.callback(req);
    }
};

fn less_promise(_: void, a: *const doc_mod.Promise, b: *const doc_mod.Promise) bool {
    return std.mem.lessThan(u8, a.id, b.id);
}

fn less_task(_: void, a: *const doc_mod.Task, b: *const doc_mod.Task) bool {
    return std.mem.lessThan(u8, a.id, b.id);
}

fn less_schedule(_: void, a: *const ScheduleDoc, b: *const ScheduleDoc) bool {
    return std.mem.lessThan(u8, a.id, b.id);
}

/// A schedule, as the wire carries it.
pub fn write_schedule_record(w: *json.Writer, s: *const ScheduleDoc) !void {
    try w.object_begin();
    try w.field_string("id", s.id);
    try w.field_string("cron", s.cron);
    try w.field_string("promiseId", s.promise_id);
    try w.field_int("promiseTimeout", s.promise_timeout);
    try w.key("promiseParam");
    try s.promise_param.write_wire(w);
    try w.key("promiseTags");
    try s.promise_tags.write_json(w);
    try w.field_int("createdAt", s.created_at);
    try w.field_int("nextRunAt", s.next_run_at);
    if (s.last_run_at) |lr| try w.field_int("lastRunAt", lr);
    try w.object_end();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

const Fixture = struct {
    allocator: std.mem.Allocator,
    mem: store_mod.MemoryStore,
    sender: sender_mod.Sender,
    scanner: Scanner,
    keys: KeySpace,
    dummy: u8 = 0,

    fn create(allocator: std.mem.Allocator) !*Fixture {
        const self = try allocator.create(Fixture);
        self.* = .{
            .allocator = allocator,
            .mem = store_mod.MemoryStore.init(allocator),
            .sender = undefined,
            .scanner = undefined,
            .keys = KeySpace.init("", 4),
        };
        self.sender = sender_mod.Sender.init(allocator, self.bus(), "http://s");
        self.sender.hold = true;
        self.scanner = Scanner.init(allocator, self.mem.store(), self.keys, &self.sender);
        return self;
    }

    fn destroy(self: *Fixture) void {
        self.sender.deinit();
        self.mem.deinit();
        self.allocator.destroy(self);
    }

    fn bus(self: *Fixture) @import("env.zig").MessageBus {
        return .{ .ptr = self, .vtable = &.{ .send = send, .serves = serves } };
    }
    fn serves(_: *anyopaque, _: []const u8) bool {
        return true;
    }
    fn send(_: *anyopaque, d: *@import("env.zig").Delivery) void {
        d.complete(.delivered, "");
    }

    /// Write a document straight into the bucket, the way a committed transition
    /// would have.
    fn seed_doc(self: *Fixture, origin: []const u8, promises: []const doc_mod.Promise, tasks: []const doc_mod.Task) !void {
        var d = Doc.init(self.allocator);
        defer d.deinit();
        const owned = d.allocator();
        for (promises) |p| {
            var copy = p;
            copy.id = try owned.dupe(u8, p.id);
            copy.tags = try p.tags.clone(owned);
            copy.callbacks = try p.callbacks.clone(owned);
            copy.listeners = try p.listeners.clone(owned);
            copy.param = try p.param.clone(owned);
            copy.value = try p.value.clone(owned);
            _ = try d.promise_insert(copy);
        }
        for (tasks) |t| {
            var copy = t;
            copy.id = try owned.dupe(u8, t.id);
            copy.pid = if (t.pid) |pid| try owned.dupe(u8, pid) else null;
            copy.resumes = try t.resumes.clone(owned);
            _ = try d.task_insert(copy);
        }
        d.reseat_timer();

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var body = std.ArrayList(u8).init(a);
        try d.encode(&body, origin);
        var key_buf = std.ArrayList(u8).init(a);
        const key = try self.keys.doc_key(&key_buf, origin);
        var op = store_mod.Operation{
            .kind = .put,
            .key = key,
            .body = body.items,
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        self.mem.store().submit(&op);
    }

    fn seed_schedule(self: *Fixture, id: []const u8, cron: []const u8, next_run_at: i64, tags_json: []const u8) !void {
        return self.seed_schedule_state(id, cron, next_run_at, tags_json, false);
    }

    /// A tombstone: the object a delete leaves behind.
    fn seed_tombstone(self: *Fixture, id: []const u8) !void {
        return self.seed_schedule_state(id, "* * * * *", 60_000, "{}", true);
    }

    fn seed_schedule_state(
        self: *Fixture,
        id: []const u8,
        cron: []const u8,
        next_run_at: i64,
        tags_json: []const u8,
        deleted: bool,
    ) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var sched = ScheduleDoc.init(self.allocator);
        defer sched.deinit();
        const owned = sched.allocator();
        sched.id = try owned.dupe(u8, id);
        sched.cron = try owned.dupe(u8, cron);
        sched.promise_id = try owned.dupe(u8, "{{.id}}.{{.timestamp}}");
        sched.promise_timeout = 60_000;
        sched.created_at = 1_000;
        sched.next_run_at = next_run_at;
        sched.promise_tags = try protocol.StringMap.from_json(owned, try json.parse(a, tags_json));
        sched.deleted = deleted;

        var body = std.ArrayList(u8).init(a);
        try sched.encode(&body);
        var key_buf = std.ArrayList(u8).init(a);
        const key = try self.keys.sched_key(&key_buf, id);
        var op = store_mod.Operation{
            .kind = .put,
            .key = key,
            .body = body.items,
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        self.mem.store().submit(&op);
    }

    const Reply = struct {
        status: i32 = 0,
        data: []const u8 = "",
        done: bool = false,
        fn callback(req: *Request) void {
            const self: *Reply = @ptrCast(@alignCast(req.context.?));
            self.status = req.status;
            self.data = req.reply_data;
            self.done = true;
        }
    };

    fn call(self: *Fixture, arena: *std.heap.ArenaAllocator, kind: []const u8, data_json: []const u8) !Reply {
        var reply = Reply{};
        var req = Request{
            .kind = Kind.parse(kind).?,
            .corr_id = "c1",
            .data = try json.parse(arena.allocator(), data_json),
            .arena = arena,
            .callback = Reply.callback,
            .context = &reply,
        };
        self.scanner.submit(&req);
        try testing.expect(reply.done);
        return reply;
    }
};

fn promise(id: []const u8, state: protocol.PromiseState) doc_mod.Promise {
    return .{
        .id = id,
        .state = state,
        .param = .empty,
        .value = .empty,
        .tags = .empty,
        .timeout_at = 9_000_000_000_000,
        .created_at = 1_000,
        .settled_at = if (state == .pending) null else 2_000,
        .timeout_armed = false,
        .callbacks = .empty,
        .listeners = .empty,
    };
}

test "a promise search pages in id order across origins" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try f.seed_doc("b", &.{ promise("b:1", .pending), promise("b:2", .resolved) }, &.{});
    try f.seed_doc("a", &.{ promise("a:1", .pending), promise("a:2", .pending) }, &.{});

    const all = try f.call(&arena, "promise.search", "{}");
    try testing.expectEqual(@as(i32, 200), all.status);
    // Ordered by id, whatever order the documents were listed in.
    const first = std.mem.indexOf(u8, all.data, "a:1").?;
    const last = std.mem.indexOf(u8, all.data, "b:2").?;
    try testing.expect(first < last);
    try testing.expect(std.mem.indexOf(u8, all.data, "cursor") == null);

    const pending = try f.call(&arena, "promise.search", "{\"state\":\"pending\"}");
    try testing.expect(std.mem.indexOf(u8, pending.data, "b:2") == null);
    try testing.expectEqual(@as(usize, 3), std.mem.count(u8, pending.data, "\"state\":\"pending\""));

    // A page and its cursor.
    const page = try f.call(&arena, "promise.search", "{\"limit\":2}");
    try testing.expect(std.mem.indexOf(u8, page.data, "\"cursor\":\"a:2\"") != null);
    const rest = try f.call(&arena, "promise.search", "{\"limit\":2,\"cursor\":\"a:2\"}");
    try testing.expect(std.mem.indexOf(u8, rest.data, "a:1") == null);
    try testing.expect(std.mem.indexOf(u8, rest.data, "b:1") != null);
    try testing.expect(std.mem.indexOf(u8, rest.data, "cursor") == null);
}

test "a search rejects a limit outside the range" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const zero = try f.call(&arena, "promise.search", "{\"limit\":0}");
    try testing.expectEqual(@as(i32, 400), zero.status);
    try testing.expectEqualStrings("\"Limit must be a positive integer\"", zero.data);

    const huge = try f.call(&arena, "promise.search", "{\"limit\":1001}");
    try testing.expectEqual(@as(i32, 400), huge.status);
    try testing.expectEqualStrings("\"Invalid 'limit' — must be between 1 and 1000\"", huge.data);

    const bogus = try f.call(&arena, "promise.search", "{\"state\":\"nope\"}");
    try testing.expectEqual(@as(i32, 400), bogus.status);
    try testing.expect(std.mem.indexOf(u8, bogus.data, "unknown variant `nope`") != null);
}

test "a search filters on tags by containment" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var tagged = promise("o:1", .pending);
    tagged.tags = try protocol.StringMap.from_json(a, try json.parse(a, "{\"k\":\"v\",\"j\":\"w\"}"));
    try f.seed_doc("o", &.{ tagged, promise("o:2", .pending) }, &.{});

    const hit = try f.call(&arena, "promise.search", "{\"tags\":{\"k\":\"v\"}}");
    try testing.expect(std.mem.indexOf(u8, hit.data, "o:1") != null);
    try testing.expect(std.mem.indexOf(u8, hit.data, "o:2") == null);

    const miss = try f.call(&arena, "promise.search", "{\"tags\":{\"k\":\"other\"}}");
    try testing.expectEqual(@as(usize, 0), std.mem.count(u8, miss.data, "\"id\""));

    // Every pair in the filter must match, not just one.
    const both = try f.call(&arena, "promise.search", "{\"tags\":{\"k\":\"v\",\"j\":\"nope\"}}");
    try testing.expectEqual(@as(usize, 0), std.mem.count(u8, both.data, "\"id\""));
}

test "a search reports stored state, not effective state" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var expired = promise("o:1", .pending);
    expired.timeout_at = 5;
    try f.seed_doc("o", &.{expired}, &.{});

    var reply = Fixture.Reply{};
    var req = Request{
        .kind = .promise_search,
        .data = try json.parse(arena.allocator(), "{}"),
        .now = 1_000_000,
        .arena = &arena,
        .callback = Fixture.Reply.callback,
        .context = &reply,
    };
    f.scanner.submit(&req);
    // Long past its deadline, and it still reads pending: a search is a read.
    try testing.expect(std.mem.indexOf(u8, reply.data, "\"state\":\"pending\"") != null);
}

test "a task search filters by state and pages" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try f.seed_doc("o", &.{ promise("o:a", .pending), promise("o:b", .pending) }, &.{
        .{ .id = "o:a", .state = .pending, .version = 0, .pid = null, .ttl = null, .resumes = .empty, .timeout = .{ .kind = .retry, .at = 7 } },
        .{ .id = "o:b", .state = .acquired, .version = 2, .pid = "w1", .ttl = 60, .resumes = .empty, .timeout = .{ .kind = .lease, .at = 9 } },
    });

    const all = try f.call(&arena, "task.search", "{}");
    try testing.expectEqual(@as(usize, 2), std.mem.count(u8, all.data, "\"id\""));
    const acquired = try f.call(&arena, "task.search", "{\"state\":\"acquired\"}");
    try testing.expect(std.mem.indexOf(u8, acquired.data, "o:b") != null);
    try testing.expect(std.mem.indexOf(u8, acquired.data, "o:a") == null);
    try testing.expect(std.mem.indexOf(u8, acquired.data, "\"pid\":\"w1\"") != null);
    const page = try f.call(&arena, "task.search", "{\"limit\":1}");
    try testing.expect(std.mem.indexOf(u8, page.data, "\"cursor\":\"o:a\"") != null);
}

test "the snapshot projects everything two servers must agree on" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var armed = promise("o:t", .pending);
    armed.timeout_armed = true;
    armed.timeout_at = 500;
    armed.tags = try protocol.StringMap.from_json(a, try json.parse(a, "{\"resonate:target\":\"http://w\"}"));
    var awaited = promise("o:c", .pending);
    var callbacks: doc_mod.StringSet = .empty;
    _ = try callbacks.insert(a, "o:t");
    awaited.callbacks = callbacks;
    var listeners: doc_mod.StringSet = .empty;
    _ = try listeners.insert(a, "http://l");
    awaited.listeners = listeners;

    try f.seed_doc("o", &.{ awaited, armed }, &.{
        .{ .id = "o:t", .state = .acquired, .version = 3, .pid = "w1", .ttl = 60, .resumes = .empty, .timeout = .{ .kind = .lease, .at = 400 } },
    });
    try f.sender.enqueue(&.{.{ .kind = .execute, .address = "http://w", .task_id = "o:t", .version = 3 }});

    const snap = try f.call(&arena, "debug.snap", "{}");
    try testing.expectEqual(@as(i32, 200), snap.status);
    // Every section, in the documented order.
    for ([_][]const u8{
        "\"promises\":", "\"promiseTimeouts\":", "\"callbacks\":",
        "\"listeners\":", "\"tasks\":",           "\"taskTimeouts\":",
        "\"messages\":",
    }) |section| {
        try testing.expect(std.mem.indexOf(u8, snap.data, section) != null);
    }
    // Only the armed deadline is reported, and only once.
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, snap.data, "\"timeout\":500"));
    try testing.expect(std.mem.indexOf(u8, snap.data, "{\"awaiter\":\"o:t\",\"awaited\":\"o:c\"}") != null);
    try testing.expect(std.mem.indexOf(u8, snap.data, "{\"id\":\"o:c\",\"address\":\"http://l\"}") != null);
    try testing.expect(std.mem.indexOf(u8, snap.data, "{\"id\":\"o:t\",\"type\":1,\"timeout\":400}") != null);
    // The message's head is blank in the snapshot: where a server lives is not
    // part of what it decided.
    try testing.expect(std.mem.indexOf(u8, snap.data, "\"head\":{}") != null);
}

test "a schedule search reads the schedule objects" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try f.seed_schedule("s1", "* * * * *", 60_000, "{\"resonate:target\":\"http://w\"}");
    try f.seed_schedule("s0", "0 * * * *", 3_600_000, "{\"resonate:target\":\"http://w\",\"k\":\"v\"}");

    const all = try f.call(&arena, "schedule.search", "{}");
    try testing.expectEqual(@as(i32, 200), all.status);
    try testing.expect(std.mem.indexOf(u8, all.data, "\"id\":\"s0\"").? < std.mem.indexOf(u8, all.data, "\"id\":\"s1\"").?);
    try testing.expect(std.mem.indexOf(u8, all.data, "\"nextRunAt\":3600000") != null);

    const tagged = try f.call(&arena, "schedule.search", "{\"tags\":{\"k\":\"v\"}}");
    try testing.expect(std.mem.indexOf(u8, tagged.data, "s0") != null);
    try testing.expect(std.mem.indexOf(u8, tagged.data, "s1") == null);

    // Schedules default to ten a page, not a hundred.
    const page = try f.call(&arena, "schedule.search", "{\"limit\":1}");
    try testing.expect(std.mem.indexOf(u8, page.data, "\"cursor\":\"s0\"") != null);

    // A deleted schedule still has an object — it carries the write counter that
    // names its timer objects — and a search must not report it.
    try f.seed_tombstone("s2");
    const after = try f.call(&arena, "schedule.search", "{}");
    try testing.expect(std.mem.indexOf(u8, after.data, "s2") == null);
    try testing.expect(std.mem.indexOf(u8, after.data, "s1") != null);
}

test "reset empties every prefix and clears what was held" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try f.seed_doc("o", &.{promise("o:a", .pending)}, &.{});
    try f.seed_schedule("s0", "* * * * *", 1, "{\"resonate:target\":\"http://w\"}");
    {
        var key_buf = std.ArrayList(u8).init(a);
        const key = try f.keys.timer_key(&key_buf, "o", 500, 1);
        var op = store_mod.Operation{
            .kind = .put,
            .key = key,
            .body = "",
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        f.mem.store().submit(&op);
    }
    try f.sender.enqueue(&.{.{ .kind = .execute, .address = "http://w", .task_id = "o:a", .version = 0 }});
    try testing.expectEqual(@as(usize, 3), f.mem.count());
    try testing.expectEqual(@as(usize, 1), f.sender.pending());

    const Hook = struct {
        var called: bool = false;
        fn on_reset(_: ?*anyopaque) void {
            called = true;
        }
    };
    Hook.called = false;
    f.scanner.on_reset = Hook.on_reset;

    const r = try f.call(&arena, "debug.reset", "{}");
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expectEqualStrings("{}", r.data);
    try testing.expectEqual(@as(usize, 0), f.mem.count());
    try testing.expectEqual(@as(usize, 0), f.sender.pending());
    try testing.expect(Hook.called);

    // And the searches now say so.
    const empty = try f.call(&arena, "promise.search", "{}");
    try testing.expectEqualStrings("{\"promises\":[]}", empty.data);
}

test "an unavailable store becomes a 503 rather than a short answer" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var rng = stdx.Random.init(5);
    try f.seed_doc("o", &.{promise("o:a", .pending)}, &.{});
    f.mem.random = &rng;
    f.mem.faults.unavailable_percent = 100;

    const r = try f.call(&arena, "promise.search", "{}");
    try testing.expectEqual(@as(i32, 503), r.status);
}

test "an object deleted between the listing and the read is not an error" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try f.seed_doc("o", &.{promise("o:a", .pending)}, &.{});
    // A key with no object: exactly what a concurrent delete leaves behind.
    {
        const a = arena.allocator();
        var op = store_mod.Operation{
            .kind = .put,
            .key = "wf/ghost",
            .body = "",
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        f.mem.store().submit(&op);
        var del = store_mod.Operation{
            .kind = .delete,
            .key = "wf/ghost",
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        f.mem.store().submit(&del);
    }
    const r = try f.call(&arena, "promise.search", "{}");
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "o:a") != null);
}
