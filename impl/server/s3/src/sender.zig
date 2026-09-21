//! The outbox: what the server owes workers, and how it gets there.
//!
//! Two message kinds and no more. `execute` offers a task; `unblock` tells a
//! listener that the promise it was waiting on has settled.
//!
//! ## At most once, and that is on purpose
//!
//! Messages are handed over strictly after the transition that produced them has
//! committed, which is what makes them true. Nothing here retries a failed
//! delivery, because the two kinds have different recoveries and neither is a
//! retry loop:
//!
//! * a lost `execute` is re-sent by the task's own retry deadline, which is
//!   committed in the document *before* the message leaves — so the recovery is
//!   already durable and a second mechanism would only duplicate it;
//! * a lost `unblock` is lost. A listener is a notification, not an obligation
//!   the protocol can keep; whoever registered it can still read the promise.
//!
//! ## The outbox is keyed, not queued
//!
//! An `execute` is keyed by task id and an `unblock` by promise id and address.
//! A second offer of the same task replaces the first rather than queueing
//! behind it: the offer says "this task is available at this version", and two
//! of those are one fact. Both collections stay sorted by key, which is the
//! order `debug.snap` reports and the order two servers have to agree on.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const env = @import("env.zig");
const protocol = @import("protocol.zig");
const handle = @import("handle.zig");

const assert = stdx.assert;

pub const Sender = struct {
    pub const Kind = enum { execute, unblock };

    /// One message the server owes.
    const Held = struct {
        kind: Kind,
        address: []u8,
        /// `execute`: the task. `unblock`: the promise.
        id: []u8,
        version: i64 = 0,
        /// `unblock` only: the promise record as it settled.
        promise_json: []u8 = &.{},

        fn deinit(self: *Held, allocator: std.mem.Allocator) void {
            allocator.free(self.address);
            allocator.free(self.id);
            if (self.promise_json.len > 0) allocator.free(self.promise_json);
        }
    };

    const InFlight = struct {
        delivery: env.Delivery,
        sender: *Sender,
        address: []u8,
        body: []u8,
    };

    allocator: std.mem.Allocator,
    bus: env.MessageBus,
    /// Stamped into every `execute` head, so a worker knows where to answer.
    server_url: []const u8,

    /// Under the debug flag, nothing leaves: messages pile up here so that
    /// `debug.snap` can report exactly what the server decided to send. That is
    /// what makes a recorded history checkable — no background delivery settles
    /// anything the trace did not ask for.
    hold: bool = false,

    executes: std.ArrayListUnmanaged(Held) = .{},
    unblocks: std.ArrayListUnmanaged(Held) = .{},

    in_flight: u32 = 0,
    concurrency: u32 = 8,

    /// Counters, for the metrics endpoint and for tests.
    sent: u64 = 0,
    delivered: u64 = 0,
    failed: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, bus: env.MessageBus, server_url: []const u8) Sender {
        return .{ .allocator = allocator, .bus = bus, .server_url = server_url };
    }

    pub fn deinit(self: *Sender) void {
        self.clear();
        self.executes.deinit(self.allocator);
        self.unblocks.deinit(self.allocator);
    }

    /// Forget everything held. `debug.reset` needs it: the objects are gone, so
    /// anything still queued is a message about state that no longer exists.
    pub fn clear(self: *Sender) void {
        for (self.executes.items) |*h| h.deinit(self.allocator);
        for (self.unblocks.items) |*h| h.deinit(self.allocator);
        self.executes.clearRetainingCapacity();
        self.unblocks.clearRetainingCapacity();
    }

    pub fn pending(self: *const Sender) usize {
        return self.executes.items.len + self.unblocks.items.len;
    }

    /// Take what a committed transition produced.
    ///
    /// Called only after the commit. An effect handed over before that would be
    /// a message about a transition that may not have happened.
    pub fn enqueue(self: *Sender, effects: []const handle.Effect) !void {
        for (effects) |e| {
            switch (e.kind) {
                .execute => try self.upsert(&self.executes, .{
                    .kind = .execute,
                    .address = try self.allocator.dupe(u8, e.address),
                    .id = try self.allocator.dupe(u8, e.task_id),
                    .version = e.version,
                }),
                .unblock => try self.upsert(&self.unblocks, .{
                    .kind = .unblock,
                    .address = try self.allocator.dupe(u8, e.address),
                    .id = try self.allocator.dupe(u8, e.promise_id),
                    .promise_json = try self.allocator.dupe(u8, e.promise_json),
                }),
            }
        }
        self.pump();
    }

    /// Insert or replace, keeping the collection sorted by its key.
    ///
    /// An `execute` keys on the task id alone: the same task offered to a new
    /// address supersedes the old offer, because the address is part of the
    /// answer rather than part of the question. An `unblock` keys on both,
    /// because two listeners on one promise are two messages.
    fn upsert(self: *Sender, into: *std.ArrayListUnmanaged(Held), held: Held) !void {
        var candidate = held;
        for (into.items) |*existing| {
            const same = std.mem.eql(u8, existing.id, candidate.id) and
                (candidate.kind == .execute or std.mem.eql(u8, existing.address, candidate.address));
            if (!same) continue;
            existing.deinit(self.allocator);
            existing.* = candidate;
            return;
        }
        errdefer candidate.deinit(self.allocator);
        try into.append(self.allocator, candidate);
        const items = into.items;
        std.mem.sort(Held, items, {}, struct {
            fn less(_: void, a: Held, b: Held) bool {
                return switch (std.mem.order(u8, a.id, b.id)) {
                    .lt => true,
                    .gt => false,
                    .eq => std.mem.lessThan(u8, a.address, b.address),
                };
            }
        }.less);
    }

    /// Hand as much to the bus as the concurrency limit allows.
    pub fn pump(self: *Sender) void {
        if (self.hold) return;
        while (self.in_flight < self.concurrency and self.pending() > 0) {
            // Executes first: an offer is work waiting to start, and a
            // notification is not.
            const from = if (self.executes.items.len > 0) &self.executes else &self.unblocks;
            var held = from.orderedRemove(0);
            self.dispatch(&held) catch {
                // No memory to render it. Dropping is honest: the retry deadline
                // covers an execute, and an unblock was never guaranteed.
                self.failed += 1;
            };
            held.deinit(self.allocator);
        }
    }

    fn dispatch(self: *Sender, held: *const Held) !void {
        const scheme = protocol.scheme_of(held.address) orelse {
            // A validated address with no scheme cannot happen; if it does,
            // saying so is better than a delivery that never completes.
            self.failed += 1;
            return;
        };
        if (!self.bus.serves(scheme)) {
            self.failed += 1;
            return;
        }

        var body = std.ArrayList(u8).init(self.allocator);
        errdefer body.deinit();
        try self.render(&body, held, self.server_url);

        const flight = try self.allocator.create(InFlight);
        errdefer self.allocator.destroy(flight);
        flight.* = .{
            .delivery = .{ .address = undefined, .body = undefined },
            .sender = self,
            .address = try self.allocator.dupe(u8, held.address),
            .body = try body.toOwnedSlice(),
        };
        flight.delivery.address = flight.address;
        flight.delivery.body = flight.body;
        flight.delivery.listen(*InFlight, flight, on_delivered);

        self.in_flight += 1;
        self.sent += 1;
        self.bus.send(&flight.delivery);
    }

    fn on_delivered(flight: *InFlight, delivery: *env.Delivery) void {
        const self = flight.sender;
        switch (delivery.outcome) {
            .delivered => self.delivered += 1,
            // Not retried here. See the module comment: an execute's retry is
            // already committed in the document, and an unblock was never owed.
            .failed, .pending => self.failed += 1,
        }
        self.allocator.free(flight.address);
        self.allocator.free(flight.body);
        self.allocator.destroy(flight);
        assert(self.in_flight > 0);
        self.in_flight -= 1;
        self.pump();
    }

    /// One message, as the wire carries it.
    ///
    /// Members in alphabetical order — `data`, `head`, `kind` — which is what a
    /// JSON encoder over a sorted map produces, and therefore what the reference
    /// implementation's messages look like byte for byte.
    fn render(self: *const Sender, out: *std.ArrayList(u8), held: *const Held, server_url: []const u8) !void {
        _ = self;
        var w = json.Writer.init(out);
        try w.object_begin();
        try w.key("data");
        switch (held.kind) {
            .execute => {
                try w.object_begin();
                try w.key("task");
                try w.object_begin();
                try w.field_string("id", held.id);
                try w.field_int("version", held.version);
                try w.object_end();
                try w.object_end();
            },
            .unblock => {
                try w.object_begin();
                try w.field_raw("promise", held.promise_json);
                try w.object_end();
            },
        }
        try w.key("head");
        try w.object_begin();
        // Only an execute carries one: a worker answering an offer needs to know
        // where to answer, and a listener being told a result has nothing to
        // answer.
        if (held.kind == .execute and server_url.len > 0) {
            try w.field_string("serverUrl", server_url);
        }
        try w.object_end();
        try w.field_string("kind", switch (held.kind) {
            .execute => "execute",
            .unblock => "unblock",
        });
        try w.object_end();
    }

    /// The held messages as `debug.snap` reports them: executes by task id, then
    /// unblocks by promise id and address, each with an empty head.
    ///
    /// The head is empty rather than carrying the server's own URL because the
    /// snapshot is compared between implementations, and where a server lives is
    /// not part of what it decided.
    pub fn write_snapshot(self: *const Sender, w: *json.Writer) !void {
        try w.array_begin();
        for ([_][]const Held{ self.executes.items, self.unblocks.items }) |group| {
            for (group) |held| {
                try w.object_begin();
                try w.field_string("address", held.address);
                try w.key("message");
                var body = std.ArrayList(u8).init(self.allocator);
                defer body.deinit();
                try self.render(&body, &held, "");
                try w.raw(body.items);
                try w.object_end();
            }
        }
        try w.array_end();
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// A bus that records rather than delivers, and can be told to fail.
const Recorder = struct {
    const Record = struct { address: []u8, body: []u8 };

    allocator: std.mem.Allocator,
    sent: std.ArrayList(Record),
    fail: bool = false,

    fn init(allocator: std.mem.Allocator) Recorder {
        return .{
            .allocator = allocator,
            .sent = std.ArrayList(Record).init(allocator),
        };
    }

    fn deinit(self: *Recorder) void {
        for (self.sent.items) |s| {
            self.allocator.free(s.address);
            self.allocator.free(s.body);
        }
        self.sent.deinit();
    }

    fn bus(self: *Recorder) env.MessageBus {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: env.MessageBus.VTable = .{ .send = send_erased, .serves = serves_erased };

    fn serves_erased(_: *anyopaque, scheme: []const u8) bool {
        return !std.mem.eql(u8, scheme, "nothing");
    }

    fn send_erased(ptr: *anyopaque, delivery: *env.Delivery) void {
        const self: *Recorder = @ptrCast(@alignCast(ptr));
        self.sent.append(.{
            .address = self.allocator.dupe(u8, delivery.address) catch unreachable,
            .body = self.allocator.dupe(u8, delivery.body) catch unreachable,
        }) catch unreachable;
        delivery.complete(if (self.fail) .failed else .delivered, "");
    }
};

fn execute_effect(address: []const u8, task_id: []const u8, version: i64) handle.Effect {
    return .{ .kind = .execute, .address = address, .task_id = task_id, .version = version };
}

fn unblock_effect(address: []const u8, promise_id: []const u8, body: []const u8) handle.Effect {
    return .{ .kind = .unblock, .address = address, .promise_id = promise_id, .promise_json = body };
}

test "an execute carries the server url and the task at its version" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();

    try s.enqueue(&.{execute_effect("http://w:1", "o:t", 3)});
    try testing.expectEqual(@as(usize, 1), rec.sent.items.len);
    try testing.expectEqualStrings("http://w:1", rec.sent.items[0].address);
    try testing.expectEqualStrings(
        "{\"data\":{\"task\":{\"id\":\"o:t\",\"version\":3}},\"head\":{\"serverUrl\":\"http://server:8001\"},\"kind\":\"execute\"}",
        rec.sent.items[0].body,
    );
    try testing.expectEqual(@as(u64, 1), s.delivered);
    try testing.expectEqual(@as(usize, 0), s.pending());
}

test "an unblock carries the promise and no server url" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();

    try s.enqueue(&.{unblock_effect("http://w:1", "o:p", "{\"id\":\"o:p\",\"state\":\"resolved\"}")});
    try testing.expectEqualStrings(
        "{\"data\":{\"promise\":{\"id\":\"o:p\",\"state\":\"resolved\"}},\"head\":{},\"kind\":\"unblock\"}",
        rec.sent.items[0].body,
    );
}

test "a second offer of the same task replaces the first" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();
    s.hold = true;

    try s.enqueue(&.{execute_effect("http://a", "o:t", 1)});
    try s.enqueue(&.{execute_effect("http://b", "o:t", 2)});
    try testing.expectEqual(@as(usize, 1), s.pending());
    try testing.expectEqual(@as(i64, 2), s.executes.items[0].version);
    try testing.expectEqualStrings("http://b", s.executes.items[0].address);

    // Two listeners on one promise are two messages; the same listener twice is one.
    try s.enqueue(&.{unblock_effect("http://a", "o:p", "{}")});
    try s.enqueue(&.{unblock_effect("http://b", "o:p", "{}")});
    try s.enqueue(&.{unblock_effect("http://a", "o:p", "{}")});
    try testing.expectEqual(@as(usize, 2), s.unblocks.items.len);
}

test "held messages are reported sorted, with empty heads" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();
    s.hold = true;

    try s.enqueue(&.{
        execute_effect("http://w", "o:z", 0),
        execute_effect("http://w", "o:a", 1),
        unblock_effect("http://w", "o:q", "{\"id\":\"o:q\"}"),
        unblock_effect("http://w", "o:b", "{\"id\":\"o:b\"}"),
    });
    try testing.expectEqual(@as(usize, 0), rec.sent.items.len);

    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    var w = json.Writer.init(&out);
    try s.write_snapshot(&w);
    try testing.expectEqualStrings(
        "[" ++
            "{\"address\":\"http://w\",\"message\":{\"data\":{\"task\":{\"id\":\"o:a\",\"version\":1}},\"head\":{},\"kind\":\"execute\"}}," ++
            "{\"address\":\"http://w\",\"message\":{\"data\":{\"task\":{\"id\":\"o:z\",\"version\":0}},\"head\":{},\"kind\":\"execute\"}}," ++
            "{\"address\":\"http://w\",\"message\":{\"data\":{\"promise\":{\"id\":\"o:b\"}},\"head\":{},\"kind\":\"unblock\"}}," ++
            "{\"address\":\"http://w\",\"message\":{\"data\":{\"promise\":{\"id\":\"o:q\"}},\"head\":{},\"kind\":\"unblock\"}}" ++
            "]",
        out.items,
    );

    s.clear();
    try testing.expectEqual(@as(usize, 0), s.pending());
}

test "a failed delivery is counted and not retried" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    rec.fail = true;
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();

    try s.enqueue(&.{execute_effect("http://w:1", "o:t", 0)});
    try testing.expectEqual(@as(u64, 1), s.failed);
    try testing.expectEqual(@as(usize, 1), rec.sent.items.len);
    try testing.expectEqual(@as(usize, 0), s.pending());
}

test "an address nothing routes is not queued forever" {
    var rec = Recorder.init(testing.allocator);
    defer rec.deinit();
    var s = Sender.init(testing.allocator, rec.bus(), "http://server:8001");
    defer s.deinit();

    try s.enqueue(&.{execute_effect("nothing://w", "o:t", 0)});
    try testing.expectEqual(@as(usize, 0), rec.sent.items.len);
    try testing.expectEqual(@as(usize, 0), s.pending());
    try testing.expectEqual(@as(u64, 1), s.failed);
}

test "the concurrency limit bounds what is in flight" {
    // A bus that holds deliveries open, so everything handed to it stays in
    // flight until the test lets it go.
    const Stalled = struct {
        held: std.ArrayList(*env.Delivery),

        fn bus(self: *@This()) env.MessageBus {
            return .{ .ptr = self, .vtable = &.{ .send = send, .serves = serves } };
        }
        fn serves(_: *anyopaque, _: []const u8) bool {
            return true;
        }
        fn send(ptr: *anyopaque, delivery: *env.Delivery) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.held.append(delivery) catch unreachable;
        }
        /// Let them all through, oldest first.
        fn release(self: *@This()) void {
            while (self.held.items.len > 0) {
                const d = self.held.orderedRemove(0);
                d.complete(.delivered, "");
            }
        }
    };
    var stalled = Stalled{ .held = std.ArrayList(*env.Delivery).init(testing.allocator) };
    defer stalled.held.deinit();
    var s = Sender.init(testing.allocator, stalled.bus(), "http://server:8001");
    defer s.deinit();
    s.concurrency = 2;

    var effects: [5]handle.Effect = undefined;
    var ids: [5][4]u8 = undefined;
    for (0..5) |i| {
        ids[i] = [_]u8{ 'o', ':', 't', @intCast('0' + i) };
        effects[i] = execute_effect("http://w", &ids[i], @intCast(i));
    }
    try s.enqueue(&effects);
    try testing.expectEqual(@as(usize, 2), stalled.held.items.len);
    try testing.expectEqual(@as(u32, 2), s.in_flight);
    try testing.expectEqual(@as(usize, 3), s.pending());

    // Completing one lets the next through, and so on until the outbox is empty.
    stalled.release();
    try testing.expectEqual(@as(u32, 0), s.in_flight);
    try testing.expectEqual(@as(usize, 0), s.pending());
    try testing.expectEqual(@as(u64, 5), s.delivered);
}
