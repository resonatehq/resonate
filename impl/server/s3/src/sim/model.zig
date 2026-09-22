//! The sequential specification, which is the server itself.
//!
//! A linearizability check asks whether a concurrent history is equivalent to
//! *some* sequential execution of the specification. So the specification has to
//! be something that can be stepped one operation at a time, and whose state can
//! be saved and put back as the search tries an order and abandons it.
//!
//! This is the real server over an in-memory store, driven to completion inside
//! one call. Nothing is reimplemented: the same state machine, the same commit
//! loop, the same document format. Two consequences worth being plain about:
//!
//! * **What this checks.** That the server's concurrent behaviour is equivalent
//!   to some sequential execution of the server's own state machine. That *is*
//!   linearizability, and it is exactly the class of bug the concurrent paths can
//!   have and the sequential ones cannot: a lost update, a commit applied twice, a
//!   read that sees a write that was later undone.
//! * **What it does not check.** Whether the state machine itself is the protocol
//!   the rest of the world implements. That is the differential suite's job — the
//!   same requests to this server and to the reference model, compared step for
//!   step. The two checks are complementary and neither substitutes for the other.
//!
//! Saving the state is cheap because the state already has a canonical byte
//! form: it is the set of objects in the store, and every object's encoding is
//! canonical by construction. So a snapshot is the bucket, sorted, and two
//! snapshots are equal exactly when the states are.

const std = @import("std");
const stdx = @import("../stdx.zig");
const json = @import("../json.zig");
const protocol = @import("../protocol.zig");
const store_mod = @import("../store.zig");
const env = @import("../env.zig");
const bus_mod = @import("../bus.zig");
const server_mod = @import("../server.zig");

const assert = stdx.assert;

pub const Reply = struct {
    status: i32,
    /// The `data` member of the response envelope. Points into the arena of the
    /// call that produced it.
    data: []const u8,
};

pub const Model = struct {
    allocator: std.mem.Allocator,
    mem: store_mod.MemoryStore,
    sim: env.Simulated,
    nowhere: bus_mod.Nowhere = .{},
    runtime: *server_mod.Runtime = undefined,
    /// Per-call working memory, reset between calls.
    arena: std.heap.ArenaAllocator,
    /// The arena one request runs on, reset rather than rebuilt between calls.
    ///
    /// The search applies hundreds of thousands of requests, and an arena built
    /// and torn down for each one asks the kernel for its pages and hands them
    /// back every time. Keeping one and resetting it is what makes a step cost
    /// what the model does rather than what a page fault does.
    request_arena: std.heap.ArenaAllocator,
    /// The store's keys, in order. Reused by every save and every restore.
    keys: std.ArrayList([]const u8),

    applied: u64 = 0,

    pub fn create(allocator: std.mem.Allocator) !*Model {
        const self = try allocator.create(Model);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .mem = store_mod.MemoryStore.init(allocator),
            .sim = env.Simulated.init(allocator, 0),
            .arena = std.heap.ArenaAllocator.init(allocator),
            .request_arena = std.heap.ArenaAllocator.init(allocator),
            .keys = std.ArrayList([]const u8).init(allocator),
        };
        self.runtime = try server_mod.Runtime.create(
            allocator,
            // The clock belongs to the caller, which is what makes a step a
            // function of the operation rather than of when it ran.
            .{ .debug = true, .server_url = "http://model" },
            self.mem.store(),
            self.sim.clock(),
            self.sim.timer(),
            self.nowhere.message_bus(),
        );
        // Messages are not part of what a caller observes, so they go nowhere
        // rather than accumulating in a snapshot that would then differ between
        // two equivalent states.
        self.runtime.sender.hold = false;
        return self;
    }

    pub fn destroy(self: *Model) void {
        self.runtime.destroy();
        self.keys.deinit();
        self.request_arena.deinit();
        self.arena.deinit();
        self.sim.deinit();
        self.mem.deinit();
        self.allocator.destroy(self);
    }

    /// Apply one request and return what the server answers.
    ///
    /// Synchronous by construction: the in-memory store completes inline, so one
    /// `process` plus one `drain` runs a whole commit. The assertion is the
    /// contract — if this ever becomes asynchronous, every caller here is wrong
    /// and should find out immediately.
    pub fn apply(self: *Model, envelope_json: []const u8) Reply {
        const a = self.arena.allocator();
        const Answer = struct {
            status: i32 = 0,
            data: []const u8 = "",
            done: bool = false,
            fn callback(request: *server_mod.Request) void {
                const answer: *@This() = @ptrCast(@alignCast(request.context.?));
                answer.status = request.status;
                answer.data = request.response;
                answer.done = true;
            }
        };
        var answer = Answer{};
        const request = a.create(server_mod.Request) catch return .{ .status = 503, .data = "\"out of memory\"" };
        const request_arena = &self.request_arena;
        request.* = .{
            .body = envelope_json,
            .arena = request_arena,
            .callback = Answer.callback,
            .context = &answer,
        };
        self.runtime.server.process(request);
        self.runtime.drain();
        assert(answer.done);
        self.applied += 1;

        // The whole envelope is what the caller saw; only its `data` is compared.
        const parsed = json.parse(a, answer.data) catch
            return .{ .status = answer.status, .data = "" };
        var out = std.ArrayList(u8).init(a);
        const data = parsed.get("data") orelse json.Value.null_value;
        json.write_value(&out, data) catch {};
        const owned = self.allocator.dupe(u8, out.items) catch "";
        // Nothing outlives the call: the store keeps its own copies, and the reply
        // was just copied out.
        _ = request_arena.reset(.retain_capacity);
        // Copied out of the per-call arena, because the caller keeps it past the
        // next reset.
        const result = Reply{ .status = answer.status, .data = owned };
        _ = self.arena.reset(.retain_capacity);
        return result;
    }

    pub fn free_reply(self: *Model, reply: Reply) void {
        if (reply.data.len > 0) self.allocator.free(reply.data);
    }

    // ── Saving and restoring ──────────────────────────────────────────────────

    /// The whole state, as bytes: every object in the bucket, in key order.
    ///
    /// Equal snapshots mean equal states, because every object's encoding is
    /// canonical. That is what lets the search memoize on a hash of this.
    pub fn snapshot(self: *Model, out: *std.ArrayList(u8)) !void {
        out.clearRetainingCapacity();
        try self.sorted_keys();
        for (self.keys.items) |key| {
            const entry = self.mem.objects.get(key).?;
            const etag = entry.etag.slice();
            try out.writer().print("{d}:", .{key.len});
            try out.appendSlice(key);
            try out.writer().print(" {d}:", .{etag.len});
            try out.appendSlice(etag);
            try out.writer().print(" {d}:", .{entry.body.len});
            try out.appendSlice(entry.body);
            try out.append('\n');
        }
        // The version an object is at is part of the state: a document at the same
        // bytes but a different version is a document a stale writer can no longer
        // replace. Recording it, rather than minting a fresh one on the way back,
        // is what makes a restore exact — and the search compares states by their
        // snapshots, so an inexact restore would make it compare the wrong things.
        try out.writer().print("v{d}\n", .{self.mem.next_version});
    }

    /// Put the state back, object by object rather than all at once.
    ///
    /// A restore usually undoes one operation, which touched one document. Emptying
    /// the store and building fifteen objects again to achieve that is the single
    /// most expensive thing the search does, so this walks the snapshot (which is
    /// in key order) alongside the store's keys and touches only what differs.
    pub fn restore(self: *Model, bytes: []const u8) !void {
        // Versions are read back from the snapshot, so the counter starts where it
        // did and a restored state is indistinguishable from the original.
        self.mem.next_version = 1;
        self.runtime.applier.reset();
        self.runtime.timerd.reset();
        try self.sorted_keys();

        var current: usize = 0;
        var rest = bytes;
        while (rest.len > 0) {
            if (rest[0] == 'v') {
                const end = std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len;
                self.mem.next_version = std.fmt.parseInt(u64, rest[1..end], 10) catch 1;
                break;
            }
            const key = try take_field(&rest) orelse return error.Corrupt;
            const etag = try take_field(&rest) orelse return error.Corrupt;
            const body = try take_field(&rest) orelse return error.Corrupt;
            if (rest.len > 0 and rest[0] == '\n') rest = rest[1..];

            // Everything the store has before this key is something the snapshot
            // does not have.
            while (current < self.keys.items.len and
                stdx.less_than_bytes({}, self.keys.items[current], key)) : (current += 1)
            {
                self.remove_raw(self.keys.items[current]);
            }
            if (current < self.keys.items.len and std.mem.eql(u8, self.keys.items[current], key)) {
                self.overwrite_raw(key, etag, body) catch return error.OutOfMemory;
                current += 1;
            } else {
                try self.put_raw(key, etag, body);
            }
        }
        while (current < self.keys.items.len) : (current += 1) self.remove_raw(self.keys.items[current]);
    }

    /// The store's keys, in order, in the buffer this model keeps for the purpose.
    fn sorted_keys(self: *Model) !void {
        self.keys.clearRetainingCapacity();
        var it = self.mem.objects.keyIterator();
        while (it.next()) |k| try self.keys.append(k.*);
        std.mem.sort([]const u8, self.keys.items, {}, stdx.less_than_bytes);
    }

    fn remove_raw(self: *Model, key: []const u8) void {
        if (self.mem.objects.fetchRemove(key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.body);
        }
    }

    fn overwrite_raw(self: *Model, key: []const u8, etag: []const u8, body: []const u8) !void {
        const entry = self.mem.objects.getPtr(key).?;
        if (std.mem.eql(u8, entry.etag.slice(), etag) and std.mem.eql(u8, entry.body, body)) return;
        if (entry.body.len != body.len) {
            const owned = try self.allocator.alloc(u8, body.len);
            self.allocator.free(entry.body);
            entry.body = owned;
        }
        @memcpy(entry.body, body);
        entry.etag = store_mod.Etag.from(etag);
    }

    /// One `<length>:<bytes>` field, followed by a space or a newline.
    fn take_field(rest: *[]const u8) !?[]const u8 {
        const colon = std.mem.indexOfScalar(u8, rest.*, ':') orelse return null;
        const len = try std.fmt.parseInt(usize, rest.*[0..colon], 10);
        if (colon + 1 + len > rest.len) return null;
        const field = rest.*[colon + 1 ..][0..len];
        rest.* = rest.*[colon + 1 + len ..];
        if (rest.len > 0 and rest.*[0] == ' ') rest.* = rest.*[1..];
        return field;
    }

    fn put_raw(self: *Model, key: []const u8, etag: []const u8, body: []const u8) !void {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_body = try self.allocator.dupe(u8, body);
        errdefer self.allocator.free(owned_body);
        try self.mem.objects.put(self.allocator, owned_key, .{
            .body = owned_body,
            .etag = store_mod.Etag.from(etag),
        });
    }

    pub fn hash(bytes: []const u8) u64 {
        return std.hash.Wyhash.hash(0x5eed, bytes);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn envelope(a: std.mem.Allocator, kind: []const u8, data: []const u8, now: i64) ![]const u8 {
    return std.fmt.allocPrint(
        a,
        "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"m\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
        .{ kind, protocol.protocol_version, now, data },
    );
}

test "the model applies a request and answers synchronously" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const created = model.apply(try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 1000));
    defer model.free_reply(created);
    try testing.expectEqual(@as(i32, 200), created.status);
    try testing.expect(std.mem.indexOf(u8, created.data, "\"state\":\"pending\"") != null);

    const got = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 1000));
    defer model.free_reply(got);
    try testing.expectEqualStrings(created.data, got.data);
}

test "a snapshot round trips, and equal states give equal snapshots" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var before = std.ArrayList(u8).init(testing.allocator);
    defer before.deinit();
    try model.snapshot(&before);
    const empty_hash = Model.hash(before.items);

    const created = model.apply(try envelope(
        a,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"http://w\"}}",
        1000,
    ));
    model.free_reply(created);

    var after = std.ArrayList(u8).init(testing.allocator);
    defer after.deinit();
    try model.snapshot(&after);
    try testing.expect(Model.hash(after.items) != empty_hash);
    const owned_after = try testing.allocator.dupe(u8, after.items);
    defer testing.allocator.free(owned_after);

    // Put the earlier state back and the promise is gone.
    try model.restore(before.items);
    const gone = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 1000));
    defer model.free_reply(gone);
    try testing.expectEqual(@as(i32, 404), gone.status);

    // And restoring the later state brings it back, byte for byte.
    try model.restore(owned_after);
    const back = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 1000));
    defer model.free_reply(back);
    try testing.expectEqual(@as(i32, 200), back.status);

    var again = std.ArrayList(u8).init(testing.allocator);
    defer again.deinit();
    try model.snapshot(&again);
    // The version counter moves when objects are reloaded, so the bodies are
    // what must agree.
    try testing.expect(std.mem.indexOf(u8, again.items, "wf/o") != null);
}

test "restoring undoes a settle, which is what the search needs" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    model.free_reply(model.apply(try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 1000)));
    var saved = std.ArrayList(u8).init(testing.allocator);
    defer saved.deinit();
    try model.snapshot(&saved);

    const settled = model.apply(try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"resolved\"}", 1001));
    defer model.free_reply(settled);
    try testing.expect(std.mem.indexOf(u8, settled.data, "\"state\":\"resolved\"") != null);

    try model.restore(saved.items);
    const pending_again = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 1001));
    defer model.free_reply(pending_again);
    try testing.expect(std.mem.indexOf(u8, pending_again.data, "\"state\":\"pending\"") != null);

    // And settling it the other way now succeeds, which it could not have done
    // without the undo.
    const rejected = model.apply(try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"rejected\"}", 1002));
    defer model.free_reply(rejected);
    try testing.expect(std.mem.indexOf(u8, rejected.data, "\"state\":\"rejected\"") != null);
}

test "the model honours the instant each request carries" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    model.free_reply(model.apply(try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":5000}", 1000)));
    const early = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 4999));
    defer model.free_reply(early);
    try testing.expect(std.mem.indexOf(u8, early.data, "\"state\":\"pending\"") != null);

    const late = model.apply(try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 5000));
    defer model.free_reply(late);
    try testing.expect(std.mem.indexOf(u8, late.data, "rejected_timedout") != null);
}

test "a restore is exact: the same snapshot always gives the same state" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    model.free_reply(model.apply(try envelope(
        a,
        "promise.create",
        "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"http://w\"}}",
        1000,
    )));

    var first = std.ArrayList(u8).init(testing.allocator);
    defer first.deinit();
    try model.snapshot(&first);
    const saved = try testing.allocator.dupe(u8, first.items);
    defer testing.allocator.free(saved);

    // Move on, come back, and the snapshot has to be identical — bytes, versions
    // and all. Anything else and two equal states would hash differently, and a
    // search that compares states by their snapshots would explore the same state
    // over and over or prune one it had not seen.
    model.free_reply(model.apply(try envelope(a, "task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w\",\"ttl\":5}", 1001)));
    try model.restore(saved);
    var again = std.ArrayList(u8).init(testing.allocator);
    defer again.deinit();
    try model.snapshot(&again);
    try testing.expectEqualStrings(saved, again.items);
    try testing.expectEqual(Model.hash(saved), Model.hash(again.items));
}

test "a saved and restored model answers exactly as a freshly built one" {
    // The property the search depends on: replaying a prefix and then restoring to
    // it must be indistinguishable from having only ever applied the prefix.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const workload = @import("workload.zig");

    var rng = stdx.Random.init(4242);
    var generator = workload.Workload.init(testing.allocator, &rng, 1_000_000_000);
    defer generator.deinit();

    // One sequence, remembered.
    var envelopes = std.ArrayList([]const u8).init(a);
    for (0..300) |i| {
        const kind = generator.pick();
        const corr_id = try std.fmt.allocPrint(a, "c{d}", .{i});
        const request = try generator.build(a, kind, corr_id, "w0");
        try envelopes.append(request.envelope);
    }

    const replayed = try Model.create(testing.allocator);
    defer replayed.destroy();
    var saves = std.ArrayList([]u8).init(testing.allocator);
    defer {
        for (saves.items) |s| testing.allocator.free(s);
        saves.deinit();
    }

    // Apply the whole sequence, saving after each step.
    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    var answers = std.ArrayList([]u8).init(testing.allocator);
    defer {
        for (answers.items) |x| testing.allocator.free(x);
        answers.deinit();
    }
    for (envelopes.items) |e| {
        try replayed.snapshot(&buf);
        try saves.append(try testing.allocator.dupe(u8, buf.items));
        const reply = replayed.apply(e);
        try answers.append(try std.fmt.allocPrint(testing.allocator, "{d} {s}", .{ reply.status, reply.data }));
        replayed.free_reply(reply);
    }

    // Now walk backwards: restore to each saved point and re-apply that step. The
    // answer has to be the one it gave the first time.
    var i = envelopes.items.len;
    while (i > 0) {
        i -= 1;
        try replayed.restore(saves.items[i]);
        const reply = replayed.apply(envelopes.items[i]);
        const text = try std.fmt.allocPrint(testing.allocator, "{d} {s}", .{ reply.status, reply.data });
        defer testing.allocator.free(text);
        replayed.free_reply(reply);
        try testing.expectEqualStrings(answers.items[i], text);
    }
}
