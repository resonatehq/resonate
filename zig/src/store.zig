//! The object store port, the key layout, and an in-memory store that honours
//! it exactly.
//!
//! ## The port is six operations and three errors
//!
//! Six operations, because that is all an object store has to offer for this to
//! work: read with a version, create-if-absent, replace-if-unchanged, write
//! blindly, delete, and list in key order.
//!
//! The three errors are the load-bearing part. A conditional write can fail two
//! ways and they demand opposite responses:
//!
//! * `precondition_failed` — someone else wrote first. Everything decided
//!   against the old version was decided against state that no longer exists,
//!   so it must be **re-decided**. Replaying it would be wrong.
//! * `conflict` — the service could not order two concurrent conditional
//!   writes. Nothing is known about whether this one landed, so **retry the
//!   same write**; if that comes back `precondition_failed`, fall into the
//!   re-decide path.
//! * `unavailable` — no answer at all.
//!
//! Collapsing the first two into one "write failed" is the classic way to lose
//! a write, which is why they are separate variants rather than a flag.
//!
//! ## Not every S3-compatible store qualifies
//!
//! Real conditional writes are required. S3, R2, GCS and Azure have them.
//! Stores that accept a stale `If-Match` silently lose writes under this
//! design, and there is no way for this code to detect that at runtime — the
//! write simply succeeds when it should not.
//!
//! ## Completion, not blocking
//!
//! Every operation is submitted and completed later through a callback. There is
//! one thread and one event loop, which is what lets the simulator replace the
//! whole store with a deterministic one and drive a hundred servers through a
//! million operations in a second.

const std = @import("std");
const stdx = @import("stdx.zig");
const assert = stdx.assert;

/// An object's version, as the store reports it. Inline rather than allocated:
/// it is copied into every conditional write and outlives no allocation.
pub const Etag = struct {
    pub const max = 96;

    bytes: [max]u8 = undefined,
    len: u8 = 0,

    pub fn from(text: []const u8) Etag {
        var e: Etag = .{};
        const n = @min(text.len, max);
        @memcpy(e.bytes[0..n], text[0..n]);
        e.len = @intCast(n);
        return e;
    }

    pub fn slice(self: *const Etag) []const u8 {
        return self.bytes[0..self.len];
    }

    pub fn eql(a: Etag, b: Etag) bool {
        return std.mem.eql(u8, a.slice(), b.slice());
    }
};

/// What an operation requires of what is already there.
pub const Precondition = union(enum) {
    /// No condition. For a write, this is a blind overwrite — used for timer
    /// objects, where the *key* carries the whole value being written, so
    /// overwriting is idempotent.
    none,
    /// A write that creates only: the object must not exist.
    absent,
    /// A write that replaces only if the object is still at this version.
    match: Etag,
    /// A **read** that only wants the body if it has changed since this version.
    /// Answered `not_modified` when it has not, which is what makes validating a
    /// cached document cost a round trip rather than a transfer.
    unchanged: Etag,
};

pub const Kind = enum { get, put, delete, list };

pub const Result = union(enum) {
    /// Not yet completed. Every operation starts here, and a store that
    /// completes an operation twice is a bug this catches.
    pending,
    /// `get` found the object.
    found: struct { body: []const u8, etag: Etag },
    /// `get` found nothing. Not an error: an origin with no document is an
    /// origin with no promises.
    not_found,
    /// `get` found the object still at the version the caller named, so it did
    /// not send the body. The caller's own copy is current.
    not_modified,
    /// `put` landed, at this version.
    written: Etag,
    /// `delete` is done. Deleting what is not there succeeds.
    deleted,
    /// `list` returned these keys, in ascending key order.
    keys: []const []const u8,
    /// The write's precondition did not hold. Re-read and re-decide.
    precondition_failed,
    /// Two conditional writes the store could not order. Retry this one.
    conflict,
    /// No answer. The caller may already have been applied.
    unavailable: []const u8,

    pub fn is_error(self: Result) bool {
        return switch (self) {
            .precondition_failed, .conflict, .unavailable => true,
            else => false,
        };
    }
};

/// One submitted operation. The caller owns it until its callback runs, and the
/// store writes the answer into `result`.
pub const Operation = struct {
    kind: Kind,
    key: []const u8,
    /// `put` only. The store must not retain it past completion.
    body: []const u8 = &.{},
    precondition: Precondition = .none,
    /// `list` only.
    max_keys: u32 = 1000,

    /// Where the store allocates whatever the answer needs — a body, a list of
    /// keys. The caller's, so the caller decides when it goes.
    arena: std.mem.Allocator,

    callback: *const fn (*Operation) void,
    context: ?*anyopaque = null,
    result: Result = .pending,

    /// Intrusive link, for whatever queue the store keeps.
    next: ?*Operation = null,

    pub fn complete(self: *Operation, result: Result) void {
        assert(self.result == .pending);
        self.result = result;
        self.callback(self);
    }
};

/// The port. A vtable rather than a comptime parameter: the indirect call costs
/// nothing next to an object-store round trip, and it keeps every layer above
/// this one free of a type parameter it would otherwise have to thread.
pub const Store = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (ptr: *anyopaque, op: *Operation) void,
    };

    pub fn submit(self: Store, op: *Operation) void {
        assert(op.result == .pending);
        self.vtable.submit(self.ptr, op);
    }
};

// ── Keys ──────────────────────────────────────────────────────────────────────

/// Zero-padded width of a deadline in a timer key. An i64 needs 19 digits; 20
/// leaves the padding stable if that ever grows.
pub const deadline_width = 20;

/// Where everything lives under the bucket.
///
/// Three prefixes and nothing else. No index, no manifest, no write-ahead log,
/// no lock objects, no lease objects, no compaction markers — the documents
/// *are* the state, and the keys of the timer objects *are* the schedule.
pub const KeySpace = struct {
    /// Empty, or ending in `/`.
    prefix: []const u8,
    /// How many prefixes the timer keys are spread across. Timer keys are
    /// deadline-ordered, so they are written in ascending order forever, and a
    /// monotone key prefix is the one access pattern object stores are worst at.
    timer_shards: u32,

    pub const default_timer_shards: u32 = 4;

    pub fn init(prefix: []const u8, timer_shards: u32) KeySpace {
        assert(prefix.len == 0 or prefix[prefix.len - 1] == '/');
        return .{ .prefix = prefix, .timer_shards = @max(timer_shards, 1) };
    }

    /// Normalize a caller-supplied prefix: at most one trailing slash.
    pub fn normalize_prefix(allocator: std.mem.Allocator, raw: []const u8) ![]const u8 {
        const trimmed = std.mem.trim(u8, raw, "/");
        if (trimmed.len == 0) return "";
        return std.fmt.allocPrint(allocator, "{s}/", .{trimmed});
    }

    pub fn doc_prefix(self: KeySpace, out: *std.ArrayList(u8)) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("wf/");
        return out.items;
    }

    pub fn sched_prefix(self: KeySpace, out: *std.ArrayList(u8)) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("sched/");
        return out.items;
    }

    pub fn timer_prefix(self: KeySpace, out: *std.ArrayList(u8)) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("t/");
        return out.items;
    }

    /// `<prefix>wf/<escaped origin>`
    pub fn doc_key(self: KeySpace, out: *std.ArrayList(u8), origin: []const u8) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("wf/");
        try encode_key(out, origin);
        return out.items;
    }

    /// `<prefix>sched/<escaped id>`
    pub fn sched_key(self: KeySpace, out: *std.ArrayList(u8), id: []const u8) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("sched/");
        try encode_key(out, id);
        return out.items;
    }

    /// `<prefix>t/<NN>/<20-digit deadline>_<escaped origin>@<generation>`
    ///
    /// The deadline is zero-padded so that lexicographic order *is* time order:
    /// that is what makes a capped ascending listing return the nearest
    /// deadlines. A negative deadline cannot occur — every deadline derives from
    /// a validated non-negative `timeoutAt` or from `now` plus a positive TTL —
    /// so clamping at zero is an honest floor rather than a reinterpretation.
    ///
    /// The generation is the commit that armed this deadline, and it is what
    /// makes disarming safe. Without it the key names only (target, deadline),
    /// and a writer removing the deadline it replaced can delete a key some other
    /// writer has just written for the *same* deadline — a schedule deleted and
    /// recreated in the same minute, or a promise timeout that becomes the
    /// nearest deadline again after a shorter one goes away. With it, the key a
    /// writer removes is the one its own predecessor wrote and nobody else can
    /// produce, because a generation belongs to exactly one commit.
    ///
    /// `@` is escaped by `encode_key`, so a raw one is unambiguously the
    /// separator.
    pub fn timer_key(
        self: KeySpace,
        out: *std.ArrayList(u8),
        origin: []const u8,
        at: i64,
        generation: u64,
    ) ![]const u8 {
        return self.timer_key_inner(out, origin, at, generation, false);
    }

    /// A schedule's timer key. A schedule id may not contain `':'` and an
    /// escaped origin cannot either, so the raw `sched:` marker is unambiguous
    /// even for an origin literally called `sched:evil`.
    pub fn sched_timer_key(
        self: KeySpace,
        out: *std.ArrayList(u8),
        id: []const u8,
        at: i64,
        generation: u64,
    ) ![]const u8 {
        return self.timer_key_inner(out, id, at, generation, true);
    }

    fn timer_key_inner(
        self: KeySpace,
        out: *std.ArrayList(u8),
        target: []const u8,
        at: i64,
        generation: u64,
        schedule: bool,
    ) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("t/");
        try out.writer().print("{d:0>2}/", .{self.shard_of(target)});
        try out.writer().print("{d:0>20}_", .{@max(at, 0)});
        if (schedule) try out.appendSlice("sched:");
        try encode_key(out, target);
        try out.writer().print("@{d}", .{generation});
        return out.items;
    }

    pub fn timer_shard_prefix(self: KeySpace, out: *std.ArrayList(u8), shard: u32) ![]const u8 {
        out.clearRetainingCapacity();
        try out.appendSlice(self.prefix);
        try out.appendSlice("t/");
        try out.writer().print("{d:0>2}/", .{shard % self.timer_shards});
        return out.items;
    }

    fn shard_of(self: KeySpace, target: []const u8) u32 {
        var h: u64 = 0xcbf29ce484222325;
        for (target) |b| {
            h ^= b;
            h *%= 0x100000001b3;
        }
        return @intCast((h >> 32) % self.timer_shards);
    }

    /// What a timer key points at.
    pub const TimerEntry = union(enum) {
        origin: struct { deadline: i64, name: []const u8, generation: u64 },
        schedule: struct { deadline: i64, id: []const u8, generation: u64 },

        pub fn deadline(self: TimerEntry) i64 {
            return switch (self) {
                .origin => |o| o.deadline,
                .schedule => |s| s.deadline,
            };
        }
    };

    /// Read a timer key back. An unparseable key is null rather than an error:
    /// something else put it there, and refusing to start because of it would
    /// make one stray object fatal.
    pub fn parse_timer_key(
        self: KeySpace,
        arena: std.mem.Allocator,
        key: []const u8,
    ) !?TimerEntry {
        _ = self;
        const slash = std.mem.lastIndexOfScalar(u8, key, '/') orelse return null;
        const name = key[slash + 1 ..];
        // 20 digits, an underscore, at least one character of target, an `@` and
        // at least one digit of generation.
        if (name.len < deadline_width + 4) return null;
        const deadline = std.fmt.parseInt(i64, name[0..deadline_width], 10) catch return null;
        if (name[deadline_width] != '_') return null;
        var target = name[deadline_width + 1 ..];
        // The generation, if the key carries one. A key without it is not one
        // this build wrote.
        const at_sign = std.mem.lastIndexOfScalar(u8, target, '@') orelse return null;
        const generation = std.fmt.parseInt(u64, target[at_sign + 1 ..], 10) catch return null;
        target = target[0..at_sign];
        if (target.len == 0) return null;
        if (std.mem.startsWith(u8, target, "sched:")) {
            const id = try decode_key(arena, target["sched:".len ..]) orelse return null;
            return .{ .schedule = .{ .deadline = deadline, .id = id, .generation = generation } };
        }
        const origin = try decode_key(arena, target) orelse return null;
        return .{ .origin = .{ .deadline = deadline, .name = origin, .generation = generation } };
    }

    /// The origin a document key names, or null if the key is not one.
    pub fn origin_of_doc_key(
        self: KeySpace,
        arena: std.mem.Allocator,
        key: []const u8,
    ) !?[]const u8 {
        var buf = std.ArrayList(u8).init(arena);
        defer buf.deinit();
        const want = try self.doc_prefix(&buf);
        if (!std.mem.startsWith(u8, key, want)) return null;
        return decode_key(arena, key[want.len..]);
    }

    pub fn id_of_sched_key(
        self: KeySpace,
        arena: std.mem.Allocator,
        key: []const u8,
    ) !?[]const u8 {
        var buf = std.ArrayList(u8).init(arena);
        defer buf.deinit();
        const want = try self.sched_prefix(&buf);
        if (!std.mem.startsWith(u8, key, want)) return null;
        return decode_key(arena, key[want.len..]);
    }
};

/// Percent-escape anything that is not safe and stable in a key.
///
/// The unreserved set is `A-Z a-z 0-9 . -` and everything else becomes `%XX`
/// with uppercase hex, byte by byte over UTF-8. Two characters are the reason
/// the set is this small: `/` would create a path segment, and `:` is the
/// origin/lineage separator this design reserves — which is what makes the
/// `sched:` marker in a timer key unambiguous.
pub fn encode_key(out: *std.ArrayList(u8), s: []const u8) !void {
    const hex = "0123456789ABCDEF";
    for (s) |b| {
        switch (b) {
            'A'...'Z', 'a'...'z', '0'...'9', '.', '-' => try out.append(b),
            else => {
                try out.append('%');
                try out.append(hex[b >> 4]);
                try out.append(hex[b & 0x0f]);
            },
        }
    }
}

/// The exact inverse. A truncated or malformed escape is null.
pub fn decode_key(arena: std.mem.Allocator, s: []const u8) !?[]const u8 {
    var out = std.ArrayList(u8).init(arena);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] != '%') {
            try out.append(s[i]);
            i += 1;
            continue;
        }
        if (i + 3 > s.len) return null;
        const b = std.fmt.parseInt(u8, s[i + 1 ..][0..2], 16) catch return null;
        try out.append(b);
        i += 3;
    }
    return try out.toOwnedSlice();
}

// ── An in-memory store ────────────────────────────────────────────────────────

/// A store in memory, with the same conditional-write semantics as S3.
///
/// This is not a test double that approximates the real thing. It is the
/// production store for a single-process deployment, the store the simulator
/// drives, and the store the differential runs against — so its CAS has to be
/// exactly right, and the fault injection has to be able to make it behave as
/// badly as a real service does.
pub const MemoryStore = struct {
    pub const Faults = struct {
        /// Percent of operations answered `unavailable` instead of being served.
        unavailable_percent: u64 = 0,
        /// Percent of conditional writes answered `conflict` — the store could
        /// not order two of them — without saying whether they landed.
        conflict_percent: u64 = 0,
        /// Percent of writes that *land* and are then reported as
        /// `unavailable`. The nastiest case a caller has to survive: it must
        /// retry, and the retry must be idempotent.
        lost_ack_percent: u64 = 0,
        /// Percent of operations held back to complete on a later drain.
        ///
        /// Not a fault: it is what a store across a network does, and it is the
        /// only way two requests can actually be in flight at once. Serving
        /// everything inline makes a concurrent workload sequential, and a
        /// linearizability check over a sequential history says nothing.
        defer_percent: u64 = 0,
        /// Serve this many operations and answer `unavailable` to every one
        /// after. Deterministic where a percentage is not, which is what a test
        /// that has to fail one *particular* operation needs.
        unavailable_after: ?u64 = null,
        /// Percent of held-back operations completed out of submission order.
        ///
        /// This one *is* a fault: a caller that depends on its own operations
        /// completing in the order it submitted them is a caller with a bug.
        reorder_percent: u64 = 0,
    };

    const Entry = struct {
        body: []u8,
        etag: Etag,
    };

    allocator: std.mem.Allocator,
    objects: std.StringHashMapUnmanaged(Entry) = .{},
    next_version: u64 = 1,

    faults: Faults = .{},
    random: ?*stdx.Random = null,

    /// Operations held back to complete later, in submission order. Draining
    /// them is the simulator's to do, which is what makes "out of order" a
    /// property of the run rather than of the clock.
    delayed: std.ArrayListUnmanaged(*Operation) = .{},

    /// Counters, for tests that assert a read costs no write.
    gets: u64 = 0,
    puts: u64 = 0,
    deletes: u64 = 0,
    lists: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) MemoryStore {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryStore) void {
        var it = self.objects.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.body);
        }
        self.objects.deinit(self.allocator);
        self.delayed.deinit(self.allocator);
    }

    pub fn store(self: *MemoryStore) Store {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Store.VTable = .{ .submit = submit_erased };

    fn submit_erased(ptr: *anyopaque, op: *Operation) void {
        const self: *MemoryStore = @ptrCast(@alignCast(ptr));
        self.submit(op);
    }

    pub fn submit(self: *MemoryStore, op: *Operation) void {
        if (self.random) |rng| {
            if (rng.chance(self.faults.defer_percent)) {
                self.delayed.append(self.allocator, op) catch {
                    // Nowhere to put it: serve it now rather than drop it.
                    self.serve(op);
                };
                return;
            }
        }
        self.serve(op);
    }

    /// Complete everything held back.
    ///
    /// Oldest first, unless reordering is injected: then the order is shuffled,
    /// because nothing about a store promises that two requests come back in the
    /// order they went out.
    pub fn drain_delayed(self: *MemoryStore) void {
        const held = self.delayed.toOwnedSlice(self.allocator) catch return;
        defer self.allocator.free(held);
        if (self.random) |rng| {
            if (held.len > 1 and rng.chance(self.faults.reorder_percent)) {
                var i = held.len;
                while (i > 1) {
                    i -= 1;
                    const j = rng.below(i + 1);
                    const swap = held[i];
                    held[i] = held[j];
                    held[j] = swap;
                }
            }
        }
        for (held) |op| self.serve(op);
    }

    pub fn delayed_count(self: *const MemoryStore) usize {
        return self.delayed.items.len;
    }

    fn serve(self: *MemoryStore, op: *Operation) void {
        if (self.faults.unavailable_after) |after| {
            if (self.gets + self.puts + self.deletes + self.lists >= after) {
                op.complete(.{ .unavailable = "injected: the store stopped answering" });
                return;
            }
        }
        if (self.random) |rng| {
            if (rng.chance(self.faults.unavailable_percent)) {
                op.complete(.{ .unavailable = "injected: the store did not answer" });
                return;
            }
        }
        switch (op.kind) {
            .get => self.serve_get(op),
            .put => self.serve_put(op),
            .delete => self.serve_delete(op),
            .list => self.serve_list(op),
        }
    }

    fn serve_get(self: *MemoryStore, op: *Operation) void {
        self.gets += 1;
        const entry = self.objects.get(op.key) orelse {
            op.complete(.not_found);
            return;
        };
        if (op.precondition == .unchanged) {
            if (Etag.eql(entry.etag, op.precondition.unchanged)) {
                op.complete(.not_modified);
                return;
            }
        }
        const body = op.arena.dupe(u8, entry.body) catch {
            op.complete(.{ .unavailable = "out of memory reading an object" });
            return;
        };
        op.complete(.{ .found = .{ .body = body, .etag = entry.etag } });
    }

    fn serve_put(self: *MemoryStore, op: *Operation) void {
        self.puts += 1;
        if (self.random) |rng| {
            if (op.precondition != .none and rng.chance(self.faults.conflict_percent)) {
                // The store could not order this against another conditional
                // write. Nothing is known about whether it landed — and here it
                // did not, which is the honest half of "nothing is known".
                op.complete(.conflict);
                return;
            }
        }
        const existing = self.objects.getPtr(op.key);
        switch (op.precondition) {
            // A read's condition means nothing on a write.
            .none, .unchanged => {},
            .absent => if (existing != null) {
                op.complete(.precondition_failed);
                return;
            },
            .match => |want| {
                const e = existing orelse {
                    op.complete(.precondition_failed);
                    return;
                };
                if (!Etag.eql(e.etag, want)) {
                    op.complete(.precondition_failed);
                    return;
                }
            },
        }

        const etag = self.mint_etag();
        const body = self.allocator.dupe(u8, op.body) catch {
            op.complete(.{ .unavailable = "out of memory writing an object" });
            return;
        };
        if (existing) |e| {
            self.allocator.free(e.body);
            e.body = body;
            e.etag = etag;
        } else {
            const key = self.allocator.dupe(u8, op.key) catch {
                self.allocator.free(body);
                op.complete(.{ .unavailable = "out of memory writing a key" });
                return;
            };
            self.objects.put(self.allocator, key, .{ .body = body, .etag = etag }) catch {
                self.allocator.free(key);
                self.allocator.free(body);
                op.complete(.{ .unavailable = "out of memory writing an object" });
                return;
            };
        }

        if (self.random) |rng| {
            if (rng.chance(self.faults.lost_ack_percent)) {
                // It landed. The caller will never know, and has to retry
                // something idempotent.
                op.complete(.{ .unavailable = "injected: the write landed and the answer was lost" });
                return;
            }
        }
        op.complete(.{ .written = etag });
    }

    fn serve_delete(self: *MemoryStore, op: *Operation) void {
        self.deletes += 1;
        if (self.objects.fetchRemove(op.key)) |kv| {
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.body);
        }
        op.complete(.deleted);
    }

    fn serve_list(self: *MemoryStore, op: *Operation) void {
        self.lists += 1;
        var matches = std.ArrayList([]const u8).init(op.arena);
        var it = self.objects.keyIterator();
        while (it.next()) |k| {
            if (std.mem.startsWith(u8, k.*, op.key)) {
                matches.append(op.arena.dupe(u8, k.*) catch {
                    op.complete(.{ .unavailable = "out of memory listing" });
                    return;
                }) catch {
                    op.complete(.{ .unavailable = "out of memory listing" });
                    return;
                };
            }
        }
        // Ascending key order, which is what the whole timer design rests on.
        std.mem.sort([]const u8, matches.items, {}, stdx.less_than_bytes);
        const n = @min(matches.items.len, op.max_keys);
        op.complete(.{ .keys = matches.items[0..n] });
    }

    fn mint_etag(self: *MemoryStore) Etag {
        var buf: [32]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "\"{x:0>16}\"", .{self.next_version}) catch unreachable;
        self.next_version += 1;
        return Etag.from(text);
    }

    /// For tests and for `debug.reset`: forget everything.
    pub fn clear(self: *MemoryStore) void {
        var it = self.objects.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.body);
        }
        self.objects.clearRetainingCapacity();
    }

    pub fn count(self: *const MemoryStore) usize {
        return self.objects.count();
    }

    /// Every key, in order. A diagnostic: what the bucket holds is the answer to
    /// "why did nothing fire", and a deadline that is not there is invisible in
    /// any projection of the documents.
    pub fn keys(self: *const MemoryStore, out: *std.ArrayList([]const u8)) !void {
        out.clearRetainingCapacity();
        var it = self.objects.keyIterator();
        while (it.next()) |k| try out.append(k.*);
        std.mem.sort([]const u8, out.items, {}, stdx.less_than_bytes);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// A synchronous driver for the tests: submit, and the completion has already
/// happened by the time `submit` returns, because the memory store serves
/// inline unless it is told to reorder.
const Sync = struct {
    result: Result = .pending,

    fn callback(op: *Operation) void {
        const self: *Sync = @ptrCast(@alignCast(op.context.?));
        self.result = op.result;
    }

    fn run(self: *Sync, s: Store, arena: std.mem.Allocator, op_in: Operation) Result {
        var op = op_in;
        op.arena = arena;
        op.callback = Sync.callback;
        op.context = self;
        self.result = .pending;
        s.submit(&op);
        return self.result;
    }
};

test "the store creates, replaces on a matching version, and refuses a stale one" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mem = MemoryStore.init(testing.allocator);
    defer mem.deinit();
    const s = mem.store();
    var d: Sync = .{};

    try testing.expect(d.run(s, a, .{ .kind = .get, .key = "k", .arena = a, .callback = undefined }) == .not_found);

    const first = d.run(s, a, .{ .kind = .put, .key = "k", .body = "one", .precondition = .absent, .arena = a, .callback = undefined });
    const etag1 = first.written;

    // Creating it again is refused: the object is there.
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "k", .body = "two", .precondition = .absent, .arena = a, .callback = undefined }) == .precondition_failed);

    const read = d.run(s, a, .{ .kind = .get, .key = "k", .arena = a, .callback = undefined });
    try testing.expectEqualStrings("one", read.found.body);
    try testing.expect(Etag.eql(etag1, read.found.etag));

    const second = d.run(s, a, .{ .kind = .put, .key = "k", .body = "two", .precondition = .{ .match = etag1 }, .arena = a, .callback = undefined });
    const etag2 = second.written;
    try testing.expect(!Etag.eql(etag1, etag2));

    // THE load-bearing assertion. A store that accepts this loses writes.
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "k", .body = "three", .precondition = .{ .match = etag1 }, .arena = a, .callback = undefined }) == .precondition_failed);
    try testing.expectEqualStrings("two", d.run(s, a, .{ .kind = .get, .key = "k", .arena = a, .callback = undefined }).found.body);

    // Matching a version on an object that is gone is also a failed
    // precondition, not a create.
    _ = d.run(s, a, .{ .kind = .delete, .key = "k", .arena = a, .callback = undefined });
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "k", .body = "x", .precondition = .{ .match = etag2 }, .arena = a, .callback = undefined }) == .precondition_failed);
    // Deleting what is not there succeeds.
    try testing.expect(d.run(s, a, .{ .kind = .delete, .key = "k", .arena = a, .callback = undefined }) == .deleted);
}

test "listing is ascending, prefix scoped and capped" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mem = MemoryStore.init(testing.allocator);
    defer mem.deinit();
    const s = mem.store();
    var d: Sync = .{};

    for ([_][]const u8{ "t/01/b", "t/00/z", "t/00/a", "wf/x" }) |k| {
        _ = d.run(s, a, .{ .kind = .put, .key = k, .body = "", .arena = a, .callback = undefined });
    }
    const keys = d.run(s, a, .{ .kind = .list, .key = "t/", .arena = a, .callback = undefined }).keys;
    try testing.expectEqual(@as(usize, 3), keys.len);
    try testing.expectEqualStrings("t/00/a", keys[0]);
    try testing.expectEqualStrings("t/00/z", keys[1]);
    try testing.expectEqualStrings("t/01/b", keys[2]);

    const capped = d.run(s, a, .{ .kind = .list, .key = "t/", .max_keys = 2, .arena = a, .callback = undefined }).keys;
    try testing.expectEqual(@as(usize, 2), capped.len);
    try testing.expectEqualStrings("t/00/a", capped[0]);
}

test "keys escape everything that could change their structure" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var buf = std.ArrayList(u8).init(a);
    const keys = KeySpace.init("p/", 4);

    try testing.expectEqualStrings("p/wf/diff", try keys.doc_key(&buf, "diff"));
    try testing.expectEqualStrings("p/wf/my.app.workflow", try keys.doc_key(&buf, "my.app.workflow"));
    try testing.expectEqualStrings("p/wf/with%20space", try keys.doc_key(&buf, "with space"));
    try testing.expectEqualStrings("p/wf/with%2Fslash", try keys.doc_key(&buf, "with/slash"));
    try testing.expectEqualStrings("p/wf/with%3Acolon", try keys.doc_key(&buf, "with:colon"));
    try testing.expectEqualStrings("p/wf/with%25percent", try keys.doc_key(&buf, "with%percent"));
    try testing.expectEqualStrings("p/wf/caf%C3%A9", try keys.doc_key(&buf, "café"));
    try testing.expectEqualStrings("p/wf/under%5Fscore", try keys.doc_key(&buf, "under_score"));

    const empty = KeySpace.init("", 1);
    try testing.expectEqualStrings("wf/o", try empty.doc_key(&buf, "o"));

    // Every round trip, including the ones that would otherwise grow structure.
    for ([_][]const u8{ "diff", "with space", "with/slash", "with:colon", "café", "a_b", "%2F" }) |origin| {
        const key = try keys.doc_key(&buf, origin);
        const owned_key = try a.dupe(u8, key);
        const back = (try keys.origin_of_doc_key(a, owned_key)).?;
        try testing.expectEqualStrings(origin, back);
    }
}

test "a timer key sorts by time and says what it points at" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var buf = std.ArrayList(u8).init(a);
    const keys = KeySpace.init("p/", 4);

    const k1 = try a.dupe(u8, try keys.timer_key(&buf, "diff", 30_000, 7));
    try testing.expect(std.mem.endsWith(u8, k1, "/00000000000000030000_diff@7"));
    const k2 = try a.dupe(u8, try keys.timer_key(&buf, "diff", 60_000, 7));
    // Same origin, so the same shard, and lexicographic order is time order.
    try testing.expect(std.mem.lessThan(u8, k1, k2));

    const entry = (try keys.parse_timer_key(a, k1)).?;
    try testing.expectEqual(@as(i64, 30_000), entry.deadline());
    try testing.expectEqualStrings("diff", entry.origin.name);
    try testing.expectEqual(@as(u64, 7), entry.origin.generation);

    // The same deadline armed by two different commits is two different keys, so
    // removing one cannot remove the other.
    const k3 = try a.dupe(u8, try keys.timer_key(&buf, "diff", 30_000, 8));
    try testing.expect(!std.mem.eql(u8, k1, k3));

    const sk = try a.dupe(u8, try keys.sched_timer_key(&buf, "s0", 60_000, 3));
    const sentry = (try keys.parse_timer_key(a, sk)).?;
    try testing.expectEqualStrings("s0", sentry.schedule.id);
    try testing.expectEqual(@as(u64, 3), sentry.schedule.generation);

    // An origin literally called `sched:evil` is still told apart from a
    // schedule, because an escaped origin can never contain a raw colon.
    const evil = try a.dupe(u8, try keys.timer_key(&buf, "sched:evil", 1, 1));
    const eentry = (try keys.parse_timer_key(a, evil)).?;
    try testing.expect(eentry == .origin);
    try testing.expectEqualStrings("sched:evil", eentry.origin.name);

    // An origin containing an `@` is escaped, so the last raw one is the marker.
    const at_origin = try a.dupe(u8, try keys.timer_key(&buf, "a@b", 1, 2));
    const at_entry = (try keys.parse_timer_key(a, at_origin)).?;
    try testing.expectEqualStrings("a@b", at_entry.origin.name);
    try testing.expectEqual(@as(u64, 2), at_entry.origin.generation);

    // A negative deadline clamps rather than breaking the padding.
    const neg = try a.dupe(u8, try keys.timer_key(&buf, "o", -5, 1));
    try testing.expectEqual(@as(i64, 0), (try keys.parse_timer_key(a, neg)).?.deadline());

    // Unparseable keys are ignored, not fatal.
    for ([_][]const u8{
        "p/t/00/short",
        "p/t/00/xxxxxxxxxxxxxxxxxxxx_o@1",
        "p/t/00/00000000000000000001o@1",
        "p/t/00/00000000000000000001_o",
        "p/t/00/00000000000000000001_o@x",
        "p/t/00/00000000000000000001_@1",
        "nope",
    }) |bad| {
        try testing.expect((try keys.parse_timer_key(a, bad)) == null);
    }
}

test "timer keys spread across shards" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var buf = std.ArrayList(u8).init(a);
    const keys = KeySpace.init("", 4);
    var seen = std.StringHashMap(void).init(a);
    for (0..40) |i| {
        const origin = try std.fmt.allocPrint(a, "origin-{d}", .{i});
        const key = try a.dupe(u8, try keys.timer_key(&buf, origin, 1, 1));
        // `t/NN/`
        try seen.put(key[2..4], {});
    }
    try testing.expect(seen.count() > 1);
}

test "a validating read is answered without a body when nothing changed" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mem = MemoryStore.init(testing.allocator);
    defer mem.deinit();
    const s = mem.store();
    var d: Sync = .{};

    const written = d.run(s, a, .{ .kind = .put, .key = "k", .body = "one", .precondition = .absent, .arena = a, .callback = undefined });
    const etag = written.written;

    // Still at that version: the store says so and sends nothing.
    try testing.expect(d.run(s, a, .{ .kind = .get, .key = "k", .precondition = .{ .unchanged = etag }, .arena = a, .callback = undefined }) == .not_modified);

    // Moved on: the body comes back.
    _ = d.run(s, a, .{ .kind = .put, .key = "k", .body = "two", .precondition = .{ .match = etag }, .arena = a, .callback = undefined });
    const fresh = d.run(s, a, .{ .kind = .get, .key = "k", .precondition = .{ .unchanged = etag }, .arena = a, .callback = undefined });
    try testing.expectEqualStrings("two", fresh.found.body);

    // And an object that is gone is gone, whatever version the caller held.
    _ = d.run(s, a, .{ .kind = .delete, .key = "k", .arena = a, .callback = undefined });
    try testing.expect(d.run(s, a, .{ .kind = .get, .key = "k", .precondition = .{ .unchanged = etag }, .arena = a, .callback = undefined }) == .not_found);
}

test "injected faults reach the caller" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var rng = stdx.Random.init(7);
    var mem = MemoryStore.init(testing.allocator);
    defer mem.deinit();
    mem.random = &rng;
    const s = mem.store();
    var d: Sync = .{};

    mem.faults.unavailable_percent = 100;
    try testing.expect(d.run(s, a, .{ .kind = .get, .key = "k", .arena = a, .callback = undefined }) == .unavailable);

    mem.faults.unavailable_percent = 0;
    mem.faults.conflict_percent = 100;
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "k", .body = "x", .precondition = .absent, .arena = a, .callback = undefined }) == .conflict);
    // An unconditional write cannot conflict: there is nothing to order it against.
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "k", .body = "x", .arena = a, .callback = undefined }) == .written);

    // A lost acknowledgement: it landed, and the caller is told nothing.
    mem.faults.conflict_percent = 0;
    mem.faults.lost_ack_percent = 100;
    try testing.expect(d.run(s, a, .{ .kind = .put, .key = "j", .body = "landed", .precondition = .absent, .arena = a, .callback = undefined }) == .unavailable);
    mem.faults.lost_ack_percent = 0;
    try testing.expectEqualStrings("landed", d.run(s, a, .{ .kind = .get, .key = "j", .arena = a, .callback = undefined }).found.body);
}

test "deferring holds operations back until the caller drains them" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var rng = stdx.Random.init(3);
    var mem = MemoryStore.init(testing.allocator);
    defer mem.deinit();
    mem.random = &rng;
    mem.faults.defer_percent = 100;
    const s = mem.store();

    var got: usize = 0;
    const Counter = struct {
        fn cb(op: *Operation) void {
            const n: *usize = @ptrCast(@alignCast(op.context.?));
            n.* += 1;
        }
    };
    var op: Operation = .{ .kind = .get, .key = "k", .arena = a, .callback = Counter.cb, .context = &got };
    s.submit(&op);
    try testing.expectEqual(@as(usize, 0), got);
    try testing.expectEqual(@as(usize, 1), mem.delayed_count());
    mem.drain_delayed();
    try testing.expectEqual(@as(usize, 1), got);
    try testing.expectEqual(@as(usize, 0), mem.delayed_count());
}
