//! The origin document: all the state one execution lineage has, in one place.
//!
//! This is the unit of atomicity. Every operation the protocol admits touches
//! exactly one origin — a callback may only be registered inside one, a task may
//! only await inside one, a fenced action must share the task's origin — so one
//! conditional write of one object commits a whole transition. That is where
//! atomicity comes from on a store that has no transactions, and the
//! same-origin rules in `protocol.zig` are what make it sufficient rather than
//! merely convenient.
//!
//! A document owns its strings out of an arena. Nothing here frees anything
//! individually: a document's lifetime ends when it is evicted or replaced, and
//! then the arena goes in one call. Mutation therefore leaks inside that
//! lifetime, which is bounded — every commit re-encodes the document, and
//! `compact` decodes those bytes back into a fresh arena.
//!
//! The encoding is canonical: two encoders given equal state produce identical
//! bytes. That is load-bearing twice over. It is what lets the applier compare
//! bytes to decide whether a write is needed at all, and what lets a writer
//! recognise its own landed write after a lost response.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");

const assert = stdx.assert;
const StringMap = protocol.StringMap;
const PromiseState = protocol.PromiseState;
const TaskState = protocol.TaskState;
const TaskTimeoutKind = protocol.TaskTimeoutKind;

/// Bumped when the encoding changes in a way an older reader would misread.
/// A document written by a newer server is refused rather than guessed at.
pub const format_version: i64 = 1;

pub const Error = error{
    OutOfMemory,
    /// The bytes are not a document this build can read.
    Corrupt,
    /// The document was written by a newer format version.
    FormatTooNew,
    /// The document does not belong under the key it was found at.
    WrongOrigin,
};

/// A promise's `param` or `value`: optional headers and an optional body.
///
/// Both optional, and absent rather than empty when unset — the wire omits
/// them, and so does the document, so that `{}` has one encoding.
pub const PromiseValue = struct {
    headers: ?StringMap = null,
    data: ?[]const u8 = null,

    pub const empty: PromiseValue = .{};

    pub fn eql(a: PromiseValue, b: PromiseValue) bool {
        if ((a.data == null) != (b.data == null)) return false;
        if (a.data) |ad| {
            if (!std.mem.eql(u8, ad, b.data.?)) return false;
        }
        if ((a.headers == null) != (b.headers == null)) return false;
        if (a.headers) |ah| {
            const bh = b.headers.?;
            if (ah.len() != bh.len()) return false;
            for (ah.entries, bh.entries) |x, y| {
                if (!std.mem.eql(u8, x.key, y.key)) return false;
                if (!std.mem.eql(u8, x.value, y.value)) return false;
            }
        }
        return true;
    }

    /// Read one off the wire. A missing member is the empty value; a member
    /// that is present but of the wrong type is a request the protocol does
    /// not admit.
    pub fn from_json(allocator: std.mem.Allocator, v: ?json.Value) !PromiseValue {
        const obj = v orelse return .empty;
        if (obj.is_null()) return .empty;
        if (!obj.is_object()) return error.NotAPromiseValue;
        var out: PromiseValue = .empty;
        if (obj.get("headers")) |h| {
            if (!h.is_null()) out.headers = try StringMap.from_json(allocator, h);
        }
        if (obj.get("data")) |d| {
            if (!d.is_null()) {
                const s = d.as_string() orelse return error.NotAPromiseValue;
                out.data = try allocator.dupe(u8, s);
            }
        }
        return out;
    }

    pub fn clone(self: PromiseValue, allocator: std.mem.Allocator) !PromiseValue {
        return .{
            .headers = if (self.headers) |h| try h.clone(allocator) else null,
            .data = if (self.data) |d| try allocator.dupe(u8, d) else null,
        };
    }

    /// Write as the wire shape: `{}` when empty, each member present only when
    /// set. The reference implementation skips a `None` rather than writing a
    /// `null`, and a client distinguishes the two.
    pub fn write_wire(self: PromiseValue, w: *json.Writer) !void {
        try w.object_begin();
        if (self.headers) |h| {
            try w.key("headers");
            try h.write_json(w);
        }
        if (self.data) |d| try w.field_string("data", d);
        try w.object_end();
    }

    pub fn is_empty(self: PromiseValue) bool {
        return self.headers == null and self.data == null;
    }
};

/// A set of strings, sorted, owned by the document's arena.
///
/// A promise's callbacks (the awaiters blocked on it), its listeners (the
/// addresses to notify), and a task's resumes (what settled while it ran) are
/// all sets, and all three are compared and encoded, so all three are sorted by
/// construction rather than by a sort at each use.
pub const StringSet = struct {
    items: []const []const u8 = &.{},

    pub const empty: StringSet = .{};

    pub fn len(self: StringSet) usize {
        return self.items.len;
    }

    pub fn contains(self: StringSet, s: []const u8) bool {
        return self.index_of(s) != null;
    }

    fn index_of(self: StringSet, s: []const u8) ?usize {
        var lo: usize = 0;
        var hi: usize = self.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.items[mid], s)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return mid,
            }
        }
        return null;
    }

    /// Where `s` belongs, keeping the set sorted.
    fn insertion_point(self: StringSet, s: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = self.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.lessThan(u8, self.items[mid], s)) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    /// Insert, or do nothing if already present. Returns whether it changed.
    pub fn insert(self: *StringSet, allocator: std.mem.Allocator, s: []const u8) !bool {
        if (self.contains(s)) return false;
        const at = self.insertion_point(s);
        const grown = try allocator.alloc([]const u8, self.items.len + 1);
        @memcpy(grown[0..at], self.items[0..at]);
        grown[at] = try allocator.dupe(u8, s);
        @memcpy(grown[at + 1 ..], self.items[at..]);
        self.items = grown;
        return true;
    }

    /// Remove, or do nothing if absent. Returns whether it changed.
    pub fn remove(self: *StringSet, allocator: std.mem.Allocator, s: []const u8) !bool {
        const at = self.index_of(s) orelse return false;
        if (self.items.len == 1) {
            self.items = &.{};
            return true;
        }
        const shrunk = try allocator.alloc([]const u8, self.items.len - 1);
        @memcpy(shrunk[0..at], self.items[0..at]);
        @memcpy(shrunk[at..], self.items[at + 1 ..]);
        self.items = shrunk;
        return true;
    }

    pub fn clear(self: *StringSet) void {
        self.items = &.{};
    }

    pub fn eql(a: StringSet, b: StringSet) bool {
        if (a.items.len != b.items.len) return false;
        for (a.items, b.items) |x, y| {
            if (!std.mem.eql(u8, x, y)) return false;
        }
        return true;
    }

    pub fn clone(self: StringSet, allocator: std.mem.Allocator) !StringSet {
        if (self.items.len == 0) return .empty;
        const items = try allocator.alloc([]const u8, self.items.len);
        for (self.items, 0..) |s, i| items[i] = try allocator.dupe(u8, s);
        return .{ .items = items };
    }
};

pub const Promise = struct {
    id: []const u8,
    state: PromiseState,
    param: PromiseValue,
    value: PromiseValue,
    tags: StringMap,
    timeout_at: i64,
    created_at: i64,
    settled_at: ?i64,

    /// Is this promise's deadline in the timeout table?
    ///
    /// Armed only for a promise that carries a task, and cleared the moment it
    /// settles. It is a flag rather than a value because the deadline is always
    /// `timeout_at` — a second copy could only ever disagree.
    timeout_armed: bool,

    /// The awaiters blocked on this promise, by promise id.
    callbacks: StringSet,
    /// The addresses to notify when it settles.
    listeners: StringSet,

    pub fn is_timer(self: Promise) bool {
        return protocol.is_timer(self.tags);
    }

    pub fn is_external(self: Promise) bool {
        return protocol.is_external(self.tags);
    }

    pub fn has_task(self: Promise) bool {
        return protocol.has_task(self.tags);
    }

    pub fn target(self: Promise) ?[]const u8 {
        return self.tags.get(protocol.tag_target);
    }

    /// What this promise's deadline settles it as.
    pub fn timeout_state(self: Promise) PromiseState {
        return protocol.timeout_state(self.tags);
    }

    /// Has the deadline passed without the promise having been settled?
    ///
    /// The question every operation asks of the ids it names before it does
    /// anything else: a promise past its deadline is settled, whether or not
    /// anything has got round to writing that down.
    pub fn expired(self: Promise, now: i64) bool {
        return self.state == .pending and now >= self.timeout_at;
    }

    /// Everything that makes two promises the same promise. `timeout_armed` is
    /// included: arming and disarming a deadline is a change worth a write.
    pub fn eql(a: Promise, b: Promise) bool {
        return std.mem.eql(u8, a.id, b.id) and
            a.state == b.state and
            a.timeout_at == b.timeout_at and
            a.created_at == b.created_at and
            eql_opt_i64(a.settled_at, b.settled_at) and
            a.timeout_armed == b.timeout_armed and
            a.param.eql(b.param) and
            a.value.eql(b.value) and
            map_eql(a.tags, b.tags) and
            StringSet.eql(a.callbacks, b.callbacks) and
            StringSet.eql(a.listeners, b.listeners);
    }
};

pub const TaskTimeout = struct {
    kind: TaskTimeoutKind,
    at: i64,
};

pub const Task = struct {
    /// A task's id is its promise's id. There is no separate task identity, and
    /// there is a task exactly when the promise carries a dispatch target.
    id: []const u8,
    state: TaskState,
    /// The fence token. Incremented only on acquisition, so a holder that lost
    /// its lease can be told apart from the one that has it now.
    version: i64,
    pid: ?[]const u8,
    ttl: ?i64,
    /// What settled while this task was running, by promise id. A count of it
    /// is what the wire calls `resumes`.
    resumes: StringSet,

    /// The one deadline a task has: a retry while it is pending, a lease while
    /// it is acquired, and none otherwise. One, not two — acquiring replaces
    /// the retry with a lease and releasing replaces the lease with a retry, so
    /// a second slot could only ever hold something stale.
    timeout: ?TaskTimeout,

    pub fn eql(a: Task, b: Task) bool {
        if (!std.mem.eql(u8, a.id, b.id)) return false;
        if (a.state != b.state or a.version != b.version) return false;
        if ((a.pid == null) != (b.pid == null)) return false;
        if (a.pid) |ap| {
            if (!std.mem.eql(u8, ap, b.pid.?)) return false;
        }
        if (!eql_opt_i64(a.ttl, b.ttl)) return false;
        if (!StringSet.eql(a.resumes, b.resumes)) return false;
        if ((a.timeout == null) != (b.timeout == null)) return false;
        if (a.timeout) |at| {
            const bt = b.timeout.?;
            if (at.kind != bt.kind or at.at != bt.at) return false;
        }
        return true;
    }
};

fn eql_opt_i64(a: ?i64, b: ?i64) bool {
    if (a == null) return b == null;
    if (b == null) return false;
    return a.? == b.?;
}

fn map_eql(a: StringMap, b: StringMap) bool {
    if (a.len() != b.len()) return false;
    for (a.entries, b.entries) |x, y| {
        if (!std.mem.eql(u8, x.key, y.key)) return false;
        if (!std.mem.eql(u8, x.value, y.value)) return false;
    }
    return true;
}

// ── The document ──────────────────────────────────────────────────────────────

pub const Doc = struct {
    arena: std.heap.ArenaAllocator,
    /// Sorted by id, always — the order every traversal, every encoding and
    /// every page of a search reports.
    promises: std.ArrayListUnmanaged(Promise) = .{},
    tasks: std.ArrayListUnmanaged(Task) = .{},

    /// The earliest deadline anything in this document has, or null.
    ///
    /// One value for the whole origin, because one timer object covers it: the
    /// key of that object carries this number, and the firing loop reloads the
    /// document and sweeps it. A per-promise timer object would be a write per
    /// promise for a wake-up that has to read the document anyway.
    timer_at: ?i64 = null,

    /// The latest instant this document has been decided at.
    ///
    /// A monotonicity hint, not state: it is excluded from the comparison that
    /// decides whether to write, because paying a PUT to advance it would make
    /// every read a write.
    clock: i64 = 0,

    /// How many times this document has been committed.
    ///
    /// Not diagnostics: it names the timer object this document's deadline is
    /// armed under, which is what makes disarming safe when several writers share
    /// the bucket. A generation belongs to exactly one commit, so the key a
    /// writer removes is one nobody else can have written.
    generation: u64 = 0,

    /// The generation whose commit wrote the timer object now on the store.
    ///
    /// Equal to `generation` when this commit moved the deadline, and to whatever
    /// it was before when the deadline did not move — because then the object did
    /// not have to be rewritten.
    timer_generation: u64 = 0,

    pub fn init(child: std.mem.Allocator) Doc {
        return .{ .arena = std.heap.ArenaAllocator.init(child) };
    }

    pub fn deinit(self: *Doc) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *Doc) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn is_empty(self: *const Doc) bool {
        return self.promises.items.len == 0 and self.tasks.items.len == 0;
    }

    // ── Lookup ────────────────────────────────────────────────────────────────

    fn search(comptime T: type, items: []T, id: []const u8) ?usize {
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, items[mid].id, id)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return mid,
            }
        }
        return null;
    }

    fn insertion_point(comptime T: type, items: []T, id: []const u8) usize {
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.lessThan(u8, items[mid].id, id)) lo = mid + 1 else hi = mid;
        }
        return lo;
    }

    pub fn promise(self: *Doc, id: []const u8) ?*Promise {
        const i = search(Promise, self.promises.items, id) orelse return null;
        return &self.promises.items[i];
    }

    pub fn promise_const(self: *const Doc, id: []const u8) ?*const Promise {
        const i = search(Promise, self.promises.items, id) orelse return null;
        return &self.promises.items[i];
    }

    pub fn task(self: *Doc, id: []const u8) ?*Task {
        const i = search(Task, self.tasks.items, id) orelse return null;
        return &self.tasks.items[i];
    }

    pub fn task_const(self: *const Doc, id: []const u8) ?*const Task {
        const i = search(Task, self.tasks.items, id) orelse return null;
        return &self.tasks.items[i];
    }

    // ── Insertion ─────────────────────────────────────────────────────────────

    /// Insert a promise, keeping the list sorted. The id must not already be
    /// present — a caller that meant "create or return the existing one" has to
    /// look first, because which of the two happened is the answer it sends.
    pub fn promise_insert(self: *Doc, p: Promise) !*Promise {
        assert(self.promise(p.id) == null);
        const at = insertion_point(Promise, self.promises.items, p.id);
        try self.promises.insert(self.allocator(), at, p);
        return &self.promises.items[at];
    }

    pub fn task_insert(self: *Doc, t: Task) !*Task {
        assert(self.task(t.id) == null);
        const at = insertion_point(Task, self.tasks.items, t.id);
        try self.tasks.insert(self.allocator(), at, t);
        return &self.tasks.items[at];
    }

    // ── Deadlines ─────────────────────────────────────────────────────────────

    /// The earliest deadline this document holds, recomputed from scratch.
    ///
    /// Only deadlines that are *live* count, and the liveness tests are the
    /// same ones the sweep applies before acting: a promise deadline counts
    /// while the promise is pending and armed, a retry while the task is
    /// pending, a lease while it is acquired. Without them the document would
    /// arm a wake-up for a row nothing would act on.
    pub fn min_deadline(self: *const Doc) ?i64 {
        var best: ?i64 = null;
        for (self.promises.items) |p| {
            if (p.state == .pending and p.timeout_armed) best = min_opt(best, p.timeout_at);
        }
        for (self.tasks.items) |t| {
            const to = t.timeout orelse continue;
            const live = switch (to.kind) {
                .retry => t.state == .pending,
                .lease => t.state == .acquired,
            };
            if (live) best = min_opt(best, to.at);
        }
        return best;
    }

    fn min_opt(a: ?i64, b: i64) ?i64 {
        if (a) |x| return @min(x, b);
        return b;
    }

    /// Recompute `timer_at`. Called once at the end of every decision, so that
    /// no transition has to remember to.
    pub fn reseat_timer(self: *Doc) void {
        self.timer_at = self.min_deadline();
    }

    // ── Comparison ────────────────────────────────────────────────────────────

    /// Did the decision touch anything worth writing an object for?
    ///
    /// The write law. `clock` and `generation` are excluded on purpose: the
    /// clock is a hint and the generation counts writes, so including either
    /// would make every read a write.
    pub fn changed(before: *const Doc, after: *const Doc) bool {
        if (before.promises.items.len != after.promises.items.len) return true;
        if (before.tasks.items.len != after.tasks.items.len) return true;
        if (!eql_opt_i64(before.timer_at, after.timer_at)) return true;
        if (before.timer_generation != after.timer_generation) return true;
        for (before.promises.items, after.promises.items) |a, b| {
            if (!a.eql(b)) return true;
        }
        for (before.tasks.items, after.tasks.items) |a, b| {
            if (!Task.eql(a, b)) return true;
        }
        return false;
    }

    // ── Encoding ──────────────────────────────────────────────────────────────

    /// The 16 hex characters that bind a document to the key it lives under.
    ///
    /// FNV-1a over the origin. Not a checksum of the body — the store's ETag is
    /// the integrity and concurrency token — but a guard against a document
    /// being read as some other origin's, which would silently graft one
    /// execution's promises onto another.
    pub fn origin_hash(origin: []const u8) [16]u8 {
        var h: u64 = 0xcbf29ce484222325;
        for (origin) |b| {
            h ^= b;
            h *%= 0x100000001b3;
        }
        var out: [16]u8 = undefined;
        _ = std.fmt.bufPrint(&out, "{x:0>16}", .{h}) catch unreachable;
        return out;
    }

    /// Serialize. One line of canonical JSON per entity: a header, then the
    /// promises in id order, then the tasks.
    ///
    /// Canonical means ASCII only, minimal escapes, integers with no exponent,
    /// no insignificant whitespace, a fixed key order, a fixed line order, and
    /// omission — never `null`, `[]` or `false` — for anything empty.
    pub fn encode(self: *const Doc, out: *std.ArrayList(u8), origin: []const u8) !void {
        out.clearRetainingCapacity();
        const hash = origin_hash(origin);

        try out.appendSlice("{\"v\":");
        try out.writer().print("{d}", .{format_version});
        try out.appendSlice(",\"og\":\"");
        try out.appendSlice(&hash);
        try out.appendSlice("\",\"cl\":");
        try out.writer().print("{d}", .{self.clock});
        try out.appendSlice(",\"gn\":");
        try out.writer().print("{d}", .{self.generation});
        if (self.timer_at) |at| {
            try out.appendSlice(",\"ta\":");
            try out.writer().print("{d}", .{at});
            try out.appendSlice(",\"tgn\":");
            try out.writer().print("{d}", .{self.timer_generation});
        }
        try out.append('}');

        for (self.promises.items) |p| {
            try out.append('\n');
            try encode_promise(out, p);
        }
        for (self.tasks.items) |t| {
            try out.append('\n');
            try encode_task(out, t);
        }
    }

    fn encode_promise(out: *std.ArrayList(u8), p: Promise) !void {
        try out.appendSlice("{\"p\":");
        try json.write_string_ascii(out, p.id, true);
        try out.appendSlice(",\"s\":\"");
        try out.appendSlice(p.state.as_str());
        try out.appendSlice("\",\"to\":");
        try out.writer().print("{d}", .{p.timeout_at});
        try out.appendSlice(",\"ca\":");
        try out.writer().print("{d}", .{p.created_at});
        if (p.settled_at) |sa| {
            try out.appendSlice(",\"sa\":");
            try out.writer().print("{d}", .{sa});
        }
        if (p.timeout_armed) try out.appendSlice(",\"ar\":true");
        if (!p.param.is_empty()) {
            try out.appendSlice(",\"pa\":");
            try encode_value(out, p.param);
        }
        if (!p.value.is_empty()) {
            try out.appendSlice(",\"va\":");
            try encode_value(out, p.value);
        }
        if (p.tags.len() > 0) {
            try out.appendSlice(",\"tg\":");
            try encode_map(out, p.tags);
        }
        if (p.callbacks.len() > 0) {
            try out.appendSlice(",\"cb\":");
            try encode_set(out, p.callbacks);
        }
        if (p.listeners.len() > 0) {
            try out.appendSlice(",\"ls\":");
            try encode_set(out, p.listeners);
        }
        try out.append('}');
    }

    fn encode_task(out: *std.ArrayList(u8), t: Task) !void {
        try out.appendSlice("{\"t\":");
        try json.write_string_ascii(out, t.id, true);
        try out.appendSlice(",\"s\":\"");
        try out.appendSlice(t.state.as_str());
        try out.appendSlice("\",\"v\":");
        try out.writer().print("{d}", .{t.version});
        if (t.pid) |pid| {
            try out.appendSlice(",\"pid\":");
            try json.write_string_ascii(out, pid, true);
        }
        if (t.ttl) |ttl| {
            try out.appendSlice(",\"ttl\":");
            try out.writer().print("{d}", .{ttl});
        }
        if (t.resumes.len() > 0) {
            try out.appendSlice(",\"rs\":");
            try encode_set(out, t.resumes);
        }
        if (t.timeout) |to| {
            try out.appendSlice(",\"tk\":");
            try out.writer().print("{d}", .{@intFromEnum(to.kind)});
            try out.appendSlice(",\"tt\":");
            try out.writer().print("{d}", .{to.at});
        }
        try out.append('}');
    }

    fn encode_value(out: *std.ArrayList(u8), v: PromiseValue) !void {
        try out.append('{');
        var first = true;
        if (v.data) |d| {
            try out.appendSlice("\"d\":");
            try json.write_string_ascii(out, d, true);
            first = false;
        }
        if (v.headers) |h| {
            if (!first) try out.append(',');
            try out.appendSlice("\"h\":");
            try encode_map(out, h);
        }
        try out.append('}');
    }

    fn encode_map(out: *std.ArrayList(u8), m: StringMap) !void {
        try out.append('{');
        for (m.entries, 0..) |e, i| {
            if (i > 0) try out.append(',');
            try json.write_string_ascii(out, e.key, true);
            try out.append(':');
            try json.write_string_ascii(out, e.value, true);
        }
        try out.append('}');
    }

    fn encode_set(out: *std.ArrayList(u8), s: StringSet) !void {
        try out.append('[');
        for (s.items, 0..) |item, i| {
            if (i > 0) try out.append(',');
            try json.write_string_ascii(out, item, true);
        }
        try out.append(']');
    }

    // ── Decoding ──────────────────────────────────────────────────────────────

    /// Read a document back. `origin` is the key it was found under, and a
    /// document whose header does not name it is refused.
    pub fn decode(child: std.mem.Allocator, bytes: []const u8, origin: []const u8) Error!Doc {
        var doc = Doc.init(child);
        errdefer doc.deinit();
        const a = doc.allocator();

        var lines = std.mem.splitScalar(u8, bytes, '\n');
        var seen_header = false;
        while (lines.next()) |line| {
            // A blank line is padding: the format leaves room for an append
            // layout, and a reader that choked on the pad would forbid it.
            if (line.len == 0) continue;
            const v = json.parse(a, line) catch return error.Corrupt;
            if (!seen_header) {
                const version = v.get_i64("v") orelse return error.Corrupt;
                if (version > format_version) return error.FormatTooNew;
                const og = v.get_string("og") orelse return error.Corrupt;
                const want = origin_hash(origin);
                if (!std.mem.eql(u8, og, &want)) return error.WrongOrigin;
                doc.clock = v.get_i64("cl") orelse 0;
                const gn = v.get_i64("gn") orelse 0;
                doc.generation = if (gn < 0) 0 else @intCast(gn);
                doc.timer_at = v.get_i64("ta");
                doc.timer_generation = blk: {
                    const tg = v.get_i64("tgn") orelse break :blk 0;
                    break :blk if (tg < 0) 0 else @intCast(tg);
                };
                seen_header = true;
                continue;
            }
            if (v.get_string("p")) |_| {
                try doc.promises.append(a, try decode_promise(a, v));
            } else if (v.get_string("t")) |_| {
                try doc.tasks.append(a, try decode_task(a, v));
            } else {
                // An entity kind this build does not know. Refusing would make
                // every added line a format break; ignoring it would silently
                // drop state on the next rewrite. Refusing is the safe half.
                return error.Corrupt;
            }
        }
        if (!seen_header) return error.Corrupt;

        // The encoder writes them sorted, so a document that arrives unsorted
        // was not written by this format.
        var i: usize = 1;
        while (i < doc.promises.items.len) : (i += 1) {
            if (!std.mem.lessThan(u8, doc.promises.items[i - 1].id, doc.promises.items[i].id)) {
                return error.Corrupt;
            }
        }
        i = 1;
        while (i < doc.tasks.items.len) : (i += 1) {
            if (!std.mem.lessThan(u8, doc.tasks.items[i - 1].id, doc.tasks.items[i].id)) {
                return error.Corrupt;
            }
        }
        return doc;
    }

    fn decode_promise(a: std.mem.Allocator, v: json.Value) Error!Promise {
        const id = v.get_string("p") orelse return error.Corrupt;
        const state_str = v.get_string("s") orelse return error.Corrupt;
        const state = PromiseState.parse(state_str) orelse return error.Corrupt;
        return .{
            .id = try a.dupe(u8, id),
            .state = state,
            .param = try decode_value(a, v.get("pa")),
            .value = try decode_value(a, v.get("va")),
            .tags = if (v.get("tg")) |t|
                StringMap.from_json(a, t) catch return error.Corrupt
            else
                StringMap.empty,
            .timeout_at = v.get_i64("to") orelse return error.Corrupt,
            .created_at = v.get_i64("ca") orelse return error.Corrupt,
            .settled_at = v.get_i64("sa"),
            .timeout_armed = blk: {
                const ar = v.get("ar") orelse break :blk false;
                break :blk ar.as_bool() orelse false;
            },
            .callbacks = try decode_set(a, v.get("cb")),
            .listeners = try decode_set(a, v.get("ls")),
        };
    }

    fn decode_task(a: std.mem.Allocator, v: json.Value) Error!Task {
        const id = v.get_string("t") orelse return error.Corrupt;
        const state_str = v.get_string("s") orelse return error.Corrupt;
        const state = TaskState.parse(state_str) orelse return error.Corrupt;
        const timeout: ?TaskTimeout = blk: {
            const tk = v.get_i64("tk") orelse break :blk null;
            const tt = v.get_i64("tt") orelse break :blk null;
            if (tk != 0 and tk != 1) return error.Corrupt;
            break :blk .{
                .kind = if (tk == 0) .retry else .lease,
                .at = tt,
            };
        };
        return .{
            .id = try a.dupe(u8, id),
            .state = state,
            .version = v.get_i64("v") orelse return error.Corrupt,
            .pid = if (v.get_string("pid")) |p| try a.dupe(u8, p) else null,
            .ttl = v.get_i64("ttl"),
            .resumes = try decode_set(a, v.get("rs")),
            .timeout = timeout,
        };
    }

    fn decode_value(a: std.mem.Allocator, v: ?json.Value) Error!PromiseValue {
        const obj = v orelse return .empty;
        var out: PromiseValue = .empty;
        if (obj.get("d")) |d| {
            const s = d.as_string() orelse return error.Corrupt;
            out.data = try a.dupe(u8, s);
        }
        if (obj.get("h")) |h| {
            out.headers = StringMap.from_json(a, h) catch return error.Corrupt;
        }
        return out;
    }

    fn decode_set(a: std.mem.Allocator, v: ?json.Value) Error!StringSet {
        const arr_v = v orelse return .empty;
        const arr = arr_v.as_array() orelse return error.Corrupt;
        if (arr.len == 0) return .empty;
        const items = try a.alloc([]const u8, arr.len);
        for (arr, 0..) |item, i| {
            const s = item.as_string() orelse return error.Corrupt;
            items[i] = try a.dupe(u8, s);
        }
        // Sorted by construction on the way out, so unsorted on the way in is
        // a document this format did not write.
        var i: usize = 1;
        while (i < items.len) : (i += 1) {
            if (!std.mem.lessThan(u8, items[i - 1], items[i])) return error.Corrupt;
        }
        return .{ .items = items };
    }

    /// Reclaim the arena by round-tripping through the canonical bytes.
    ///
    /// Mutation allocates and never frees, which is bounded by a document's
    /// lifetime — and a commit is where that lifetime naturally ends, because
    /// the canonical bytes have just been produced anyway.
    pub fn compact(self: *Doc, bytes: []const u8, origin: []const u8) Error!void {
        const child = self.arena.child_allocator;
        var fresh = try Doc.decode(child, bytes, origin);
        // `decode` does not carry these: they are header fields the caller set
        // on the document it just encoded, and re-reading them is only correct
        // because `encode` wrote them.
        fresh.clock = self.clock;
        fresh.generation = self.generation;
        fresh.timer_at = self.timer_at;
        fresh.timer_generation = self.timer_generation;
        self.deinit();
        self.* = fresh;
    }
};

// ── Schedules ─────────────────────────────────────────────────────────────────

/// A schedule is not origin-scoped: it has no promises and no tasks of its own,
/// only a template for the ones it fires. So it gets a key of its own rather
/// than a place in a document.
pub const ScheduleDoc = struct {
    arena: std.heap.ArenaAllocator,
    id: []const u8 = "",
    cron: []const u8 = "",
    promise_id: []const u8 = "",
    promise_timeout: i64 = 0,
    promise_param: PromiseValue = .empty,
    promise_tags: StringMap = .empty,
    created_at: i64 = 0,
    next_run_at: i64 = 0,
    last_run_at: ?i64 = null,

    /// How many times this schedule has been written, and which of those writes
    /// armed the timer object now on the store. The same mechanism the origin
    /// documents use, and for the same reason: a schedule deleted and created
    /// again inside one minute would otherwise have one writer remove the other's
    /// deadline.
    generation: u64 = 0,
    timer_generation: u64 = 0,

    /// A schedule that has been deleted, but whose object has not gone yet.
    ///
    /// Deleting cannot be a read followed by an unconditional remove: two callers
    /// would both read it, both remove it, and both report that they were the one
    /// who did. So a delete is a compare-and-swap that writes this flag — exactly
    /// one caller can win it — and the object is removed afterwards. A tombstone
    /// left behind by a process that died in between reads as absent and is
    /// reclaimed by the next create.
    deleted: bool = false,

    pub fn init(child: std.mem.Allocator) ScheduleDoc {
        return .{ .arena = std.heap.ArenaAllocator.init(child) };
    }

    pub fn deinit(self: *ScheduleDoc) void {
        self.arena.deinit();
    }

    pub fn allocator(self: *ScheduleDoc) std.mem.Allocator {
        return self.arena.allocator();
    }

    pub fn encode(self: *const ScheduleDoc, out: *std.ArrayList(u8)) !void {
        out.clearRetainingCapacity();
        try out.appendSlice("{\"v\":");
        try out.writer().print("{d}", .{format_version});
        try out.appendSlice(",\"id\":");
        try json.write_string_ascii(out, self.id, true);
        try out.appendSlice(",\"cron\":");
        try json.write_string_ascii(out, self.cron, true);
        try out.appendSlice(",\"pid\":");
        try json.write_string_ascii(out, self.promise_id, true);
        try out.appendSlice(",\"pto\":");
        try out.writer().print("{d}", .{self.promise_timeout});
        try out.appendSlice(",\"ca\":");
        try out.writer().print("{d}", .{self.created_at});
        try out.appendSlice(",\"nr\":");
        try out.writer().print("{d}", .{self.next_run_at});
        try out.appendSlice(",\"gn\":");
        try out.writer().print("{d}", .{self.generation});
        try out.appendSlice(",\"tgn\":");
        try out.writer().print("{d}", .{self.timer_generation});
        if (self.last_run_at) |lr| {
            try out.appendSlice(",\"lr\":");
            try out.writer().print("{d}", .{lr});
        }
        if (self.deleted) try out.appendSlice(",\"del\":true");
        if (!self.promise_param.is_empty()) {
            try out.appendSlice(",\"pa\":");
            try Doc.encode_value(out, self.promise_param);
        }
        if (self.promise_tags.len() > 0) {
            try out.appendSlice(",\"tg\":");
            try Doc.encode_map(out, self.promise_tags);
        }
        try out.append('}');
    }

    pub fn decode(child: std.mem.Allocator, bytes: []const u8, id: []const u8) Error!ScheduleDoc {
        var sched = ScheduleDoc.init(child);
        errdefer sched.deinit();
        const a = sched.allocator();
        const v = json.parse(a, bytes) catch return error.Corrupt;
        const version = v.get_i64("v") orelse return error.Corrupt;
        if (version > format_version) return error.FormatTooNew;
        const stored_id = v.get_string("id") orelse return error.Corrupt;
        if (!std.mem.eql(u8, stored_id, id)) return error.WrongOrigin;
        sched.id = try a.dupe(u8, stored_id);
        sched.cron = try a.dupe(u8, v.get_string("cron") orelse return error.Corrupt);
        sched.promise_id = try a.dupe(u8, v.get_string("pid") orelse return error.Corrupt);
        sched.promise_timeout = v.get_i64("pto") orelse return error.Corrupt;
        sched.created_at = v.get_i64("ca") orelse return error.Corrupt;
        sched.next_run_at = v.get_i64("nr") orelse return error.Corrupt;
        sched.generation = blk: {
            const g = v.get_i64("gn") orelse break :blk 0;
            break :blk if (g < 0) 0 else @intCast(g);
        };
        sched.timer_generation = blk: {
            const g = v.get_i64("tgn") orelse break :blk 0;
            break :blk if (g < 0) 0 else @intCast(g);
        };
        sched.last_run_at = v.get_i64("lr");
        sched.deleted = blk: {
            const del = v.get("del") orelse break :blk false;
            break :blk del.as_bool() orelse false;
        };
        sched.promise_param = try Doc.decode_value(a, v.get("pa"));
        sched.promise_tags = if (v.get("tg")) |t|
            StringMap.from_json(a, t) catch return error.Corrupt
        else
            StringMap.empty;
        return sched;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn test_doc() !Doc {
    var doc = Doc.init(testing.allocator);
    const a = doc.allocator();
    _ = try doc.promise_insert(.{
        .id = try a.dupe(u8, "o:b"),
        .state = .pending,
        .param = .{ .data = try a.dupe(u8, "hello") },
        .value = .empty,
        .tags = try StringMap.from_json(a, try json.parse(a, "{\"resonate:target\":\"poll://any@g\"}")),
        .timeout_at = 5_000,
        .created_at = 1_000,
        .settled_at = null,
        .timeout_armed = true,
        .callbacks = .empty,
        .listeners = .empty,
    });
    _ = try doc.promise_insert(.{
        .id = try a.dupe(u8, "o:a"),
        .state = .resolved,
        .param = .empty,
        .value = .{ .data = try a.dupe(u8, "café"), .headers = try StringMap.from_json(a, try json.parse(a, "{\"x\":\"1\"}")) },
        .tags = .empty,
        .timeout_at = 9_000,
        .created_at = 1_000,
        .settled_at = 2_000,
        .timeout_armed = false,
        .callbacks = .empty,
        .listeners = .empty,
    });
    _ = try doc.task_insert(.{
        .id = try a.dupe(u8, "o:b"),
        .state = .acquired,
        .version = 3,
        .pid = try a.dupe(u8, "pid-1"),
        .ttl = 60_000,
        .resumes = .empty,
        .timeout = .{ .kind = .lease, .at = 4_000 },
    });
    doc.clock = 1_234;
    doc.generation = 7;
    doc.reseat_timer();
    return doc;
}

test "a document round trips through its canonical encoding" {
    var doc = try test_doc();
    defer doc.deinit();

    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    try doc.encode(&buf, "o");

    var back = try Doc.decode(testing.allocator, buf.items, "o");
    defer back.deinit();

    try testing.expectEqual(@as(usize, 2), back.promises.items.len);
    try testing.expectEqualStrings("o:a", back.promises.items[0].id);
    try testing.expectEqualStrings("o:b", back.promises.items[1].id);
    try testing.expectEqualStrings("café", back.promises.items[0].value.data.?);
    try testing.expectEqualStrings("1", back.promises.items[0].value.headers.?.get("x").?);
    try testing.expect(back.promises.items[1].timeout_armed);
    try testing.expectEqual(@as(i64, 1_234), back.clock);
    try testing.expectEqual(@as(u64, 7), back.generation);
    try testing.expectEqual(@as(i64, 4_000), back.timer_at.?);
    try testing.expectEqual(@as(i64, 3), back.tasks.items[0].version);
    try testing.expectEqualStrings("pid-1", back.tasks.items[0].pid.?);

    // And re-encoding the decoded document gives the same bytes: the encoding
    // is canonical, which is what lets a writer recognise its own write.
    var buf2 = std.ArrayList(u8).init(testing.allocator);
    defer buf2.deinit();
    try back.encode(&buf2, "o");
    try testing.expectEqualStrings(buf.items, buf2.items);
    try testing.expect(!Doc.changed(&doc, &back));
}

test "the encoding is ASCII and one line per entity" {
    var doc = try test_doc();
    defer doc.deinit();
    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    try doc.encode(&buf, "o");
    for (buf.items) |b| try testing.expect(b < 0x80);
    // header + 2 promises + 1 task = 4 lines, no trailing newline.
    try testing.expectEqual(@as(usize, 3), std.mem.count(u8, buf.items, "\n"));
    try testing.expect(buf.items[buf.items.len - 1] != '\n');
}

test "a document refuses a key it does not belong under" {
    var doc = try test_doc();
    defer doc.deinit();
    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    try doc.encode(&buf, "o");
    try testing.expectError(error.WrongOrigin, Doc.decode(testing.allocator, buf.items, "other"));
}

test "a document from a newer format is refused rather than misread" {
    const bytes = "{\"v\":99,\"og\":\"" ++ "0000000000000000" ++ "\",\"cl\":0,\"gn\":0}";
    try testing.expectError(error.FormatTooNew, Doc.decode(testing.allocator, bytes, "x"));
}

test "corrupt documents are refused" {
    for ([_][]const u8{
        "",
        "not json",
        "{\"v\":1}",
        "{}",
    }) |bad| {
        const r = Doc.decode(testing.allocator, bad, "o");
        try testing.expect(std.meta.isError(r));
    }
}

test "the write law ignores the clock and the generation" {
    var a = try test_doc();
    defer a.deinit();
    var b = try test_doc();
    defer b.deinit();
    try testing.expect(!Doc.changed(&a, &b));
    b.clock += 10_000;
    b.generation += 1;
    try testing.expect(!Doc.changed(&a, &b));
    // A moved deadline is a change.
    b.tasks.items[0].timeout.?.at += 1;
    b.reseat_timer();
    try testing.expect(Doc.changed(&a, &b));
}

test "the earliest live deadline is the one armed" {
    var doc = try test_doc();
    defer doc.deinit();
    // The lease at 4000 beats the promise deadline at 5000.
    try testing.expectEqual(@as(i64, 4_000), doc.min_deadline().?);
    // A lease on a task that is no longer acquired is not live.
    doc.tasks.items[0].state = .pending;
    try testing.expectEqual(@as(i64, 5_000), doc.min_deadline().?);
    // Neither is a promise deadline once the promise has settled.
    doc.promises.items[1].state = .resolved;
    try testing.expect(doc.min_deadline() == null);
}

test "a string set stays sorted through inserts and removes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var set: StringSet = .empty;
    for ([_][]const u8{ "m", "a", "z", "a", "c" }) |s| _ = try set.insert(a, s);
    try testing.expectEqual(@as(usize, 4), set.len());
    try testing.expectEqualStrings("a", set.items[0]);
    try testing.expectEqualStrings("c", set.items[1]);
    try testing.expectEqualStrings("m", set.items[2]);
    try testing.expectEqualStrings("z", set.items[3]);
    try testing.expect(try set.remove(a, "m"));
    try testing.expect(!try set.remove(a, "m"));
    try testing.expectEqual(@as(usize, 3), set.len());
    try testing.expect(!set.contains("m"));
    try testing.expect(set.contains("z"));
}

test "a schedule document round trips" {
    var sched = ScheduleDoc.init(testing.allocator);
    defer sched.deinit();
    const a = sched.allocator();
    sched.id = try a.dupe(u8, "s0");
    sched.cron = try a.dupe(u8, "* * * * *");
    sched.promise_id = try a.dupe(u8, "{{.id}}.{{.timestamp}}");
    sched.promise_timeout = 60_000;
    sched.created_at = 1_000;
    sched.next_run_at = 60_000;
    sched.last_run_at = null;
    sched.promise_tags = try StringMap.from_json(a, try json.parse(a, "{\"resonate:target\":\"poll://any@g\"}"));

    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    try sched.encode(&buf);

    var back = try ScheduleDoc.decode(testing.allocator, buf.items, "s0");
    defer back.deinit();
    try testing.expectEqualStrings("* * * * *", back.cron);
    try testing.expectEqualStrings("{{.id}}.{{.timestamp}}", back.promise_id);
    try testing.expectEqual(@as(i64, 60_000), back.promise_timeout);
    try testing.expectEqual(@as(i64, 60_000), back.next_run_at);
    try testing.expect(back.last_run_at == null);
    try testing.expectEqualStrings("poll://any@g", back.promise_tags.get("resonate:target").?);
    try testing.expectError(error.WrongOrigin, ScheduleDoc.decode(testing.allocator, buf.items, "s1"));
}

test "the timer generation is part of the document and part of the write law" {
    var a = try test_doc();
    defer a.deinit();
    var b = try test_doc();
    defer b.deinit();
    a.timer_generation = 3;
    b.timer_generation = 3;
    try testing.expect(!Doc.changed(&a, &b));
    // Re-arming under a new commit is a change, even at the same instant: it is a
    // different object on the store.
    b.timer_generation = 4;
    try testing.expect(Doc.changed(&a, &b));

    var buf = std.ArrayList(u8).init(testing.allocator);
    defer buf.deinit();
    try b.encode(&buf, "o");
    var back = try Doc.decode(testing.allocator, buf.items, "o");
    defer back.deinit();
    try testing.expectEqual(@as(u64, 4), back.timer_generation);
}
