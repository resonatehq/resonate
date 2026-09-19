//! The protocol: its vocabulary, its validation rules, and how a record is
//! written back onto the wire.
//!
//! Everything here is a definition or a pure function over one. No state, no
//! I/O, no clock. What an operation *does* is `state.zig`'s; what the protocol
//! *admits* is this file's, and the split matters because the same admission
//! rules have to hold for a request that arrives over HTTP and for one the
//! simulator synthesises.
//!
//! The strings are load-bearing. A status code, a field name and a rejection
//! message are all part of the protocol, so they are written out literally
//! rather than derived — a reader comparing this file against the reference
//! model should be able to do it by eye.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const assert = stdx.assert;

pub const protocol_version = "2026-04-01";
pub const supported_versions = [_][]const u8{"2026-04-01"};

/// How long a task stays pending before the server offers it again.
pub const pending_retry_ttl: i64 = 30_000;

/// The sections of `debug.snap`, in the order it writes them.
///
/// Named here rather than left implicit in `scan`, because the differential
/// compares them one at a time: a snapshot is long, and "these two differ" is
/// not a useful thing to be told about two thousand characters.
pub const snapshot_sections = [_][]const u8{
    "promises",
    "promiseTimeouts",
    "callbacks",
    "listeners",
    "tasks",
    "taskTimeouts",
    "messages",
};

/// How many branch siblings a task response carries.
pub const preload_limit_default: u32 = 10;

/// The default page size of each search, and the ceiling a caller may ask for.
pub const search_limit_default: i64 = 100;
pub const schedule_search_limit_default: i64 = 10;
pub const search_limit_max: i64 = 1000;

// ── States ────────────────────────────────────────────────────────────────────

pub const PromiseState = enum {
    pending,
    resolved,
    rejected,
    rejected_canceled,
    rejected_timedout,

    pub fn as_str(self: PromiseState) []const u8 {
        return switch (self) {
            .pending => "pending",
            .resolved => "resolved",
            .rejected => "rejected",
            .rejected_canceled => "rejected_canceled",
            .rejected_timedout => "rejected_timedout",
        };
    }

    pub fn parse(s: []const u8) ?PromiseState {
        const names = [_]PromiseState{ .pending, .resolved, .rejected, .rejected_canceled, .rejected_timedout };
        for (names) |n| {
            if (std.mem.eql(u8, s, n.as_str())) return n;
        }
        return null;
    }

    pub fn is_settled(self: PromiseState) bool {
        return self != .pending;
    }
};

pub const TaskState = enum {
    pending,
    acquired,
    suspended,
    halted,
    fulfilled,

    pub fn as_str(self: TaskState) []const u8 {
        return switch (self) {
            .pending => "pending",
            .acquired => "acquired",
            .suspended => "suspended",
            .halted => "halted",
            .fulfilled => "fulfilled",
        };
    }

    pub fn parse(s: []const u8) ?TaskState {
        const names = [_]TaskState{ .pending, .acquired, .suspended, .halted, .fulfilled };
        for (names) |n| {
            if (std.mem.eql(u8, s, n.as_str())) return n;
        }
        return null;
    }
};

/// The verdicts a caller may settle a promise with. `rejected_timedout` is not
/// among them: only a deadline produces that, which is why this is a type of
/// its own rather than a subset checked at each call site.
pub const SettleState = enum {
    resolved,
    rejected,
    rejected_canceled,

    pub fn as_str(self: SettleState) []const u8 {
        return switch (self) {
            .resolved => "resolved",
            .rejected => "rejected",
            .rejected_canceled => "rejected_canceled",
        };
    }

    pub fn parse(s: []const u8) ?SettleState {
        const names = [_]SettleState{ .resolved, .rejected, .rejected_canceled };
        for (names) |n| {
            if (std.mem.eql(u8, s, n.as_str())) return n;
        }
        return null;
    }

    pub fn to_promise_state(self: SettleState) PromiseState {
        return switch (self) {
            .resolved => .resolved,
            .rejected => .rejected,
            .rejected_canceled => .rejected_canceled,
        };
    }
};

/// Which of a task's two deadlines an entry in the task timeout table is.
///
/// A task holds at most one: acquiring replaces the retry deadline with a
/// lease, releasing replaces the lease with a retry. The numbering is on the
/// wire, in `debug.snap`'s `taskTimeouts[].type`.
pub const TaskTimeoutKind = enum(u8) {
    retry = 0,
    lease = 1,
};

// ── Tags ──────────────────────────────────────────────────────────────────────

pub const tag_target = "resonate:target";
pub const tag_timer = "resonate:timer";
pub const tag_scope = "resonate:scope";
pub const tag_external = "resonate:external";
pub const tag_origin = "resonate:origin";
pub const tag_branch = "resonate:branch";
pub const tag_parent = "resonate:parent";
pub const tag_prefix = "resonate:prefix";
pub const tag_delay = "resonate:delay";
pub const tag_schedule = "resonate:schedule";

/// The origin of an id: everything before the first ':'.
///
/// An id is `<origin>:<lineage>`, lineage segments separated by '.'. The origin
/// is what makes two ids part of one execution, which is why a callback may
/// only be registered within one.
pub fn origin(id: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, id, ':')) |i| return id[0..i];
    return id;
}

/// A sorted, owned map of string to string — a promise's tags, or the headers
/// of its param.
///
/// Sorted by key, always. The reference model holds these in a hash map and
/// compares them after parsing, where order cannot be observed; this server
/// writes JSON that the simulator compares byte for byte, so the order has to
/// come from the data rather than from the insertion history.
pub const StringMap = struct {
    pub const Entry = struct { key: []const u8, value: []const u8 };

    entries: []Entry = &.{},

    pub const empty: StringMap = .{};

    pub fn get(self: StringMap, key: []const u8) ?[]const u8 {
        // Binary search: sorted by construction.
        var lo: usize = 0;
        var hi: usize = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.entries[mid].key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return self.entries[mid].value,
            }
        }
        return null;
    }

    pub fn has(self: StringMap, key: []const u8) bool {
        return self.get(key) != null;
    }

    pub fn is(self: StringMap, key: []const u8, value: []const u8) bool {
        const v = self.get(key) orelse return false;
        return std.mem.eql(u8, v, value);
    }

    pub fn len(self: StringMap) usize {
        return self.entries.len;
    }

    /// Build from a JSON object whose every member is a string. A member that
    /// is not a string is a request the protocol does not admit, so this
    /// returns an error rather than skipping it.
    pub fn from_json(allocator: std.mem.Allocator, v: json.Value) !StringMap {
        const obj = v.as_object() orelse return error.NotAStringMap;
        var entries = try allocator.alloc(Entry, obj.entries.len);
        errdefer allocator.free(entries);
        var n: usize = 0;
        for (obj.entries) |e| {
            const s = e.value.as_string() orelse return error.NotAStringMap;
            entries[n] = .{
                .key = try allocator.dupe(u8, e.key),
                .value = try allocator.dupe(u8, s),
            };
            n += 1;
        }
        std.mem.sort(Entry, entries[0..n], {}, struct {
            fn less(_: void, a: Entry, b: Entry) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.less);
        return .{ .entries = entries[0..n] };
    }

    pub fn clone(self: StringMap, allocator: std.mem.Allocator) !StringMap {
        const entries = try allocator.alloc(Entry, self.entries.len);
        errdefer allocator.free(entries);
        for (self.entries, 0..) |e, i| {
            entries[i] = .{
                .key = try allocator.dupe(u8, e.key),
                .value = try allocator.dupe(u8, e.value),
            };
        }
        return .{ .entries = entries };
    }

    pub fn deinit(self: *StringMap, allocator: std.mem.Allocator) void {
        for (self.entries) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        }
        if (self.entries.len > 0) allocator.free(self.entries);
        self.entries = &.{};
    }

    /// Insert or replace, keeping the map sorted. Used where the server adds a
    /// tag of its own — the four lineage tags a schedule stamps onto what it
    /// fires.
    pub fn put(self: *StringMap, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        for (self.entries) |*e| {
            if (std.mem.eql(u8, e.key, key)) {
                const v = try allocator.dupe(u8, value);
                allocator.free(e.value);
                e.value = v;
                return;
            }
        }
        var list = std.ArrayListUnmanaged(Entry){
            .items = self.entries,
            .capacity = self.entries.len,
        };
        try list.append(allocator, .{
            .key = try allocator.dupe(u8, key),
            .value = try allocator.dupe(u8, value),
        });
        self.entries = list.items;
        std.mem.sort(Entry, self.entries, {}, struct {
            fn less(_: void, a: Entry, b: Entry) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.less);
    }

    pub fn write_json(self: StringMap, w: *json.Writer) !void {
        try w.object_begin();
        for (self.entries) |e| try w.field_string(e.key, e.value);
        try w.object_end();
    }

    /// Does every pair in `filter` appear here with the same value? The
    /// containment test a tag filter in a search means.
    pub fn contains_all(self: StringMap, filter: StringMap) bool {
        for (filter.entries) |f| {
            const v = self.get(f.key) orelse return false;
            if (!std.mem.eql(u8, v, f.value)) return false;
        }
        return true;
    }
};

/// `resonate:timer = true` — the one tag that decides what a deadline produces:
/// resolved for a timer, rejected_timedout for everything else.
pub fn is_timer(tags: StringMap) bool {
    return tags.is(tag_timer, "true");
}

/// Who may be blocked on this promise.
///
/// External if any one of four things holds, and they are alternatives rather
/// than a hierarchy: a global scope, the explicit escape hatch, a dispatch
/// target, or a timer. An external promise may be awaited *and* is armed — one
/// rule, because the server owes an observation exactly where someone can be
/// blocked.
pub fn is_external(tags: StringMap) bool {
    return tags.is(tag_scope, "global") or
        tags.is(tag_external, "true") or
        tags.has(tag_target) or
        is_timer(tags);
}

/// A promise carries a task exactly when it names somewhere to run.
pub fn has_task(tags: StringMap) bool {
    return tags.has(tag_target);
}

/// What a deadline settles this promise as.
pub fn timeout_state(tags: StringMap) PromiseState {
    return if (is_timer(tags)) .resolved else .rejected_timedout;
}

// ── Addresses ─────────────────────────────────────────────────────────────────

/// Is this a Resonate address?
///
/// Deliberately shallow: an address is valid if it parses as a URI with a
/// scheme. What lies past the scheme belongs to whichever transport is
/// registered for it, and this function must not know — validation has to be a
/// pure function of the string, identical on every deployment, or a server's
/// enabled transports would change which requests it accepts.
///
/// The one thing past the scheme that matters: the schemes the URL standard
/// calls *special* require a host, so `http://` is not an address while
/// `bash://` is.
pub fn is_valid_address(address: []const u8) bool {
    if (address.len == 0) return false;
    const colon = std.mem.indexOfScalar(u8, address, ':') orelse return false;
    if (colon == 0) return false;
    const scheme = address[0..colon];
    if (!std.ascii.isAlphabetic(scheme[0])) return false;
    for (scheme[1..]) |c| {
        if (!std.ascii.isAlphanumeric(c) and c != '+' and c != '-' and c != '.') return false;
    }
    const rest = address[colon + 1 ..];
    if (is_special_scheme(scheme)) {
        // A special scheme is authority-based, and an empty authority is not a
        // host. `file` is the exception the standard makes: an empty host means
        // the local machine.
        const is_file = std.ascii.eqlIgnoreCase(scheme, "file");
        if (!std.mem.startsWith(u8, rest, "//")) return is_file;
        const authority = rest[2..];
        const end = std.mem.indexOfAny(u8, authority, "/?#") orelse authority.len;
        const host_part = authority[0..end];
        // Strip userinfo, if any.
        const host = if (std.mem.lastIndexOfScalar(u8, host_part, '@')) |at|
            host_part[at + 1 ..]
        else
            host_part;
        if (host.len == 0 and !is_file) return false;
        for (host) |c| {
            if (c == ' ' or c < 0x20) return false;
        }
        return true;
    }
    // Any other scheme: `mailto:a@b.c`, `foo:bar`, `bash://` are all URIs.
    for (address) |c| {
        if (c < 0x20 or c == 0x7f) return false;
    }
    return true;
}

fn is_special_scheme(scheme: []const u8) bool {
    for ([_][]const u8{ "http", "https", "ws", "wss", "ftp", "file" }) |s| {
        if (std.ascii.eqlIgnoreCase(scheme, s)) return true;
    }
    return false;
}

/// The scheme of an address — all the routing information there is.
pub fn scheme_of(address: []const u8) ?[]const u8 {
    if (!is_valid_address(address)) return null;
    const colon = std.mem.indexOfScalar(u8, address, ':').?;
    return address[0..colon];
}

// ── Envelope ──────────────────────────────────────────────────────────────────

/// Why an envelope is not a request.
///
/// Structural only. Whether `kind` names an operation this build has, and
/// whether `data` holds what that operation needs, are the state machine's
/// questions — it is the only party that knows.
pub const Invalid = union(enum) {
    unparseable,
    empty_kind,
    data_not_object,
    unsupported_version: []const u8,
};

pub const Envelope = struct {
    kind: []const u8,
    corr_id: []const u8,
    version: []const u8,
    auth: ?[]const u8,
    debug_time: ?i64,
    data: json.Value,
};

/// Bytes to a request, or the reason they are not one.
///
/// Parsing and validating are two failures with one answer — reject at the
/// edge — so they are offered together and there is no path that does one and
/// forgets the other.
pub fn parse_envelope(arena: std.mem.Allocator, body: []const u8) union(enum) {
    ok: Envelope,
    invalid: Invalid,
} {
    const root = json.parse(arena, body) catch return .{ .invalid = .unparseable };
    const kind_v = root.get("kind") orelse return .{ .invalid = .unparseable };
    const kind = kind_v.as_string() orelse return .{ .invalid = .unparseable };
    const head = root.get("head") orelse return .{ .invalid = .unparseable };
    if (!head.is_object()) return .{ .invalid = .unparseable };
    const corr_id_v = head.get("corrId") orelse return .{ .invalid = .unparseable };
    const corr_id = corr_id_v.as_string() orelse return .{ .invalid = .unparseable };
    const version_v = head.get("version") orelse return .{ .invalid = .unparseable };
    const version = version_v.as_string() orelse return .{ .invalid = .unparseable };
    const data = root.get("data") orelse return .{ .invalid = .unparseable };

    if (kind.len == 0) return .{ .invalid = .empty_kind };
    if (!data.is_object()) return .{ .invalid = .data_not_object };
    var known = false;
    for (supported_versions) |v| {
        if (std.mem.eql(u8, v, version)) known = true;
    }
    if (!known) return .{ .invalid = .{ .unsupported_version = version } };

    const auth: ?[]const u8 = blk: {
        const a = head.get("auth") orelse break :blk null;
        break :blk a.as_string();
    };
    const debug_time: ?i64 = blk: {
        const t = head.get("resonate:debug_time") orelse break :blk null;
        break :blk t.as_i64();
    };

    return .{ .ok = .{
        .kind = kind,
        .corr_id = corr_id,
        .version = version,
        .auth = auth,
        .debug_time = debug_time,
        .data = data,
    } };
}

/// `kind` and `corrId` dug out of bytes that would not parse.
///
/// Best effort, and only for the error path: a client that sent malformed JSON
/// still gets an answer it can correlate.
pub fn salvage_context(arena: std.mem.Allocator, body: []const u8) struct {
    kind: []const u8,
    corr_id: []const u8,
} {
    const root = json.parse(arena, body) catch return .{ .kind = "unknown", .corr_id = "0" };
    const kind = blk: {
        const k = root.get("kind") orelse break :blk "unknown";
        break :blk k.as_string() orelse "unknown";
    };
    const corr_id = blk: {
        const h = root.get("head") orelse break :blk "0";
        const c = h.get("corrId") orelse break :blk "0";
        break :blk c.as_string() orelse "0";
    };
    return .{ .kind = kind, .corr_id = corr_id };
}

/// The message a client sees for a structurally invalid envelope. Written here
/// so two transports cannot word the same rejection differently.
pub fn invalid_message(out: *std.ArrayList(u8), invalid: Invalid) ![]const u8 {
    switch (invalid) {
        // The parser's own complaint is not reproduced: it is a different
        // parser's wording and no client keys off it.
        .unparseable => return "Invalid request envelope: not a valid request",
        .empty_kind => return "Missing or invalid 'kind' field — must be a non-empty string",
        .data_not_object => return "Invalid 'data' field — must be an object",
        .unsupported_version => |got| {
            out.clearRetainingCapacity();
            try out.writer().print(
                "Unsupported protocol version '{s}', supported versions: [\"{s}\"]",
                .{ got, supported_versions[0] },
            );
            return out.items;
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "origin is everything before the first colon" {
    try testing.expectEqualStrings("diff", origin("diff:p1"));
    try testing.expectEqualStrings("diff", origin("diff:p1.2.3"));
    try testing.expectEqualStrings("my.app.workflow", origin("my.app.workflow:1"));
    try testing.expectEqualStrings("bare", origin("bare"));
}

test "an address is any URI with a scheme, and a special scheme needs a host" {
    for ([_][]const u8{
        "http://worker:9999", "https://worker/path", "poll://uni@group",
        "poll://any@group/id", "gcps://project/topic", "bash://",
        "bash://docker/alpine", "unknown://x/y",      "mailto:a@b.c",
        "foo:bar",             "poll://group",        "poll://bogus@group",
        "gcps://project",
    }) |addr| {
        try testing.expect(is_valid_address(addr));
    }
    for ([_][]const u8{ "", "not a url", "/relative", "http://", "://x", "1http://x" }) |addr| {
        try testing.expect(!is_valid_address(addr));
    }
}

test "external is any of four alternatives" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cases = [_]struct { src: []const u8, want: bool }{
        .{ .src = "{}", .want = false },
        .{ .src = "{\"resonate:scope\":\"global\"}", .want = true },
        .{ .src = "{\"resonate:scope\":\"local\"}", .want = false },
        .{ .src = "{\"resonate:external\":\"true\"}", .want = true },
        .{ .src = "{\"resonate:external\":\"false\"}", .want = false },
        .{ .src = "{\"resonate:target\":\"poll://any@g\"}", .want = true },
        .{ .src = "{\"resonate:timer\":\"true\"}", .want = true },
        .{ .src = "{\"other\":\"x\"}", .want = false },
    };
    for (cases) |c| {
        const tags = try StringMap.from_json(a, try json.parse(a, c.src));
        try testing.expectEqual(c.want, is_external(tags));
    }
}

test "a string map is sorted and searchable" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const tags = try StringMap.from_json(a, try json.parse(a, "{\"z\":\"1\",\"a\":\"2\",\"m\":\"3\"}"));
    try testing.expectEqualStrings("a", tags.entries[0].key);
    try testing.expectEqualStrings("m", tags.entries[1].key);
    try testing.expectEqualStrings("z", tags.entries[2].key);
    try testing.expectEqualStrings("2", tags.get("a").?);
    try testing.expectEqualStrings("3", tags.get("m").?);
    try testing.expectEqualStrings("1", tags.get("z").?);
    try testing.expect(tags.get("q") == null);
}

test "an envelope is admitted only with a known version, a kind and an object" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    switch (parse_envelope(a, "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"c\",\"version\":\"2026-04-01\"},\"data\":{\"id\":\"p\"}}")) {
        .ok => |e| {
            try testing.expectEqualStrings("promise.get", e.kind);
            try testing.expectEqualStrings("c", e.corr_id);
            try testing.expect(e.debug_time == null);
        },
        .invalid => return error.TestUnexpectedResult,
    }
    switch (parse_envelope(a, "{\"kind\":\"\",\"head\":{\"corrId\":\"c\",\"version\":\"2026-04-01\"},\"data\":{}}")) {
        .invalid => |i| try testing.expect(i == .empty_kind),
        .ok => return error.TestUnexpectedResult,
    }
    switch (parse_envelope(a, "{\"kind\":\"k\",\"head\":{\"corrId\":\"c\",\"version\":\"2026-04-01\"},\"data\":[]}")) {
        .invalid => |i| try testing.expect(i == .data_not_object),
        .ok => return error.TestUnexpectedResult,
    }
    switch (parse_envelope(a, "{\"kind\":\"k\",\"head\":{\"corrId\":\"c\",\"version\":\"1999-01-01\"},\"data\":{}}")) {
        .invalid => |i| try testing.expect(i == .unsupported_version),
        .ok => return error.TestUnexpectedResult,
    }
    switch (parse_envelope(a, "not json")) {
        .invalid => |i| try testing.expect(i == .unparseable),
        .ok => return error.TestUnexpectedResult,
    }
    // debug_time rides in the head under its reserved name.
    switch (parse_envelope(a, "{\"kind\":\"k\",\"head\":{\"corrId\":\"c\",\"version\":\"2026-04-01\",\"resonate:debug_time\":1234},\"data\":{}}")) {
        .ok => |e| try testing.expectEqual(@as(i64, 1234), e.debug_time.?),
        .invalid => return error.TestUnexpectedResult,
    }
}
