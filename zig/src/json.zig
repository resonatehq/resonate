//! JSON, by hand.
//!
//! The wire protocol is JSON and this build has no dependencies, so the parser
//! and the writer live here. Two things shape the design:
//!
//! * **Order is preserved.** An object keeps its keys in the order they were
//!   parsed, and the writer emits fields in the order the caller names them.
//!   Two servers that disagree only on key order are still the same server, but
//!   a diff that reorders keys is a diff nobody can read — and the simulator
//!   compares responses byte for byte where it can.
//! * **One arena per document.** Parsing allocates strings, arrays and objects
//!   out of an arena the caller owns and frees in one call. Nothing here frees
//!   anything individually.

const std = @import("std");
const stdx = @import("stdx.zig");
const assert = stdx.assert;

pub const max_depth = 96;

pub const Error = error{
    Unexpected,
    UnexpectedEnd,
    DepthExceeded,
    InvalidNumber,
    InvalidString,
    InvalidEscape,
    OutOfMemory,
};

pub const Value = union(enum) {
    null,
    bool: bool,
    /// A JSON number that is an exact integer. The protocol's timestamps,
    /// versions and TTLs are all integers, and rounding one through an f64 is
    /// how a `timeoutAt` far in the future stops comparing equal.
    int: i64,
    float: f64,
    string: []const u8,
    array: []Value,
    object: Object,

    pub const null_value: Value = .null;

    pub fn is_object(self: Value) bool {
        return self == .object;
    }

    pub fn is_null(self: Value) bool {
        return self == .null;
    }

    /// The member named `key`, or null — for objects only; anything else is
    /// null rather than an error, because every caller here is reading an
    /// untrusted document and has a default in mind.
    pub fn get(self: Value, key: []const u8) ?Value {
        return switch (self) {
            .object => |o| o.get(key),
            else => null,
        };
    }

    pub fn as_string(self: Value) ?[]const u8 {
        return switch (self) {
            .string => |s| s,
            else => null,
        };
    }

    pub fn as_i64(self: Value) ?i64 {
        return switch (self) {
            .int => |i| i,
            // A number written as `1e3` or `5.0` is still an integer if it
            // lands on one. Clients do this; refusing it would be a protocol
            // difference nobody asked for.
            .float => |f| if (@floor(f) == f and f >= -9.2e18 and f <= 9.2e18)
                @as(i64, @intFromFloat(f))
            else
                null,
            else => null,
        };
    }

    pub fn as_bool(self: Value) ?bool {
        return switch (self) {
            .bool => |b| b,
            else => null,
        };
    }

    pub fn as_array(self: Value) ?[]Value {
        return switch (self) {
            .array => |a| a,
            else => null,
        };
    }

    pub fn as_object(self: Value) ?Object {
        return switch (self) {
            .object => |o| o,
            else => null,
        };
    }

    /// `get(key)` when the member is a string.
    pub fn get_string(self: Value, key: []const u8) ?[]const u8 {
        const v = self.get(key) orelse return null;
        return v.as_string();
    }

    pub fn get_i64(self: Value, key: []const u8) ?i64 {
        const v = self.get(key) orelse return null;
        return v.as_i64();
    }
};

pub const Entry = struct {
    key: []const u8,
    value: Value,
};

pub const Object = struct {
    entries: []Entry = &.{},

    pub const empty: Object = .{};

    pub fn get(self: Object, key: []const u8) ?Value {
        // Linear: protocol objects have a handful of members, and a map would
        // cost an allocation and an iteration order per object.
        for (self.entries) |e| {
            if (std.mem.eql(u8, e.key, key)) return e.value;
        }
        return null;
    }

    pub fn has(self: Object, key: []const u8) bool {
        return self.get(key) != null;
    }

    pub fn len(self: Object) usize {
        return self.entries.len;
    }
};

// ── Parsing ───────────────────────────────────────────────────────────────────

pub const Parser = struct {
    source: []const u8,
    pos: usize = 0,
    arena: std.mem.Allocator,
    depth: u32 = 0,

    pub fn init(arena: std.mem.Allocator, source: []const u8) Parser {
        return .{ .source = source, .arena = arena };
    }

    /// Parse one complete document. Trailing whitespace is fine; trailing
    /// anything else is not.
    pub fn parse(self: *Parser) Error!Value {
        const v = try self.parse_value();
        self.skip_whitespace();
        if (self.pos != self.source.len) return error.Unexpected;
        return v;
    }

    fn skip_whitespace(self: *Parser) void {
        while (self.pos < self.source.len) : (self.pos += 1) {
            switch (self.source[self.pos]) {
                ' ', '\t', '\n', '\r' => {},
                else => return,
            }
        }
    }

    fn peek(self: *Parser) Error!u8 {
        if (self.pos >= self.source.len) return error.UnexpectedEnd;
        return self.source[self.pos];
    }

    fn expect(self: *Parser, c: u8) Error!void {
        if (try self.peek() != c) return error.Unexpected;
        self.pos += 1;
    }

    fn literal(self: *Parser, word: []const u8) Error!void {
        if (self.source.len - self.pos < word.len) return error.UnexpectedEnd;
        if (!std.mem.eql(u8, self.source[self.pos..][0..word.len], word)) return error.Unexpected;
        self.pos += word.len;
    }

    fn parse_value(self: *Parser) Error!Value {
        self.skip_whitespace();
        const c = try self.peek();
        return switch (c) {
            'n' => blk: {
                try self.literal("null");
                break :blk .null;
            },
            't' => blk: {
                try self.literal("true");
                break :blk .{ .bool = true };
            },
            'f' => blk: {
                try self.literal("false");
                break :blk .{ .bool = false };
            },
            '"' => .{ .string = try self.parse_string() },
            '[' => try self.parse_array(),
            '{' => try self.parse_object(),
            '-', '0'...'9' => try self.parse_number(),
            else => error.Unexpected,
        };
    }

    fn parse_array(self: *Parser) Error!Value {
        if (self.depth >= max_depth) return error.DepthExceeded;
        self.depth += 1;
        defer self.depth -= 1;

        try self.expect('[');
        var items: std.ArrayListUnmanaged(Value) = .{};
        self.skip_whitespace();
        if (try self.peek() == ']') {
            self.pos += 1;
            return .{ .array = try items.toOwnedSlice(self.arena) };
        }
        while (true) {
            const v = try self.parse_value();
            try items.append(self.arena, v);
            self.skip_whitespace();
            switch (try self.peek()) {
                ',' => self.pos += 1,
                ']' => {
                    self.pos += 1;
                    return .{ .array = try items.toOwnedSlice(self.arena) };
                },
                else => return error.Unexpected,
            }
        }
    }

    fn parse_object(self: *Parser) Error!Value {
        if (self.depth >= max_depth) return error.DepthExceeded;
        self.depth += 1;
        defer self.depth -= 1;

        try self.expect('{');
        var entries: std.ArrayListUnmanaged(Entry) = .{};
        self.skip_whitespace();
        if (try self.peek() == '}') {
            self.pos += 1;
            return .{ .object = .{ .entries = try entries.toOwnedSlice(self.arena) } };
        }
        while (true) {
            self.skip_whitespace();
            const key = try self.parse_string();
            self.skip_whitespace();
            try self.expect(':');
            const value = try self.parse_value();
            // A duplicate key overwrites, which is what serde_json does. The
            // alternative — two entries with one name — would make `get`
            // depend on which one it found first.
            var replaced = false;
            for (entries.items) |*e| {
                if (std.mem.eql(u8, e.key, key)) {
                    e.value = value;
                    replaced = true;
                    break;
                }
            }
            if (!replaced) try entries.append(self.arena, .{ .key = key, .value = value });
            self.skip_whitespace();
            switch (try self.peek()) {
                ',' => self.pos += 1,
                '}' => {
                    self.pos += 1;
                    return .{ .object = .{ .entries = try entries.toOwnedSlice(self.arena) } };
                },
                else => return error.Unexpected,
            }
        }
    }

    fn parse_number(self: *Parser) Error!Value {
        const start = self.pos;
        if (try self.peek() == '-') self.pos += 1;
        // Integer part.
        const int_start = self.pos;
        while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) self.pos += 1;
        if (self.pos == int_start) return error.InvalidNumber;
        // JSON forbids leading zeros ("01"), and accepting them would make two
        // encodings of the same number, which the log must not have.
        if (self.source[int_start] == '0' and self.pos - int_start > 1) return error.InvalidNumber;

        var is_float = false;
        if (self.pos < self.source.len and self.source[self.pos] == '.') {
            is_float = true;
            self.pos += 1;
            const frac_start = self.pos;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) self.pos += 1;
            if (self.pos == frac_start) return error.InvalidNumber;
        }
        if (self.pos < self.source.len and (self.source[self.pos] == 'e' or self.source[self.pos] == 'E')) {
            is_float = true;
            self.pos += 1;
            if (self.pos < self.source.len and (self.source[self.pos] == '+' or self.source[self.pos] == '-')) {
                self.pos += 1;
            }
            const exp_start = self.pos;
            while (self.pos < self.source.len and std.ascii.isDigit(self.source[self.pos])) self.pos += 1;
            if (self.pos == exp_start) return error.InvalidNumber;
        }
        const text = self.source[start..self.pos];
        if (!is_float) {
            if (std.fmt.parseInt(i64, text, 10)) |i| return .{ .int = i } else |_| {}
        }
        const f = std.fmt.parseFloat(f64, text) catch return error.InvalidNumber;
        return .{ .float = f };
    }

    /// A JSON string, unescaped into arena memory.
    ///
    /// The fast path matters: most strings in a request carry no escape at all,
    /// and those are returned as a slice of the source with no copy.
    fn parse_string(self: *Parser) Error![]const u8 {
        try self.expect('"');
        const start = self.pos;
        var escaped = false;
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == '"') {
                const raw = self.source[start..self.pos];
                self.pos += 1;
                if (!escaped) return raw;
                return try self.unescape(raw);
            }
            if (c == '\\') {
                escaped = true;
                self.pos += 1;
                if (self.pos >= self.source.len) return error.UnexpectedEnd;
                self.pos += 1;
                continue;
            }
            // Unescaped control characters are not strings.
            if (c < 0x20) return error.InvalidString;
            self.pos += 1;
        }
        return error.UnexpectedEnd;
    }

    fn unescape(self: *Parser, raw: []const u8) Error![]const u8 {
        var out: std.ArrayListUnmanaged(u8) = .{};
        try out.ensureTotalCapacity(self.arena, raw.len);
        var i: usize = 0;
        while (i < raw.len) {
            const c = raw[i];
            if (c != '\\') {
                try out.append(self.arena, c);
                i += 1;
                continue;
            }
            i += 1;
            if (i >= raw.len) return error.InvalidEscape;
            const e = raw[i];
            i += 1;
            switch (e) {
                '"' => try out.append(self.arena, '"'),
                '\\' => try out.append(self.arena, '\\'),
                '/' => try out.append(self.arena, '/'),
                'b' => try out.append(self.arena, 0x08),
                'f' => try out.append(self.arena, 0x0c),
                'n' => try out.append(self.arena, '\n'),
                'r' => try out.append(self.arena, '\r'),
                't' => try out.append(self.arena, '\t'),
                'u' => {
                    if (i + 4 > raw.len) return error.InvalidEscape;
                    const hi = std.fmt.parseInt(u16, raw[i..][0..4], 16) catch return error.InvalidEscape;
                    i += 4;
                    var code_point: u21 = hi;
                    if (hi >= 0xD800 and hi <= 0xDBFF) {
                        // A high surrogate needs its pair, or the document is
                        // not valid UTF-16 and we cannot produce UTF-8.
                        if (i + 6 > raw.len or raw[i] != '\\' or raw[i + 1] != 'u') return error.InvalidEscape;
                        const lo = std.fmt.parseInt(u16, raw[i + 2 ..][0..4], 16) catch return error.InvalidEscape;
                        if (lo < 0xDC00 or lo > 0xDFFF) return error.InvalidEscape;
                        i += 6;
                        code_point = 0x10000 +
                            ((@as(u21, hi) - 0xD800) << 10) +
                            (@as(u21, lo) - 0xDC00);
                    } else if (hi >= 0xDC00 and hi <= 0xDFFF) {
                        return error.InvalidEscape;
                    }
                    var buf: [4]u8 = undefined;
                    const n = std.unicode.utf8Encode(code_point, &buf) catch return error.InvalidEscape;
                    try out.appendSlice(self.arena, buf[0..n]);
                },
                else => return error.InvalidEscape,
            }
        }
        return try out.toOwnedSlice(self.arena);
    }
};

/// Parse `source` into arena memory.
pub fn parse(arena: std.mem.Allocator, source: []const u8) Error!Value {
    var p = Parser.init(arena, source);
    return p.parse();
}

// ── Writing ───────────────────────────────────────────────────────────────────

/// Escape and write one JSON string, quotes included.
pub fn write_string(out: *std.ArrayList(u8), s: []const u8) !void {
    return write_string_ascii(out, s, false);
}

/// As `write_string`, and with `ascii_only` set every byte above 0x7f is
/// written as a `\uXXXX` escape instead of passed through.
///
/// The durable document format needs it: two encoders given equal state must
/// produce identical bytes, and passing UTF-8 through would make that depend on
/// nothing, while escaping it makes the encoding total and the bytes
/// inspectable in a terminal. The wire format does not need it, and paying for
/// it there would inflate every tag a client sent.
pub fn write_string_ascii(out: *std.ArrayList(u8), s: []const u8, ascii_only: bool) !void {
    try out.append('"');
    var i: usize = 0;
    var chunk_start: usize = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        const escape: ?[]const u8 = switch (c) {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            0x08 => "\\b",
            0x0c => "\\f",
            0x00...0x07, 0x0b, 0x0e...0x1f => null,
            0x80...0xff => if (ascii_only) null else continue,
            else => continue,
        };
        try out.appendSlice(s[chunk_start..i]);
        if (escape) |e| {
            try out.appendSlice(e);
            chunk_start = i + 1;
        } else if (c < 0x80) {
            // The remaining control characters have no short form.
            var buf: [6]u8 = undefined;
            _ = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
            try out.appendSlice(&buf);
            chunk_start = i + 1;
        } else {
            // A whole code point at a time, as one or two UTF-16 escapes. An
            // ill-formed sequence is escaped byte by byte as U+FFFD rather than
            // refused: the encoder's job is to be total.
            const seq_len = std.unicode.utf8ByteSequenceLength(c) catch 0;
            var code_point: u21 = 0xFFFD;
            var consumed: usize = 1;
            if (seq_len > 0 and i + seq_len <= s.len) {
                if (std.unicode.utf8Decode(s[i..][0..seq_len])) |cp| {
                    code_point = cp;
                    consumed = seq_len;
                } else |_| {}
            }
            var buf: [12]u8 = undefined;
            if (code_point < 0x10000) {
                const n = (std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{code_point}) catch unreachable).len;
                try out.appendSlice(buf[0..n]);
            } else {
                const v = code_point - 0x10000;
                const hi: u16 = @intCast(0xD800 + (v >> 10));
                const lo: u16 = @intCast(0xDC00 + (v & 0x3FF));
                const n = (std.fmt.bufPrint(&buf, "\\u{x:0>4}\\u{x:0>4}", .{ hi, lo }) catch unreachable).len;
                try out.appendSlice(buf[0..n]);
            }
            i += consumed - 1;
            chunk_start = i + 1;
        }
    }
    try out.appendSlice(s[chunk_start..]);
    try out.append('"');
}

/// Write a parsed value back out. Used where the protocol carries a document
/// through untouched — a fence action, a snapshot message — and for tests.
pub fn write_value(out: *std.ArrayList(u8), v: Value) !void {
    switch (v) {
        .null => try out.appendSlice("null"),
        .bool => |b| try out.appendSlice(if (b) "true" else "false"),
        .int => |i| try out.writer().print("{d}", .{i}),
        .float => |f| try write_float(out, f),
        .string => |s| try write_string(out, s),
        .array => |items| {
            try out.append('[');
            for (items, 0..) |item, i| {
                if (i > 0) try out.append(',');
                try write_value(out, item);
            }
            try out.append(']');
        },
        .object => |o| {
            try out.append('{');
            for (o.entries, 0..) |e, i| {
                if (i > 0) try out.append(',');
                try write_string(out, e.key);
                try out.append(':');
                try write_value(out, e.value);
            }
            try out.append('}');
        },
    }
}

fn write_float(out: *std.ArrayList(u8), f: f64) !void {
    // A float that lands on an integer is written without a fractional part,
    // the way every other JSON encoder does it.
    if (@floor(f) == f and @abs(f) < 1e15) {
        try out.writer().print("{d}", .{@as(i64, @intFromFloat(f))});
    } else {
        try out.writer().print("{d}", .{f});
    }
}

/// A streaming object/array writer.
///
/// The responses this server sends are built field by field rather than from a
/// tree: there is no intermediate document to allocate, and the field order in
/// the code is the field order on the wire.
pub const Writer = struct {
    out: *std.ArrayList(u8),
    /// One bit per open level: has this level written a member yet?
    comma: [max_depth]bool = [_]bool{false} ** max_depth,
    depth: u32 = 0,

    pub fn init(out: *std.ArrayList(u8)) Writer {
        return .{ .out = out };
    }

    fn separate(self: *Writer) !void {
        if (self.depth > 0) {
            if (self.comma[self.depth - 1]) try self.out.append(',');
            self.comma[self.depth - 1] = true;
        }
    }

    fn push(self: *Writer) void {
        assert(self.depth < max_depth);
        self.comma[self.depth] = false;
        self.depth += 1;
    }

    fn pop(self: *Writer) void {
        assert(self.depth > 0);
        self.depth -= 1;
    }

    pub fn object_begin(self: *Writer) !void {
        try self.separate();
        try self.out.append('{');
        self.push();
    }

    pub fn object_end(self: *Writer) !void {
        try self.out.append('}');
        self.pop();
    }

    pub fn array_begin(self: *Writer) !void {
        try self.separate();
        try self.out.append('[');
        self.push();
    }

    pub fn array_end(self: *Writer) !void {
        try self.out.append(']');
        self.pop();
    }

    /// Name the next member. Must be followed by exactly one value.
    pub fn key(self: *Writer, name: []const u8) !void {
        try self.separate();
        try write_string(self.out, name);
        try self.out.append(':');
        // The value that follows is this member's, not a sibling, so it must
        // not emit a comma of its own.
        self.comma[self.depth - 1] = false;
        // ...but the member itself has been written, so the next key must.
        // Tracked by re-arming in `value_written`.
    }

    fn value_written(self: *Writer) void {
        if (self.depth > 0) self.comma[self.depth - 1] = true;
    }

    pub fn string(self: *Writer, s: []const u8) !void {
        try self.separate();
        try write_string(self.out, s);
        self.value_written();
    }

    pub fn int(self: *Writer, i: i64) !void {
        try self.separate();
        try self.out.writer().print("{d}", .{i});
        self.value_written();
    }

    pub fn boolean(self: *Writer, b: bool) !void {
        try self.separate();
        try self.out.appendSlice(if (b) "true" else "false");
        self.value_written();
    }

    pub fn null_(self: *Writer) !void {
        try self.separate();
        try self.out.appendSlice("null");
        self.value_written();
    }

    /// Splice an already-parsed document in.
    pub fn value(self: *Writer, v: Value) !void {
        try self.separate();
        try write_value(self.out, v);
        self.value_written();
    }

    /// Splice already-encoded JSON bytes in. The caller owns their validity.
    pub fn raw(self: *Writer, bytes: []const u8) !void {
        try self.separate();
        try self.out.appendSlice(bytes);
        self.value_written();
    }

    // Convenience: a named member in one call.
    pub fn field_string(self: *Writer, name: []const u8, s: []const u8) !void {
        try self.key(name);
        try self.string(s);
    }

    pub fn field_int(self: *Writer, name: []const u8, i: i64) !void {
        try self.key(name);
        try self.int(i);
    }

    pub fn field_bool(self: *Writer, name: []const u8, b: bool) !void {
        try self.key(name);
        try self.boolean(b);
    }

    pub fn field_value(self: *Writer, name: []const u8, v: Value) !void {
        try self.key(name);
        try self.value(v);
    }

    pub fn field_raw(self: *Writer, name: []const u8, bytes: []const u8) !void {
        try self.key(name);
        try self.raw(bytes);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn round_trip(source: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const v = try parse(arena.allocator(), source);
    var out = std.ArrayList(u8).init(testing.allocator);
    errdefer out.deinit();
    try write_value(&out, v);
    return out.toOwnedSlice();
}

test "round trips a protocol envelope" {
    const source =
        \\{"kind":"promise.create","head":{"corrId":"c1","version":"2026-04-01"},"data":{"id":"p1","timeoutAt":9000000000000,"param":{},"tags":{"resonate:target":"poll://any@g"}}}
    ;
    const out = try round_trip(source);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings(source, out);
}

test "preserves large integers exactly" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const v = try parse(arena.allocator(), "{\"t\":9007199254740993}");
    try testing.expectEqual(@as(i64, 9007199254740993), v.get_i64("t").?);
}

test "unescapes strings including surrogate pairs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const v = try parse(arena.allocator(), "\"a\\nb\\u0041\\ud83d\\ude00\"");
    try testing.expectEqualStrings("a\nbA\u{1F600}", v.as_string().?);
}

test "escapes control characters on the way out" {
    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    try write_string(&out, "a\x00b\"c\\d\ne");
    try testing.expectEqualStrings("\"a\\u0000b\\\"c\\\\d\\ne\"", out.items);
}

test "rejects what JSON does not admit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_][]const u8{
        "{",              "}",        "[1,]",  "{\"a\":}",
        "01",             "-",        "1.",    "1e",
        "\"unterminated", "tru",      "{\"a\"}", "nul",
        "{\"a\":1}trail", "\"\x01\"",
    }) |bad| {
        try testing.expectError(error.Unexpected, blk: {
            break :blk parse(arena.allocator(), bad) catch |e| switch (e) {
                // Any refusal is the right refusal; the test only asserts that
                // the parser does not accept.
                else => error.Unexpected,
            };
        });
    }
}

test "duplicate keys take the last value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const v = try parse(arena.allocator(), "{\"a\":1,\"a\":2}");
    try testing.expectEqual(@as(i64, 2), v.get_i64("a").?);
    try testing.expectEqual(@as(usize, 1), v.as_object().?.len());
}

test "Writer emits members in the order they are named" {
    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    var w = Writer.init(&out);
    try w.object_begin();
    try w.field_string("kind", "promise.get");
    try w.key("head");
    try w.object_begin();
    try w.field_string("corrId", "c1");
    try w.field_int("status", 200);
    try w.object_end();
    try w.key("data");
    try w.array_begin();
    try w.int(1);
    try w.int(2);
    try w.object_begin();
    try w.field_bool("ok", true);
    try w.object_end();
    try w.array_end();
    try w.object_end();
    try testing.expectEqualStrings(
        "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"c1\",\"status\":200},\"data\":[1,2,{\"ok\":true}]}",
        out.items,
    );
}

test "ascii only escaping is canonical and total" {
    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    try write_string_ascii(&out, "café \u{1F600} x", true);
    try testing.expectEqualStrings("\"caf\\u00e9 \\ud83d\\ude00 x\"", out.items);

    // Ill-formed UTF-8 still produces a string, so the encoder is total.
    out.clearRetainingCapacity();
    try write_string_ascii(&out, "a\xffb", true);
    try testing.expectEqualStrings("\"a\\ufffdb\"", out.items);

    // Without the flag, UTF-8 passes through untouched.
    out.clearRetainingCapacity();
    try write_string_ascii(&out, "café", false);
    try testing.expectEqualStrings("\"café\"", out.items);
}
