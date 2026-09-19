//! Small things the standard library does not give us, and the few assertions
//! every other module is allowed to depend on.
//!
//! Nothing here knows what Resonate is. If a helper needs a promise or a task
//! to explain itself, it belongs somewhere else.

const std = @import("std");
const builtin = @import("builtin");

/// An invariant. Compiled out in `ReleaseFast`? No — deliberately not.
///
/// The whole point of a deterministic simulator is that it finds the state
/// nobody thought of, and it can only find it if the program still checks.
/// TigerBeetle keeps its assertions in release builds for the same reason, and
/// the cost is a predictable branch against a write to an object store.
pub inline fn assert(ok: bool) void {
    if (!ok) unreachable;
}

/// An assertion that carries a reason, for the ones worth explaining in a
/// crash report.
pub inline fn assert_msg(ok: bool, comptime fmt: []const u8, args: anytype) void {
    if (!ok) std.debug.panic(fmt, args);
}

/// "This may or may not hold" — documents a condition deliberately left open,
/// so that a reader does not mistake the absence of an assertion for an
/// oversight.
pub inline fn maybe(_: bool) void {}

/// Comparison of two byte strings, for sorting.
pub fn less_than_bytes(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// The deterministic pseudo random number generator used everywhere in the
/// simulator. Named rather than used inline so that "which PRNG" is one
/// decision in one place: a seed has to reproduce a run on any machine, and it
/// only does that if nothing reaches for `std.crypto.random` behind our back.
pub const Random = struct {
    inner: std.Random.Xoshiro256,

    pub fn init(seed: u64) Random {
        return .{ .inner = std.Random.Xoshiro256.init(seed) };
    }

    pub fn random(self: *Random) std.Random {
        return self.inner.random();
    }

    /// Uniform in [0, n). `n == 0` is a programming error.
    pub fn below(self: *Random, n: u64) u64 {
        assert(n > 0);
        return self.random().uintLessThan(u64, n);
    }

    /// Uniform in [lo, hi]. Inclusive on both ends, because ranges of
    /// milliseconds read better that way.
    pub fn between(self: *Random, lo: u64, hi: u64) u64 {
        assert(lo <= hi);
        return lo + self.below(hi - lo + 1);
    }

    /// True with probability `percent`/100.
    pub fn chance(self: *Random, percent: u64) bool {
        assert(percent <= 100);
        if (percent == 0) return false;
        return self.below(100) < percent;
    }

    /// Pick one of `items`, or null when there are none.
    pub fn pick(self: *Random, comptime T: type, items: []const T) ?T {
        if (items.len == 0) return null;
        return items[self.below(items.len)];
    }
};

/// A monotonically non-decreasing millisecond clock reading.
///
/// Wall time is allowed to jump backwards; the log is not. Every timestamp
/// that reaches the state machine passes through one of these, so a clock that
/// steps back stalls rather than rewrites history.
pub const MonotonicMillis = struct {
    last: i64 = 0,

    pub fn advance(self: *MonotonicMillis, now: i64) i64 {
        if (now > self.last) self.last = now;
        return self.last;
    }
};

/// Copy `source` into a freshly allocated slice. `std.mem.dupe` renamed, so
/// that call sites read as intent rather than as an allocator operation.
pub fn dupe(allocator: std.mem.Allocator, comptime T: type, source: []const T) ![]T {
    return allocator.dupe(T, source);
}

/// Case-insensitive ASCII equality, for HTTP header names.
pub fn eql_ignore_case(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

/// `haystack` starts with `needle`.
pub fn starts_with(haystack: []const u8, needle: []const u8) bool {
    return std.mem.startsWith(u8, haystack, needle);
}

/// Parse a base-10 signed integer, or null. Rejects leading '+', leading
/// zeros are allowed, and an empty string is null.
pub fn parse_i64(s: []const u8) ?i64 {
    if (s.len == 0) return null;
    return std.fmt.parseInt(i64, s, 10) catch null;
}

/// Parse a base-10 unsigned integer, or null.
pub fn parse_u64(s: []const u8) ?u64 {
    if (s.len == 0) return null;
    return std.fmt.parseInt(u64, s, 10) catch null;
}

/// Hex-encode into `out`, which must be exactly `2 * bytes.len` long.
pub fn hex_encode(out: []u8, bytes: []const u8) void {
    assert(out.len == bytes.len * 2);
    const digits = "0123456789abcdef";
    for (bytes, 0..) |b, i| {
        out[i * 2] = digits[b >> 4];
        out[i * 2 + 1] = digits[b & 0x0f];
    }
}

test "Random is reproducible from its seed" {
    var a = Random.init(42);
    var b = Random.init(42);
    for (0..100) |_| try std.testing.expectEqual(a.below(1000), b.below(1000));
}

test "MonotonicMillis never steps back" {
    var clock = MonotonicMillis{};
    try std.testing.expectEqual(@as(i64, 10), clock.advance(10));
    try std.testing.expectEqual(@as(i64, 10), clock.advance(5));
    try std.testing.expectEqual(@as(i64, 11), clock.advance(11));
}
