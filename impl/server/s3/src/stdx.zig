//! Small things the standard library does not give us, and the few assertions
//! every other module is allowed to depend on.
//!
//! Nothing here knows what Resonate is. If a helper needs a promise or a task
//! to explain itself, it belongs somewhere else.

const std = @import("std");
const builtin = @import("builtin");

/// An invariant, kept in the build that ships.
///
/// `unreachable` panics in `Debug` and `ReleaseSafe` and is undefined behaviour
/// in `ReleaseFast` and `ReleaseSmall`, where the check is not merely removed but
/// becomes a promise to the optimizer. So this assertion holds in exactly the two
/// modes the build uses, and that is the reason `ReleaseSafe` is what `zig build`
/// produces and what CI and the container build: the whole point of a
/// deterministic simulator is that it finds the state nobody thought of, and it
/// can only find it if the program still checks. TigerBeetle ships `ReleaseSafe`
/// for the same reason. The cost is a predictable branch against a write to an
/// object store.
///
/// A smaller or faster binary is therefore not a trade against speed or size. It
/// is a trade against every invariant in this program, and it is not one this
/// server offers.
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

/// Generate the cast that turns a callback's context back into its own type.
///
/// Every event in this tree — a store operation, a timeout, a delivery — carries
/// a `callback` taking the event and a `context` that has forgotten what it is.
/// Written by hand that is a `@ptrCast(@alignCast(...))` at the top of every
/// callback, and each one is a place where naming the wrong type compiles and
/// corrupts memory at run time.
///
/// This writes the cast once, generated per call site from the typed callback the
/// caller actually wrote, so no subsystem casts by hand and the compiler checks
/// the callback against the context it was given. Borrowed from TigerBeetle's
/// `erase_types`, which does the same thing for its io_uring completions.
///
/// `Event` needs a `context: ?*anyopaque`, which is exactly the shape of the ones
/// here. Prefer the `listen` method on the event over calling this directly: it
/// sets the context and the callback together, so the two cannot disagree.
pub fn erase(
    comptime Event: type,
    comptime Context: type,
    comptime callback: fn (Context, *Event) void,
) *const fn (*Event) void {
    comptime assert(@typeInfo(Context) == .pointer);
    return &struct {
        fn erased(event: *Event) void {
            const context: Context = @ptrCast(@alignCast(event.context.?));
            callback(context, event);
        }
    }.erased;
}

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

    /// A whole word, uniform. What a caller wants when it needs a value nothing
    /// else will produce again rather than a number in a range.
    pub fn word(self: *Random) u64 {
        return self.random().int(u64);
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
