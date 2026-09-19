//! Cron expressions: parsing, and the next instant one fires.
//!
//! The shape follows the `cron` crate the reference implementation uses, and
//! the differences from POSIX crontab are deliberate because they are that
//! crate's:
//!
//! * six or seven fields — `sec min hour dom month dow [year]`. A five-field
//!   expression is promoted by prepending a `0` seconds field, so the familiar
//!   `* * * * *` means "every minute, on the minute".
//! * day-of-week is 1-7 with **Sunday = 1** and Saturday = 7, not 0-6. A bare
//!   `0` is accepted as Sunday as well, because that is how most of the world
//!   writes it and refusing it buys nothing.
//! * when both day-of-month and day-of-week are restricted, a date must satisfy
//!   **both**. POSIX takes the union; this takes the intersection.
//!
//! Everything is UTC. A schedule is a server-side deadline, and a server that
//! moved its schedules when a zone's rules changed would be a worse server.

const std = @import("std");
const stdx = @import("stdx.zig");
const assert = stdx.assert;

pub const Error = error{InvalidCron};

/// The years a schedule may reach. Past this the answer is "never", which the
/// caller turns into a retry — the same shape the reference implementation has.
const year_min: i64 = 1970;
const year_max: i64 = 2200;

/// One field of an expression, as a bitset over its unit's range.
///
/// A bitset rather than a list of ranges: matching is the hot operation — the
/// search below tests candidate dates one at a time — and a 64-bit test is the
/// cheapest form of it.
const Field = struct {
    bits: u64 = 0,
    /// True when the field was written `*`. Needed on its own: whether
    /// day-of-month and day-of-week are *restricted* decides how they combine,
    /// and `*` expands to every bit, which no longer says how it was written.
    star: bool = false,

    fn contains(self: Field, v: u32) bool {
        assert(v < 64);
        return (self.bits & (@as(u64, 1) << @intCast(v))) != 0;
    }

    fn set(self: *Field, v: u32) void {
        assert(v < 64);
        self.bits |= @as(u64, 1) << @intCast(v);
    }
};

pub const Schedule = struct {
    seconds: Field,
    minutes: Field,
    hours: Field,
    days_of_month: Field,
    months: Field,
    days_of_week: Field,
    years: Field2,

    /// Years do not fit in 64 bits, so they get their own two-word set over
    /// `year_min..year_max`.
    const Field2 = struct {
        bits: [4]u64 = [_]u64{0} ** 4,
        star: bool = false,

        fn contains(self: Field2, year: i64) bool {
            if (year < year_min or year > year_max) return false;
            const i: u64 = @intCast(year - year_min);
            if (i >= 256) return self.star;
            return (self.bits[i / 64] & (@as(u64, 1) << @intCast(i % 64))) != 0;
        }

        fn set(self: *Field2, year: i64) void {
            if (year < year_min or year > year_max) return;
            const i: u64 = @intCast(year - year_min);
            if (i >= 256) return;
            self.bits[i / 64] |= @as(u64, 1) << @intCast(i % 64);
        }
    };
};

const month_names = [_][]const u8{
    "jan", "feb", "mar", "apr", "may", "jun",
    "jul", "aug", "sep", "oct", "nov", "dec",
};

/// Sunday first, because day-of-week here is 1-based with Sunday = 1.
const day_names = [_][]const u8{ "sun", "mon", "tue", "wed", "thu", "fri", "sat" };

const Unit = enum {
    second,
    minute,
    hour,
    day_of_month,
    month,
    day_of_week,
    year,

    fn min(self: Unit) u32 {
        return switch (self) {
            .second, .minute, .hour => 0,
            .day_of_month, .month => 1,
            .day_of_week => 1,
            .year => @intCast(year_min),
        };
    }

    fn max(self: Unit) u32 {
        return switch (self) {
            .second, .minute => 59,
            .hour => 23,
            .day_of_month => 31,
            .month => 12,
            .day_of_week => 7,
            .year => @intCast(year_max),
        };
    }
};

/// Parse one expression. Five fields are promoted with a leading `0`.
pub fn parse(expression: []const u8) Error!Schedule {
    var fields: [7][]const u8 = undefined;
    var count: usize = 0;
    var it = std.mem.tokenizeAny(u8, expression, " \t");
    while (it.next()) |tok| {
        if (count == 7) return error.InvalidCron;
        fields[count] = tok;
        count += 1;
    }
    var offset: usize = 0;
    if (count == 5) {
        // The familiar form: minute-resolution, so seconds are pinned to zero.
        offset = 1;
        var shifted: [7][]const u8 = undefined;
        shifted[0] = "0";
        for (fields[0..5], 0..) |f, i| shifted[i + 1] = f;
        fields = shifted;
        count = 6;
        offset = 0;
    } else if (count != 6 and count != 7) {
        return error.InvalidCron;
    }

    var s: Schedule = .{
        .seconds = try parse_field(fields[0], .second),
        .minutes = try parse_field(fields[1], .minute),
        .hours = try parse_field(fields[2], .hour),
        .days_of_month = try parse_field(fields[3], .day_of_month),
        .months = try parse_field(fields[4], .month),
        .days_of_week = try parse_field(fields[5], .day_of_week),
        .years = .{ .star = true },
    };
    if (count == 7) {
        s.years = try parse_year_field(fields[6]);
    } else {
        var y: Schedule.Field2 = .{ .star = true };
        var year = year_min;
        while (year <= year_max) : (year += 1) y.set(year);
        s.years = y;
    }
    return s;
}

pub fn is_valid(expression: []const u8) bool {
    _ = parse(expression) catch return false;
    return true;
}

fn parse_field(text: []const u8, unit: Unit) Error!Field {
    var field: Field = .{};
    if (text.len == 0) return error.InvalidCron;
    if (std.mem.eql(u8, text, "*") or std.mem.eql(u8, text, "?")) {
        field.star = true;
        var v = unit.min();
        while (v <= unit.max()) : (v += 1) field.set(v);
        return field;
    }
    var it = std.mem.splitScalar(u8, text, ',');
    var any = false;
    while (it.next()) |part| {
        if (part.len == 0) return error.InvalidCron;
        try parse_term(part, unit, &field);
        any = true;
    }
    if (!any) return error.InvalidCron;
    return field;
}

/// One comma-separated term: `*`, `*/n`, `a`, `a-b`, `a-b/n`, `a/n`.
fn parse_term(term: []const u8, unit: Unit, field: *Field) Error!void {
    var step: u32 = 1;
    var range_text = term;
    if (std.mem.indexOfScalar(u8, term, '/')) |slash| {
        range_text = term[0..slash];
        const step_text = term[slash + 1 ..];
        step = std.fmt.parseInt(u32, step_text, 10) catch return error.InvalidCron;
        if (step == 0) return error.InvalidCron;
    }

    var lo: u32 = undefined;
    var hi: u32 = undefined;
    if (std.mem.eql(u8, range_text, "*") or std.mem.eql(u8, range_text, "?")) {
        lo = unit.min();
        hi = unit.max();
    } else if (std.mem.indexOfScalar(u8, range_text, '-')) |dash| {
        lo = try parse_value(range_text[0..dash], unit);
        hi = try parse_value(range_text[dash + 1 ..], unit);
        if (lo > hi) return error.InvalidCron;
    } else {
        lo = try parse_value(range_text, unit);
        // `a/n` counts up from `a` to the end of the unit, which is what every
        // cron implementation does with it even though POSIX does not say so.
        hi = if (step == 1) lo else unit.max();
    }
    var v = lo;
    while (v <= hi) : (v += step) field.set(v);
}

fn parse_value(text: []const u8, unit: Unit) Error!u32 {
    if (text.len == 0) return error.InvalidCron;
    if (std.ascii.isAlphabetic(text[0])) {
        const names: []const []const u8 = switch (unit) {
            .month => &month_names,
            .day_of_week => &day_names,
            else => return error.InvalidCron,
        };
        for (names, 0..) |name, i| {
            if (text.len >= 3 and std.ascii.eqlIgnoreCase(text[0..3], name)) {
                // Month names are 1-based; day names are 1-based with Sunday
                // first, which is exactly the index plus one in both cases.
                if (text.len != 3) return error.InvalidCron;
                return @intCast(i + 1);
            }
        }
        return error.InvalidCron;
    }
    const v = std.fmt.parseInt(u32, text, 10) catch return error.InvalidCron;
    // Sunday is 1 here, so a written 0 means 1.
    if (unit == .day_of_week and v == 0) return 1;
    if (v < unit.min() or v > unit.max()) return error.InvalidCron;
    return v;
}

fn parse_year_field(text: []const u8) Error!Schedule.Field2 {
    var f: Schedule.Field2 = .{};
    if (std.mem.eql(u8, text, "*") or std.mem.eql(u8, text, "?")) {
        f.star = true;
        var y = year_min;
        while (y <= year_max) : (y += 1) f.set(y);
        return f;
    }
    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |part| {
        if (part.len == 0) return error.InvalidCron;
        var step: i64 = 1;
        var range_text = part;
        if (std.mem.indexOfScalar(u8, part, '/')) |slash| {
            range_text = part[0..slash];
            step = std.fmt.parseInt(i64, part[slash + 1 ..], 10) catch return error.InvalidCron;
            if (step <= 0) return error.InvalidCron;
        }
        var lo: i64 = undefined;
        var hi: i64 = undefined;
        if (std.mem.eql(u8, range_text, "*")) {
            lo = year_min;
            hi = year_max;
        } else if (std.mem.indexOfScalar(u8, range_text, '-')) |dash| {
            lo = std.fmt.parseInt(i64, range_text[0..dash], 10) catch return error.InvalidCron;
            hi = std.fmt.parseInt(i64, range_text[dash + 1 ..], 10) catch return error.InvalidCron;
            if (lo > hi) return error.InvalidCron;
        } else {
            lo = std.fmt.parseInt(i64, range_text, 10) catch return error.InvalidCron;
            hi = if (step == 1) lo else year_max;
        }
        var y = lo;
        while (y <= hi) : (y += step) f.set(y);
    }
    return f;
}

// ── Calendar ──────────────────────────────────────────────────────────────────

pub const DateTime = struct {
    year: i64,
    month: u32, // 1-12
    day: u32, // 1-31
    hour: u32,
    minute: u32,
    second: u32,
    /// 1 = Sunday, matching the day-of-week field.
    weekday: u32,
};

/// Days since the Unix epoch for a civil date. Howard Hinnant's algorithm,
/// which is exact for the whole proleptic Gregorian calendar.
pub fn days_from_civil(y_in: i64, m: u32, d: u32) i64 {
    var y = y_in;
    y -= @intFromBool(m <= 2);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400; // [0, 399]
    const mp: i64 = @intCast((m + 9) % 12); // March = 0
    const doy = @divTrunc(153 * mp + 2, 5) + @as(i64, @intCast(d)) - 1; // [0, 365]
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy; // [0, 146096]
    return era * 146097 + doe - 719468;
}

pub fn civil_from_days(z_in: i64) struct { year: i64, month: u32, day: u32 } {
    const z = z_in + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097; // [0, 146096]
    const yoe = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153); // [0, 11], March = 0
    const d = doy - @divTrunc(153 * mp + 2, 5) + 1; // [1, 31]
    const m = if (mp < 10) mp + 3 else mp - 9; // [1, 12]
    return .{
        .year = y + @intFromBool(m <= 2),
        .month = @intCast(m),
        .day = @intCast(d),
    };
}

pub fn from_unix_seconds(secs: i64) DateTime {
    const days = @divFloor(secs, 86400);
    var rem = secs - days * 86400;
    assert(rem >= 0 and rem < 86400);
    const civil = civil_from_days(days);
    const hour: u32 = @intCast(@divTrunc(rem, 3600));
    rem -= @as(i64, hour) * 3600;
    const minute: u32 = @intCast(@divTrunc(rem, 60));
    const second: u32 = @intCast(rem - @as(i64, minute) * 60);
    // 1970-01-01 was a Thursday. Sunday = 1, so Thursday = 5.
    const dow_from_epoch = @mod(days + 4, 7); // 0 = Sunday
    return .{
        .year = civil.year,
        .month = civil.month,
        .day = civil.day,
        .hour = hour,
        .minute = minute,
        .second = second,
        .weekday = @intCast(dow_from_epoch + 1),
    };
}

pub fn to_unix_seconds(dt: DateTime) i64 {
    return days_from_civil(dt.year, dt.month, dt.day) * 86400 +
        @as(i64, dt.hour) * 3600 + @as(i64, dt.minute) * 60 + @as(i64, dt.second);
}

fn days_in_month(year: i64, month: u32) u32 {
    const lengths = [_]u32{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (month == 2 and is_leap(year)) return 29;
    return lengths[month - 1];
}

fn is_leap(year: i64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

// ── Next occurrence ───────────────────────────────────────────────────────────

/// The next instant strictly after `after_seconds` at which `schedule` fires,
/// or null if there is none inside the supported year range.
pub fn next_after(schedule: Schedule, after_seconds: i64) ?i64 {
    var t = after_seconds + 1;
    var dt = from_unix_seconds(t);
    if (dt.year < year_min) {
        dt = .{ .year = year_min, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
        t = to_unix_seconds(dt);
        dt = from_unix_seconds(t);
    }

    // Walk the calendar coarse to fine, snapping each field forward to the next
    // value the schedule admits and resetting everything below it. Bounded by
    // the year range, so this terminates whatever the expression says.
    var guard: u32 = 0;
    while (dt.year <= year_max) {
        guard += 1;
        // A pathological expression (`30 2 30 2 *` — half past two on the 30th
        // of February) walks every day in the range without ever matching. The
        // bound is generous enough that no real expression reaches it and tight
        // enough that nothing hangs.
        if (guard > 4_000_000) return null;

        if (!schedule.years.contains(dt.year)) {
            dt = .{ .year = dt.year + 1, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
            continue;
        }
        if (!schedule.months.contains(dt.month)) {
            if (dt.month == 12) {
                dt = .{ .year = dt.year + 1, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
            } else {
                dt = .{ .year = dt.year, .month = dt.month + 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
            }
            continue;
        }
        if (!day_matches(schedule, dt)) {
            if (dt.day >= days_in_month(dt.year, dt.month)) {
                if (dt.month == 12) {
                    dt = .{ .year = dt.year + 1, .month = 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
                } else {
                    dt = .{ .year = dt.year, .month = dt.month + 1, .day = 1, .hour = 0, .minute = 0, .second = 0, .weekday = 0 };
                }
            } else {
                const next_day = to_unix_seconds(.{
                    .year = dt.year,
                    .month = dt.month,
                    .day = dt.day + 1,
                    .hour = 0,
                    .minute = 0,
                    .second = 0,
                    .weekday = 0,
                });
                dt = from_unix_seconds(next_day);
            }
            continue;
        }
        if (!schedule.hours.contains(dt.hour)) {
            if (dt.hour == 23) {
                const next_day = to_unix_seconds(.{
                    .year = dt.year,
                    .month = dt.month,
                    .day = dt.day,
                    .hour = 0,
                    .minute = 0,
                    .second = 0,
                    .weekday = 0,
                }) + 86400;
                dt = from_unix_seconds(next_day);
            } else {
                dt.hour += 1;
                dt.minute = 0;
                dt.second = 0;
            }
            continue;
        }
        if (!schedule.minutes.contains(dt.minute)) {
            if (dt.minute == 59) {
                dt.hour += 1;
                dt.minute = 0;
                dt.second = 0;
                if (dt.hour == 24) {
                    const next_day = to_unix_seconds(.{
                        .year = dt.year,
                        .month = dt.month,
                        .day = dt.day,
                        .hour = 0,
                        .minute = 0,
                        .second = 0,
                        .weekday = 0,
                    }) + 86400;
                    dt = from_unix_seconds(next_day);
                }
            } else {
                dt.minute += 1;
                dt.second = 0;
            }
            continue;
        }
        if (!schedule.seconds.contains(dt.second)) {
            if (dt.second == 59) {
                dt.minute += 1;
                dt.second = 0;
                if (dt.minute == 60) {
                    dt.minute = 0;
                    dt.hour += 1;
                    if (dt.hour == 24) {
                        const next_day = to_unix_seconds(.{
                            .year = dt.year,
                            .month = dt.month,
                            .day = dt.day,
                            .hour = 0,
                            .minute = 0,
                            .second = 0,
                            .weekday = 0,
                        }) + 86400;
                        dt = from_unix_seconds(next_day);
                    }
                }
            } else {
                dt.second += 1;
            }
            continue;
        }
        return to_unix_seconds(dt);
    }
    return null;
}

fn day_matches(schedule: Schedule, dt: DateTime) bool {
    if (dt.day > days_in_month(dt.year, dt.month)) return false;
    // Intersection, not union: see the module comment.
    return schedule.days_of_month.contains(dt.day) and schedule.days_of_week.contains(dt.weekday);
}

/// The next firing, in Unix milliseconds, strictly after `after_ms`.
///
/// Sub-second precision in `after_ms` is dropped before the search, and the
/// answer is always a whole second — a schedule has second resolution, and
/// rounding it here rather than at each call site is what keeps the timeout
/// table and the `nextRunAt` a caller reads in agreement.
///
/// A schedule with no next firing retries in a minute rather than vanishing.
pub fn next_after_ms(schedule: Schedule, after_ms: i64) i64 {
    const after_seconds = @divFloor(after_ms, 1000);
    const next = next_after(schedule, after_seconds) orelse return after_ms + 60_000;
    return next * 1000;
}

/// Parse and compute in one call, the way a caller holding only the text wants
/// it. An unparseable expression retries in a minute.
pub fn compute_next(expression: []const u8, after_ms: i64) i64 {
    const schedule = parse(expression) catch return after_ms + 60_000;
    return next_after_ms(schedule, after_ms);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "the calendar round trips" {
    const cases = [_]struct { secs: i64, y: i64, mo: u32, d: u32, h: u32, mi: u32, s: u32, wd: u32 }{
        // 1970-01-01T00:00:00Z was a Thursday.
        .{ .secs = 0, .y = 1970, .mo = 1, .d = 1, .h = 0, .mi = 0, .s = 0, .wd = 5 },
        // 2000-02-29T12:34:56Z was a Tuesday.
        .{ .secs = 951827696, .y = 2000, .mo = 2, .d = 29, .h = 12, .mi = 34, .s = 56, .wd = 3 },
        // 2026-09-19T00:00:00Z is a Saturday, so weekday 7.
        .{ .secs = 1789776000, .y = 2026, .mo = 9, .d = 19, .h = 0, .mi = 0, .s = 0, .wd = 7 },
        // Before the epoch.
        .{ .secs = -86400, .y = 1969, .mo = 12, .d = 31, .h = 0, .mi = 0, .s = 0, .wd = 4 },
    };
    for (cases) |c| {
        const dt = from_unix_seconds(c.secs);
        try testing.expectEqual(c.y, dt.year);
        try testing.expectEqual(c.mo, dt.month);
        try testing.expectEqual(c.d, dt.day);
        try testing.expectEqual(c.h, dt.hour);
        try testing.expectEqual(c.mi, dt.minute);
        try testing.expectEqual(c.s, dt.second);
        try testing.expectEqual(c.wd, dt.weekday);
        try testing.expectEqual(c.secs, to_unix_seconds(dt));
    }
}

test "every-minute is the next minute boundary, strictly after" {
    // The one expression the differential harness uses.
    const s = try parse("* * * * *");
    try testing.expectEqual(@as(i64, 60_000), next_after_ms(s, 0));
    try testing.expectEqual(@as(i64, 60_000), next_after_ms(s, 1));
    try testing.expectEqual(@as(i64, 60_000), next_after_ms(s, 59_999));
    try testing.expectEqual(@as(i64, 120_000), next_after_ms(s, 60_000));
    // The harness's epoch anchor.
    try testing.expectEqual(@as(i64, 1_000_020_000), next_after_ms(s, 1_000_000_000));
}

test "a five field expression is promoted with a zero seconds field" {
    const five = try parse("30 4 * * *");
    const six = try parse("0 30 4 * * *");
    try testing.expectEqual(next_after_ms(six, 0), next_after_ms(five, 0));
    // 04:30 on the first day.
    try testing.expectEqual(@as(i64, (4 * 3600 + 30 * 60) * 1000), next_after_ms(five, 0));
}

test "ranges, lists and steps" {
    const s = try parse("0,30 * * * * *");
    try testing.expectEqual(@as(i64, 30_000), next_after_ms(s, 0));
    try testing.expectEqual(@as(i64, 60_000), next_after_ms(s, 30_000));

    const q = try parse("*/15 * * * * *");
    try testing.expectEqual(@as(i64, 15_000), next_after_ms(q, 1_000));
    try testing.expectEqual(@as(i64, 45_000), next_after_ms(q, 30_000));

    const h = try parse("0 0 9-17 * * *");
    // 1970-01-01T00:00:00 -> 09:00.
    try testing.expectEqual(@as(i64, 9 * 3600 * 1000), next_after_ms(h, 0));
    // 17:00 is the last, so after it comes 09:00 the next day.
    try testing.expectEqual(
        @as(i64, (86400 + 9 * 3600) * 1000),
        next_after_ms(h, 17 * 3600 * 1000),
    );
}

test "day of week is one based with Sunday first" {
    // 1970-01-01 was a Thursday, so the first Sunday after the epoch is the 4th.
    for ([_][]const u8{ "0 0 0 * * 1", "0 0 0 * * SUN", "0 0 0 * * 0" }) |expr| {
        const sun = try parse(expr);
        try testing.expectEqual(@as(i64, 3 * 86400 * 1000), next_after_ms(sun, 0));
    }
    // 7 is Saturday, not Sunday: the 3rd.
    const sat = try parse("0 0 0 * * 7");
    try testing.expectEqual(@as(i64, 2 * 86400 * 1000), next_after_ms(sat, 0));
    try testing.expectEqual(
        next_after_ms(sat, 0),
        next_after_ms(try parse("0 0 0 * * SAT"), 0),
    );
    // Monday is 2: the 5th.
    const mon = try parse("0 0 0 * * 2");
    try testing.expectEqual(@as(i64, 4 * 86400 * 1000), next_after_ms(mon, 0));
}

test "month names and a specific date" {
    const s = try parse("0 0 0 25 DEC *");
    const christmas_1970 = days_from_civil(1970, 12, 25) * 86400 * 1000;
    try testing.expectEqual(christmas_1970, next_after_ms(s, 0));
}

test "an impossible date yields the retry fallback" {
    // Half past two on the 30th of February, which never happens.
    const s = try parse("0 30 2 30 2 *");
    try testing.expectEqual(@as(i64, 60_000), next_after_ms(s, 0));
}

test "invalid expressions are refused" {
    for ([_][]const u8{
        "",         "* * *",       "* * * * * * * *", "60 * * * * *",
        "* 60 * * * *", "* * 24 * * *", "* * * 32 * *",   "* * * * 13 *",
        "* * * * * 8", "a * * * *",  "*/0 * * * *",     "5-1 * * * *",
        "* * * * JAN", "1,, * * * *",
    }) |bad| {
        try testing.expect(!is_valid(bad));
    }
    for ([_][]const u8{
        "* * * * *",   "0 * * * * *",          "0 0 0 1 1 * 2030",
        "*/5 * * * *", "0 0 9-17 * * MON-FRI", "0 0 0 * * *",
        "0 0 0 * * 0", "30 4 1,15 * 2-6",      "0 0 12 ? * ?",
    }) |good| {
        try testing.expect(is_valid(good));
    }
}

test "compute_next tolerates a broken expression" {
    try testing.expectEqual(@as(i64, 1_060_000), compute_next("not a cron", 1_000_000));
}
