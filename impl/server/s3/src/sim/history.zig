//! Reading a recorded history.
//!
//! The format is the one the Rust tree's `conctrace` example writes: newline
//! delimited JSON, one object per operation, with the real-time instants the
//! client saw. That is what makes the file checkable rather than merely
//! inspectable — without `call` and `return` there is no real-time order to
//! respect, and without that a checker can only ask whether one particular order
//! works, which refutes almost any concurrent run.
//!
//! ```json
//! {"kind":"promise.create","now":1000,"req":{...},"res":{...},"call":123,"return":456,"client":3}
//! ```
//!
//! `res` absent or null means the client never learned the result: the operation
//! may or may not have been applied, and the checker treats it accordingly. A
//! whole envelope in `req` is accepted too, since that is what a recorder that
//! logs what it sent produces.

const std = @import("std");
const stdx = @import("../stdx.zig");
const json = @import("../json.zig");
const protocol = @import("../protocol.zig");
const checker = @import("checker.zig");

pub const Error = error{
    /// A line that is not an operation.
    Malformed,
    OutOfMemory,
};

pub const Loaded = struct {
    operations: []checker.Operation,
    /// Lines that were not operations and were skipped.
    skipped: usize,
};

/// Parse a whole history. Everything it returns lives in `arena`.
pub fn parse(arena: std.mem.Allocator, text: []const u8) Error!Loaded {
    var operations = std.ArrayList(checker.Operation).init(arena);
    var skipped: usize = 0;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) continue;
        const operation = parse_line(arena, line) catch {
            skipped += 1;
            continue;
        };
        try operations.append(operation);
    }
    return .{ .operations = try operations.toOwnedSlice(), .skipped = skipped };
}

fn parse_line(arena: std.mem.Allocator, line: []const u8) Error!checker.Operation {
    const root = json.parse(arena, line) catch return error.Malformed;
    if (!root.is_object()) return error.Malformed;

    const call = root.get_i64("call") orelse return error.Malformed;
    const ret_raw = root.get_i64("return");
    const now = root.get_i64("now") orelse return error.Malformed;
    const client: u32 = blk: {
        const v = root.get_i64("client") orelse break :blk 0;
        break :blk if (v < 0) 0 else @intCast(@as(u64, @intCast(v)) & 0xffff_ffff);
    };

    const response = root.get("res");
    const answered = response != null and !response.?.is_null();

    // The request: either a whole envelope, or the `data` beside a top-level kind.
    const request = root.get("req") orelse return error.Malformed;
    const kind: []const u8 = blk: {
        if (request.get_string("kind")) |k| break :blk k;
        break :blk root.get_string("kind") orelse return error.Malformed;
    };
    const data: json.Value = blk: {
        if (request.get("kind") != null) {
            break :blk request.get("data") orelse return error.Malformed;
        }
        break :blk request;
    };

    // A fence echoes the correlation id into its answer, so replaying it needs the
    // same one the recorder used.
    const corr_id: []const u8 = blk: {
        if (answered) {
            if (response.?.get("head")) |head| {
                if (head.get_string("corrId")) |c| break :blk c;
            }
        }
        if (request.get("head")) |head| {
            if (head.get_string("corrId")) |c| break :blk c;
        }
        break :blk "history";
    };

    var data_text = std.ArrayList(u8).init(arena);
    try json.write_value(&data_text, data);
    const envelope = try std.fmt.allocPrint(
        arena,
        "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"{s}\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
        .{ kind, corr_id, protocol.protocol_version, now, data_text.items },
    );

    var status: i32 = 0;
    var observed: []const u8 = "";
    if (answered) {
        const head = response.?.get("head") orelse return error.Malformed;
        const status_value = head.get_i64("status") orelse return error.Malformed;
        status = @intCast(status_value);
        var out = std.ArrayList(u8).init(arena);
        try json.write_value(&out, response.?.get("data") orelse json.Value.null_value);
        observed = out.items;
    }

    return .{
        .call = call,
        .ret = if (answered) (ret_raw orelse call) else checker.never_returned,
        .now = now,
        .envelope = envelope,
        .status = status,
        .data = observed,
        .answered = answered,
        .client = client,
        .kind = try arena.dupe(u8, kind),
    };
}

/// Which operations a linearizability check can speak about.
///
/// The cross-origin reads cannot be: a search or a snapshot is a listing followed
/// by a read of each object, each at its own instant, and was never atomic. They
/// change nothing, so dropping them is sound; keeping them would refute a correct
/// server.
pub fn checkable(operation: checker.Operation) bool {
    for ([_][]const u8{
        "promise.search",
        "task.search",
        "schedule.search",
        "debug.snap",
        "debug.reset",
    }) |excluded| {
        if (std.mem.eql(u8, operation.kind, excluded)) return false;
    }
    return true;
}

/// Read a history from a file.
pub fn load_file(arena: std.mem.Allocator, path: []const u8, max_bytes: usize) !Loaded {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const text = try file.readToEndAlloc(arena, max_bytes);
    return parse(arena, text);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "a recorded line becomes an operation the checker can use" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text =
        \\{"kind":"promise.create","now":1000,"req":{"id":"o:a","timeoutAt":9000},"res":{"kind":"promise.create","head":{"corrId":"c7","status":200,"version":"2026-04-01"},"data":{"promise":{"id":"o:a"}}},"call":100,"return":200,"client":2}
    ;
    const loaded = try parse(a, text);
    try testing.expectEqual(@as(usize, 1), loaded.operations.len);
    const op = loaded.operations[0];
    try testing.expectEqual(@as(i64, 100), op.call);
    try testing.expectEqual(@as(i64, 200), op.ret);
    try testing.expectEqual(@as(i64, 1000), op.now);
    try testing.expectEqual(@as(i32, 200), op.status);
    try testing.expectEqual(@as(u32, 2), op.client);
    try testing.expect(op.answered);
    try testing.expectEqualStrings("promise.create", op.kind);
    try testing.expectEqualStrings("{\"promise\":{\"id\":\"o:a\"}}", op.data);
    // The envelope is rebuilt with the instant and the correlation id the
    // recorder used, because a fence echoes the latter into its answer.
    try testing.expect(std.mem.indexOf(u8, op.envelope, "\"resonate:debug_time\":1000") != null);
    try testing.expect(std.mem.indexOf(u8, op.envelope, "\"corrId\":\"c7\"") != null);
    try testing.expect(std.mem.indexOf(u8, op.envelope, "\"id\":\"o:a\"") != null);
}

test "an operation with no answer is left open" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const loaded = try parse(a,
        \\{"kind":"promise.get","now":5,"req":{"id":"o:a"},"res":null,"call":1,"return":2,"client":0}
    );
    const op = loaded.operations[0];
    try testing.expect(!op.answered);
    try testing.expectEqual(checker.never_returned, op.ret);
}

test "a whole envelope in the request field is accepted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const loaded = try parse(a,
        \\{"now":7,"req":{"kind":"promise.get","head":{"corrId":"z"},"data":{"id":"o:b"}},"res":{"head":{"status":404},"data":"Promise not found"},"call":1,"return":2}
    );
    const op = loaded.operations[0];
    try testing.expectEqualStrings("promise.get", op.kind);
    try testing.expectEqual(@as(i32, 404), op.status);
    try testing.expectEqualStrings("\"Promise not found\"", op.data);
    try testing.expect(std.mem.indexOf(u8, op.envelope, "\"id\":\"o:b\"") != null);
}

test "lines that are not operations are counted, not fatal" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const loaded = try parse(a,
        \\not json
        \\{"kind":"promise.get","now":1,"req":{"id":"x"},"res":{"head":{"status":404},"data":"x"},"call":1,"return":2}
        \\{"missing":"everything"}
        \\
    );
    try testing.expectEqual(@as(usize, 1), loaded.operations.len);
    try testing.expectEqual(@as(usize, 2), loaded.skipped);
}

test "the cross-origin reads are excluded from a check" {
    const cases = [_]struct { kind: []const u8, want: bool }{
        .{ .kind = "promise.get", .want = true },
        .{ .kind = "promise.create", .want = true },
        .{ .kind = "task.acquire", .want = true },
        .{ .kind = "debug.tick", .want = true },
        .{ .kind = "schedule.create", .want = true },
        .{ .kind = "promise.search", .want = false },
        .{ .kind = "task.search", .want = false },
        .{ .kind = "schedule.search", .want = false },
        .{ .kind = "debug.snap", .want = false },
        .{ .kind = "debug.reset", .want = false },
    };
    for (cases) |c| {
        try testing.expectEqual(c.want, checkable(.{
            .call = 0,
            .ret = 1,
            .now = 0,
            .envelope = "",
            .status = 200,
            .data = "{}",
            .kind = c.kind,
        }));
    }
}
