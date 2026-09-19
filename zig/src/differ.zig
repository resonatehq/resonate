//! The differential: this server against another one, request for request.
//!
//! The simulator asks whether this server is consistent with *itself* — whether
//! a concurrent history over this state machine has a sequential explanation in
//! the same state machine. That is exactly the class of bug concurrency can
//! introduce, and it is silent about a different one: whether the state machine
//! is the protocol everybody else implements. A server can be perfectly
//! linearizable and wrong about what `task.suspend` returns.
//!
//! So this drives one seeded trajectory into two servers over HTTP and compares
//! what they say. Both get the same envelope, carrying the same instant in
//! `resonate:debug_time`, so time is an input rather than a race. After each
//! request the response `data` must agree; then `debug.snap` must agree —
//! promises, tasks, callbacks, listeners, both timeout tables and the queued
//! messages — which is what makes the comparison about the whole state and not
//! only about the answers.
//!
//! Both servers must be started in debug mode, which is what makes `debug.reset`,
//! `debug.snap` and `debug.tick` answer and what stops anything running on wall
//! time. Nothing here is a fault injector: a difference is a difference, and
//! injecting failures into two independent processes would produce differences
//! that mean nothing.
//!
//! What a pass means is bounded by the trajectory it ran. What a failure means is
//! not: the two implementations disagree about the protocol, and the step that
//! shows it is printed with both answers.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const http = @import("http.zig");
const io_mod = @import("io.zig");
const net = @import("net.zig");
const protocol = @import("protocol.zig");
const workload_mod = @import("sim/workload.zig");

const assert = stdx.assert;

const usage =
    \\Usage: differ --a <url> --b <url> [options]
    \\
    \\  --a <url>              the server under test        [default: http://127.0.0.1:8021/]
    \\  --b <url>              the server to agree with     [default: http://127.0.0.1:8022/]
    \\  --label-a <name>       what to call it in a report  [default: a]
    \\  --label-b <name>       what to call the other       [default: b]
    \\  --seed <n>             which trajectory to run                  [default: 1]
    \\  --operations <n>       how many requests                        [default: 500]
    \\  --snap-every <n>       compare the whole state every n requests [default: 1]
    \\  --keep-going           report every difference instead of the first
    \\  --ignore-messages      do not compare the outbox — for a peer that
    \\                         delivers its messages rather than holding them
    \\  --verbose              print every request
    \\
    \\Both servers must be running in debug mode. This sends `debug.reset` first.
    \\
;

pub fn main() u8 {
    const allocator = std.heap.smp_allocator;

    const argv = std.process.argsAlloc(allocator) catch return 1;
    defer std.process.argsFree(allocator, argv);

    var options = Options{};
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        const next = struct {
            fn get(a: [][:0]u8, index: *usize) ?[]const u8 {
                if (index.* + 1 >= a.len) return null;
                index.* += 1;
                return a[index.*];
            }
        }.get;
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            std.io.getStdOut().writeAll(usage) catch {};
            return 0;
        } else if (std.mem.eql(u8, arg, "--a")) {
            options.a.url = next(argv, &i) orelse return missing(arg);
        } else if (std.mem.eql(u8, arg, "--b")) {
            options.b.url = next(argv, &i) orelse return missing(arg);
        } else if (std.mem.eql(u8, arg, "--label-a")) {
            options.a.label = next(argv, &i) orelse return missing(arg);
        } else if (std.mem.eql(u8, arg, "--label-b")) {
            options.b.label = next(argv, &i) orelse return missing(arg);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            options.seed = parse(next(argv, &i) orelse return missing(arg)) orelse return 1;
        } else if (std.mem.eql(u8, arg, "--operations")) {
            options.operations = parse(next(argv, &i) orelse return missing(arg)) orelse return 1;
        } else if (std.mem.eql(u8, arg, "--snap-every")) {
            options.snap_every = parse(next(argv, &i) orelse return missing(arg)) orelse return 1;
        } else if (std.mem.eql(u8, arg, "--keep-going")) {
            options.keep_going = true;
        } else if (std.mem.eql(u8, arg, "--ignore-messages")) {
            options.ignore_messages = true;
        } else if (std.mem.eql(u8, arg, "--verbose")) {
            options.verbose = true;
        } else {
            std.debug.print("unknown option: {s}\n\n{s}", .{ arg, usage });
            return 1;
        }
    }

    var differ = Differ.create(allocator, options) catch |e| {
        std.debug.print("could not start: {s}\n", .{@errorName(e)});
        return 1;
    };
    defer differ.destroy();

    const differences = differ.run() catch |e| {
        std.debug.print("could not finish: {s}\n", .{@errorName(e)});
        return 1;
    };
    return if (differences == 0) 0 else 1;
}

fn missing(arg: []const u8) u8 {
    std.debug.print("{s} needs a value\n\n{s}", .{ arg, usage });
    return 1;
}

fn parse(text: []const u8) ?u64 {
    return std.fmt.parseInt(u64, text, 10) catch {
        std.debug.print("not a number: {s}\n", .{text});
        return null;
    };
}

const Peer = struct {
    label: []const u8,
    url: []const u8,
};

const Options = struct {
    a: Peer = .{ .label = "a", .url = "http://127.0.0.1:8021/" },
    b: Peer = .{ .label = "b", .url = "http://127.0.0.1:8022/" },
    seed: u64 = 1,
    operations: u64 = 500,
    snap_every: u64 = 1,
    keep_going: bool = false,
    /// Leave the outbox out of the comparison.
    ///
    /// Only for a peer that *delivers* its messages instead of holding them for
    /// the snapshot, where the section says how far delivery got rather than what
    /// the server decided to send. It narrows what the run proves, so the report
    /// says it was used.
    ignore_messages: bool = false,
    verbose: bool = false,
};

/// One request that is not a request.
const Edge = struct {
    body: []const u8,
    /// Whether the two servers owe each other the same *words*.
    ///
    /// `.answer` is the normal case: the rejection is the protocol's, so its
    /// message is part of the protocol. `.status` is for a rejection the
    /// *parsers* write — "expected ident at line 1 column 2", "invalid type:
    /// string, expected i64", a byte offset — which is one library's prose,
    /// not something another implementation can reproduce or a client can key
    /// off. There the status is the whole of what is owed, and comparing the
    /// words would only ever report that two parsers are two parsers.
    compare: enum { answer, status } = .answer,
};

/// Requests that are not requests.
///
/// Sent verbatim, so a body that is not JSON is a case rather than an
/// impossibility. These belong to the *edge*: whether a message is a request at
/// all is settled before any operation sees it, and the trajectory only ever
/// sends envelopes the protocol admits — so without this, the part of the surface
/// a client is most likely to reach by accident is the part nothing compares.
const edges = [_]Edge{
    // Not JSON. Every parser has its own words for this.
    .{ .body = "nonsense", .compare = .status },
    .{ .body = "", .compare = .status },
    .{ .body = "{", .compare = .status },
    // JSON, but not an envelope.
    .{ .body = "[]", .compare = .status },
    .{ .body = "\"a string\"", .compare = .status },
    .{ .body = "{}", .compare = .status },
    // An envelope missing each of its parts in turn. A parser that deserializes
    // into a struct says which field and where in the input, which is the same
    // prose problem.
    .{ .body = "{\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}", .compare = .status },
    .{ .body = "{\"kind\":\"promise.get\",\"data\":{\"id\":\"o:a\"}}", .compare = .status },
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}}", .compare = .status },
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\"},\"data\":{\"id\":\"o:a\"}}", .compare = .status },
    // An empty kind, a version nobody speaks, and a `data` that is not an object
    // are the protocol's own rejections, worded by the protocol.
    .{ .body = "{\"kind\":\"\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}" },
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\",\"version\":\"1999-01-01\"},\"data\":{\"id\":\"o:a\"}}" },
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":[]}" },
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":null}" },
    // An operation nobody has.
    .{ .body = "{\"kind\":\"promise.explode\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}" },
    // Operations the protocol has, asked for wrongly: the state machine's
    // rejections, and every word of them the protocol's.
    .{ .body = "{\"kind\":\"promise.get\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}" },
    .{ .body = "{\"kind\":\"promise.create\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}" },
    .{
        .body = "{\"kind\":\"promise.create\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
            "\"data\":{\"id\":\"o:a\",\"timeoutAt\":\"soon\"}}",
        // A type error inside `data` is the deserializer's prose again.
        .compare = .status,
    },
    .{ .body = "{\"kind\":\"promise.create\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
        "\"data\":{\"id\":\"\",\"timeoutAt\":1}}" },
    .{ .body = "{\"kind\":\"promise.settle\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
        "\"data\":{\"id\":\"o:a\",\"state\":\"sideways\"}}" },
    .{ .body = "{\"kind\":\"task.acquire\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
        "\"data\":{\"id\":\"o:a\",\"version\":0,\"pid\":\"w\",\"ttl\":0}}" },
    .{ .body = "{\"kind\":\"promise.register_listener\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
        "\"data\":{\"awaited\":\"o:a\",\"address\":\"not a url\"}}" },
    .{ .body = "{\"kind\":\"schedule.create\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"}," ++
        "\"data\":{\"id\":\"s\",\"cron\":\"not a cron\",\"promiseId\":\"p\"," ++
        "\"promiseTimeout\":1,\"promiseTags\":{}}}" },
    .{ .body = "{\"kind\":\"promise.search\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{\"limit\":0}}" },
    .{ .body = "{\"kind\":\"promise.search\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{\"limit\":9999}}" },
    // The console's read model, which is not served here.
    .{ .body = "{\"kind\":\"ui.promises\",\"head\":{\"corrId\":\"e\",\"version\":\"2026-04-01\"},\"data\":{}}" },
};

/// One answer, as the caller saw it.
const Answer = struct {
    /// Zero when the exchange did not complete at all.
    status: u16,
    /// The whole response envelope.
    envelope: []const u8,
    /// Its `data` member, which is what a specification talks about.
    data: []const u8,
};

const Differ = struct {
    allocator: std.mem.Allocator,
    options: Options,
    loop: io_mod.Loop,
    client: net.Client,
    random: stdx.Random,
    /// The trajectory, and the clock. The workload owns the instant every request
    /// carries and only a sweep moves it, so both servers are asked the same
    /// question at the same time.
    workload: workload_mod.Workload,
    differences: u64 = 0,

    fn create(allocator: std.mem.Allocator, options: Options) !*Differ {
        const self = try allocator.create(Differ);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .options = options,
            .loop = try io_mod.Loop.init(allocator),
            .client = undefined,
            .random = stdx.Random.init(options.seed),
            .workload = undefined,
        };
        self.client = net.Client.init(allocator, &self.loop);
        self.workload = workload_mod.Workload.init(allocator, &self.random, 1_000_000_000);
        return self;
    }

    fn destroy(self: *Differ) void {
        self.workload.deinit();
        self.client.deinit();
        self.loop.deinit();
        self.allocator.destroy(self);
    }

    fn run(self: *Differ) !u64 {
        const stdout = std.io.getStdOut().writer();
        try stdout.print("{s} {s}\n{s} {s}\n", .{
            self.options.a.label,
            self.options.a.url,
            self.options.b.label,
            self.options.b.url,
        });

        {
            // A clean bucket on both sides, or the first comparison is of two
            // unrelated histories.
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const body = try self.wrap(arena.allocator(), "debug.reset", "{}", self.workload.now, "reset");
            const a = try self.post(arena.allocator(), self.options.a, body);
            const b = try self.post(arena.allocator(), self.options.b, body);
            if (a.status != 200 or b.status != 200) {
                try stdout.print(
                    "debug.reset was refused ({s} {d}, {s} {d}) — are both servers in debug mode?\n",
                    .{ self.options.a.label, a.status, self.options.b.label, b.status },
                );
                return 1;
            }
        }

        // The edge first: see `edges`.
        for (edges, 0..) |edge, i| {
            const body = edge.body;
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            const a = try self.post(scratch, self.options.a, body);
            const b = try self.post(scratch, self.options.b, body);
            if (self.options.verbose) {
                try stdout.print("edge {d} {d} {d} {s}\n", .{ i, a.status, b.status, body });
            }
            const words_too = edge.compare == .answer;
            if (a.status == b.status and
                (!words_too or json.equal_text(scratch, a.data, b.data))) continue;
            self.differences += 1;
            try stdout.print(
                \\
                \\DIFFERENT ANSWER at the edge, case {d}
                \\  request  {s}
                \\  {s: <8} {d} {s}
                \\  {s: <8} {d} {s}
                \\
            , .{
                i,
                body,
                self.options.a.label,
                a.status,
                a.data,
                self.options.b.label,
                b.status,
                b.data,
            });
            if (!self.options.keep_going) return self.differences;
        }

        var step: u64 = 0;
        while (step < self.options.operations) : (step += 1) {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const scratch = arena.allocator();

            const corr_id = try std.fmt.allocPrint(scratch, "d-{d}", .{step});
            const kind = self.workload.pick();
            const request = try self.workload.build(scratch, kind, corr_id, "w0");

            const a = try self.post(scratch, self.options.a, request.envelope);
            const b = try self.post(scratch, self.options.b, request.envelope);
            if (self.options.verbose) {
                try stdout.print("{d:>5} {d} {d} {s}\n", .{ step, a.status, b.status, request.envelope });
            }

            // The generator follows the server under test: it is the one whose
            // reachable states this is trying to cover.
            self.workload.observe(kind, @intCast(a.status), a.data, scratch);

            // A `debug.snap` answer is the state, so the outbox has to be left out
            // of it too where it is being left out of the comparison.
            const answers_agree = a.status == b.status and
                (json.equal_text(scratch, a.data, b.data) or
                (self.options.ignore_messages and kind == .debug_snap and
                try self.same_but_messages(scratch, a.data, b.data)));
            if (!answers_agree) {
                self.differences += 1;
                try stdout.print(
                    \\
                    \\DIFFERENT ANSWER at step {d} ({s})
                    \\  request  {s}
                    \\  {s: <8} {d} {s}
                    \\  {s: <8} {d} {s}
                    \\
                , .{
                    step,
                    kind.wire(),
                    request.envelope,
                    self.options.a.label,
                    a.status,
                    a.data,
                    self.options.b.label,
                    b.status,
                    b.data,
                });
                if (!self.options.keep_going) return self.differences;
            }

            if (self.options.snap_every > 0 and (step + 1) % self.options.snap_every == 0) {
                if (try self.compare_state(scratch, step, kind)) {
                    if (!self.options.keep_going) return self.differences;
                }
            }
        }

        // The whole state at the end, whatever the sampling was.
        {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            _ = try self.compare_state(arena.allocator(), self.options.operations, null);
        }

        var uncovered = std.ArrayList(workload_mod.Kind).init(self.allocator);
        defer uncovered.deinit();
        try self.workload.uncovered(&uncovered);
        try stdout.print("\n  requests        {d} and {d} at the edge\n  differences     {d}\n", .{
            self.options.operations,
            edges.len,
            self.differences,
        });
        if (self.options.ignore_messages) {
            try stdout.print("  not compared    the outbox\n", .{});
        }
        if (uncovered.items.len > 0) {
            // Not a failure, but it bounds what the run proved.
            try stdout.print("  never succeeded ", .{});
            for (uncovered.items) |kind| try stdout.print("{s} ", .{@tagName(kind)});
            try stdout.print("\n", .{});
        }
        try stdout.print("  {s}\n", .{if (self.differences == 0) "AGREED" else "DISAGREED"});
        return self.differences;
    }

    /// `debug.snap` on both, compared section by section.
    ///
    /// Section by section because the snapshot is long and the difference is
    /// usually in one of them: printing both whole snapshots hides what changed
    /// inside them.
    fn compare_state(
        self: *Differ,
        scratch: std.mem.Allocator,
        step: u64,
        kind: ?workload_mod.Kind,
    ) !bool {
        const stdout = std.io.getStdOut().writer();
        const body = try self.wrap(scratch, "debug.snap", "{}", self.workload.now, "snap");
        const a = try self.post(scratch, self.options.a, body);
        const b = try self.post(scratch, self.options.b, body);
        if (a.status != 200 or b.status != 200) {
            self.differences += 1;
            try stdout.print(
                "\nSNAPSHOT REFUSED after step {d}: {s} {d}, {s} {d}\n",
                .{ step, self.options.a.label, a.status, self.options.b.label, b.status },
            );
            return true;
        }
        if (json.equal_text(scratch, a.data, b.data)) return false;
        if (self.options.ignore_messages and try self.same_but_messages(scratch, a.data, b.data)) {
            return false;
        }

        self.differences += 1;
        try stdout.print("\nDIFFERENT STATE after step {d}", .{step});
        if (kind) |k| try stdout.print(" ({s})", .{k.wire()});
        try stdout.print("\n", .{});

        const parsed_a = json.parse(scratch, a.data) catch null;
        const parsed_b = json.parse(scratch, b.data) catch null;
        if (parsed_a == null or parsed_b == null or parsed_a.? != .object or parsed_b.? != .object) {
            try stdout.print("  {s: <8} {s}\n  {s: <8} {s}\n", .{
                self.options.a.label, a.data,
                self.options.b.label, b.data,
            });
            return true;
        }
        for (protocol.snapshot_sections) |section| {
            if (self.options.ignore_messages and std.mem.eql(u8, section, "messages")) continue;
            const va = parsed_a.?.get(section);
            const vb = parsed_b.?.get(section);
            if (va == null and vb == null) continue;
            var ta = std.ArrayList(u8).init(scratch);
            var tb = std.ArrayList(u8).init(scratch);
            try json.write_value(&ta, va orelse json.Value.null_value);
            try json.write_value(&tb, vb orelse json.Value.null_value);
            if (json.equal_text(scratch, ta.items, tb.items)) continue;
            try stdout.print("  {s}\n    {s: <8} {s}\n    {s: <8} {s}\n", .{
                section,
                self.options.a.label,
                ta.items,
                self.options.b.label,
                tb.items,
            });
        }
        return true;
    }

    /// One envelope, with the instant in it.
    /// Whether two snapshots agree everywhere except the outbox.
    fn same_but_messages(
        self: *Differ,
        scratch: std.mem.Allocator,
        a: []const u8,
        b: []const u8,
    ) !bool {
        _ = self;
        const parsed_a = json.parse(scratch, a) catch return false;
        const parsed_b = json.parse(scratch, b) catch return false;
        if (parsed_a != .object or parsed_b != .object) return false;
        for (protocol.snapshot_sections) |section| {
            if (std.mem.eql(u8, section, "messages")) continue;
            var ta = std.ArrayList(u8).init(scratch);
            var tb = std.ArrayList(u8).init(scratch);
            try json.write_value(&ta, parsed_a.get(section) orelse json.Value.null_value);
            try json.write_value(&tb, parsed_b.get(section) orelse json.Value.null_value);
            if (!json.equal_text(scratch, ta.items, tb.items)) return false;
        }
        return true;
    }

    fn wrap(
        self: *Differ,
        scratch: std.mem.Allocator,
        kind: []const u8,
        data: []const u8,
        now: i64,
        corr_id: []const u8,
    ) ![]const u8 {
        _ = self;
        return std.fmt.allocPrint(
            scratch,
            "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"{s}\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
            .{ kind, corr_id, protocol.protocol_version, now, data },
        );
    }

    /// POST one envelope and wait for the answer.
    ///
    /// Synchronous on purpose. A differential compares two servers at the same
    /// point in one sequence, so there is nothing to overlap: concurrency here
    /// would only make a difference harder to attribute.
    fn post(self: *Differ, scratch: std.mem.Allocator, peer: Peer, body: []const u8) !Answer {
        const Waiter = struct {
            done: bool = false,
            fn on_response(call: *net.Call) void {
                const waiter: *@This() = @ptrCast(@alignCast(call.context.?));
                waiter.done = true;
            }
        };
        var waiter = Waiter{};
        var call = net.Call{
            .method = "POST",
            .url = peer.url,
            .headers = &.{.{ .name = "Content-Type", .value = "application/json" }},
            .body = body,
            .arena = scratch,
            .callback = Waiter.on_response,
            .context = &waiter,
        };
        self.client.send(&call);
        var spins: u64 = 0;
        while (!waiter.done) {
            spins += 1;
            if (spins > 1_000_000) return error.NeverAnswered;
            try self.loop.tick();
        }
        if (call.status == 0) {
            std.debug.print("{s} did not answer: {s}\n", .{ peer.label, call.failure });
            return error.Unreachable;
        }

        // The `data` member, which is the part two implementations owe each other.
        // A body that is not an envelope is reported as itself rather than as
        // nothing, because that is a difference worth seeing.
        const parsed = json.parse(scratch, call.response_body) catch
            return .{ .status = call.status, .envelope = call.response_body, .data = call.response_body };
        const data = parsed.get("data") orelse json.Value.null_value;
        var out = std.ArrayList(u8).init(scratch);
        try json.write_value(&out, data);
        return .{ .status = call.status, .envelope = call.response_body, .data = out.items };
    }
};
