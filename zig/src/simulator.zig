//! The simulator's command line.
//!
//! Three things it does:
//!
//! * `run` — one deterministic run from a seed, reported in full.
//! * `soak` — many runs, stopping at the first seed that fails, so the failure
//!   comes with the seed that reproduces it.
//! * `check` — the linearizability checker over a history recorded from a real
//!   server over a real network, which is the only way to check the thing that
//!   actually ships.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const store_mod = @import("store.zig");
const simulation = @import("sim/simulation.zig");
const checker = @import("sim/checker.zig");
const history_mod = @import("sim/history.zig");
const Model = @import("sim/model.zig").Model;

const usage =
    \\simulator — deterministic simulation testing for resonate
    \\
    \\Usage:
    \\  simulator run   [options]          one run from one seed
    \\  simulator soak  [options]          many runs, until one fails
    \\  simulator check <history file>     check a recorded concurrent history
    \\
    \\Options for run and soak:
    \\  --seed <n>             [default: taken from the clock for run, 1 for soak]
    \\  --runs <n>             how many seeds a soak tries          [default: 100]
    \\  --servers <n>          servers sharing one bucket           [default: 2]
    \\  --clients <n>          callers in flight at once            [default: 3]
    \\  --operations <n>       requests per run                     [default: 200]
    \\  --crash <percent>      chance per step a server is killed   [default: 0]
    \\  --unavailable <pct>    store operations with no answer      [default: 0]
    \\  --conflict <pct>       conditional writes it will not order [default: 0]
    \\  --lost-ack <pct>       writes that land and report failure  [default: 0]
    \\  --defer <pct>          operations completed on a later turn [default: 80]
    \\  --reorder <pct>        held operations completed out of order [default: 0]
    \\  --no-check             skip the linearizability search
    \\  --dump <file>          write the recorded history for `simulator check`
    \\  --verbose
    \\
    \\Options for check:
    \\  --max-steps <n>        the search's budget         [default: 20000000]
    \\  --no-time-order        allow orders in which the instants go backwards
    \\                         [the default for `check`: a recorder stamps an
    \\                         instant before it sends, and the server does not
    \\                         order by it]
    \\  --time-order           require the instants not to go backwards
    \\                         [the default for `run` and `soak`: the simulator's
    \\                         clock moves only on a sweep, and a sweep is a
    \\                         barrier]
    \\
;

const Command = enum { run, soak, check };

pub fn main() u8 {
    // The search allocates and frees hundreds of thousands of times a second, and
    // a general purpose allocator hands every large block back to the kernel as it
    // goes — which costs more than the model does. This one keeps what it has.
    // Leaks are the test binary's to catch, where the allocator checks for them.
    const allocator = std.heap.smp_allocator;

    const argv = std.process.argsAlloc(allocator) catch return 1;
    defer std.process.argsFree(allocator, argv);

    if (argv.len < 2 or std.mem.eql(u8, argv[1], "--help") or std.mem.eql(u8, argv[1], "-h")) {
        std.io.getStdOut().writeAll(usage) catch {};
        return if (argv.len < 2) 1 else 0;
    }
    const command: Command = blk: {
        if (std.mem.eql(u8, argv[1], "run")) break :blk .run;
        if (std.mem.eql(u8, argv[1], "soak")) break :blk .soak;
        if (std.mem.eql(u8, argv[1], "check")) break :blk .check;
        std.debug.print("unknown command: {s}\n\n{s}", .{ argv[1], usage });
        return 1;
    };

    var options = simulation.Options{ .seed = 0 };
    var seed_given = false;
    var runs: u32 = 100;
    var path: ?[]const u8 = null;
    var max_steps: u64 = 20_000_000;
    // On for a simulated run and off for a recorded one, and the flags below say
    // so either way. The difference is who stamped the instants: the simulator
    // moves its clock only on a sweep, and a sweep is a barrier, so every request
    // between two of them carries the same instant and no order can make them go
    // backwards. A recorder stamps an instant and *then* sends, from several
    // clients at once, so a request carrying an earlier instant is routinely
    // applied after one carrying a later instant — the server does not order by
    // it, and a checker that insists on it refutes correct servers.
    var enforce_time_order = std.mem.eql(u8, argv[1], "check") == false;

    var i: usize = 2;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (!std.mem.startsWith(u8, arg, "--")) {
            if (path == null) {
                path = arg;
                continue;
            }
            std.debug.print("unexpected argument: {s}\n", .{arg});
            return 1;
        }
        if (std.mem.eql(u8, arg, "--verbose")) {
            options.verbose = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-check")) {
            options.check = false;
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-time-order")) {
            enforce_time_order = false;
            continue;
        }
        if (std.mem.eql(u8, arg, "--time-order")) {
            enforce_time_order = true;
            continue;
        }
        i += 1;
        if (i >= argv.len) {
            std.debug.print("{s} needs a value\n", .{arg});
            return 1;
        }
        const value = argv[i];
        if (std.mem.eql(u8, arg, "--dump")) {
            options.dump = value;
            continue;
        }
        const number = std.fmt.parseInt(u64, value, 10) catch {
            std.debug.print("{s} needs a number, got {s}\n", .{ arg, value });
            return 1;
        };
        if (std.mem.eql(u8, arg, "--seed")) {
            options.seed = number;
            seed_given = true;
        } else if (std.mem.eql(u8, arg, "--runs")) {
            runs = @intCast(number);
        } else if (std.mem.eql(u8, arg, "--servers")) {
            options.servers = @intCast(number);
        } else if (std.mem.eql(u8, arg, "--clients")) {
            options.clients = @intCast(number);
        } else if (std.mem.eql(u8, arg, "--operations")) {
            options.operations = @intCast(number);
        } else if (std.mem.eql(u8, arg, "--crash")) {
            options.crash_percent = number;
        } else if (std.mem.eql(u8, arg, "--unavailable")) {
            options.faults.unavailable_percent = number;
        } else if (std.mem.eql(u8, arg, "--conflict")) {
            options.faults.conflict_percent = number;
        } else if (std.mem.eql(u8, arg, "--lost-ack")) {
            options.faults.lost_ack_percent = number;
        } else if (std.mem.eql(u8, arg, "--defer")) {
            options.faults.defer_percent = number;
        } else if (std.mem.eql(u8, arg, "--reorder")) {
            options.faults.reorder_percent = number;
        } else if (std.mem.eql(u8, arg, "--max-steps")) {
            max_steps = number;
        } else {
            std.debug.print("unknown option: {s}\n\n{s}", .{ arg, usage });
            return 1;
        }
    }
    options.check_options = .{ .max_steps = max_steps, .enforce_time_order = enforce_time_order };

    const stdout = std.io.getStdOut().writer();
    switch (command) {
        .run => {
            if (!seed_given) options.seed = @bitCast(std.time.milliTimestamp());
            const report = simulation.run(allocator, options) catch |e| {
                stdout.print("seed {d}: {s}\n", .{ options.seed, @errorName(e) }) catch {};
                return 1;
            };
            report.write(stdout) catch {};
            return if (report.ok()) 0 else 1;
        },
        .soak => {
            if (!seed_given) options.seed = 1;
            const first = options.seed;
            var run_index: u32 = 0;
            while (run_index < runs) : (run_index += 1) {
                options.seed = first + run_index;
                const report = simulation.run(allocator, options) catch |e| {
                    stdout.print("seed {d}: {s}\n", .{ options.seed, @errorName(e) }) catch {};
                    return 1;
                };
                if (!report.ok()) {
                    stdout.print("FAILED\n", .{}) catch {};
                    report.write(stdout) catch {};
                    // Every knob, because a run without the faults that broke it
                    // is a different run.
                    stdout.print(
                        "\nreproduce with: simulator run --seed {d} --servers {d} --clients {d}" ++
                            " --operations {d} --conflict {d} --reorder {d} --defer {d}" ++
                            " --unavailable {d} --lost-ack {d} --crash {d} --verbose\n",
                        .{
                            options.seed,
                            options.servers,
                            options.clients,
                            options.operations,
                            options.faults.conflict_percent,
                            options.faults.reorder_percent,
                            options.faults.defer_percent,
                            options.faults.unavailable_percent,
                            options.faults.lost_ack_percent,
                            options.crash_percent,
                        },
                    ) catch {};
                    return 1;
                }
                if (report.verdict == .exhausted) {
                    stdout.print("seed {d}: the search gave up; nothing proved either way\n", .{options.seed}) catch {};
                }
                if (options.verbose or run_index % 25 == 0) {
                    stdout.print("seed {d}: {d} ops, {d} at once, {s}\n", .{
                        options.seed,
                        report.operations,
                        report.concurrency.max,
                        @tagName(report.verdict),
                    }) catch {};
                }
            }
            stdout.print("{d} runs from seed {d}: all clear\n", .{ runs, first }) catch {};
            return 0;
        },
        .check => {
            const file = path orelse {
                std.debug.print("check needs a history file\n\n{s}", .{usage});
                return 1;
            };
            return check_history(allocator, file, .{
                .max_steps = max_steps,
                .enforce_time_order = enforce_time_order,
            }) catch |e| {
                stdout.print("{s}: {s}\n", .{ file, @errorName(e) }) catch {};
                return 1;
            };
        },
    }
}

fn check_history(allocator: std.mem.Allocator, path: []const u8, options: checker.Options) !u8 {
    const stdout = std.io.getStdOut().writer();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const loaded = try history_mod.load_file(arena.allocator(), path, 512 * 1024 * 1024);
    var checkable = std.ArrayList(checker.Operation).init(arena.allocator());
    for (loaded.operations) |op| {
        if (history_mod.checkable(op)) try checkable.append(op);
    }

    const measured = checker.Concurrency.measure(checkable.items);
    try stdout.print(
        \\{s}
        \\  operations      {d} recorded, {d} checkable, {d} lines skipped
        \\  answered        {d} ({d} succeeded)
        \\  concurrency     {d} at once, {d} overlapping pairs
        \\
    , .{
        path,
        loaded.operations.len,
        checkable.items.len,
        loaded.skipped,
        measured.answered,
        measured.succeeded,
        measured.max,
        measured.overlapping_pairs,
    });

    // Two ways a green result means nothing, and both are worth refusing.
    if (measured.succeeded == 0) {
        try stdout.print("  REFUSED         nothing succeeded, so the history says nothing\n", .{});
        return 1;
    }
    if (measured.max < 2) {
        try stdout.print("  REFUSED         nothing overlapped, so this is a sequential run\n", .{});
        return 1;
    }

    const model = try Model.create(allocator);
    defer model.destroy();
    const result = try checker.check(allocator, model, checkable.items, options);
    switch (result) {
        .linearizable => |order| {
            defer allocator.free(order);
            try stdout.print("  LINEARIZABLE    an order consistent with real time explains every answer\n", .{});
            return 0;
        },
        .violation => |violation| {
            defer allocator.free(violation.prefix);
            defer allocator.free(violation.abandoned);
            defer if (violation.final_data.len > 0) allocator.free(violation.final_data);
            defer if (violation.expected_data.len > 0) allocator.free(violation.expected_data);
            try stdout.print(
                "  NOT LINEARIZABLE\n    the deepest order reached placed {d} of {d} operations\n",
                .{ violation.prefix.len, checkable.items.len },
            );
            if (violation.blocked) |index| {
                const op = checkable.items[index];
                try stdout.print(
                    \\    the deepest dead end was operation {d} ({s}) from client {d}
                    \\      called   {d}
                    \\      returned {d}
                    \\      request  {s}
                    \\      observed {d} {s}
                    \\      model    {d} {s}
                    \\
                , .{
                    index,
                    op.kind,
                    op.client,
                    op.call,
                    op.ret,
                    op.envelope,
                    op.status,
                    op.data,
                    violation.expected_status,
                    violation.expected_data,
                });
            }
            try stdout.print(
                "    the order it reached, by operation index (~n: taken to have never happened):\n      ",
                .{},
            );
            for (violation.prefix, violation.abandoned) |index, gone| {
                if (gone) try stdout.print("~", .{});
                try stdout.print("{d} ", .{index});
            }
            try stdout.print("\n", .{});
            return 1;
        },
        .exhausted => |exhausted| {
            try stdout.print(
                "  GAVE UP         {d} steps over {d} states; raise --max-steps\n",
                .{ exhausted.steps, exhausted.states },
            );
            return 2;
        },
    }
}

comptime {
    _ = @import("sim/model.zig");
    _ = @import("sim/checker.zig");
    _ = @import("sim/workload.zig");
    _ = @import("sim/simulation.zig");
    _ = @import("sim/history.zig");
}
