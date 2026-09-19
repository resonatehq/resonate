//! The simulator.
//!
//! One process, one seed, no wall clock and no sockets. The simulator owns every
//! input the server has: the clock, the object store, the network, and when a
//! server dies. A seed reproduces a run exactly, which is the only property that
//! makes a failure worth anything — a bug you cannot re-run is a bug you cannot
//! fix.
//!
//! What it builds:
//!
//! * **Several servers over one bucket.** Separate caches, separate actors,
//!   separate backoffs. This is the case the single-writer-per-origin
//!   optimisation must not be *needed* for: correctness comes from the
//!   compare-and-swap, and the actors only reduce how often one is lost.
//! * **A store that misbehaves.** Injected unavailability, conflicts the service
//!   will not order, writes that land and are then reported as failures, and
//!   completions out of order.
//! * **Servers that die.** A crash takes the caches, the deadline queue and every
//!   in-flight decision with it. The callers of those decisions are told nothing,
//!   which is exactly the case a retry has to survive.
//! * **Clients that overlap.** Each holds one request at a time and records when
//!   it was sent and when it was answered, which is the history the
//!   linearizability check needs.
//!
//! What it then checks:
//!
//! 1. **Linearizability.** Is the recorded history equivalent to *some* sequential
//!    execution of the specification? See `checker.zig`.
//! 2. **Refinement of the durable state.** Replay the order the checker found
//!    against a fresh model, and compare the model's whole state against what a
//!    cold server reads out of the bucket. The responses agreeing is not enough:
//!    the state left behind has to be the state that order produces.
//! 3. **Coverage.** Every operation reached a success. A run that only exercised
//!    failure paths has checked nothing, and it says so rather than passing.

const std = @import("std");
const stdx = @import("../stdx.zig");
const json = @import("../json.zig");
const protocol = @import("../protocol.zig");
const store_mod = @import("../store.zig");
const env = @import("../env.zig");
const bus_mod = @import("../bus.zig");
const server_mod = @import("../server.zig");
const checker = @import("checker.zig");
const workload_mod = @import("workload.zig");
const Model = @import("model.zig").Model;

const assert = stdx.assert;

pub const Options = struct {
    seed: u64,
    /// How many servers share the bucket.
    servers: u32 = 2,
    /// How many callers are in flight at once.
    clients: u32 = 3,
    /// How many requests the run issues.
    operations: u32 = 200,
    /// How the store misbehaves. `defer_percent` is not misbehaviour: it is what
    /// makes two requests overlap at all, so it defaults high.
    faults: store_mod.MemoryStore.Faults = .{ .defer_percent = 80 },
    /// Chance per step that a server is killed and replaced.
    crash_percent: u64 = 0,
    /// Run the linearizability search. Off for a fault-heavy run whose point is
    /// that the server survives rather than what order it chose.
    check: bool = true,
    /// The budget the search gets.
    check_options: checker.Options = .{},
    verbose: bool = false,
    /// Where to write the recorded history, in the same format a recording from a
    /// real server over a real network uses — so the same checker reads both.
    dump: ?[]const u8 = null,
};

pub const Verdict = enum { linearizable, violation, exhausted, not_checked };

pub const Report = struct {
    seed: u64,
    operations: usize = 0,
    answered: usize = 0,
    succeeded: usize = 0,
    unanswered: usize = 0,
    /// How many operations went into the check. The cross-origin reads do not.
    checked: usize = 0,
    concurrency: checker.Concurrency = .{ .max = 0, .overlapping_pairs = 0, .answered = 0, .succeeded = 0 },
    verdict: Verdict = .not_checked,
    /// Set when the verdict is a violation.
    blocked_kind: []const u8 = "",
    prefix_length: usize = 0,
    crashes: u32 = 0,
    commits: u64 = 0,
    contentions: u64 = 0,
    conflicts: u64 = 0,
    store_objects: usize = 0,
    /// Whether the state a cold server reads equals the state the order produces.
    refined: bool = false,
    refinement_checked: bool = false,
    /// Operation kinds that never succeeded.
    uncovered: usize = 0,
    /// Statuses seen, so a run that only ever got 400s is visible.
    status_2xx: usize = 0,
    status_3xx: usize = 0,
    status_4xx: usize = 0,
    /// "The store did not answer", which is the honest answer when the store did
    /// not answer — so it is a failure only when nothing was injected that could
    /// stop it answering.
    status_503: usize = 0,
    /// Any other 5xx. Never acceptable: it means the server could not make sense
    /// of its own state.
    status_5xx: usize = 0,
    /// Whether the run injected anything that legitimately produces a 503.
    faults_injected: bool = false,

    pub fn ok(self: Report) bool {
        if (self.verdict == .violation) return false;
        if (self.status_5xx > 0) return false;
        if (self.status_503 > 0 and !self.faults_injected) return false;
        if (self.refinement_checked and !self.refined) return false;
        return true;
    }

    pub fn write(self: Report, writer: anytype) !void {
        try writer.print(
            \\seed {d}
            \\  operations      {d} ({d} answered, {d} never answered)
            \\  statuses        {d} 2xx, {d} 3xx, {d} 4xx, {d} 503, {d} other 5xx
            \\  concurrency     {d} at once, {d} overlapping pairs
            \\  crashes         {d}
            \\  commits         {d} ({d} lost races, {d} unordered conflicts)
            \\  objects         {d}
            \\  checked         {d} operations -> {s}
            \\  refinement      {s}
            \\  coverage        {d} operation kinds never succeeded
            \\
        , .{
            self.seed,
            self.operations,
            self.answered,
            self.unanswered,
            self.status_2xx,
            self.status_3xx,
            self.status_4xx,
            self.status_503,
            self.status_5xx,
            self.concurrency.max,
            self.concurrency.overlapping_pairs,
            self.crashes,
            self.commits,
            self.contentions,
            self.conflicts,
            self.store_objects,
            self.checked,
            @tagName(self.verdict),
            if (!self.refinement_checked) "not checked" else if (self.refined) "holds" else "BROKEN",
            self.uncovered,
        });
        if (self.blocked_kind.len > 0) {
            try writer.print("  blocked on     {s} after {d} operations\n", .{ self.blocked_kind, self.prefix_length });
        }
    }
};

/// One request in flight.
const Pending = struct {
    simulation: *Simulation,
    client: u32,
    server: u32,
    /// Where in the history this operation's record lives.
    index: usize,
    kind: workload_mod.Kind,
    request: server_mod.Request,
    arena: std.heap.ArenaAllocator,
    /// Set when the server it was sent to died first.
    abandoned: bool = false,
};

const Instance = struct {
    runtime: *server_mod.Runtime,
    random: stdx.Random,
};

pub const Simulation = struct {
    allocator: std.mem.Allocator,
    options: Options,
    random: stdx.Random,
    sim: env.Simulated,
    mem: store_mod.MemoryStore,
    nowhere: bus_mod.Nowhere = .{},
    instances: []Instance,
    /// One slot per client: the request it is waiting on, if any.
    in_flight: []?*Pending,
    /// Everything the history points at lives here for the whole run.
    arena: std.heap.ArenaAllocator,
    history: std.ArrayListUnmanaged(checker.Operation) = .{},
    workload: workload_mod.Workload,
    /// The recorder's clock, in nanoseconds. Advanced by the driver, so overlap is
    /// a property of the run rather than of how fast the machine is.
    wall: i64 = 0,
    outstanding: u32 = 0,
    report: Report,
    /// Requests whose server died while holding them. Their memory outlives the
    /// crash because nothing can prove the dead server let go of it, so it is
    /// released when the run ends rather than when the caller gave up.
    abandoned: std.ArrayListUnmanaged(*Pending) = .{},

    pub fn create(allocator: std.mem.Allocator, options: Options) !*Simulation {
        const self = try allocator.create(Simulation);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .options = options,
            .random = stdx.Random.init(options.seed),
            .sim = env.Simulated.init(allocator, 1_000_000_000),
            .mem = store_mod.MemoryStore.init(allocator),
            .instances = try allocator.alloc(Instance, @max(options.servers, 1)),
            .in_flight = try allocator.alloc(?*Pending, @max(options.clients, 1)),
            .arena = std.heap.ArenaAllocator.init(allocator),
            .workload = undefined,
            .report = .{ .seed = options.seed },
        };
        self.workload = workload_mod.Workload.init(allocator, &self.random, 1_000_000_000);
        self.mem.random = &self.random;
        self.mem.faults = options.faults;
        for (self.in_flight) |*slot| slot.* = null;
        for (self.instances, 0..) |*instance, i| {
            instance.* = .{
                .runtime = try self.build_runtime(),
                // A seed per server, so two servers do not back off in lockstep.
                .random = stdx.Random.init(options.seed ^ (0x9e37_79b9 *% (i + 1))),
            };
            instance.runtime.applier.random = &instance.random;
        }
        return self;
    }

    fn build_runtime(self: *Simulation) !*server_mod.Runtime {
        const runtime = try server_mod.Runtime.create(
            self.allocator,
            // The clock belongs to the caller, so the trace decides when things
            // happen and the run is reproducible.
            .{ .debug = true, .server_url = "http://sim", .prefix = "sim" },
            self.mem.store(),
            self.sim.clock(),
            self.sim.timer(),
            self.nowhere.message_bus(),
        );
        // Messages are not what this checks, and a growing outbox would show up
        // as a state difference between two equivalent runs.
        runtime.sender.hold = false;
        return runtime;
    }

    pub fn destroy(self: *Simulation) void {
        for (self.instances) |instance| instance.runtime.destroy();
        self.allocator.free(self.instances);
        for (self.in_flight) |slot| {
            if (slot) |pending| {
                pending.arena.deinit();
                self.allocator.destroy(pending);
            }
        }
        self.allocator.free(self.in_flight);
        for (self.abandoned.items) |pending| {
            pending.arena.deinit();
            self.allocator.destroy(pending);
        }
        self.abandoned.deinit(self.allocator);
        self.history.deinit(self.allocator);
        self.workload.deinit();
        self.arena.deinit();
        self.mem.deinit();
        self.sim.deinit();
        self.allocator.destroy(self);
    }

    pub fn run(self: *Simulation) !Report {
        var issued: u32 = 0;
        var exclusive_in_flight = false;
        var guard: u64 = 0;

        while (issued < self.options.operations or self.outstanding > 0) {
            guard += 1;
            if (guard > @as(u64, self.options.operations) * 2_000 + 100_000) return error.SimulationStuck;

            // Issue. An exclusive operation waits for the system to go quiet and
            // holds everything else off while it runs, which is what makes it a
            // barrier the recorded history can hold it to.
            if (!exclusive_in_flight and issued < self.options.operations) {
                for (self.in_flight, 0..) |slot, client| {
                    if (slot != null) continue;
                    if (issued >= self.options.operations) break;
                    const kind = self.workload.pick();
                    if (kind.exclusive()) {
                        if (self.outstanding > 0) break;
                        try self.issue(@intCast(client), kind);
                        issued += 1;
                        exclusive_in_flight = true;
                        break;
                    }
                    try self.issue(@intCast(client), kind);
                    issued += 1;
                    // Not every client every step: a step that fills every slot
                    // every time produces the same overlap pattern forever.
                    if (self.random.chance(40)) break;
                }
            }

            // Time moves a little, so intervals overlap and deadlines can fall.
            self.wall += @intCast(self.random.between(1, 40));
            _ = self.sim.advance_to(self.sim.now + @as(i64, @intCast(self.random.between(0, 3))));
            if (self.random.chance(30)) self.mem.drain_delayed();
            for (self.instances) |instance| instance.runtime.drain();

            if (exclusive_in_flight and self.outstanding == 0) exclusive_in_flight = false;

            if (self.options.crash_percent > 0 and self.random.chance(self.options.crash_percent)) {
                try self.crash(@intCast(self.random.below(self.instances.len)));
            }

            // Anything the store is still holding back has to land eventually, or
            // the run cannot finish.
            if (self.outstanding > 0 and self.mem.delayed_count() > 0 and self.random.chance(50)) {
                self.mem.drain_delayed();
            }
        }

        self.mem.drain_delayed();
        for (self.instances) |instance| instance.runtime.drain();
        return try self.finish();
    }

    fn issue(self: *Simulation, client: u32, kind: workload_mod.Kind) !void {
        const run_arena = self.arena.allocator();
        const corr_id = try std.fmt.allocPrint(run_arena, "c{d}-{d}", .{ client, self.history.items.len });
        const pid = try std.fmt.allocPrint(run_arena, "w{d}", .{client});
        const request = try self.workload.build(run_arena, kind, corr_id, pid);

        const index = self.history.items.len;
        try self.history.append(self.allocator, .{
            .call = self.wall,
            .ret = checker.never_returned,
            .now = request.now,
            .envelope = request.envelope,
            .status = 0,
            .data = "",
            .answered = false,
            .client = client,
            .kind = kind.wire(),
        });

        const server_index: u32 = @intCast(self.random.below(self.instances.len));
        const pending = try self.allocator.create(Pending);
        pending.* = .{
            .simulation = self,
            .client = client,
            .server = server_index,
            .index = index,
            .kind = kind,
            .request = undefined,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
        };
        pending.request = .{
            .body = request.envelope,
            .arena = &pending.arena,
            .callback = on_answered,
            .context = pending,
        };
        self.in_flight[client] = pending;
        self.outstanding += 1;
        self.instances[server_index].runtime.server.process(&pending.request);
    }

    fn on_answered(request: *server_mod.Request) void {
        const pending: *Pending = @ptrCast(@alignCast(request.context.?));
        const self = pending.simulation;
        if (pending.abandoned) return;

        const run_arena = self.arena.allocator();
        const entry = &self.history.items[pending.index];
        entry.ret = self.wall;
        entry.status = request.status;
        entry.answered = true;
        // The answer is the envelope; only its `data` is what a specification
        // talks about.
        const parsed = json.parse(run_arena, request.response) catch {
            entry.data = "";
            self.settle(pending);
            return;
        };
        var out = std.ArrayList(u8).init(run_arena);
        json.write_value(&out, parsed.get("data") orelse json.Value.null_value) catch {};
        entry.data = out.items;

        self.workload.observe(pending.kind, request.status, entry.data, run_arena);
        self.settle(pending);
    }

    fn settle(self: *Simulation, pending: *Pending) void {
        self.in_flight[pending.client] = null;
        assert(self.outstanding > 0);
        self.outstanding -= 1;
        pending.arena.deinit();
        self.allocator.destroy(pending);
    }

    /// Kill a server and put a fresh one in its place.
    ///
    /// Everything it held goes: the cache, the deadline queue, the actors, and
    /// every decision in flight. The callers of those decisions are told nothing —
    /// which is the case every operation's idempotence exists for.
    fn crash(self: *Simulation, index: u32) !void {
        // Quiesce the store first. Anything it is still holding points into a
        // server that is about to go, and draining once is not enough: draining
        // lets the servers submit more.
        var rounds: u32 = 0;
        while (self.mem.delayed_count() > 0) {
            rounds += 1;
            if (rounds > 1_000) break;
            self.mem.drain_delayed();
            for (self.instances) |instance| instance.runtime.drain();
        }

        var abandoned: u32 = 0;
        for (self.in_flight, 0..) |slot, client| {
            const pending = slot orelse continue;
            if (pending.server != index) continue;
            // The caller never learns what happened, which the checker reads as
            // "this may or may not have been applied".
            pending.abandoned = true;
            self.history.items[pending.index].answered = false;
            self.history.items[pending.index].ret = checker.never_returned;
            self.in_flight[client] = null;
            assert(self.outstanding > 0);
            self.outstanding -= 1;
            abandoned += 1;
            // The arena stays until the run ends: the destroyed server may still
            // hold a pointer to the request inside it.
            try self.abandoned.append(self.allocator, pending);
        }
        self.instances[index].runtime.destroy();
        self.instances[index].runtime = try self.build_runtime();
        self.instances[index].runtime.applier.random = &self.instances[index].random;
        self.report.crashes += 1;
        if (self.options.verbose) {
            std.debug.print("crash: server {d}, {d} callers told nothing\n", .{ index, abandoned });
        }
    }

    fn finish(self: *Simulation) !Report {
        var report = self.report;
        report.faults_injected = self.options.crash_percent > 0 or
            self.options.faults.unavailable_percent > 0 or
            self.options.faults.lost_ack_percent > 0 or
            self.options.faults.conflict_percent > 0;
        report.operations = self.history.items.len;
        report.store_objects = self.mem.count();
        for (self.instances) |instance| {
            report.commits += instance.runtime.applier.commits;
            report.contentions += instance.runtime.applier.contentions;
            report.conflicts += instance.runtime.applier.conflicts;
        }
        for (self.history.items) |op| {
            if (!op.answered) {
                report.unanswered += 1;
                continue;
            }
            report.answered += 1;
            if (op.status >= 200 and op.status < 300) {
                report.status_2xx += 1;
                report.succeeded += 1;
            } else if (op.status < 400) {
                report.status_3xx += 1;
                report.succeeded += 1;
            } else if (op.status < 500) {
                report.status_4xx += 1;
            } else if (op.status == 503) {
                report.status_503 += 1;
            } else {
                report.status_5xx += 1;
            }
        }

        var missing = std.ArrayList(workload_mod.Kind).init(self.allocator);
        defer missing.deinit();
        try self.workload.uncovered(&missing);
        report.uncovered = missing.items.len;
        if (self.options.verbose and missing.items.len > 0) {
            std.debug.print("never succeeded:", .{});
            for (missing.items) |kind| std.debug.print(" {s}", .{@tagName(kind)});
            std.debug.print("\n", .{});
        }

        if (self.options.dump) |path| try self.write_history(path);
        if (!self.options.check) return report;

        // Only the operations a specification can speak about. The cross-origin
        // reads are surveys, not atomic steps, and were never promised to be.
        var checkable = std.ArrayList(checker.Operation).init(self.allocator);
        defer checkable.deinit();
        for (self.history.items) |op| {
            const kind = kind_of(op.kind) orelse continue;
            if (!kind.checkable()) continue;
            try checkable.append(op);
        }
        report.checked = checkable.items.len;
        report.concurrency = checker.Concurrency.measure(checkable.items);

        const model = try Model.create(self.allocator);
        defer model.destroy();
        const result = try checker.check(self.allocator, model, checkable.items, self.options.check_options);
        switch (result) {
            .linearizable => |order| {
                defer self.allocator.free(order);
                report.verdict = .linearizable;
                // The responses agreeing is not enough: the state the order leaves
                // behind has to be the state the bucket holds.
                report.refined = try self.check_refinement(checkable.items, order);
                report.refinement_checked = true;
            },
            .violation => |violation| {
                defer self.allocator.free(violation.prefix);
                defer if (violation.expected_data.len > 0) self.allocator.free(violation.expected_data);
                report.verdict = .violation;
                report.prefix_length = violation.prefix.len;
                if (violation.blocked) |i| report.blocked_kind = checkable.items[i].kind;
            },
            .exhausted => report.verdict = .exhausted,
        }
        return report;
    }

    /// Write the history out, one operation per line.
    ///
    /// The same shape a recording from a real server has, so `simulator check`
    /// reads a simulated history and a real one with the same code — and a
    /// simulated failure can be handed to someone as a file.
    fn write_history(self: *Simulation, path: []const u8) !void {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        var buffered = std.io.bufferedWriter(file.writer());
        const w = buffered.writer();
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        for (self.history.items) |op| {
            _ = arena.reset(.retain_capacity);
            const a = arena.allocator();
            const envelope = json.parse(a, op.envelope) catch continue;
            var request_data = std.ArrayList(u8).init(a);
            try json.write_value(&request_data, envelope.get("data") orelse json.Value.null_value);
            try w.print("{{\"kind\":\"{s}\",\"now\":{d},\"call\":{d},\"return\":", .{
                op.kind, op.now, op.call,
            });
            if (op.ret == checker.never_returned) {
                try w.print("null", .{});
            } else {
                try w.print("{d}", .{op.ret});
            }
            try w.print(",\"client\":{d},\"req\":{s},\"res\":", .{ op.client, request_data.items });
            if (op.answered) {
                const corr_id = blk: {
                    const head = envelope.get("head") orelse break :blk "sim";
                    break :blk head.get_string("corrId") orelse "sim";
                };
                try w.print(
                    "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"{s}\",\"status\":{d},\"version\":\"{s}\"}},\"data\":{s}}}",
                    .{ op.kind, corr_id, op.status, protocol.protocol_version, op.data },
                );
            } else {
                try w.print("null", .{});
            }
            try w.print("}}\n", .{});
        }
        try buffered.flush();
    }

    fn kind_of(wire: []const u8) ?workload_mod.Kind {
        var it = std.EnumSet(workload_mod.Kind).initFull().iterator();
        while (it.next()) |kind| {
            if (std.mem.eql(u8, kind.wire(), wire)) return kind;
        }
        return null;
    }

    /// Replay the order against a fresh model and compare the whole state.
    ///
    /// The linearizability check says the answers were explicable. This says the
    /// state was too: a server that answered correctly and then wrote something
    /// else down would pass the first and fail this.
    fn check_refinement(
        self: *Simulation,
        operations: []const checker.Operation,
        order: []const usize,
    ) !bool {
        const model = try Model.create(self.allocator);
        defer model.destroy();
        for (order) |i| model.free_reply(model.apply(operations[i].envelope));

        const at = self.workload.now;
        const snap_envelope = try std.fmt.allocPrint(
            self.allocator,
            "{{\"kind\":\"debug.snap\",\"head\":{{\"corrId\":\"r\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{{}}}}",
            .{ protocol.protocol_version, at },
        );
        defer self.allocator.free(snap_envelope);

        // The model's store has no faults, so its snapshot is answered inline.
        const expected = model.apply(snap_envelope);
        defer model.free_reply(expected);

        // A cold server: no cache, no deadline queue, nothing but the bucket.
        const cold = try self.build_runtime();
        defer cold.destroy();
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const Answer = struct {
            status: i32 = 0,
            data: []const u8 = "",
            done: bool = false,
            fn callback(request: *server_mod.Request) void {
                const answer: *@This() = @ptrCast(@alignCast(request.context.?));
                answer.status = request.status;
                answer.data = request.response;
                answer.done = true;
            }
        };
        var answer = Answer{};
        var request_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer request_arena.deinit();
        var request = server_mod.Request{
            .body = snap_envelope,
            .arena = &request_arena,
            .callback = Answer.callback,
            .context = &answer,
        };
        cold.server.process(&request);
        // The store holds operations back, so a cold read takes several rounds:
        // the snapshot lists the bucket and then reads every object in it.
        var rounds: u32 = 0;
        while (!answer.done) {
            rounds += 1;
            if (rounds > 100_000) return false;
            self.mem.drain_delayed();
            cold.drain();
        }

        const parsed = json.parse(arena.allocator(), answer.data) catch return false;
        const actual = parsed.get("data") orelse return false;
        var actual_text = std.ArrayList(u8).init(arena.allocator());
        try json.write_value(&actual_text, actual);

        // The prefix of a document is stored under a prefix, so a cold server sees
        // exactly what the live ones wrote. Everything must match.
        const matches = json.equal_text(arena.allocator(), expected.data, actual_text.items);
        if (!matches and self.options.verbose) {
            std.debug.print("refinement broken\n  expected {s}\n  actual   {s}\n", .{ expected.data, actual_text.items });
        }
        return matches;
    }
};

pub fn run(allocator: std.mem.Allocator, options: Options) !Report {
    const simulation = try Simulation.create(allocator, options);
    defer simulation.destroy();
    return simulation.run();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "a clean run is linearizable and refines" {
    const report = try run(testing.allocator, .{
        .seed = 20260919,
        .servers = 2,
        .clients = 3,
        .operations = 150,
    });
    try testing.expectEqual(Verdict.linearizable, report.verdict);
    try testing.expect(report.refined);
    try testing.expectEqual(@as(usize, 0), report.status_5xx);
    // A run where nothing overlapped would have checked nothing about concurrency.
    try testing.expect(report.concurrency.max > 1);
    try testing.expect(report.concurrency.overlapping_pairs > 0);
    try testing.expect(report.succeeded > 0);
    try testing.expect(report.ok());
}

test "the same seed produces the same run" {
    const first = try run(testing.allocator, .{ .seed = 7, .operations = 80 });
    const second = try run(testing.allocator, .{ .seed = 7, .operations = 80 });
    try testing.expectEqual(first.operations, second.operations);
    try testing.expectEqual(first.status_2xx, second.status_2xx);
    try testing.expectEqual(first.status_4xx, second.status_4xx);
    try testing.expectEqual(first.commits, second.commits);
    try testing.expectEqual(first.contentions, second.contentions);
    try testing.expectEqual(first.store_objects, second.store_objects);
    try testing.expectEqual(first.verdict, second.verdict);
}

test "a different seed produces a different run" {
    const a = try run(testing.allocator, .{ .seed = 1, .operations = 80 });
    const b = try run(testing.allocator, .{ .seed = 2, .operations = 80 });
    try testing.expect(a.status_2xx != b.status_2xx or a.commits != b.commits);
}

test "a store that loses races is survived, and nothing is lost" {
    const report = try run(testing.allocator, .{
        .seed = 99,
        .servers = 3,
        .clients = 3,
        .operations = 120,
        .faults = .{ .defer_percent = 80, .conflict_percent = 20, .reorder_percent = 30 },
    });
    // Conflicts and contention are expected; failing the callers is not.
    try testing.expectEqual(@as(usize, 0), report.status_5xx);
    try testing.expectEqual(@as(usize, 0), report.status_503);
    try testing.expectEqual(Verdict.linearizable, report.verdict);
    try testing.expect(report.refined);
}

test "a store that stops answering produces 503s and no wrong answers" {
    const report = try run(testing.allocator, .{
        .seed = 5,
        .servers = 2,
        .clients = 3,
        .operations = 120,
        .faults = .{ .defer_percent = 80, .unavailable_percent = 15, .lost_ack_percent = 10 },
        // The 503s are not operations a specification can place, so the search is
        // not what this run is about.
        .check = false,
    });
    // A 503 is the honest answer to "the store did not answer"; anything else in
    // the 500s would be the server failing to make sense of its own state.
    try testing.expect(report.status_503 > 0);
    try testing.expectEqual(@as(usize, 0), report.status_5xx);
    try testing.expect(report.succeeded > 0);
    // And a run whose faults explain its 503s is a run that passed.
    try testing.expect(report.ok());
}

test "servers that die are replaced, and their callers survive it" {
    const report = try run(testing.allocator, .{
        .seed = 4242,
        .servers = 3,
        .clients = 3,
        .operations = 200,
        .crash_percent = 3,
        // A crashed server's callers learn nothing, and an operation that may or
        // may not have happened makes the search conservative rather than
        // informative.
        .check = false,
    });
    try testing.expect(report.crashes > 0);
    try testing.expectEqual(@as(usize, 0), report.status_5xx);
    try testing.expect(report.succeeded > 0);
    try testing.expect(report.ok());
}

test "several servers over one bucket agree on what is in it" {
    const report = try run(testing.allocator, .{
        .seed = 31337,
        .servers = 4,
        .clients = 4,
        .operations = 160,
    });
    try testing.expectEqual(Verdict.linearizable, report.verdict);
    try testing.expect(report.refined);
    // Four writers on two origins: races are the point.
    try testing.expect(report.commits > 0);
}
