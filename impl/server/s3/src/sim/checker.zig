//! The linearizability checker.
//!
//! A concurrent history is linearizable when there exists **some** total order of
//! its operations that (a) respects real time — if one operation returned before
//! another was called, it comes first — and (b) is a legal sequential execution of
//! the specification. This searches for such an order, and a failure to find one
//! is a refutation: the server did something no sequential execution could
//! explain.
//!
//! ## Why a search, and not a replay
//!
//! Replaying the order the harness happened to record and asking whether the model
//! accepts it refutes almost any concurrent run, because return order is only one
//! of many legal linearizations. Only "does *any* consistent order work" is a
//! statement about the server rather than about the recorder.
//!
//! ## The algorithm
//!
//! Wing and Gong's search with Lowe's pruning. Walk the history keeping a set of
//! operations still to place. At each step only the operations that *could* come
//! next are candidates: those called before the earliest return among the ones
//! still unplaced — anything later is forced after something unplaced. Try each
//! candidate against the model; on a match, recurse; on a mismatch or a dead end,
//! put the state back and try the next. Memoize on (which operations are placed,
//! what state that leaves) so an order that reaches a state already explored is
//! abandoned at once. That memo is what turns a factorial search into one that
//! finishes.
//!
//! ## Operations whose result the caller never learned
//!
//! A request that timed out may or may not have been applied. It is given an
//! infinite return time and any response is accepted, and it must still be placed
//! somewhere. That is the conservative reading — it can turn a linearizable
//! history into a reported violation but never the reverse — and it is what
//! Porcupine does.

const std = @import("std");
const stdx = @import("../stdx.zig");
const json = @import("../json.zig");
const model_mod = @import("model.zig");

const assert = stdx.assert;
const Model = model_mod.Model;

pub const never_returned = std.math.maxInt(i64);

pub const Operation = struct {
    /// When the client sent it, in nanoseconds on the recorder's clock.
    call: i64,
    /// When the client had its answer. `never_returned` when it never did.
    ret: i64,
    /// The instant the request itself carried, which the model decides at.
    now: i64,
    /// The whole request envelope, ready for the model.
    envelope: []const u8,
    /// What the client observed.
    status: i32,
    /// The `data` member of the response the client observed.
    data: []const u8,
    /// False when the caller never learned the result.
    answered: bool = true,
    client: u32 = 0,
    /// For diagnostics only.
    kind: []const u8 = "",
};

/// What the state must be once every operation has been placed.
///
/// Without this, the search stops at "the answers are explicable", and two
/// different orders can explain the same answers while leaving different state
/// behind — a settle before an acquire and a settle after it produce identical
/// replies and a different resume count. Checking the state against a
/// *particular* order would then fail a correct server. Checking it as part of
/// the search asks the right question: is there an order that explains the
/// answers **and** leaves the state that is actually there?
pub const Final = struct {
    /// A request whose answer characterises the whole state, which in this
    /// protocol is `debug.snap`.
    envelope: []const u8,
    /// What that request answered on the real server.
    expected_data: []const u8,
};

pub const Options = struct {
    /// Reject orders in which the instants the requests carry would go backwards.
    ///
    /// Sound only where the instants were stamped in the order they were applied,
    /// and that depends on who stamped them.
    ///
    /// In a simulated run they are: the clock moves only on a sweep, a sweep is
    /// issued as a barrier, and so every request between two of them carries the
    /// same instant. Enforcing it there rules out orders the server could not have
    /// produced, which prunes the search enormously and stops it excusing a
    /// violation with an order that never happened.
    ///
    /// A *recorder* stamps an instant and then sends, from several clients at
    /// once, so a request carrying an earlier instant is routinely applied after
    /// one carrying a later instant — the server does not order by it. Enforcing it
    /// there refutes correct servers, which is why `simulator check` does not.
    enforce_time_order: bool = true,
    /// How many model steps the search may take before giving up.
    max_steps: u64 = 20_000_000,
    /// How many explored states to remember. Past this the search still runs, it
    /// just stops pruning.
    max_memo: usize = 4_000_000,
    /// The state the order must end in. See `Final`.
    final: ?Final = null,
};

pub const Result = union(enum) {
    /// The order found, as indices into the operations given.
    linearizable: []const usize,
    /// No order works. Carries the longest prefix the search reached.
    violation: Violation,
    /// The budget ran out. Says nothing either way.
    exhausted: struct { steps: u64, states: usize },

    pub const Violation = struct {
        /// The deepest prefix any order reached, in order. Owned by the caller.
        prefix: []const usize,
        /// The operation that could not be placed at that depth, which is the
        /// closest thing to "the one that broke". Reported with both answers so a
        /// reader can see what the specification would have said.
        blocked: ?usize,
        /// The model's answer for it. Owned by the caller when non-empty.
        expected_status: i32 = 0,
        expected_data: []const u8 = "",
        actual_status: i32 = 0,
        actual_data: []const u8 = "",
        /// For each entry of `prefix`, whether the search got there by deciding
        /// the operation never took effect at all. Only an indeterminate
        /// operation can be one. Owned by the caller.
        abandoned: []const bool = &.{},
        /// How many complete orders explained every answer and then left the
        /// wrong state. Non-zero says the answers are all explicable and the
        /// state is what does not fit, which is a different bug from an answer
        /// nothing can explain.
        final_mismatches: u64 = 0,
        /// The state the last such order left, to be read against the state that
        /// is actually there. Owned by the caller when non-empty.
        final_data: []const u8 = "",
    };
};

/// Whether the specification has to explain this operation's answer — and
/// whether it has to have happened at all.
///
/// A 503 is this protocol's "there is no answer": the caller was told that its
/// request may or may not have been applied, and a caller whose answer never
/// arrived was told even less. Both cases leave two faithful histories, one in
/// which the request took effect and one in which it did not, so such an
/// operation is placed for its effect alone, with no answer to match, and the
/// search may also leave it out entirely.
///
/// It is still held to *when* it may have taken effect, through the `ret` it
/// carries. A server answers only once the store has answered it, so nothing a
/// 503 started can land after the caller gave up, and its real return time
/// stands. A caller that was told nothing has no such bound, and its `ret` is
/// `never_returned`. If a server is ever given a path that keeps writing after
/// it has answered, that assumption is what has to be relaxed here.
fn indeterminate(op: Operation) bool {
    return !op.answered or op.status == 503;
}

/// A bitset over the operations, sized at run time.
const Placed = struct {
    words: []u64,

    fn init(allocator: std.mem.Allocator, n: usize) !Placed {
        const words = try allocator.alloc(u64, (n + 63) / 64);
        @memset(words, 0);
        return .{ .words = words };
    }

    fn set(self: *Placed, i: usize) void {
        self.words[i / 64] |= @as(u64, 1) << @intCast(i % 64);
    }

    fn clear(self: *Placed, i: usize) void {
        self.words[i / 64] &= ~(@as(u64, 1) << @intCast(i % 64));
    }

    fn get(self: *const Placed, i: usize) bool {
        return (self.words[i / 64] & (@as(u64, 1) << @intCast(i % 64))) != 0;
    }

    fn hash(self: *const Placed) u64 {
        return std.hash.Wyhash.hash(0xb175e7, std.mem.sliceAsBytes(self.words));
    }

    /// The two decisions together: which operations are decided, and which of
    /// them happened. Two paths that placed the same operations and applied
    /// different ones have different futures even when the state is the same,
    /// because the clock only advances over the ones that happened.
    fn hash_with(self: *const Placed, other: *const Placed) u64 {
        var h = std.hash.Wyhash.init(0xb175e7);
        h.update(std.mem.sliceAsBytes(self.words));
        h.update(std.mem.sliceAsBytes(other.words));
        return h.final();
    }
};

/// Reused buffers for the state the search has to be able to put back.
///
/// One snapshot is taken per step and freed on every backtrack, and a snapshot
/// of a whole bucket is large enough that a general purpose allocator asks the
/// kernel for it and gives it back each time. Handing the buffers round instead
/// is the difference between a search that spends its time in the kernel and one
/// that spends it on the model.
const Snapshots = struct {
    allocator: std.mem.Allocator,
    spare: std.ArrayList([]u8),

    const Held = struct {
        /// The buffer, which may be larger than what is in it.
        buffer: []u8,
        len: usize,

        fn bytes(self: Held) []const u8 {
            return self.buffer[0..self.len];
        }
    };

    fn init(allocator: std.mem.Allocator) Snapshots {
        return .{ .allocator = allocator, .spare = std.ArrayList([]u8).init(allocator) };
    }

    fn deinit(self: *Snapshots) void {
        for (self.spare.items) |buffer| self.allocator.free(buffer);
        self.spare.deinit();
    }

    fn take(self: *Snapshots, bytes: []const u8) !Held {
        var buffer: []u8 = if (self.spare.items.len > 0) self.spare.pop().? else &.{};
        if (buffer.len < bytes.len) {
            buffer = if (buffer.len == 0)
                try self.allocator.alloc(u8, bytes.len)
            else
                try self.allocator.realloc(buffer, bytes.len);
        }
        @memcpy(buffer[0..bytes.len], bytes);
        return .{ .buffer = buffer, .len = bytes.len };
    }

    fn give(self: *Snapshots, held: Held) void {
        if (held.buffer.len == 0) return;
        self.spare.append(held.buffer) catch self.allocator.free(held.buffer);
    }
};

/// One level of the search: which operation was placed, what the state was
/// before it, and how far through the candidates this level has got.
const Frame = struct {
    op: usize,
    state: Snapshots.Held,
    /// False when this level decided the operation never took effect, in which
    /// case there is no state to put back.
    applied: bool,
    /// The next candidate to try when this level is returned to.
    next_candidate: usize,
    /// The instant before this operation, so undoing restores the constraint.
    previous_now: i64,
};

pub fn check(
    allocator: std.mem.Allocator,
    model: *Model,
    operations: []const Operation,
    options: Options,
) !Result {
    if (operations.len == 0) return .{ .linearizable = &.{} };

    // Call order. Every candidate rule below is stated in terms of it.
    const order = try allocator.alloc(usize, operations.len);
    defer allocator.free(order);
    for (order, 0..) |*slot, i| slot.* = i;
    const Sorter = struct {
        ops: []const Operation,
        fn less(self: @This(), a: usize, b: usize) bool {
            if (self.ops[a].call != self.ops[b].call) return self.ops[a].call < self.ops[b].call;
            return a < b;
        }
    };
    std.mem.sort(usize, order, Sorter{ .ops = operations }, Sorter.less);
    // One slot per move: see the candidate loop.
    const slots = order.len * 2;

    var placed = try Placed.init(allocator, operations.len);
    defer allocator.free(placed.words);
    // Of the decided operations, the ones the search decided actually happened.
    var applied = try Placed.init(allocator, operations.len);
    defer allocator.free(applied.words);

    var snapshots = Snapshots.init(allocator);
    defer snapshots.deinit();

    var stack = std.ArrayList(Frame).init(allocator);
    defer {
        for (stack.items) |frame| snapshots.give(frame.state);
        stack.deinit();
    }

    var memo = std.AutoHashMap(u128, void).init(allocator);
    defer memo.deinit();

    var deepest = try allocator.alloc(usize, operations.len);
    defer allocator.free(deepest);
    var deepest_abandoned = try allocator.alloc(bool, operations.len);
    defer allocator.free(deepest_abandoned);
    var deepest_len: usize = 0;

    var snapshot = std.ArrayList(u8).init(allocator);
    defer snapshot.deinit();

    // The model is handed back in the state it was found in, so one model can
    // serve more than one check: a search that succeeds otherwise leaves the
    // order it found applied to it, and the next check would start from there.
    var initial = std.ArrayList(u8).init(allocator);
    defer initial.deinit();
    try model.snapshot(&initial);
    defer model.restore(initial.items) catch {};
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var steps: u64 = 0;
    var last_now: i64 = std.math.minInt(i64);
    // The deepest point at which something failed to match, which is the most
    // informative thing the search knows when it comes back empty handed.
    var blocked: ?usize = null;
    var blocked_depth: usize = 0;
    var blocked_expected_status: i32 = 0;
    var blocked_expected: []const u8 = "";

    var start_at: usize = 0;
    var state_mismatches: u64 = 0;
    var state_left: []const u8 = "";
    while (true) {
        if (stack.items.len == operations.len) {
            var accepted = true;
            if (options.final) |final| {
                // Every answer is explained. Does this order also leave the state
                // that is there?
                _ = scratch.reset(.retain_capacity);
                const reply = model.apply(final.envelope);
                accepted = json.equal_text(scratch.allocator(), reply.data, final.expected_data);
                if (!accepted) {
                    state_mismatches += 1;
                    // Keep the most recent one: any of them says what kind of
                    // difference it is, and the last is the cheapest to keep.
                    if (state_left.len > 0) allocator.free(state_left);
                    state_left = allocator.dupe(u8, reply.data) catch "";
                }
                model.free_reply(reply);
            }
            if (accepted) {
                if (blocked_expected.len > 0) allocator.free(blocked_expected);
                if (state_left.len > 0) allocator.free(state_left);
                const found = try allocator.alloc(usize, operations.len);
                for (stack.items, 0..) |frame, i| found[i] = frame.op;
                return .{ .linearizable = found };
            }
            // A dead end like any other: put the last choice back and try the next.
            start_at = slots;
        }
        if (steps > options.max_steps) {
            if (blocked_expected.len > 0) allocator.free(blocked_expected);
            if (state_left.len > 0) allocator.free(state_left);
            return .{ .exhausted = .{ .steps = steps, .states = memo.count() } };
        }

        // The earliest return among the operations still to place. Anything
        // called after it cannot come next: something unplaced must precede it.
        var min_ret: i64 = never_returned;
        for (order) |i| {
            if (placed.get(i)) continue;
            if (operations[i].ret < min_ret) min_ret = operations[i].ret;
        }

        var advanced = false;
        var candidate_index = start_at;
        // Two moves per operation: place it, or — for one the caller never got an
        // answer to — decide it never happened. Every placement comes first, in
        // call order, and only then the giving up, because an order that explains
        // the history by leaving nothing out is the one worth looking for and
        // giving up early would explore a great many histories that are missing
        // writes nobody said were missing.
        //
        // Giving up is nonetheless available at every node, which is what keeps
        // the pruning below sound: an operation the search has to reach past can
        // be taken out of the way first.
        while (candidate_index < slots and stack.items.len < operations.len) : (candidate_index += 1) {
            const abandon = candidate_index >= order.len;
            const i = order[if (abandon) candidate_index - order.len else candidate_index];
            if (placed.get(i)) continue;
            const op = operations[i];

            if (abandon) {
                if (!indeterminate(op)) continue;
                // Only where it is forced. Giving up on an operation changes
                // neither the state nor the clock, so the *moment* it is given up
                // on is free — and a search that may give up anywhere explores
                // that freedom as if it mattered, which is where the time goes.
                //
                // The one moment it has to be available is when this operation is
                // what the search cannot get past: an operation called after this
                // one returned can only come after it, so as long as this one is
                // undecided nothing later can be placed. Deciding it exactly
                // there loses no order — everything else the search would do in
                // between commutes with a decision that does nothing.
                if (op.ret != min_ret) continue;
                // Nothing happened: no state to remember, and the clock stays
                // where it was.
                placed.set(i);
                try model.snapshot(&snapshot);
                const key = (@as(u128, placed.hash_with(&applied)) << 64) |
                    @as(u128, Model.hash(snapshot.items));
                if (memo.contains(key)) {
                    placed.clear(i);
                    continue;
                }
                if (memo.count() < options.max_memo) try memo.put(key, {});
                try stack.append(.{
                    .op = i,
                    .state = .{ .buffer = &.{}, .len = 0 },
                    .applied = false,
                    .next_candidate = candidate_index + 1,
                    .previous_now = last_now,
                });
                if (stack.items.len > deepest_len) {
                    deepest_len = stack.items.len;
                    for (stack.items, 0..) |frame, k| {
                        deepest[k] = frame.op;
                        deepest_abandoned[k] = !frame.applied;
                    }
                }
                start_at = 0;
                advanced = true;
                break;
            }

            // Past the earliest return: every remaining candidate is forced to
            // come after something still unplaced. The placements are in call
            // order, so every one after this is too — skip to the giving up.
            if (op.call > min_ret) {
                candidate_index = order.len - 1;
                continue;
            }
            if (options.enforce_time_order and op.now < last_now) continue;

            try model.snapshot(&snapshot);
            const before = try snapshots.take(snapshot.items);
            const reply = model.apply(op.envelope);
            steps += 1;
            defer model.free_reply(reply);

            _ = scratch.reset(.retain_capacity);
            const matches = indeterminate(op) or
                (reply.status == op.status and
                json.equal_text(scratch.allocator(), reply.data, op.data));

            if (!matches) {
                // Deeper than anything seen before means this is a better
                // explanation of where the history stops making sense.
                if (blocked == null or stack.items.len >= blocked_depth) {
                    blocked = i;
                    blocked_depth = stack.items.len;
                    blocked_expected_status = reply.status;
                    if (blocked_expected.len > 0) allocator.free(blocked_expected);
                    blocked_expected = allocator.dupe(u8, reply.data) catch "";
                }
                try model.restore(before.bytes());
                snapshots.give(before);
                continue;
            }

            // A state already reached with the same operations placed leads
            // nowhere new.
            placed.set(i);
            applied.set(i);
            try model.snapshot(&snapshot);
            const key = (@as(u128, placed.hash_with(&applied)) << 64) |
                @as(u128, Model.hash(snapshot.items));
            if (memo.contains(key)) {
                placed.clear(i);
                applied.clear(i);
                try model.restore(before.bytes());
                snapshots.give(before);
                continue;
            }
            if (memo.count() < options.max_memo) try memo.put(key, {});

            try stack.append(.{
                .op = i,
                .state = before,
                .applied = true,
                .next_candidate = candidate_index + 1,
                .previous_now = last_now,
            });
            if (stack.items.len > deepest_len) {
                deepest_len = stack.items.len;
                for (stack.items, 0..) |frame, k| {
                    deepest[k] = frame.op;
                    deepest_abandoned[k] = !frame.applied;
                }
            }
            last_now = @max(last_now, op.now);
            start_at = 0;
            advanced = true;
            break;
        }
        if (advanced) continue;

        // Nothing could come next. Undo the last choice and try the one after it.
        if (stack.items.len == 0) {
            const prefix = try allocator.alloc(usize, deepest_len);
            @memcpy(prefix, deepest[0..deepest_len]);
            const abandoned = try allocator.alloc(bool, deepest_len);
            @memcpy(abandoned, deepest_abandoned[0..deepest_len]);
            return .{ .violation = .{
                .prefix = prefix,
                .abandoned = abandoned,
                .blocked = blocked,
                .expected_status = blocked_expected_status,
                .expected_data = blocked_expected,
                .actual_status = if (blocked) |b| operations[b].status else 0,
                .actual_data = if (blocked) |b| operations[b].data else "",
                .final_mismatches = state_mismatches,
                .final_data = state_left,
            } };
        }
        const frame = stack.pop().?;
        placed.clear(frame.op);
        if (frame.applied) {
            applied.clear(frame.op);
            try model.restore(frame.state.bytes());
        }
        snapshots.give(frame.state);
        last_now = frame.previous_now;
        start_at = frame.next_candidate;
    }
}

/// How much of the history actually overlapped.
///
/// A history with no overlap is a sequential run, and a checker that passes one
/// has said nothing about concurrency. Reporting it is what keeps a green result
/// from being vacuous.
pub const Concurrency = struct {
    /// The largest number of operations in flight at once.
    max: usize,
    /// How many pairs overlapped at all.
    overlapping_pairs: usize,
    /// How many operations the caller got an answer for.
    answered: usize,
    /// How many of those succeeded.
    succeeded: usize,

    pub fn measure(operations: []const Operation) Concurrency {
        var result = Concurrency{ .max = 0, .overlapping_pairs = 0, .answered = 0, .succeeded = 0 };
        for (operations) |op| {
            if (op.answered) result.answered += 1;
            if (op.answered and op.status >= 200 and op.status < 300) result.succeeded += 1;
        }
        for (operations, 0..) |a, i| {
            var concurrent: usize = 1;
            for (operations, 0..) |b, j| {
                if (i == j) continue;
                // Half-open intervals, so two operations that merely touch do not
                // count as overlapping.
                if (a.call < b.ret and b.call < a.ret) {
                    concurrent += 1;
                    if (j > i) result.overlapping_pairs += 1;
                }
            }
            if (concurrent > result.max) result.max = concurrent;
        }
        return result;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const protocol = @import("../protocol.zig");

fn envelope(a: std.mem.Allocator, kind: []const u8, data: []const u8, now: i64) ![]const u8 {
    return std.fmt.allocPrint(
        a,
        "{{\"kind\":\"{s}\",\"head\":{{\"corrId\":\"c\",\"version\":\"{s}\",\"resonate:debug_time\":{d}}},\"data\":{s}}}",
        .{ kind, protocol.protocol_version, now, data },
    );
}

/// Run one request against a throwaway model to find out what it *should*
/// answer, so a test can build a history that is correct by construction.
fn oracle_answer(
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    prior: []const []const u8,
    envelope_json: []const u8,
) !struct { status: i32, data: []const u8 } {
    const model = try Model.create(allocator);
    defer model.destroy();
    for (prior) |e| model.free_reply(model.apply(e));
    const reply = model.apply(envelope_json);
    defer model.free_reply(reply);
    return .{ .status = reply.status, .data = try arena.dupe(u8, reply.data) };
}

test "a sequential history is linearizable in the order it was recorded" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const settle = try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"resolved\"}", 200);
    const get = try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 300);

    var history = std.ArrayList(Operation).init(a);
    var prior = std.ArrayList([]const u8).init(a);
    for ([_][]const u8{ create, settle, get }, 0..) |e, i| {
        const answer = try oracle_answer(testing.allocator, a, prior.items, e);
        try prior.append(e);
        try history.append(.{
            .call = @intCast(i * 100),
            .ret = @intCast(i * 100 + 50),
            .now = @intCast(100 + i * 100),
            .envelope = e,
            .status = answer.status,
            .data = answer.data,
        });
    }

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, history.items, .{});
    switch (result) {
        .linearizable => |found| {
            defer testing.allocator.free(found);
            try testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, found);
        },
        else => return error.ShouldHaveBeenLinearizable,
    }
}

test "two concurrent settles are linearizable in whichever order the answers say" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const resolve = try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"resolved\"}", 200);
    const reject = try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"rejected\"}", 200);

    // The second settle is idempotent, so both callers see `rejected` if the
    // rejection went first. That is the order the search has to find.
    const created = try oracle_answer(testing.allocator, a, &.{}, create);
    const first_reject = try oracle_answer(testing.allocator, a, &.{create}, reject);
    const then_resolve = try oracle_answer(testing.allocator, a, &.{ create, reject }, resolve);

    const history = [_]Operation{
        .{ .call = 0, .ret = 10, .now = 100, .envelope = create, .status = created.status, .data = created.data },
        // Recorded in the other order on purpose: the search must not depend on
        // the order the recorder happened to see.
        .{ .call = 20, .ret = 40, .now = 200, .envelope = resolve, .status = then_resolve.status, .data = then_resolve.data },
        .{ .call = 21, .ret = 41, .now = 200, .envelope = reject, .status = first_reject.status, .data = first_reject.data },
    };

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, &history, .{});
    switch (result) {
        .linearizable => |found| {
            defer testing.allocator.free(found);
            try testing.expectEqual(@as(usize, 0), found[0]);
            // The rejection came first, whatever the recorder saw.
            try testing.expectEqual(@as(usize, 2), found[1]);
            try testing.expectEqual(@as(usize, 1), found[2]);
        },
        else => return error.ShouldHaveBeenLinearizable,
    }
}

test "an answer no order can explain is refuted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const created = try oracle_answer(testing.allocator, a, &.{}, create);
    // A read that returns `resolved` when nothing ever settled the promise.
    const get = try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 200);
    const forged = "{\"promise\":{\"id\":\"o:a\",\"state\":\"resolved\",\"param\":{},\"value\":{},\"tags\":{},\"timeoutAt\":9000000000000,\"createdAt\":100,\"settledAt\":200}}";

    const history = [_]Operation{
        .{ .call = 0, .ret = 10, .now = 100, .envelope = create, .status = created.status, .data = created.data },
        .{ .call = 20, .ret = 30, .now = 200, .envelope = get, .status = 200, .data = forged, .kind = "promise.get" },
    };

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, &history, .{});
    switch (result) {
        .violation => |v| {
            defer testing.allocator.free(v.prefix);
            defer testing.allocator.free(v.abandoned);
            defer if (v.expected_data.len > 0) testing.allocator.free(v.expected_data);
            try testing.expectEqual(@as(usize, 1), v.blocked.?);
            // It got as far as the create and no further.
            try testing.expectEqual(@as(usize, 1), v.prefix.len);
            // And it says what the specification would have answered instead.
            try testing.expect(std.mem.indexOf(u8, v.expected_data, "\"state\":\"pending\"") != null);
        },
        else => return error.ShouldHaveBeenRefuted,
    }
}

test "real time order is respected: a read that returned before a write cannot see it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const created = try oracle_answer(testing.allocator, a, &.{}, create);
    const get = try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 100);
    const after_create = try oracle_answer(testing.allocator, a, &.{create}, get);

    // The read returned at 5, strictly before the create was called at 10 — so no
    // order can put the create first, and the read's answer says it did.
    const history = [_]Operation{
        .{ .call = 0, .ret = 5, .now = 100, .envelope = get, .status = after_create.status, .data = after_create.data },
        .{ .call = 10, .ret = 20, .now = 100, .envelope = create, .status = created.status, .data = created.data },
    };

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, &history, .{});
    switch (result) {
        .violation => |v| {
            testing.allocator.free(v.prefix);
            testing.allocator.free(v.abandoned);
            if (v.expected_data.len > 0) testing.allocator.free(v.expected_data);
        },
        else => return error.ShouldHaveBeenRefuted,
    }
}

test "an operation whose result the caller never learned may be placed anywhere" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const settle = try envelope(a, "promise.settle", "{\"id\":\"o:a\",\"state\":\"resolved\"}", 150);
    const get = try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 200);

    const created = try oracle_answer(testing.allocator, a, &.{}, create);
    const resolved = try oracle_answer(testing.allocator, a, &.{ create, settle }, get);

    const history = [_]Operation{
        .{ .call = 0, .ret = 10, .now = 100, .envelope = create, .status = created.status, .data = created.data },
        // Timed out: the client never learned whether it landed. The later read
        // says it did.
        .{ .call = 20, .ret = never_returned, .now = 150, .envelope = settle, .status = 0, .data = "", .answered = false },
        .{ .call = 30, .ret = 40, .now = 200, .envelope = get, .status = resolved.status, .data = resolved.data },
    };

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, &history, .{});
    switch (result) {
        .linearizable => |found| {
            defer testing.allocator.free(found);
            try testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, found);
        },
        else => return error.ShouldHaveBeenLinearizable,
    }
}

test "a request answered with 503 may have taken effect, or not" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const create = try envelope(a, "promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}", 100);
    const get = try envelope(a, "promise.get", "{\"id\":\"o:a\"}", 200);
    const seen = try oracle_answer(testing.allocator, a, &.{create}, get);
    const absent = try oracle_answer(testing.allocator, a, &.{}, get);
    try testing.expectEqual(@as(i32, 404), absent.status);

    const model = try Model.create(testing.allocator);
    defer model.destroy();

    // The write landed and the caller was told nothing about it, which the read
    // that follows settles.
    {
        const history = [_]Operation{
            .{
                .call = 0,
                .ret = 10,
                .now = 100,
                .envelope = create,
                .status = 503,
                .data = "\"the store did not answer\"",
                .kind = "promise.create",
            },
            .{ .call = 20, .ret = 30, .now = 200, .envelope = get, .status = seen.status, .data = seen.data },
        };
        const result = try check(testing.allocator, model, &history, .{});
        switch (result) {
            .linearizable => |found| testing.allocator.free(found),
            else => return error.ShouldHaveBeenLinearizable,
        }
    }

    // The same 503, and this time it never landed. The search has to be allowed
    // to leave it out: a 503 that had to have happened would refute a server for
    // being honest about not knowing.
    {
        const history = [_]Operation{
            .{
                .call = 0,
                .ret = 10,
                .now = 100,
                .envelope = create,
                .status = 503,
                .data = "\"the store did not answer\"",
                .kind = "promise.create",
            },
            .{ .call = 20, .ret = 30, .now = 200, .envelope = get, .status = absent.status, .data = absent.data },
        };
        const result = try check(testing.allocator, model, &history, .{});
        switch (result) {
            .linearizable => |found| testing.allocator.free(found),
            else => return error.ShouldHaveBeenLinearizable,
        }
    }

    // What a 503 does not excuse: an answer that no order explains is still a
    // violation, whatever else was left out.
    {
        const forged = "{\"promise\":{\"id\":\"o:a\",\"state\":\"resolved\",\"param\":{},\"value\":{},\"tags\":{},\"timeoutAt\":9000000000000,\"createdAt\":100,\"settledAt\":200}}";
        const history = [_]Operation{
            .{
                .call = 0,
                .ret = 10,
                .now = 100,
                .envelope = create,
                .status = 503,
                .data = "\"the store did not answer\"",
                .kind = "promise.create",
            },
            .{ .call = 20, .ret = 30, .now = 200, .envelope = get, .status = 200, .data = forged, .kind = "promise.get" },
        };
        const result = try check(testing.allocator, model, &history, .{});
        switch (result) {
            .violation => |v| {
                defer testing.allocator.free(v.prefix);
                defer testing.allocator.free(v.abandoned);
                defer if (v.expected_data.len > 0) testing.allocator.free(v.expected_data);
                try testing.expectEqual(@as(usize, 1), v.blocked.?);
            },
            else => return error.ShouldHaveBeenRefuted,
        }
    }
}

test "a long concurrent history over a small id space is linearizable" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Built by running a real sequence and then claiming the operations
    // overlapped: every answer is one the model produced, so a correct checker
    // must find an order — and the order it finds need not be the one used here.
    const truth = try Model.create(testing.allocator);
    defer truth.destroy();

    var history = std.ArrayList(Operation).init(a);
    var rng = stdx.Random.init(20260919);
    var now: i64 = 1_000_000;
    var clock: i64 = 0;
    for (0..40) |_| {
        const which = rng.below(4);
        const id = try std.fmt.allocPrint(a, "c:p{d}", .{rng.below(3)});
        const e = switch (which) {
            0 => try envelope(a, "promise.create", try std.fmt.allocPrint(
                a,
                "{{\"id\":\"{s}\",\"timeoutAt\":9000000000000,\"tags\":{{\"resonate:scope\":\"global\"}}}}",
                .{id},
            ), now),
            1 => try envelope(a, "promise.settle", try std.fmt.allocPrint(
                a,
                "{{\"id\":\"{s}\",\"state\":\"resolved\",\"value\":{{\"data\":\"v\"}}}}",
                .{id},
            ), now),
            2 => try envelope(a, "promise.get", try std.fmt.allocPrint(a, "{{\"id\":\"{s}\"}}", .{id}), now),
            else => try envelope(a, "promise.register_listener", try std.fmt.allocPrint(
                a,
                "{{\"awaited\":\"{s}\",\"address\":\"http://l\"}}",
                .{id},
            ), now),
        };
        const reply = truth.apply(e);
        const data = try a.dupe(u8, reply.data);
        truth.free_reply(reply);
        // Overlapping intervals: each operation is still open when the next
        // starts, so the recorded order is only one of many candidates.
        try history.append(.{
            .call = clock,
            .ret = clock + 25,
            .now = now,
            .envelope = e,
            .status = reply.status,
            .data = data,
        });
        clock += 10;
        if (rng.chance(30)) now += 1;
    }

    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, history.items, .{});
    switch (result) {
        .linearizable => |found| {
            defer testing.allocator.free(found);
            try testing.expectEqual(history.items.len, found.len);
        },
        .violation => |v| {
            defer testing.allocator.free(v.prefix);
            defer testing.allocator.free(v.abandoned);
            defer if (v.expected_data.len > 0) testing.allocator.free(v.expected_data);
            std.debug.print("\nrefuted at {d} of {d}, blocked={any}\n", .{ v.prefix.len, history.items.len, v.blocked });
            return error.ShouldHaveBeenLinearizable;
        },
        .exhausted => return error.SearchGaveUp,
    }
}

test "concurrency is measured so a sequential run cannot pass for a concurrent one" {
    const sequential = [_]Operation{
        .{ .call = 0, .ret = 10, .now = 0, .envelope = "", .status = 200, .data = "{}" },
        .{ .call = 10, .ret = 20, .now = 0, .envelope = "", .status = 200, .data = "{}" },
        .{ .call = 20, .ret = 30, .now = 0, .envelope = "", .status = 404, .data = "{}" },
    };
    const measured = Concurrency.measure(&sequential);
    try testing.expectEqual(@as(usize, 1), measured.max);
    try testing.expectEqual(@as(usize, 0), measured.overlapping_pairs);
    try testing.expectEqual(@as(usize, 3), measured.answered);
    try testing.expectEqual(@as(usize, 2), measured.succeeded);

    const concurrent = [_]Operation{
        .{ .call = 0, .ret = 100, .now = 0, .envelope = "", .status = 200, .data = "{}" },
        .{ .call = 10, .ret = 110, .now = 0, .envelope = "", .status = 200, .data = "{}" },
        .{ .call = 20, .ret = 30, .now = 0, .envelope = "", .status = 200, .data = "{}" },
    };
    const overlapped = Concurrency.measure(&concurrent);
    try testing.expectEqual(@as(usize, 3), overlapped.max);
    try testing.expectEqual(@as(usize, 3), overlapped.overlapping_pairs);
}

test "an empty history is linearizable and says nothing" {
    const model = try Model.create(testing.allocator);
    defer model.destroy();
    const result = try check(testing.allocator, model, &.{}, .{});
    try testing.expect(result == .linearizable);
}
