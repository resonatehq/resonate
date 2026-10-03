//! The spec's conformance catalogue, checked one step at a time.
//!
//! `spec/spec/02-abstract/properties.lean` names every property an
//! implementation of the protocol must satisfy. Each is a predicate over the
//! WHOLE server state — `s.promises.all fun p => ...` — or over a pair of
//! states one step apart. Checked as written, that is a stop-the-world view
//! this server does not have and should not build.
//!
//! It does not need one. Every operation touches exactly one origin's document
//! (`doc.zig`), and the spec proves the reduction this file relies on:
//!
//!   * `frame.lean`: after a step, a lookup finds either the row that was
//!     there, or a row the step WROTE.
//!   * `entries.lean` (`perStore`): a per-row property holds everywhere if it
//!     holds of every row each step writes.
//!
//! So each entry is checked on one step's footprint: the rows of one document
//! that the step wrote, the same rows before it, and their partners in the same
//! document — the task with a promise's id, a callback's awaiter. A row the
//! step did not write held when it was last written, and nothing has moved it
//! since; every `.trans` entry here holds of a row and an identical copy of it.
//!
//! One hook, in the applier, around each request it applies:
//!
//!     var before = catalogue.Snapshot.take(doc, origin, allocator);
//!     defer before.release();
//!     ... apply the request ...
//!     before.check(now, doc);
//!
//! Per request rather than per batch: a batch commits several requests at
//! once, and the `.trans` entries are about one step. Acquire, release and
//! acquire again in one batch moves a version by two, which is three legal
//! steps and one illegal-looking jump.
//!
//! Every entry is a skulld property under the spec's own name, so a finding
//! reads the same in Lean, Go, TypeScript and here. A violation reports, then
//! panics with that name: the step that broke it never reaches the store.
//!
//! When it runs: under skulld, in tests, and wherever the root declares
//! `pub const catalogue_always = true` (the simulator does). Otherwise
//! `Snapshot.take` returns at once and nothing else happens.

const std = @import("std");
const builtin = @import("builtin");
const root = @import("root");
const skull = @import("skull.zig");
const doc_mod = @import("doc.zig");
const protocol = @import("protocol.zig");

const Doc = doc_mod.Doc;
const Promise = doc_mod.Promise;
const Task = doc_mod.Task;
const PromiseState = protocol.PromiseState;
const TaskState = protocol.TaskState;

const always: bool = builtin.is_test or (@hasDecl(root, "catalogue_always") and root.catalogue_always);

/// Whether to check this step. Decided per step: under skulld, only once the
/// agent has answered.
pub fn active() bool {
    if (always) return true;
    return skull.active();
}

// ── The entries ───────────────────────────────────────────────────────────────
//
// Names are the spec's, verbatim; `catalogue_names_are_the_specs` holds them
// to it. The predicate each one checks is written beside it in `check_*`.

const E = skull.Always;

// Promise rows.
pub const well_formed_promise_created_at_lte_timeout_at = E("well_formed_promise_created_at_lte_timeout_at");
pub const well_formed_promise_pending_created_before_deadline = E("well_formed_promise_pending_created_before_deadline");
pub const well_formed_promise_settled_at_lte_timeout_at = E("well_formed_promise_settled_at_lte_timeout_at");
pub const well_formed_promise_created_at_lte_settled_at = E("well_formed_promise_created_at_lte_settled_at");
pub const well_formed_promise_settled_at_iff_not_pending = E("well_formed_promise_settled_at_iff_not_pending");
pub const well_formed_promise_pending_has_no_value = E("well_formed_promise_pending_has_no_value");
pub const well_formed_promise_deadline_verdict_matches_timer_tag = E("well_formed_promise_deadline_verdict_matches_timer_tag");
pub const well_formed_promise_deadline_settlement_has_no_value = E("well_formed_promise_deadline_settlement_has_no_value");
pub const well_formed_promise_timedout_is_server_owned = E("well_formed_promise_timedout_is_server_owned");
pub const well_formed_promise_callbacks_unique = E("well_formed_promise_callbacks_unique");
pub const well_formed_promise_listeners_unique = E("well_formed_promise_listeners_unique");
pub const well_formed_promise_obligations_require_external = E("well_formed_promise_obligations_require_external");
pub const well_formed_promise_awaiter_is_not_self = E("well_formed_promise_awaiter_is_not_self");
pub const well_formed_promise_created_at_lte_now = E("well_formed_promise_created_at_lte_now");
pub const well_formed_promise_settled_at_lte_now = E("well_formed_promise_settled_at_lte_now");

// Task rows.
pub const well_formed_task_acquired_iff_has_pid = E("well_formed_task_acquired_iff_has_pid");
pub const well_formed_task_acquired_iff_has_ttl = E("well_formed_task_acquired_iff_has_ttl");
pub const well_formed_task_acquired_iff_has_lease_timeout_at = E("well_formed_task_acquired_iff_has_lease_timeout_at");
pub const well_formed_task_pending_iff_has_retry_timeout_at = E("well_formed_task_pending_iff_has_retry_timeout_at");
pub const well_formed_task_fulfilled_is_cleared = E("well_formed_task_fulfilled_is_cleared");
pub const well_formed_task_suspended_is_cleared = E("well_formed_task_suspended_is_cleared");
pub const well_formed_task_halted_is_cleared = E("well_formed_task_halted_is_cleared");
pub const well_formed_task_suspended_has_no_resumes = E("well_formed_task_suspended_has_no_resumes");
pub const well_formed_task_resumes_unique = E("well_formed_task_resumes_unique");
pub const well_formed_task_acquired_version_positive = E("well_formed_task_acquired_version_positive");

// Rows of one document, related.
pub const consistent_task_iff_kind_task = E("consistent_task_iff_kind_task");
pub const consistent_settled_promise_has_fulfilled_task = E("consistent_settled_promise_has_fulfilled_task");
pub const consistent_settled_task_promise_settled = E("consistent_settled_task_promise_settled");
pub const consistent_callback_awaiter_is_targeted = E("consistent_callback_awaiter_is_targeted");

// One step.
pub const preserved_promise_birth_fields_immutable = E("preserved_promise_birth_fields_immutable");
pub const preserved_settled_promise_record = E("preserved_settled_promise_record");
pub const monotone_promise_set_grows = E("monotone_promise_set_grows");
pub const monotone_task_set_grows = E("monotone_task_set_grows");
pub const monotone_task_version_increases_only_on_acquisition = E("monotone_task_version_increases_only_on_acquisition");
pub const preserved_fulfilled_task = E("preserved_fulfilled_task");
pub const preserved_promise_state_frozen_once_settled = E("preserved_promise_state_frozen_once_settled");
pub const preserved_promise_settlement_is_one_way = E("preserved_promise_settlement_is_one_way");
pub const consistent_promise_settled_at_moves_with_state = E("consistent_promise_settled_at_moves_with_state");
pub const preserved_promise_value_until_settlement = E("preserved_promise_value_until_settlement");
pub const preserved_promise_no_duplicate_ids = E("preserved_promise_no_duplicate_ids");
pub const consistent_promise_state_edge_admissible = E("consistent_promise_state_edge_admissible");
pub const consistent_task_state_edge_admissible = E("consistent_task_state_edge_admissible");
pub const preserved_task_acquisition_only_from_pending = E("preserved_task_acquisition_only_from_pending");
pub const preserved_task_suspension_only_from_acquired = E("preserved_task_suspension_only_from_acquired");
pub const preserved_task_halted_only_reenters_via_pending = E("preserved_task_halted_only_reenters_via_pending");

// ── The hook ──────────────────────────────────────────────────────────────────

/// The document as it was before one step: a copy, because the step mutates
/// the document in place.
pub const Snapshot = struct {
    before: ?Doc = null,

    pub fn take(d: *const Doc, origin: []const u8, allocator: std.mem.Allocator) Snapshot {
        if (!active()) return .{};
        // Through the canonical encoding: the copy is exactly what a reader of
        // the stored bytes would see, and it owns everything it points into.
        var bytes = std.ArrayList(u8).init(allocator);
        defer bytes.deinit();
        d.encode(&bytes, origin) catch return .{};
        const copy = Doc.decode(allocator, bytes.items, origin) catch return .{};
        return .{ .before = copy };
    }

    pub fn release(self: *Snapshot) void {
        if (self.before) |*b| b.deinit();
        self.before = null;
    }

    /// Check the step that took `before` to `after`, at `now`.
    pub fn check(self: *const Snapshot, now: i64, after: *const Doc) void {
        const before = &(self.before orelse return);
        step(now, before, after);
    }
};

/// Steps checked since start: evidence the hook runs at all.
pub var steps_checked: u64 = 0;

/// For tests: collect violated entries' names instead of stopping.
var collecting: ?*std.ArrayList([]const u8) = null;

/// One step, `a` to `b`, of one document.
pub fn step(now: i64, a: *const Doc, b: *const Doc) void {
    steps_checked += 1;
    // Rows never disappear: a row of `a` missing from `b` is the one case the
    // footprint walk below cannot see, so it is looked for directly.
    for (a.promises.items) |p| {
        const q = find(Promise, b.promises.items, p.id);
        verdict(monotone_promise_set_grows, q != null, .{ .id = p.id });
    }
    for (a.tasks.items) |t| {
        verdict(monotone_task_set_grows, find(Task, b.tasks.items, t.id) != null, .{ .id = t.id });
    }

    for (b.promises.items, 0..) |*q, i| {
        const p = find(Promise, a.promises.items, q.id);
        if (p != null and Promise.eql(p.?.*, q.*)) continue; // not written by this step
        check_promise_row(now, q);
        check_promise_partners(b, q);
        check_promise_step(p, q);
        // Ids are the document's keys and the list is sorted by them: a
        // written row is unique if it differs from its neighbours.
        const unique = (i == 0 or std.mem.order(u8, b.promises.items[i - 1].id, q.id) == .lt) and
            (i + 1 == b.promises.items.len or std.mem.order(u8, q.id, b.promises.items[i + 1].id) == .lt);
        verdict(preserved_promise_no_duplicate_ids, unique, .{ .id = q.id });
    }
    for (b.tasks.items) |*u| {
        const t = find(Task, a.tasks.items, u.id);
        if (t != null and Task.eql(t.?.*, u.*)) continue;
        check_task_row(u);
        check_task_partners(b, u);
        check_task_step(t, u);
    }
}

// ── Promise rows ──────────────────────────────────────────────────────────────

fn check_promise_row(now: i64, p: *const Promise) void {
    const id = .{ .id = p.id };
    verdict(well_formed_promise_created_at_lte_timeout_at, p.created_at <= p.timeout_at, id);
    verdict(well_formed_promise_pending_created_before_deadline, p.state != .pending or p.created_at < p.timeout_at, id);
    if (p.settled_at) |x| {
        verdict(well_formed_promise_settled_at_lte_timeout_at, x <= p.timeout_at, id);
        verdict(well_formed_promise_created_at_lte_settled_at, p.created_at <= x, id);
        verdict(well_formed_promise_settled_at_lte_now, x <= now, .{ .id = p.id, .settled_at = x, .now = now });
    }
    verdict(well_formed_promise_settled_at_iff_not_pending, (p.state != .pending) == (p.settled_at != null), id);
    verdict(well_formed_promise_pending_has_no_value, p.state != .pending or value_is_empty(p.value), id);

    const settled_by_deadline = p.settled_at != null and p.settled_at.? == p.timeout_at;
    const deadline_verdict: PromiseState = if (p.is_timer()) .resolved else .rejected_timedout;
    verdict(well_formed_promise_deadline_verdict_matches_timer_tag, !settled_by_deadline or p.state == deadline_verdict, id);
    verdict(well_formed_promise_deadline_settlement_has_no_value, !settled_by_deadline or value_is_empty(p.value), id);
    verdict(well_formed_promise_timedout_is_server_owned, p.state != .rejected_timedout or settled_by_deadline, id);

    verdict(well_formed_promise_callbacks_unique, all_unique(p.callbacks.items), id);
    verdict(well_formed_promise_listeners_unique, all_unique(p.listeners.items), id);
    const no_obligations = p.callbacks.len() == 0 and p.listeners.len() == 0;
    verdict(well_formed_promise_obligations_require_external, no_obligations or p.is_external(), id);
    verdict(well_formed_promise_awaiter_is_not_self, !p.callbacks.contains(p.id), id);
    verdict(well_formed_promise_created_at_lte_now, p.created_at <= now, .{ .id = p.id, .created_at = p.created_at, .now = now });
}

fn check_promise_partners(b: *const Doc, p: *const Promise) void {
    const task = find(Task, b.tasks.items, p.id);
    verdict(consistent_task_iff_kind_task, (task != null) == p.has_task(), .{ .id = p.id });
    verdict(consistent_settled_promise_has_fulfilled_task, p.state == .pending or task == null or task.?.state == .fulfilled, .{ .id = p.id });
    for (p.callbacks.items) |awaiter| {
        const q = find(Promise, b.promises.items, awaiter);
        verdict(consistent_callback_awaiter_is_targeted, q != null and q.?.has_task(), .{ .id = p.id, .awaiter = awaiter });
    }
}

fn check_promise_step(before: ?*const Promise, q: *const Promise) void {
    const p = before orelse {
        // Born in this step: only the "one way" entry speaks of a new row.
        return;
    };
    const id = .{ .id = q.id };
    verdict(
        preserved_promise_birth_fields_immutable,
        p.param.eql(q.param) and map_eql(p.tags, q.tags) and p.timeout_at == q.timeout_at and p.created_at == q.created_at,
        id,
    );
    if (p.state != .pending) {
        verdict(
            preserved_settled_promise_record,
            q.state == p.state and opt_eql(q.settled_at, p.settled_at) and q.value.eql(p.value),
            id,
        );
        verdict(preserved_promise_state_frozen_once_settled, q.state == p.state, id);
    }
    verdict(preserved_promise_settlement_is_one_way, q.state != .pending or p.state == .pending, id);
    verdict(
        consistent_promise_settled_at_moves_with_state,
        (!opt_eql(q.settled_at, p.settled_at)) == (q.state != p.state),
        .{ .id = q.id, .from = @tagName(p.state), .to = @tagName(q.state) },
    );
    verdict(preserved_promise_value_until_settlement, q.state != .pending or q.value.eql(p.value), id);
    verdict(
        consistent_promise_state_edge_admissible,
        promise_edge_admissible(p.state, q.state),
        .{ .id = q.id, .from = @tagName(p.state), .to = @tagName(q.state) },
    );
}

fn promise_edge_admissible(from: PromiseState, to: PromiseState) bool {
    return from == to or from == .pending;
}

// ── Task rows ─────────────────────────────────────────────────────────────────

fn check_task_row(t: *const Task) void {
    const id = .{ .id = t.id, .state = @tagName(t.state) };
    const acquired = t.state == .acquired;
    const lease = t.timeout != null and t.timeout.?.kind == .lease;
    const retry = t.timeout != null and t.timeout.?.kind == .retry;
    const cleared = t.pid == null and t.ttl == null and t.timeout == null;
    verdict(well_formed_task_acquired_iff_has_pid, acquired == (t.pid != null), id);
    verdict(well_formed_task_acquired_iff_has_ttl, acquired == (t.ttl != null), id);
    verdict(well_formed_task_acquired_iff_has_lease_timeout_at, acquired == lease, id);
    verdict(well_formed_task_pending_iff_has_retry_timeout_at, (t.state == .pending) == retry, id);
    verdict(well_formed_task_fulfilled_is_cleared, t.state != .fulfilled or (cleared and t.resumes.len() == 0), id);
    verdict(well_formed_task_suspended_is_cleared, t.state != .suspended or cleared, id);
    verdict(well_formed_task_halted_is_cleared, t.state != .halted or cleared, id);
    verdict(well_formed_task_suspended_has_no_resumes, t.state != .suspended or t.resumes.len() == 0, id);
    verdict(well_formed_task_resumes_unique, all_unique(t.resumes.items), id);
    verdict(well_formed_task_acquired_version_positive, !acquired or t.version >= 1, id);
}

fn check_task_partners(b: *const Doc, t: *const Task) void {
    const p = find(Promise, b.promises.items, t.id);
    verdict(consistent_task_iff_kind_task, p != null and p.?.has_task(), .{ .id = t.id });
    verdict(consistent_settled_task_promise_settled, t.state != .fulfilled or (p != null and p.?.state != .pending), .{ .id = t.id });
}

fn check_task_step(before: ?*const Task, u: *const Task) void {
    const t = before orelse return;
    const edge = .{ .id = u.id, .from = @tagName(t.state), .to = @tagName(u.state), .from_version = t.version, .to_version = u.version };
    const granted = t.state == .pending and u.state == .acquired;
    verdict(
        monotone_task_version_increases_only_on_acquisition,
        if (granted) u.version == t.version + 1 else u.version == t.version,
        edge,
    );
    if (t.state == .fulfilled) {
        verdict(
            preserved_fulfilled_task,
            u.state == .fulfilled and u.version == t.version and u.resumes.len() == 0 and
                u.pid == null and u.ttl == null and u.timeout == null,
            edge,
        );
    }
    verdict(consistent_task_state_edge_admissible, task_edge_admissible(t.state, u.state), edge);
    verdict(preserved_task_acquisition_only_from_pending, u.state != .acquired or t.state == .pending or t.state == .acquired, edge);
    verdict(preserved_task_suspension_only_from_acquired, u.state != .suspended or t.state == .acquired or t.state == .suspended, edge);
    verdict(
        preserved_task_halted_only_reenters_via_pending,
        t.state != .halted or u.state == .halted or u.state == .pending or u.state == .fulfilled,
        edge,
    );
}

/// The spec's admissible pairs, written out rather than derived from the
/// handlers, so the check is independent of the code it checks.
fn task_edge_admissible(from: TaskState, to: TaskState) bool {
    if (from == to) return true;
    return switch (from) {
        .pending => to == .acquired or to == .halted or to == .fulfilled,
        .acquired => to == .pending or to == .suspended or to == .halted or to == .fulfilled,
        .suspended => to == .pending or to == .halted or to == .fulfilled,
        .halted => to == .pending or to == .fulfilled,
        .fulfilled => false,
    };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Report the entry's verdict, and stop on a violation — naming it, so a crash
/// in the simulator says which entry rather than "reached unreachable code".
fn verdict(comptime Entry: type, ok: bool, details: anytype) void {
    if (collecting) |list| {
        if (!ok) list.append(Entry.message) catch {};
        return;
    }
    Entry.record(ok, details);
    if (!ok) std.debug.panic("catalogue: {s} violated", .{Entry.message});
}

fn find(comptime T: type, items: []const T, id: []const u8) ?*const T {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        switch (std.mem.order(u8, items[mid].id, id)) {
            .eq => return &items[mid],
            .lt => lo = mid + 1,
            .gt => hi = mid,
        }
    }
    return null;
}

fn value_is_empty(v: doc_mod.PromiseValue) bool {
    return v.data == null and (v.headers == null or v.headers.?.len() == 0);
}

fn all_unique(items: []const []const u8) bool {
    for (items, 0..) |x, i| {
        for (items[i + 1 ..]) |y| {
            if (std.mem.eql(u8, x, y)) return false;
        }
    }
    return true;
}

fn opt_eql(a: ?i64, b: ?i64) bool {
    if (a == null) return b == null;
    return b != null and a.? == b.?;
}

fn map_eql(a: protocol.StringMap, b: protocol.StringMap) bool {
    if (a.len() != b.len()) return false;
    for (a.entries, b.entries) |x, y| {
        if (!std.mem.eql(u8, x.key, y.key) or !std.mem.eql(u8, x.value, y.value)) return false;
    }
    return true;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "every entry here is the spec's, by name" {
    const spec_path = "../../../spec/spec/02-abstract/properties.lean";
    const source = std.fs.cwd().readFileAlloc(testing.allocator, spec_path, 1 << 20) catch return error.SkipZigTest;
    defer testing.allocator.free(source);

    // The names in the spec's `catalogue` list: `{ name := "..."`.
    var spec = std.StringHashMap(void).init(testing.allocator);
    defer spec.deinit();
    const start = std.mem.indexOf(u8, source, "def catalogue : List Named") orelse return error.NoCatalogue;
    var rest = source[start..];
    const end = std.mem.indexOf(u8, rest, "\ndef ") orelse rest.len;
    rest = rest[0..end];
    while (std.mem.indexOf(u8, rest, "name := \"")) |i| {
        rest = rest[i + "name := \"".len ..];
        const close = std.mem.indexOfScalar(u8, rest, '"') orelse break;
        try spec.put(rest[0..close], {});
        rest = rest[close..];
    }
    try testing.expect(spec.count() > 50);

    const ours = comptime skull.catalog(@This());
    inline for (ours) |entry| {
        if (!spec.contains(entry.message)) {
            std.debug.print("not in the spec's catalogue: {s}\n", .{entry.message});
            return error.NotInSpec;
        }
    }
    std.debug.print("catalogue: {d} of the spec's {d} entries are checked\n", .{ ours.len, spec.count() });
}

var test_target_tags = [_]protocol.StringMap.Entry{.{ .key = protocol.tag_target, .value = "http://w" }};

fn test_promise(id: []const u8, state: PromiseState, settled_at: ?i64, data: ?[]const u8) Promise {
    return .{
        .id = id,
        .state = state,
        .param = .{},
        .value = .{ .data = data },
        .tags = .{ .entries = &test_target_tags },
        .timeout_at = 1_000,
        .created_at = 100,
        .settled_at = settled_at,
        .timeout_armed = false,
        .callbacks = .{},
        .listeners = .{},
    };
}

fn test_task(id: []const u8, state: TaskState, version: i64) Task {
    const acquired = state == .acquired;
    return .{
        .id = id,
        .state = state,
        .version = version,
        .pid = if (acquired) "pid" else null,
        .ttl = if (acquired) 30_000 else null,
        .resumes = .{},
        .timeout = switch (state) {
            .acquired => .{ .kind = .lease, .at = 900 },
            .pending => .{ .kind = .retry, .at = 900 },
            else => null,
        },
    };
}

/// The names `step` finds violated, `a` to `b`.
fn violations(a: *const Doc, b: *const Doc) !std.ArrayList([]const u8) {
    var list = std.ArrayList([]const u8).init(testing.allocator);
    collecting = &list;
    defer collecting = null;
    step(500, a, b);
    return list;
}

fn one_row_doc(p: Promise, t: Task) !Doc {
    var d = Doc.init(testing.allocator);
    try d.promises.append(d.arena.allocator(), p);
    try d.tasks.append(d.arena.allocator(), t);
    return d;
}

test "a legal step breaks nothing, and the hook counts it" {
    var a = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .pending, 1));
    defer a.deinit();
    var b = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .acquired, 2));
    defer b.deinit();
    const before = steps_checked;
    var found = try violations(&a, &b);
    defer found.deinit();
    try testing.expectEqual(@as(usize, 0), found.items.len);
    try testing.expectEqual(before + 1, steps_checked);
}

test "a version that jumps by two on acquisition is caught" {
    var a = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .pending, 1));
    defer a.deinit();
    var b = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .acquired, 3));
    defer b.deinit();
    var found = try violations(&a, &b);
    defer found.deinit();
    try testing.expectEqual(@as(usize, 1), found.items.len);
    try testing.expectEqualStrings("monotone_task_version_increases_only_on_acquisition", found.items[0]);
}

test "a settled promise whose value changes is caught, by every entry that says so" {
    var a = try one_row_doc(test_promise("o:a", .resolved, 200, "v1"), test_task("o:a", .fulfilled, 1));
    defer a.deinit();
    var b = try one_row_doc(test_promise("o:a", .resolved, 200, "v2"), test_task("o:a", .fulfilled, 1));
    defer b.deinit();
    var found = try violations(&a, &b);
    defer found.deinit();
    try testing.expectEqual(@as(usize, 1), found.items.len);
    try testing.expectEqualStrings("preserved_settled_promise_record", found.items[0]);
}

test "a suspended task that skipped acquisition is caught twice over" {
    var a = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .pending, 1));
    defer a.deinit();
    var b = try one_row_doc(test_promise("o:a", .pending, null, null), test_task("o:a", .suspended, 1));
    defer b.deinit();
    var found = try violations(&a, &b);
    defer found.deinit();
    try testing.expectEqual(@as(usize, 2), found.items.len);
    try testing.expectEqualStrings("consistent_task_state_edge_admissible", found.items[0]);
    try testing.expectEqualStrings("preserved_task_suspension_only_from_acquired", found.items[1]);
}

test "the task edges are the spec's list" {
    // Spot checks of the written-out list against the spec's pairs.
    try testing.expect(task_edge_admissible(.pending, .acquired));
    try testing.expect(task_edge_admissible(.acquired, .suspended));
    try testing.expect(!task_edge_admissible(.pending, .suspended));
    try testing.expect(!task_edge_admissible(.suspended, .acquired));
    try testing.expect(!task_edge_admissible(.fulfilled, .pending));
    try testing.expect(task_edge_admissible(.halted, .pending));
    try testing.expect(!task_edge_admissible(.halted, .acquired));
}
