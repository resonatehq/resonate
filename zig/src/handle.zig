//! The state machine: one request in, one reply and a set of effects out.
//!
//! A pure function of `(document, request, now)`. That purity is what makes the
//! storage layer above it correct: a compare-and-swap that loses its race has
//! to *re-decide* against fresh state rather than replay what it decided
//! against stale state, and re-deciding is only safe because nothing here
//! depends on anything but its inputs. There is no clock in this file, no
//! allocation that outlives the call except into the document, and no I/O.
//!
//! Effects — messages to deliver, deadlines to arm — are returned rather than
//! performed. The caller commits the document first and then performs them, in
//! that order, because a message about a transition that did not commit is a
//! lie and a transition that committed without its deadline armed is a promise
//! nobody will keep.
//!
//! ## The shape every operation shares
//!
//! 1. **Parse and validate.** Structural admission only; see `protocol.zig`.
//! 2. **Settle the ghosts.** Almost every operation first settles any promise it
//!    names whose deadline has passed. A promise past its deadline *is* settled
//!    whether or not anything has written that down yet, and an operation that
//!    read it as pending would answer from a state that no longer exists.
//!    `task.heartbeat` is the one operation that does not do this, and it
//!    compensates by refusing to extend a lease on an expired promise.
//! 3. **Decide.** Read the current state, check the preconditions, mutate.
//! 4. **Reply**, and hand back what has to happen next.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const doc_mod = @import("doc.zig");
const cron = @import("cron.zig");

const assert = stdx.assert;
const Doc = doc_mod.Doc;
const Promise = doc_mod.Promise;
const Task = doc_mod.Task;
const PromiseValue = doc_mod.PromiseValue;
const StringSet = doc_mod.StringSet;
const StringMap = protocol.StringMap;
const PromiseState = protocol.PromiseState;
const TaskState = protocol.TaskState;
const SettleState = protocol.SettleState;
const TaskTimeoutKind = protocol.TaskTimeoutKind;

pub const Config = struct {
    /// How many branch siblings a task response carries.
    preload_limit: u32 = protocol.preload_limit_default,
    /// How long a pending task waits before it is offered again.
    pending_retry_ttl: i64 = protocol.pending_retry_ttl,
};

/// A message the server owes a worker.
///
/// Rendered at decision time rather than at delivery: the promise an `unblock`
/// carries is the promise *as it settled*, and the document moves on.
pub const Effect = struct {
    pub const Kind = enum { execute, unblock };

    kind: Kind,
    address: []const u8,
    /// `execute` only. Also the outbox's upsert key: a second offer of the same
    /// task replaces the first rather than queueing behind it.
    task_id: []const u8 = "",
    version: i64 = 0,
    /// `unblock` only: the promise record, already JSON.
    promise_json: []const u8 = "",
    /// `unblock` only. Together with the address it is the outbox's key: a
    /// promise settles once, so a second unblock for the same pair is the same
    /// message.
    promise_id: []const u8 = "",
};

/// What an operation answers, before the envelope is wrapped round it.
pub const Reply = struct {
    status: i32,
    /// The `data` member, already JSON. An error carries a JSON string here,
    /// which is what the protocol says an error body is.
    data: []const u8,
};

pub const Outcome = struct {
    reply: Reply,
    effects: []const Effect,
};

/// Everything a decision needs that is not the document.
///
/// `scratch` is per-decision: the reply and the effects live in it and are
/// consumed by the caller before it is reset. Mutations go into the document's
/// own arena instead, because they outlive the call.
const Ctx = struct {
    doc: *Doc,
    now: i64,
    cfg: Config,
    scratch: std.mem.Allocator,
    buf: std.ArrayList(u8),
    effects: std.ArrayListUnmanaged(Effect) = .{},
    /// Echoed into the inner envelope a fence answers with. Nothing else reads
    /// it: a correlation id is the transport's business, and this is the one
    /// place the protocol puts one inside a body.
    corr_id: []const u8 = "",

    fn init(d: *Doc, now: i64, cfg: Config, scratch: std.mem.Allocator) Ctx {
        return .{
            .doc = d,
            .now = now,
            .cfg = cfg,
            .scratch = scratch,
            .buf = std.ArrayList(u8).init(scratch),
        };
    }

    /// Memory that outlives the call: the document's.
    fn owned(self: *Ctx) std.mem.Allocator {
        return self.doc.allocator();
    }

    fn writer(self: *Ctx) json.Writer {
        self.buf.clearRetainingCapacity();
        return json.Writer.init(&self.buf);
    }

    fn done(self: *Ctx, status: i32) !Outcome {
        return .{
            .reply = .{ .status = status, .data = try self.scratch.dupe(u8, self.buf.items) },
            .effects = try self.effects.toOwnedSlice(self.scratch),
        };
    }

    /// An error reply: the status, and the reason as a JSON string.
    fn fail(self: *Ctx, status: i32, message: []const u8) !Outcome {
        self.buf.clearRetainingCapacity();
        try json.write_string(&self.buf, message);
        return .{
            .reply = .{ .status = status, .data = try self.scratch.dupe(u8, self.buf.items) },
            // An operation that failed emitted nothing. Anything already
            // recorded was recorded by a ghost timeout, which is a real
            // transition and keeps its effects.
            .effects = try self.effects.toOwnedSlice(self.scratch),
        };
    }

    fn failf(self: *Ctx, status: i32, comptime fmt: []const u8, args: anytype) !Outcome {
        const message = try std.fmt.allocPrint(self.scratch, fmt, args);
        return self.fail(status, message);
    }

    /// An empty success — `200 {}`, which is what the operations that answer
    /// nothing answer.
    fn ok_empty(self: *Ctx) !Outcome {
        self.buf.clearRetainingCapacity();
        try self.buf.appendSlice("{}");
        return self.done(200);
    }

    fn emit_execute(self: *Ctx, address: []const u8, task_id: []const u8, version: i64) !void {
        try self.effects.append(self.scratch, .{
            .kind = .execute,
            .address = try self.scratch.dupe(u8, address),
            .task_id = try self.scratch.dupe(u8, task_id),
            .version = version,
        });
    }

    fn emit_unblock(self: *Ctx, address: []const u8, promise: *const Promise) !void {
        var body = std.ArrayList(u8).init(self.scratch);
        errdefer body.deinit();
        var w = json.Writer.init(&body);
        // `now = 0`: the record is the promise as it settled, and a settled
        // promise needs no deadline projection.
        try write_promise_record(&w, 0, promise);
        try self.effects.append(self.scratch, .{
            .kind = .unblock,
            .address = try self.scratch.dupe(u8, address),
            .promise_json = try body.toOwnedSlice(),
        });
    }
};

// ── Record rendering ──────────────────────────────────────────────────────────

/// A promise, as the wire carries it.
///
/// `now` is the projection: a promise still stored as pending whose deadline has
/// passed reads as settled, because it is. Pass `0` where the caller wants
/// stored state instead — a search, a console read, a preload sibling — which is
/// exactly what the reference implementation does, and why a pending-but-expired
/// promise reads `pending` from a search and `rejected_timedout` from a get.
pub fn write_promise_record(w: *json.Writer, now: i64, p: *const Promise) !void {
    var state = p.state;
    var settled_at = p.settled_at;
    if (p.state == .pending and now > 0 and now >= p.timeout_at) {
        state = p.timeout_state();
        // Not `now`: the promise timed out when its deadline fell, and
        // reporting the instant somebody noticed would make the same promise
        // read differently to two callers.
        settled_at = p.timeout_at;
    }
    try w.object_begin();
    try w.field_string("id", p.id);
    try w.field_string("state", state.as_str());
    try w.key("param");
    try p.param.write_wire(w);
    try w.key("value");
    try p.value.write_wire(w);
    try w.key("tags");
    try p.tags.write_json(w);
    try w.field_int("timeoutAt", p.timeout_at);
    try w.field_int("createdAt", p.created_at);
    if (settled_at) |sa| try w.field_int("settledAt", sa);
    try w.object_end();
}

/// A task, as the wire carries it. `state` may be an effective state the
/// document does not hold, which is why it is a parameter.
pub fn write_task_record(
    w: *json.Writer,
    id: []const u8,
    state: TaskState,
    version: i64,
    resumes: i64,
    ttl: ?i64,
    pid: ?[]const u8,
) !void {
    try w.object_begin();
    try w.field_string("id", id);
    try w.field_string("state", state.as_str());
    try w.field_int("version", version);
    try w.field_int("resumes", resumes);
    if (ttl) |t| try w.field_int("ttl", t);
    if (pid) |p| try w.field_string("pid", p);
    try w.object_end();
}

fn write_task(w: *json.Writer, t: *const Task) !void {
    try write_task_record(w, t.id, t.state, t.version, @intCast(t.resumes.len()), t.ttl, t.pid);
}

/// The branch siblings a task response carries.
///
/// Every promise sharing this one's `resonate:branch`, itself excluded, in id
/// order, truncated to the configured limit. A worker resuming an execution
/// reads its siblings' results out of this rather than asking for them one at a
/// time.
fn write_preload(w: *json.Writer, d: *const Doc, promise_id: []const u8, limit: u32) !void {
    try w.array_begin();
    const self_promise = d.promise_const(promise_id) orelse {
        try w.array_end();
        return;
    };
    const branch = self_promise.tags.get(protocol.tag_branch) orelse {
        try w.array_end();
        return;
    };
    if (branch.len == 0) {
        try w.array_end();
        return;
    }
    var n: u32 = 0;
    for (d.promises.items) |*p| {
        if (n >= limit) break;
        if (std.mem.eql(u8, p.id, promise_id)) continue;
        const b = p.tags.get(protocol.tag_branch) orelse continue;
        if (!std.mem.eql(u8, b, branch)) continue;
        try write_promise_record(w, 0, p);
        n += 1;
    }
    try w.array_end();
}

// ── Deadlines ─────────────────────────────────────────────────────────────────

fn set_task_timeout(t: *Task, kind: TaskTimeoutKind, at: i64) void {
    t.timeout = .{ .kind = kind, .at = at };
}

fn clear_task_timeout(t: *Task) void {
    t.timeout = null;
}

// ── The settlement chain ──────────────────────────────────────────────────────

/// Settle any promise this operation names whose deadline has already passed.
///
/// The ghost pass. A promise past its deadline is settled; writing it down
/// lazily, when something asks, is what keeps the system from needing a sweep
/// to be correct — the sweep only makes it timely.
fn try_timeout(ctx: *Ctx, ids: []const []const u8) !void {
    for (ids) |id| {
        if (id.len == 0) continue;
        const p = ctx.doc.promise(id) orelse continue;
        if (!p.expired(ctx.now)) continue;
        p.state = p.timeout_state();
        p.settled_at = p.timeout_at;
        p.timeout_armed = false;
        try trigger_settlement(ctx, id);
    }
}

/// Everything a promise settling sets off: its own task is finished, the
/// awaiters blocked on it are resumed, and the listeners are told.
fn trigger_settlement(ctx: *Ctx, promise_id: []const u8) !void {
    try trigger_fulfilled(ctx, promise_id);
    try trigger_callbacks(ctx, promise_id);
    try trigger_listeners(ctx, promise_id);
}

/// A settled promise's task is over.
///
/// It also stops being an awaiter: a task that will never run again cannot be
/// resumed, so the registrations naming it are dropped rather than left to fire
/// into nothing.
fn trigger_fulfilled(ctx: *Ctx, promise_id: []const u8) !void {
    const t = ctx.doc.task(promise_id) orelse return;
    if (t.state == .fulfilled) return;
    t.state = .fulfilled;
    t.pid = null;
    t.ttl = null;
    t.resumes.clear();
    clear_task_timeout(t);
    const owned = ctx.owned();
    for (ctx.doc.promises.items) |*p| {
        _ = try p.callbacks.remove(owned, promise_id);
    }
}

/// Resume whoever was blocked on this promise.
///
/// A suspended awaiter is woken — set pending, armed for retry, and offered to
/// its worker. An awaiter that is still running only records the resume, because
/// it will see the result when it asks. Either way the resume *is* recorded,
/// which is what a woken worker reads to know why it was woken.
fn trigger_callbacks(ctx: *Ctx, promise_id: []const u8) !void {
    const owned = ctx.owned();
    const settled = ctx.doc.promise(promise_id) orelse return;
    if (settled.callbacks.len() == 0) return;
    // Taken, not iterated: resuming an awaiter mutates the document, and the
    // registrations are consumed by this pass whatever each one turns out to do.
    const awaiters = settled.callbacks;
    settled.callbacks = .empty;

    for (awaiters.items) |awaiter_id| {
        const awaiter = ctx.doc.promise(awaiter_id) orelse continue;
        // An awaiter that has itself settled, or whose deadline has passed, has
        // nothing to resume.
        if (awaiter.state != .pending or ctx.now >= awaiter.timeout_at) continue;
        const address = awaiter.tags.get(protocol.tag_target);
        const t = ctx.doc.task(awaiter_id) orelse continue;
        switch (t.state) {
            .suspended => {
                t.state = .pending;
                t.resumes.clear();
                _ = try t.resumes.insert(owned, promise_id);
                set_task_timeout(t, .retry, ctx.now + ctx.cfg.pending_retry_ttl);
                if (address) |addr| try ctx.emit_execute(addr, awaiter_id, t.version);
            },
            .pending, .acquired, .halted => {
                _ = try t.resumes.insert(owned, promise_id);
            },
            .fulfilled => {},
        }
    }
}

/// Tell the listeners. One message each, carrying the promise as it settled.
fn trigger_listeners(ctx: *Ctx, promise_id: []const u8) !void {
    const p = ctx.doc.promise(promise_id) orelse return;
    if (p.listeners.len() == 0) return;
    const addresses = p.listeners;
    p.listeners = .empty;
    for (addresses.items) |address| {
        try ctx.emit_unblock(address, p);
    }
}

/// The state a task is *in*, as against the state it is stored as.
///
/// A task whose promise has settled is finished, whether or not anything has
/// written that down. Every operation that reports or branches on a task state
/// asks this rather than reading the field.
fn effective_task_state(d: *const Doc, now: i64, id: []const u8) ?TaskState {
    const t = d.task_const(id) orelse return null;
    if (t.state == .fulfilled) return .fulfilled;
    const p = d.promise_const(id) orelse return t.state;
    if (p.state != .pending or now >= p.timeout_at) return .fulfilled;
    return t.state;
}

// ── Reading a request ─────────────────────────────────────────────────────────

/// An accumulating reader over a request's `data`.
///
/// The first complaint wins and every read after it is a no-op, so a handler
/// names the fields it wants in declaration order and checks once. That matters
/// beyond brevity: which violation a caller is told about has to be a function
/// of the request, not of the order a hash map happened to iterate in.
const Parse = struct {
    scratch: std.mem.Allocator,
    data: json.Value,
    failure: ?[]const u8 = null,

    fn init(scratch: std.mem.Allocator, data: json.Value) Parse {
        return .{ .scratch = scratch, .data = data };
    }

    fn set(self: *Parse, message: []const u8) void {
        if (self.failure == null) self.failure = message;
    }

    fn setf(self: *Parse, comptime fmt: []const u8, args: anytype) void {
        if (self.failure != null) return;
        self.failure = std.fmt.allocPrint(self.scratch, fmt, args) catch "Validation failed";
    }

    fn failed(self: *const Parse) bool {
        return self.failure != null;
    }

    /// A missing member, worded as the reference implementation's deserializer
    /// words it — a client that matches on the text keeps working.
    fn missing(self: *Parse, name: []const u8) void {
        self.setf("Invalid request: missing field `{s}`", .{name});
    }

    fn wrong_type(self: *Parse, name: []const u8, expected: []const u8) void {
        self.setf("Invalid request: invalid type for field `{s}`, expected {s}", .{ name, expected });
    }

    /// A required string, with the message for an empty one.
    fn string(self: *Parse, name: []const u8, empty_message: []const u8) []const u8 {
        if (self.failed()) return "";
        const v = self.data.get(name) orelse {
            self.missing(name);
            return "";
        };
        const s = v.as_string() orelse {
            self.wrong_type(name, "a string");
            return "";
        };
        if (s.len == 0) self.set(empty_message);
        return s;
    }

    fn opt_string(self: *Parse, name: []const u8) ?[]const u8 {
        if (self.failed()) return null;
        const v = self.data.get(name) orelse return null;
        if (v.is_null()) return null;
        return v.as_string() orelse {
            self.wrong_type(name, "a string");
            return null;
        };
    }

    /// A required integer, with a minimum and the message for breaching it.
    fn int(self: *Parse, name: []const u8, minimum: i64, below_message: []const u8) i64 {
        if (self.failed()) return 0;
        const v = self.data.get(name) orelse {
            self.missing(name);
            return 0;
        };
        const i = v.as_i64() orelse {
            self.wrong_type(name, "an integer");
            return 0;
        };
        if (i < minimum) self.set(below_message);
        return i;
    }

    fn opt_int(self: *Parse, name: []const u8) ?i64 {
        if (self.failed()) return null;
        const v = self.data.get(name) orelse return null;
        if (v.is_null()) return null;
        return v.as_i64() orelse {
            self.wrong_type(name, "an integer");
            return null;
        };
    }

    fn object(self: *Parse, name: []const u8) ?json.Value {
        if (self.failed()) return null;
        const v = self.data.get(name) orelse {
            self.missing(name);
            return null;
        };
        if (!v.is_object()) {
            self.wrong_type(name, "an object");
            return null;
        }
        return v;
    }

    fn array(self: *Parse, name: []const u8) []json.Value {
        if (self.failed()) return &.{};
        const v = self.data.get(name) orelse {
            self.missing(name);
            return &.{};
        };
        return v.as_array() orelse {
            self.wrong_type(name, "an array");
            return &.{};
        };
    }

    /// A string-to-string map member, defaulting to empty when absent.
    fn map(self: *Parse, name: []const u8) StringMap {
        if (self.failed()) return .empty;
        const v = self.data.get(name) orelse return .empty;
        if (v.is_null()) return .empty;
        return StringMap.from_json(self.scratch, v) catch {
            self.wrong_type(name, "an object of strings");
            return .empty;
        };
    }

    /// A `param` or `value`, defaulting to empty when absent.
    fn promise_value(self: *Parse, name: []const u8) PromiseValue {
        if (self.failed()) return .empty;
        return PromiseValue.from_json(self.scratch, self.data.get(name)) catch {
            self.wrong_type(name, "a value with optional headers and data");
            return .empty;
        };
    }

    fn settle_state(self: *Parse, name: []const u8) SettleState {
        if (self.failed()) return .resolved;
        const v = self.data.get(name) orelse {
            self.missing(name);
            return .resolved;
        };
        const s = v.as_string() orelse {
            self.wrong_type(name, "a string");
            return .resolved;
        };
        return SettleState.parse(s) orelse {
            self.setf(
                "Invalid request: unknown variant `{s}`, expected one of `resolved`, `rejected`, `rejected_canceled`",
                .{s},
            );
            return .resolved;
        };
    }
};

/// The validations `promise.create`'s data carries beyond its field types.
///
/// The lineage tags are prefixes of the id, which is what makes an id say where
/// it came from. Two separators: a bare root joins its first lineage segment
/// with ':', and an ancestor that already carries lineage joins deeper segments
/// with '.'.
fn validate_promise_create(p: *Parse, id: []const u8, timeout_at: i64, tags: StringMap) void {
    if (p.failed()) return;
    if (std.mem.indexOfScalar(u8, id, 0) != null) {
        p.set("Promise ID must not contain null bytes");
        return;
    }
    if (tags.get(protocol.tag_origin)) |og| {
        // The origin is everything before an id's first ':', so an origin that
        // held one would be unrepresentable: no id could split back to it.
        // '.' is deliberately allowed — it separates lineage segments below the
        // origin, so a dotted root id round-trips.
        if (std.mem.indexOfScalar(u8, og, ':') != null) {
            p.set("resonate:origin must not contain ':'");
            return;
        }
        if (!prefixed_by(id, og, ':')) {
            p.set("Promise ID must be prefixed by resonate:origin");
            return;
        }
    }
    if (tags.get(protocol.tag_branch)) |br| {
        if (!prefixed_by(id, br, separator_after(br))) {
            p.set("Promise ID must be prefixed by resonate:branch");
            return;
        }
    }
    if (tags.get(protocol.tag_parent)) |pa| {
        if (!prefixed_by(id, pa, separator_after(pa))) {
            p.set("Promise ID must be prefixed by resonate:parent");
            return;
        }
    }
    if (tags.get(protocol.tag_prefix)) |px| {
        if (std.mem.indexOfScalar(u8, px, ':') != null) {
            p.set("resonate:prefix must not contain ':'");
            return;
        }
    }
    if (tags.get(protocol.tag_delay)) |delay_str| {
        const delay = stdx.parse_i64(delay_str) orelse {
            p.set("resonate:delay must be a non-negative integer");
            return;
        };
        if (delay < 0) {
            p.set("resonate:delay must be a non-negative integer");
            return;
        }
        if (delay >= timeout_at) {
            p.set("resonate:delay must be less than timeoutAt");
            return;
        }
        if (!tags.has(protocol.tag_target)) {
            p.set("resonate:delay requires a resonate:target tag");
            return;
        }
    }
}

fn separator_after(ancestor: []const u8) u8 {
    return if (std.mem.indexOfScalar(u8, ancestor, ':') != null) '.' else ':';
}

fn prefixed_by(id: []const u8, ancestor: []const u8, separator: u8) bool {
    if (std.mem.eql(u8, id, ancestor)) return true;
    if (id.len <= ancestor.len) return false;
    if (!std.mem.startsWith(u8, id, ancestor)) return false;
    return id[ancestor.len] == separator;
}

/// The callback validations: a promise cannot await itself, and awaiting only
/// happens inside one origin — which is what makes a single document enough to
/// commit it.
fn validate_callback(p: *Parse, awaited: []const u8, awaiter: []const u8) void {
    if (p.failed()) return;
    if (std.mem.eql(u8, awaited, awaiter)) {
        p.set("Awaited and awaiter must be different promises");
        return;
    }
    if (!std.mem.eql(u8, protocol.origin(awaiter), protocol.origin(awaited))) {
        p.set("Awaiter and awaited must belong to the same origin");
        return;
    }
}

// ── Creating a promise ────────────────────────────────────────────────────────

/// Everything `promise.create` needs, gathered so that the three operations
/// that create a promise — `promise.create`, `task.create`, `task.fence` —
/// share one implementation rather than three that drift.
const Creation = struct {
    id: []const u8,
    timeout_at: i64,
    param: PromiseValue,
    tags: StringMap,
};

fn read_creation(p: *Parse) Creation {
    const id = p.string("id", "Promise ID is required");
    const timeout_at = p.int("timeoutAt", 0, "TimeoutAt must be a non-negative integer");
    const param = p.promise_value("param");
    const tags = p.map("tags");
    validate_promise_create(p, id, timeout_at, tags);
    return .{ .id = id, .timeout_at = timeout_at, .param = param, .tags = tags };
}

/// Insert a promise that is known not to exist, and arm what it implies.
///
/// A promise created past its own deadline is born settled — the state a
/// deadline produces, `createdAt` and `settledAt` both its `timeoutAt`, and its
/// task, if it has one, already fulfilled. That is not a special case bolted on:
/// the alternative is a promise that exists in a state it could never have been
/// observed in.
fn create_promise(ctx: *Ctx, c: Creation) !*Promise {
    const owned = ctx.owned();
    const already_timedout = ctx.now >= c.timeout_at;
    const tags = try c.tags.clone(owned);
    const state: PromiseState = if (already_timedout) protocol.timeout_state(tags) else .pending;
    const created_at: i64 = if (already_timedout) c.timeout_at else ctx.now;
    const settled_at: ?i64 = if (already_timedout) c.timeout_at else null;
    const address = tags.get(protocol.tag_target);

    _ = try ctx.doc.promise_insert(.{
        .id = try owned.dupe(u8, c.id),
        .state = state,
        .param = try c.param.clone(owned),
        .value = .empty,
        .tags = tags,
        .timeout_at = c.timeout_at,
        .created_at = created_at,
        .settled_at = settled_at,
        // Armed only for a promise that carries a task, and only while pending.
        // A promise nobody can be blocked on costs the server nothing.
        .timeout_armed = !already_timedout and address != null,
        .callbacks = .empty,
        .listeners = .empty,
    });

    if (address) |addr| {
        if (already_timedout) {
            _ = try ctx.doc.task_insert(.{
                .id = try owned.dupe(u8, c.id),
                .state = .fulfilled,
                .version = 0,
                .pid = null,
                .ttl = null,
                .resumes = .empty,
                .timeout = null,
            });
        } else {
            const t = try ctx.doc.task_insert(.{
                .id = try owned.dupe(u8, c.id),
                .state = .pending,
                .version = 0,
                .pid = null,
                .ttl = null,
                .resumes = .empty,
                .timeout = null,
            });
            // A delay says when the work may start, as an absolute instant. Until
            // then the task is pending but unoffered: the retry deadline is the
            // delay itself, and the first offer is the one the deadline makes.
            const delay = blk: {
                const d = tags.get(protocol.tag_delay) orelse break :blk null;
                break :blk stdx.parse_i64(d);
            };
            if (delay != null and ctx.now < delay.?) {
                set_task_timeout(t, .retry, delay.?);
            } else {
                set_task_timeout(t, .retry, created_at + ctx.cfg.pending_retry_ttl);
                try ctx.emit_execute(addr, c.id, 0);
            }
        }
    }
    // The pointer may have moved: inserting the task did not touch the promise
    // list, but returning a fresh lookup is cheap and cannot be stale.
    return ctx.doc.promise(c.id).?;
}

// ── Promise operations ────────────────────────────────────────────────────────

fn op_promise_get(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Promise ID is required");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const promise = ctx.doc.promise(id) orelse return ctx.fail(404, "Promise not found");
    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, ctx.now, promise);
    try w.object_end();
    return ctx.done(200);
}

fn op_promise_create(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const c = read_creation(&p);
    if (p.failure) |message| return ctx.fail(400, message);
    if (c.tags.get(protocol.tag_target)) |addr| {
        if (!protocol.is_valid_address(addr)) return ctx.fail(400, "Invalid resonate:target address");
    }

    try try_timeout(ctx, &.{c.id});
    // Idempotent on the id: creating a promise that exists is not a conflict,
    // it is the same request arriving twice, and the answer is the promise.
    if (ctx.doc.promise(c.id)) |existing| {
        var w = ctx.writer();
        try w.object_begin();
        try w.key("promise");
        try write_promise_record(&w, ctx.now, existing);
        try w.object_end();
        return ctx.done(200);
    }

    const created = try create_promise(ctx, c);
    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    // The record as created, so a promise born already timed out reports the
    // state it was born in rather than being projected a second time.
    try write_promise_record(&w, 0, created);
    try w.object_end();
    return ctx.done(200);
}

fn op_promise_settle(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Promise ID is required");
    const state = p.settle_state("state");
    const value = p.promise_value("value");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const promise = ctx.doc.promise(id) orelse return ctx.fail(404, "Promise not found");
    // Settling a settled promise is idempotent and reports what it holds: the
    // first verdict stands, because something has already acted on it.
    if (promise.state != .pending) {
        var w = ctx.writer();
        try w.object_begin();
        try w.key("promise");
        try write_promise_record(&w, ctx.now, promise);
        try w.object_end();
        return ctx.done(200);
    }

    promise.state = state.to_promise_state();
    promise.value = try value.clone(ctx.owned());
    promise.settled_at = ctx.now;
    promise.timeout_armed = false;

    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, 0, promise);
    try w.object_end();
    const out = try ctx.done(200);
    // After the reply is rendered: the settlement chain mutates the document,
    // and the record answered is the promise, not what it set off.
    try trigger_settlement(ctx, id);
    return .{ .reply = out.reply, .effects = try ctx.effects.toOwnedSlice(ctx.scratch) };
}

fn op_promise_register_callback(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const awaited_id = p.string("awaited", "Awaited promise ID is required");
    const awaiter_id = p.string("awaiter", "Awaiter promise ID is required");
    validate_callback(&p, awaited_id, awaiter_id);
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{ awaited_id, awaiter_id });

    if (ctx.doc.promise(awaited_id) == null) return ctx.fail(404, "Awaited promise not found");
    const awaiter = ctx.doc.promise(awaiter_id) orelse
        return ctx.fail(422, "Awaiter promise not found");
    // Only something with somewhere to run can be resumed.
    if (!awaiter.has_task()) return ctx.fail(422, "Awaiter promise has no resonate:target tag");
    const awaiter_state = awaiter.state;
    const awaiter_address = awaiter.tags.get(protocol.tag_target);

    const awaited = ctx.doc.promise(awaited_id).?;
    if (!awaited.is_external()) return ctx.fail(422, "Awaited promise is not awaitable");
    const awaited_pending = awaited.state == .pending;
    const awaiter_pending = awaiter_state == .pending;

    // The answer is the awaited promise, whatever the registration did.
    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, ctx.now, awaited);
    try w.object_end();
    const rendered = try ctx.scratch.dupe(u8, ctx.buf.items);

    const owned = ctx.owned();
    if (awaited_pending and awaiter_pending) {
        _ = try ctx.doc.promise(awaited_id).?.callbacks.insert(owned, awaiter_id);
    } else if (!awaited_pending and awaiter_pending) {
        // Nothing to wait for: the awaited promise has already settled, so this
        // is a resume rather than a registration. A suspended awaiter is woken;
        // one that is still running only records it — and it *is* recorded
        // either way, which is what a woken worker reads.
        if (ctx.doc.task(awaiter_id)) |t| {
            switch (t.state) {
                .suspended => {
                    t.state = .pending;
                    _ = try t.resumes.insert(owned, awaited_id);
                    set_task_timeout(t, .retry, ctx.now + ctx.cfg.pending_retry_ttl);
                    if (awaiter_address) |addr| try ctx.emit_execute(addr, awaiter_id, t.version);
                },
                .pending, .acquired => {
                    _ = try t.resumes.insert(owned, awaited_id);
                },
                .halted, .fulfilled => {},
            }
        }
    }

    return .{
        .reply = .{ .status = 200, .data = rendered },
        .effects = try ctx.effects.toOwnedSlice(ctx.scratch),
    };
}

fn op_promise_register_listener(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const awaited_id = p.string("awaited", "Awaited promise ID is required");
    const address = p.string("address", "Address is required");
    if (p.failure) |message| return ctx.fail(400, message);
    if (!protocol.is_valid_address(address)) return ctx.fail(400, "Invalid listener address");

    try try_timeout(ctx, &.{awaited_id});
    const awaited = ctx.doc.promise(awaited_id) orelse
        return ctx.fail(404, "Awaited promise not found");
    // A listener is an obligation, and the server owes an observation only
    // where someone can be blocked.
    if (!awaited.is_external()) return ctx.fail(422, "Awaited promise is not awaitable");

    if (awaited.state == .pending) {
        _ = try awaited.listeners.insert(ctx.owned(), address);
    }
    const promise = ctx.doc.promise(awaited_id).?;
    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, ctx.now, promise);
    try w.object_end();
    return ctx.done(200);
}

// ── Task operations ───────────────────────────────────────────────────────────

fn op_task_get(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    const state = effective_task_state(ctx.doc, ctx.now, id) orelse t.state;
    var w = ctx.writer();
    try w.object_begin();
    try w.key("task");
    // A finished task holds no lease, so it reports none — even in the window
    // before anything wrote the state down.
    try write_task_record(
        &w,
        id,
        state,
        t.version,
        @intCast(t.resumes.len()),
        if (state == .fulfilled) null else t.ttl,
        if (state == .fulfilled) null else t.pid,
    );
    try w.object_end();
    return ctx.done(200);
}

/// `task.create`: claim work, creating it if it is not there.
///
/// The caller *is* the worker, so the task comes back already acquired and no
/// offer is sent — there is nobody to offer it to who is not already holding
/// the answer.
fn op_task_create(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const pid = p.string("pid", "Process ID is required");
    const ttl = p.int("ttl", 1, "TTL must be a positive integer");
    const action = p.object("action");
    var c: Creation = .{ .id = "", .timeout_at = 0, .param = .empty, .tags = .empty };
    if (action) |a| {
        var ap = Parse.init(ctx.scratch, a.get("data") orelse json.Value.null_value);
        if (a.get("data") == null or !(a.get("data").?.is_object())) {
            p.wrong_type("action", "an object with an object `data`");
        } else {
            c = read_creation(&ap);
            if (ap.failure) |message| p.set(message);
        }
    }
    if (!p.failed()) {
        if (!c.tags.has(protocol.tag_target)) p.set("Action must have a resonate:target tag");
        if (c.tags.has(protocol.tag_delay)) p.set("Action must not have a resonate:delay tag");
    }
    if (p.failure) |message| return ctx.fail(400, message);
    if (c.tags.get(protocol.tag_target)) |addr| {
        if (!protocol.is_valid_address(addr)) return ctx.fail(400, "Invalid resonate:target address");
    }

    const promise_id = c.id;
    try try_timeout(ctx, &.{promise_id});

    if (ctx.doc.task(promise_id) != null) {
        const effective = effective_task_state(ctx.doc, ctx.now, promise_id).?;
        switch (effective) {
            .pending => {
                const t = ctx.doc.task(promise_id).?;
                t.state = .acquired;
                t.version += 1;
                t.pid = try ctx.owned().dupe(u8, pid);
                t.ttl = ttl;
                t.resumes.clear();
                set_task_timeout(t, .lease, ctx.now + ttl);
            },
            // Finished: report it. A worker asking for work that is already
            // done gets the answer, not a conflict.
            .fulfilled => {},
            // Somebody else holds it, or it is suspended or halted.
            .acquired, .suspended, .halted => return ctx.fail(409, "Already exists"),
        }
        return try reply_task_promise_preload(ctx, promise_id);
    }

    if (ctx.doc.promise(promise_id) != null) {
        // A promise with no task cannot grow one: whether it has a task is
        // decided by its tags, once, when it is created.
        return ctx.fail(422, "The promise does not have a resonate:target tag");
    }

    const owned = ctx.owned();
    const already_timedout = ctx.now >= c.timeout_at;
    const tags = try c.tags.clone(owned);
    _ = try ctx.doc.promise_insert(.{
        .id = try owned.dupe(u8, promise_id),
        .state = if (already_timedout) protocol.timeout_state(tags) else .pending,
        .param = try c.param.clone(owned),
        .value = .empty,
        .tags = tags,
        .timeout_at = c.timeout_at,
        .created_at = if (already_timedout) c.timeout_at else ctx.now,
        .settled_at = if (already_timedout) c.timeout_at else null,
        .timeout_armed = !already_timedout,
        .callbacks = .empty,
        .listeners = .empty,
    });
    _ = try ctx.doc.task_insert(.{
        .id = try owned.dupe(u8, promise_id),
        .state = if (already_timedout) .fulfilled else .acquired,
        // Version 1, not 0: the task is created *and* acquired, and a fence
        // token that never moved would not distinguish this holder from the
        // next one.
        .version = if (already_timedout) 0 else 1,
        .pid = if (already_timedout) null else try owned.dupe(u8, pid),
        .ttl = if (already_timedout) null else ttl,
        .resumes = .empty,
        .timeout = if (already_timedout) null else .{ .kind = .lease, .at = ctx.now + ttl },
    });
    return try reply_task_promise_preload(ctx, promise_id);
}

fn reply_task_promise_preload(ctx: *Ctx, id: []const u8) !Outcome {
    const t = ctx.doc.task(id).?;
    const promise = ctx.doc.promise(id).?;
    var w = ctx.writer();
    try w.object_begin();
    try w.key("task");
    try write_task(&w, t);
    try w.key("promise");
    try write_promise_record(&w, 0, promise);
    try w.key("preload");
    try write_preload(&w, ctx.doc, id, ctx.cfg.preload_limit);
    try w.object_end();
    return ctx.done(200);
}

fn op_task_acquire(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    const version = p.int("version", 0, "Version must be a non-negative integer");
    const pid = p.string("pid", "Process ID is required");
    const ttl = p.int("ttl", 1, "TTL must be a positive integer");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .pending) return ctx.fail(409, "Task is not pending");
    // The fence token. A worker holding a stale offer is refused rather than
    // handed a lease that would race the holder.
    if (t.version != version) return ctx.fail(409, "Version mismatch");
    if (ctx.doc.promise(id) == null) return ctx.fail(404, "Task not found");

    t.state = .acquired;
    t.version = version + 1;
    t.pid = try ctx.owned().dupe(u8, pid);
    t.ttl = ttl;
    t.resumes.clear();
    set_task_timeout(t, .lease, ctx.now + ttl);
    return try reply_task_promise_preload(ctx, id);
}

fn op_task_release(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    const version = p.int("version", 0, "Version must be a non-negative integer");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .acquired or t.version != version)
        return ctx.fail(409, "Task version mismatch or invalid state");

    const address = if (ctx.doc.promise(id)) |pr| pr.tags.get(protocol.tag_target) else null;
    const current_version = t.version;
    t.state = .pending;
    t.pid = null;
    t.ttl = null;
    // Releasing does not bump the version: the work was not done, so the offer
    // that goes out is the same offer.
    set_task_timeout(t, .retry, ctx.now + ctx.cfg.pending_retry_ttl);
    if (address) |addr| try ctx.emit_execute(addr, id, current_version);
    return ctx.ok_empty();
}

fn op_task_fulfill(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    const version = p.int("version", 0, "Version must be a non-negative integer");
    var action_id: []const u8 = "";
    var settle: SettleState = .resolved;
    var value: PromiseValue = .empty;
    if (p.object("action")) |a| {
        const ad = a.get("data");
        if (ad == null or !ad.?.is_object()) {
            p.wrong_type("action", "an object with an object `data`");
        } else {
            var ap = Parse.init(ctx.scratch, ad.?);
            action_id = ap.string("id", "Promise ID is required");
            settle = ap.settle_state("state");
            value = ap.promise_value("value");
            if (ap.failure) |message| p.set(message);
        }
    }
    if (!p.failed() and !std.mem.eql(u8, action_id, id)) p.set("Action ID must match the task ID");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{action_id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .acquired or t.version != version)
        return ctx.fail(409, "Task version mismatch or invalid state");

    const promise = ctx.doc.promise(action_id) orelse return ctx.fail(404, "Promise not found");
    if (promise.state != .pending) {
        // Already settled by someone else, or by its own deadline. The holder
        // still gets its task closed out, and the answer is the verdict that
        // stands rather than the one it offered.
        var w = ctx.writer();
        try w.object_begin();
        try w.key("promise");
        try write_promise_record(&w, ctx.now, promise);
        try w.object_end();
        const rendered = try ctx.scratch.dupe(u8, ctx.buf.items);
        try trigger_fulfilled(ctx, id);
        return .{
            .reply = .{ .status = 200, .data = rendered },
            .effects = try ctx.effects.toOwnedSlice(ctx.scratch),
        };
    }

    promise.state = settle.to_promise_state();
    promise.value = try value.clone(ctx.owned());
    promise.settled_at = ctx.now;
    promise.timeout_armed = false;
    var w = ctx.writer();
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, 0, promise);
    try w.object_end();
    const rendered = try ctx.scratch.dupe(u8, ctx.buf.items);
    try trigger_settlement(ctx, action_id);
    return .{
        .reply = .{ .status = 200, .data = rendered },
        .effects = try ctx.effects.toOwnedSlice(ctx.scratch),
    };
}

/// `task.suspend`: stop running and block on a set of promises.
///
/// The 300 is the interesting case. If any awaited promise has *already*
/// settled there is nothing to block on, so the task keeps its lease and is
/// told to carry on — with the branch siblings preloaded, since that is what it
/// was about to go and read.
fn op_task_suspend(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    const version = p.int("version", 0, "Version must be a non-negative integer");
    const actions = p.array("actions");
    if (!p.failed() and actions.len == 0) p.set("Actions array cannot be empty");

    var awaited = std.ArrayList([]const u8).init(ctx.scratch);
    if (!p.failed()) {
        for (actions) |a| {
            const ad = a.get("data");
            if (ad == null or !ad.?.is_object()) {
                p.wrong_type("actions", "objects with an object `data`");
                break;
            }
            var ap = Parse.init(ctx.scratch, ad.?);
            const aw = ap.string("awaited", "Awaited promise ID is required");
            const awaiter = ap.string("awaiter", "Awaiter promise ID is required");
            validate_callback(&ap, aw, awaiter);
            if (ap.failure) |message| {
                p.set(message);
                break;
            }
            try awaited.append(aw);
        }
    }
    if (!p.failed()) {
        for (actions) |a| {
            const awaiter = a.get("data").?.get_string("awaiter").?;
            if (!std.mem.eql(u8, awaiter, id)) {
                p.set("All action awaiter IDs must match the task ID");
                break;
            }
        }
    }
    if (!p.failed()) {
        for (awaited.items) |aw| {
            if (std.mem.eql(u8, aw, id)) {
                p.set("Action awaited promise must not equal the task ID");
                break;
            }
        }
    }
    if (!p.failed()) {
        // Whether the same promise is named twice is decided by the request
        // alone, so it is decided here. One suspend awaiting `p` twice and one
        // awaiting it once are different requests, and only the second is well
        // formed — deduplicating on the way in would turn a request the caller
        // did not mean into a silent success.
        for (awaited.items, 0..) |a, i| {
            for (awaited.items[i + 1 ..]) |b| {
                if (std.mem.eql(u8, a, b)) {
                    p.set("Awaited promise IDs must be unique");
                    break;
                }
            }
            if (p.failed()) break;
        }
    }
    if (!p.failed()) {
        for (awaited.items) |aw| {
            if (!std.mem.eql(u8, protocol.origin(aw), protocol.origin(id))) {
                p.set("Awaited promise must belong to the same origin as the task");
                break;
            }
        }
    }
    if (p.failure) |message| return ctx.fail(400, message);

    {
        var ids = std.ArrayList([]const u8).init(ctx.scratch);
        try ids.append(id);
        try ids.appendSlice(awaited.items);
        try try_timeout(ctx, ids.items);
    }

    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .acquired or t.version != version)
        return ctx.fail(409, "Task is not acquired or version mismatch");

    for (awaited.items) |aw| {
        if (ctx.doc.promise(aw) == null) return ctx.fail(422, "Awaited promise not found");
    }
    for (awaited.items) |aw| {
        if (!ctx.doc.promise(aw).?.is_external()) return ctx.fail(422, "Awaited promise is not awaitable");
    }

    var any_settled = false;
    for (awaited.items) |aw| {
        if (ctx.doc.promise(aw).?.state != .pending) any_settled = true;
    }
    if (any_settled) {
        ctx.doc.task(id).?.resumes.clear();
        var w = ctx.writer();
        try w.object_begin();
        try w.key("preload");
        try write_preload(&w, ctx.doc, id, ctx.cfg.preload_limit);
        try w.object_end();
        return ctx.done(300);
    }

    const owned = ctx.owned();
    for (awaited.items) |aw| {
        _ = try ctx.doc.promise(aw).?.callbacks.insert(owned, id);
    }
    const task = ctx.doc.task(id).?;
    task.state = .suspended;
    task.pid = null;
    task.ttl = null;
    task.resumes.clear();
    // No deadline: a suspended task is waiting on a promise, and every promise
    // has a deadline of its own. A retry here would offer work nobody can do.
    clear_task_timeout(task);
    return ctx.ok_empty();
}

/// `task.fence`: do something else, but only while still holding this task.
///
/// The fence is what makes a worker that lost its lease harmless: the action
/// and the version check commit together, in one write, so an action from a
/// holder that has been superseded cannot land. That is also why the action has
/// to name the task's own origin — a cross-origin action would need two
/// objects, and this design commits one.
fn op_task_fence(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    const version = p.int("version", 0, "Version must be a non-negative integer");
    const action = p.object("action");
    var action_kind: []const u8 = "";
    var action_data: json.Value = json.Value.null_value;
    if (action) |a| {
        action_kind = blk: {
            const k = a.get("kind") orelse {
                p.missing("kind");
                break :blk "";
            };
            break :blk k.as_string() orelse {
                p.wrong_type("kind", "a string");
                break :blk "";
            };
        };
        action_data = a.get("data") orelse blk: {
            p.missing("data");
            break :blk json.Value.null_value;
        };
    }
    const action_id: []const u8 = blk: {
        if (action_data.is_null()) break :blk "";
        break :blk action_data.get_string("id") orelse "";
    };
    if (!p.failed() and action_id.len > 0 and std.mem.eql(u8, action_id, id)) {
        p.set("Action ID must not equal the task ID");
    }
    if (p.failure) |message| return ctx.fail(400, message);

    // This backend's rule, not the protocol's: the version check and the action
    // commit as one write on one document, so an action naming another origin
    // has no atomic home here.
    if (action_id.len > 0 and !std.mem.eql(u8, protocol.origin(action_id), protocol.origin(id))) {
        return ctx.fail(400, "Action must belong to the task's origin");
    }

    try try_timeout(ctx, &.{ id, action_id });
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .acquired or t.version != version) return ctx.fail(409, "Version mismatch");

    if (std.mem.eql(u8, action_kind, "promise.create")) {
        var ap = Parse.init(ctx.scratch, action_data);
        const c = read_creation(&ap);
        if (ap.failure) |message| return ctx.fail(400, message);
        if (c.tags.get(protocol.tag_target)) |addr| {
            if (!protocol.is_valid_address(addr)) return ctx.fail(400, "Invalid resonate:target address");
        }
        try try_timeout(ctx, &.{c.id});
        const existing = ctx.doc.promise(c.id);
        const inner_now: i64 = if (existing == null) 0 else ctx.now;
        const promise = if (existing) |e| e else try create_promise(ctx, c);
        return try reply_fence(ctx, id, action_kind, 200, promise, inner_now);
    }
    if (std.mem.eql(u8, action_kind, "promise.settle")) {
        var ap = Parse.init(ctx.scratch, action_data);
        const settle_id = ap.string("id", "Promise ID is required");
        const settle = ap.settle_state("state");
        const value = ap.promise_value("value");
        if (ap.failure) |message| return ctx.fail(400, message);

        var settled_now = false;
        if (ctx.doc.promise(settle_id)) |promise| {
            if (promise.state == .pending) {
                promise.state = settle.to_promise_state();
                promise.value = try value.clone(ctx.owned());
                promise.settled_at = ctx.now;
                promise.timeout_armed = false;
                settled_now = true;
            }
        }
        // Rendered before the chain runs, and the chain after: the inner answer
        // is the promise, not what settling it set off.
        const promise_opt = ctx.doc.promise(settle_id);
        if (promise_opt == null) {
            return try reply_fence_missing(ctx, id, action_kind);
        }
        const out = try reply_fence(ctx, id, action_kind, 200, promise_opt.?, if (settled_now) 0 else ctx.now);
        if (settled_now) {
            try trigger_settlement(ctx, settle_id);
            return .{ .reply = out.reply, .effects = try ctx.effects.toOwnedSlice(ctx.scratch) };
        }
        return out;
    }
    return ctx.fail(400, "Invalid fence action kind");
}

fn reply_fence(
    ctx: *Ctx,
    task_id: []const u8,
    action_kind: []const u8,
    inner_status: i32,
    promise: *const Promise,
    inner_now: i64,
) !Outcome {
    var w = ctx.writer();
    try w.object_begin();
    try w.key("action");
    try w.object_begin();
    try w.field_string("kind", action_kind);
    try w.key("head");
    try w.object_begin();
    try w.field_string("corrId", ctx.corr_id);
    try w.field_int("status", inner_status);
    try w.field_string("version", protocol.protocol_version);
    try w.object_end();
    try w.key("data");
    try w.object_begin();
    try w.key("promise");
    try write_promise_record(&w, inner_now, promise);
    try w.object_end();
    try w.object_end();
    try w.key("preload");
    try write_preload(&w, ctx.doc, task_id, ctx.cfg.preload_limit);
    try w.object_end();
    return ctx.done(200);
}

fn reply_fence_missing(ctx: *Ctx, task_id: []const u8, action_kind: []const u8) !Outcome {
    var w = ctx.writer();
    try w.object_begin();
    try w.key("action");
    try w.object_begin();
    try w.field_string("kind", action_kind);
    try w.key("head");
    try w.object_begin();
    try w.field_string("corrId", ctx.corr_id);
    try w.field_int("status", 404);
    try w.field_string("version", protocol.protocol_version);
    try w.object_end();
    try w.field_string("data", "Promise not found");
    try w.object_end();
    try w.key("preload");
    try write_preload(&w, ctx.doc, task_id, ctx.cfg.preload_limit);
    try w.object_end();
    return ctx.done(200);
}

/// `task.heartbeat`: I am still working on these.
///
/// The one operation that does not settle ghosts first, so it compensates by
/// refusing to extend the lease of a task whose promise has already passed its
/// deadline — otherwise a lease would outlive the work it protects for as long
/// as it took the sweep to arrive.
fn op_task_heartbeat(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const pid = p.string("pid", "Process ID is required");
    const tasks = p.array("tasks");
    if (!p.failed() and tasks.len == 0) p.set("Tasks array must not be empty");
    var refs = std.ArrayList(struct { id: []const u8, version: i64 }).init(ctx.scratch);
    if (!p.failed()) {
        for (tasks) |t| {
            const id = blk: {
                const v = t.get("id") orelse {
                    p.missing("id");
                    break :blk "";
                };
                break :blk v.as_string() orelse {
                    p.wrong_type("id", "a string");
                    break :blk "";
                };
            };
            const version = blk: {
                const v = t.get("version") orelse {
                    p.missing("version");
                    break :blk @as(i64, 0);
                };
                break :blk v.as_i64() orelse {
                    p.wrong_type("version", "an integer");
                    break :blk @as(i64, 0);
                };
            };
            if (p.failed()) break;
            try refs.append(.{ .id = id, .version = version });
        }
    }
    if (!p.failed() and refs.items.len > 1) {
        const first = protocol.origin(refs.items[0].id);
        for (refs.items[1..]) |r| {
            if (!std.mem.eql(u8, protocol.origin(r.id), first)) {
                p.set("All tasks must belong to the same origin");
                break;
            }
        }
    }
    if (p.failure) |message| return ctx.fail(400, message);

    for (refs.items) |r| {
        const promise_live = blk: {
            const promise = ctx.doc.promise_const(r.id) orelse break :blk false;
            break :blk promise.state != .pending or promise.timeout_at > ctx.now;
        };
        if (!promise_live) continue;
        const t = ctx.doc.task(r.id) orelse continue;
        if (t.state != .acquired) continue;
        if (t.version != r.version) continue;
        const holder = t.pid orelse continue;
        if (!std.mem.eql(u8, holder, pid)) continue;
        const ttl = t.ttl orelse continue;
        set_task_timeout(t, .lease, ctx.now + ttl);
    }
    return ctx.ok_empty();
}

fn op_task_halt(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    // Finished is not halted, and pretending otherwise would let a caller
    // un-finish work.
    if (t.state == .fulfilled) return ctx.fail(409, "Task is fulfilled");
    if (t.state == .halted) return ctx.ok_empty();
    t.state = .halted;
    t.pid = null;
    t.ttl = null;
    clear_task_timeout(t);
    return ctx.ok_empty();
}

fn op_task_continue(ctx: *Ctx, data: json.Value) !Outcome {
    var p = Parse.init(ctx.scratch, data);
    const id = p.string("id", "Task ID is required");
    if (p.failure) |message| return ctx.fail(400, message);

    try try_timeout(ctx, &.{id});
    const t = ctx.doc.task(id) orelse return ctx.fail(404, "Task not found");
    if (t.state != .halted) return ctx.fail(409, "Task is not halted");
    const address = if (ctx.doc.promise(id)) |pr| pr.tags.get(protocol.tag_target) else null;
    const version = t.version;
    t.state = .pending;
    set_task_timeout(t, .retry, ctx.now + ctx.cfg.pending_retry_ttl);
    if (address) |addr| try ctx.emit_execute(addr, id, version);
    return ctx.ok_empty();
}

// ── The sweep ─────────────────────────────────────────────────────────────────

/// Fire everything in this document that is due.
///
/// Collect first, then mutate. Three passes over three kinds of deadline, and
/// within the promise deadlines two phases — settle them all, then run the
/// settlement chains — because a chain can reach a promise that is itself due,
/// and a single interleaved pass would make the answer depend on the order the
/// document happened to be in.
pub fn drain(d: *Doc, now: i64, cfg: Config, scratch: std.mem.Allocator) !Outcome {
    var ctx = Ctx.init(d, now, cfg, scratch);

    var expired = std.ArrayList([]const u8).init(scratch);
    for (d.promises.items) |*p| {
        if (p.timeout_armed and now >= p.timeout_at) try expired.append(p.id);
    }
    var leases = std.ArrayList(struct { id: []const u8, version: i64 }).init(scratch);
    var retries = std.ArrayList([]const u8).init(scratch);
    for (d.tasks.items) |*t| {
        const to = t.timeout orelse continue;
        if (now < to.at) continue;
        switch (to.kind) {
            .lease => if (t.state == .acquired) try leases.append(.{ .id = t.id, .version = t.version }),
            .retry => if (t.state == .pending) try retries.append(t.id),
        }
    }

    for (expired.items) |id| {
        const p = d.promise(id).?;
        p.state = p.timeout_state();
        p.settled_at = p.timeout_at;
        p.timeout_armed = false;
    }
    for (expired.items) |id| {
        try trigger_settlement(&ctx, id);
    }

    // A lost lease returns the work to the queue and offers it again. The
    // version is unchanged: the work was not done.
    for (leases.items) |l| {
        const t = d.task(l.id) orelse continue;
        if (t.state != .acquired or t.version != l.version) continue;
        const address = if (d.promise(l.id)) |pr| pr.tags.get(protocol.tag_target) else null;
        const version = t.version;
        t.state = .pending;
        t.pid = null;
        t.ttl = null;
        set_task_timeout(t, .retry, now + cfg.pending_retry_ttl);
        if (address) |addr| try ctx.emit_execute(addr, l.id, version);
    }

    // A pending task nobody picked up is offered again. Nothing about the task
    // changes but its next deadline — the offer may simply have been lost.
    for (retries.items) |id| {
        const t = d.task(id) orelse continue;
        if (t.state != .pending) continue;
        const address = if (d.promise(id)) |pr| pr.tags.get(protocol.tag_target) else null;
        const version = t.version;
        set_task_timeout(t, .retry, now + cfg.pending_retry_ttl);
        if (address) |addr| try ctx.emit_execute(addr, id, version);
    }

    return .{
        .reply = .{ .status = 200, .data = "{}" },
        .effects = try ctx.effects.toOwnedSlice(scratch),
    };
}

/// Create the promise a schedule fires.
///
/// Not `promise.create`: the instant that matters is the scheduled one, not the
/// one the sweep happens to run at, so `createdAt` is the tick the schedule was
/// due for even when that is in the past. A delay tag is not honoured either —
/// a schedule already says when.
pub fn schedule_fire(
    d: *Doc,
    promise_id: []const u8,
    fired_at: i64,
    promise_timeout: i64,
    param: PromiseValue,
    tags: StringMap,
    now: i64,
    cfg: Config,
    scratch: std.mem.Allocator,
) !Outcome {
    var ctx = Ctx.init(d, now, cfg, scratch);
    if (d.promise(promise_id) != null) {
        // The schedule already fired for this instant. Two servers sweeping the
        // same schedule must not produce two runs.
        return .{ .reply = .{ .status = 200, .data = "{}" }, .effects = &.{} };
    }
    const owned = d.allocator();
    const timeout_at = fired_at + promise_timeout;
    const already_timedout = fired_at >= timeout_at;
    const owned_tags = try tags.clone(owned);
    const address = owned_tags.get(protocol.tag_target);
    _ = try d.promise_insert(.{
        .id = try owned.dupe(u8, promise_id),
        .state = if (already_timedout) protocol.timeout_state(owned_tags) else .pending,
        .param = try param.clone(owned),
        .value = .empty,
        .tags = owned_tags,
        .timeout_at = timeout_at,
        .created_at = fired_at,
        .settled_at = if (already_timedout) timeout_at else null,
        .timeout_armed = !already_timedout and address != null,
        .callbacks = .empty,
        .listeners = .empty,
    });
    if (address) |addr| {
        if (already_timedout) {
            _ = try d.task_insert(.{
                .id = try owned.dupe(u8, promise_id),
                .state = .fulfilled,
                .version = 0,
                .pid = null,
                .ttl = null,
                .resumes = .empty,
                .timeout = null,
            });
        } else {
            _ = try d.task_insert(.{
                .id = try owned.dupe(u8, promise_id),
                .state = .pending,
                .version = 0,
                .pid = null,
                .ttl = null,
                .resumes = .empty,
                .timeout = .{ .kind = .retry, .at = now + cfg.pending_retry_ttl },
            });
            try ctx.emit_execute(addr, promise_id, 0);
        }
    }
    return .{
        .reply = .{ .status = 200, .data = "{}" },
        .effects = try ctx.effects.toOwnedSlice(scratch),
    };
}

// ── Dispatch ──────────────────────────────────────────────────────────────────

/// The operations one origin's document can answer on its own.
///
/// Everything else the protocol has — the searches, the schedules, the debug
/// namespace — spans origins, so it is answered a layer up, over a listing.
pub const Op = enum {
    promise_get,
    promise_create,
    promise_settle,
    promise_register_callback,
    promise_register_listener,
    task_get,
    task_create,
    task_acquire,
    task_release,
    task_fulfill,
    task_suspend,
    task_fence,
    task_heartbeat,
    task_halt,
    task_continue,

    pub fn parse(kind: []const u8) ?Op {
        const table = .{
            .{ "promise.get", Op.promise_get },
            .{ "promise.create", Op.promise_create },
            .{ "promise.settle", Op.promise_settle },
            .{ "promise.register_callback", Op.promise_register_callback },
            .{ "promise.register_listener", Op.promise_register_listener },
            .{ "task.get", Op.task_get },
            .{ "task.create", Op.task_create },
            .{ "task.acquire", Op.task_acquire },
            .{ "task.release", Op.task_release },
            .{ "task.fulfill", Op.task_fulfill },
            .{ "task.suspend", Op.task_suspend },
            .{ "task.fence", Op.task_fence },
            .{ "task.heartbeat", Op.task_heartbeat },
            .{ "task.halt", Op.task_halt },
            .{ "task.continue", Op.task_continue },
        };
        inline for (table) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) return entry[1];
        }
        return null;
    }
};

/// Which document this request belongs to.
///
/// Every operation names one origin; which field carries it differs, and this is
/// the whole of that knowledge. A registration is routed by its *awaiter*,
/// because the awaiter is what the operation changes; a listener by its awaited,
/// for the same reason.
///
/// Returns null when the request does not name an id at all, which is a request
/// that will be refused for exactly that reason once it is parsed — so the
/// caller routes it anywhere and lets the state machine answer.
pub fn origin_of_request(op: Op, data: json.Value) ?[]const u8 {
    const field: []const u8 = switch (op) {
        .promise_register_callback => "awaiter",
        .promise_register_listener => "awaited",
        .task_create => return blk: {
            const action = data.get("action") orelse break :blk null;
            const action_data = action.get("data") orelse break :blk null;
            const id = action_data.get_string("id") orelse break :blk null;
            break :blk protocol.origin(id);
        },
        .task_heartbeat => return blk: {
            const tasks_v = data.get("tasks") orelse break :blk null;
            const tasks = tasks_v.as_array() orelse break :blk null;
            if (tasks.len == 0) break :blk null;
            const id = tasks[0].get_string("id") orelse break :blk null;
            break :blk protocol.origin(id);
        },
        else => "id",
    };
    const id = data.get_string(field) orelse return null;
    return protocol.origin(id);
}

/// Apply one request to one document.
pub fn handle(
    d: *Doc,
    op: Op,
    corr_id: []const u8,
    data: json.Value,
    now: i64,
    cfg: Config,
    scratch: std.mem.Allocator,
) !Outcome {
    var ctx = Ctx.init(d, now, cfg, scratch);
    ctx.corr_id = corr_id;
    // The clock is a hint, and a hint that went backwards would be worse than
    // none: it is what a reader uses to tell a stale document from a fresh one.
    if (now > d.clock) d.clock = now;
    const outcome = switch (op) {
        .promise_get => try op_promise_get(&ctx, data),
        .promise_create => try op_promise_create(&ctx, data),
        .promise_settle => try op_promise_settle(&ctx, data),
        .promise_register_callback => try op_promise_register_callback(&ctx, data),
        .promise_register_listener => try op_promise_register_listener(&ctx, data),
        .task_get => try op_task_get(&ctx, data),
        .task_create => try op_task_create(&ctx, data),
        .task_acquire => try op_task_acquire(&ctx, data),
        .task_release => try op_task_release(&ctx, data),
        .task_fulfill => try op_task_fulfill(&ctx, data),
        .task_suspend => try op_task_suspend(&ctx, data),
        .task_fence => try op_task_fence(&ctx, data),
        .task_heartbeat => try op_task_heartbeat(&ctx, data),
        .task_halt => try op_task_halt(&ctx, data),
        .task_continue => try op_task_continue(&ctx, data),
    };
    // Every decision ends by re-seating the deadline, so that no transition has
    // to remember to and none can forget.
    d.reseat_timer();
    return outcome;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// A test harness: a document, an arena for the requests, and a `now` the test
/// moves by hand.
const Fixture = struct {
    doc: Doc,
    arena: std.heap.ArenaAllocator,
    now: i64 = 1_000_000_000,
    cfg: Config = .{},
    last_effects: []const Effect = &.{},

    fn init() Fixture {
        return .{
            .doc = Doc.init(testing.allocator),
            .arena = std.heap.ArenaAllocator.init(testing.allocator),
        };
    }

    fn deinit(self: *Fixture) void {
        self.doc.deinit();
        self.arena.deinit();
    }

    fn call(self: *Fixture, kind: []const u8, data_json: []const u8) !struct { status: i32, data: []const u8 } {
        const a = self.arena.allocator();
        const data = try json.parse(a, data_json);
        const op = Op.parse(kind).?;
        const outcome = try handle(&self.doc, op, "c1", data, self.now, self.cfg, a);
        self.last_effects = outcome.effects;
        return .{ .status = outcome.reply.status, .data = outcome.reply.data };
    }

    fn sweep(self: *Fixture) !void {
        const outcome = try drain(&self.doc, self.now, self.cfg, self.arena.allocator());
        self.last_effects = outcome.effects;
        self.doc.reseat_timer();
    }

    fn executes(self: *const Fixture) usize {
        var n: usize = 0;
        for (self.last_effects) |e| {
            if (e.kind == .execute) n += 1;
        }
        return n;
    }
};

const far_future = 9_000_000_000_000;

test "a promise is created once and read back" {
    var f = Fixture.init();
    defer f.deinit();

    const created = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"param":{"data":"x"},"tags":{"k":"v"}}
    );
    try testing.expectEqual(@as(i32, 200), created.status);
    try testing.expect(std.mem.indexOf(u8, created.data, "\"state\":\"pending\"") != null);
    try testing.expect(std.mem.indexOf(u8, created.data, "\"createdAt\":1000000000") != null);
    // No target, so no task and no offer.
    try testing.expectEqual(@as(usize, 0), f.executes());
    try testing.expect(f.doc.task("o:a") == null);

    // Creating it again is the same request arriving twice.
    const again = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":1,"param":{},"tags":{}}
    );
    try testing.expectEqual(@as(i32, 200), again.status);
    try testing.expectEqualStrings(created.data, again.data);

    const got = try f.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 200), got.status);
    try testing.expectEqualStrings(created.data, got.data);

    const missing = try f.call("promise.get", "{\"id\":\"o:nope\"}");
    try testing.expectEqual(@as(i32, 404), missing.status);
    try testing.expectEqualStrings("\"Promise not found\"", missing.data);
}

test "a target makes a task, an offer, and a retry deadline" {
    var f = Fixture.init();
    defer f.deinit();
    const r = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expectEqual(@as(usize, 1), f.executes());
    try testing.expectEqualStrings("poll://any@g", f.last_effects[0].address);
    try testing.expectEqualStrings("o:a", f.last_effects[0].task_id);
    try testing.expectEqual(@as(i64, 0), f.last_effects[0].version);

    const t = f.doc.task("o:a").?;
    try testing.expectEqual(TaskState.pending, t.state);
    try testing.expectEqual(@as(i64, 0), t.version);
    try testing.expectEqual(TaskTimeoutKind.retry, t.timeout.?.kind);
    try testing.expectEqual(f.now + protocol.pending_retry_ttl, t.timeout.?.at);
    try testing.expect(f.doc.promise("o:a").?.timeout_armed);
    // The earliest live deadline is the retry, not the far-off promise timeout.
    try testing.expectEqual(f.now + protocol.pending_retry_ttl, f.doc.timer_at.?);
}

test "a delayed promise waits before it is offered" {
    var f = Fixture.init();
    defer f.deinit();
    const delay = f.now + 5_000;
    const body = try std.fmt.allocPrint(f.arena.allocator(),
        "{{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{{\"resonate:target\":\"poll://any@g\",\"resonate:delay\":\"{d}\"}}}}",
        .{delay},
    );
    _ = try f.call("promise.create", body);
    try testing.expectEqual(@as(usize, 0), f.executes());
    try testing.expectEqual(delay, f.doc.task("o:a").?.timeout.?.at);

    // The deadline is what makes the first offer.
    f.now = delay;
    try f.sweep();
    try testing.expectEqual(@as(usize, 1), f.executes());
}

test "a promise created past its deadline is born settled" {
    var f = Fixture.init();
    defer f.deinit();
    const r = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":5,"tags":{"resonate:target":"poll://any@g"}}
    );
    try testing.expect(std.mem.indexOf(u8, r.data, "\"state\":\"rejected_timedout\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"createdAt\":5") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"settledAt\":5") != null);
    try testing.expectEqual(TaskState.fulfilled, f.doc.task("o:a").?.state);
    try testing.expectEqual(@as(usize, 0), f.executes());
    try testing.expect(f.doc.timer_at == null);
}

test "a timer resolves rather than rejects when its deadline falls" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":5,"tags":{"resonate:timer":"true"}}
    );
    try testing.expectEqual(PromiseState.resolved, f.doc.promise("o:t").?.state);
}

test "settling is idempotent and the first verdict stands" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    const first = try f.call("promise.settle",
        \\{"id":"o:a","state":"resolved","value":{"data":"done"}}
    );
    try testing.expectEqual(@as(i32, 200), first.status);
    try testing.expect(std.mem.indexOf(u8, first.data, "\"state\":\"resolved\"") != null);
    try testing.expect(std.mem.indexOf(u8, first.data, "\"settledAt\":1000000000") != null);

    const second = try f.call("promise.settle",
        \\{"id":"o:a","state":"rejected","value":{"data":"no"}}
    );
    try testing.expectEqual(@as(i32, 200), second.status);
    try testing.expectEqualStrings(first.data, second.data);

    const nope = try f.call("promise.settle", "{\"id\":\"o:x\",\"state\":\"resolved\"}");
    try testing.expectEqual(@as(i32, 404), nope.status);
}

test "a lease is acquired, heartbeaten, released and re-offered" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    const acquired = try f.call("task.acquire",
        \\{"id":"o:a","version":0,"pid":"w1","ttl":60000}
    );
    try testing.expectEqual(@as(i32, 200), acquired.status);
    try testing.expect(std.mem.indexOf(u8, acquired.data, "\"version\":1") != null);
    try testing.expect(std.mem.indexOf(u8, acquired.data, "\"pid\":\"w1\"") != null);
    const t = f.doc.task("o:a").?;
    try testing.expectEqual(TaskTimeoutKind.lease, t.timeout.?.kind);
    try testing.expectEqual(f.now + 60_000, t.timeout.?.at);

    // The stale offer is refused.
    const stale = try f.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w2\",\"ttl\":1}");
    try testing.expectEqual(@as(i32, 409), stale.status);
    try testing.expectEqualStrings("\"Task is not pending\"", stale.data);

    // A heartbeat from the holder extends the lease; one from anyone else does not.
    f.now += 1_000;
    _ = try f.call("task.heartbeat", "{\"pid\":\"w2\",\"tasks\":[{\"id\":\"o:a\",\"version\":1}]}");
    try testing.expectEqual(@as(i64, 1_000_000_000 + 60_000), f.doc.task("o:a").?.timeout.?.at);
    _ = try f.call("task.heartbeat", "{\"pid\":\"w1\",\"tasks\":[{\"id\":\"o:a\",\"version\":1}]}");
    try testing.expectEqual(f.now + 60_000, f.doc.task("o:a").?.timeout.?.at);

    // Releasing offers it again at the same version.
    const released = try f.call("task.release", "{\"id\":\"o:a\",\"version\":1}");
    try testing.expectEqual(@as(i32, 200), released.status);
    try testing.expectEqualStrings("{}", released.data);
    try testing.expectEqual(@as(usize, 1), f.executes());
    try testing.expectEqual(@as(i64, 1), f.last_effects[0].version);
    try testing.expectEqual(TaskState.pending, f.doc.task("o:a").?.state);
    try testing.expectEqual(TaskTimeoutKind.retry, f.doc.task("o:a").?.timeout.?.kind);
}

test "an expired lease returns the work to the queue" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w1\",\"ttl\":1000}");
    f.now += 1_000;
    try f.sweep();
    try testing.expectEqual(TaskState.pending, f.doc.task("o:a").?.state);
    try testing.expect(f.doc.task("o:a").?.pid == null);
    try testing.expectEqual(@as(usize, 1), f.executes());
    // The version did not move: the work was not done.
    try testing.expectEqual(@as(i64, 1), f.doc.task("o:a").?.version);
}

test "fulfilling settles the promise and finishes the task" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");
    const r = try f.call("task.fulfill",
        \\{"id":"o:a","version":1,"action":{"kind":"promise.settle","head":{},"data":{"id":"o:a","state":"resolved","value":{"data":"42"}}}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"state\":\"resolved\"") != null);
    try testing.expectEqual(PromiseState.resolved, f.doc.promise("o:a").?.state);
    try testing.expectEqual(TaskState.fulfilled, f.doc.task("o:a").?.state);
    try testing.expect(f.doc.task("o:a").?.timeout == null);
    try testing.expect(f.doc.timer_at == null);

    // The action's id has to be the task's.
    const wrong = try f.call("task.fulfill",
        \\{"id":"o:a","version":1,"action":{"kind":"promise.settle","head":{},"data":{"id":"o:b","state":"resolved"}}}
    );
    try testing.expectEqual(@as(i32, 400), wrong.status);
    try testing.expectEqualStrings("\"Action ID must match the task ID\"", wrong.data);
}

test "suspending blocks on a promise and settling it resumes the worker" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");
    _ = try f.call("promise.create",
        \\{"id":"o:child","timeoutAt":9000000000000,"tags":{"resonate:scope":"global"}}
    );

    const suspended = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"promise.register_callback","head":{},"data":{"awaited":"o:child","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(@as(i32, 200), suspended.status);
    try testing.expectEqual(TaskState.suspended, f.doc.task("o:t").?.state);
    try testing.expect(f.doc.task("o:t").?.timeout == null);
    try testing.expect(f.doc.promise("o:child").?.callbacks.contains("o:t"));

    _ = try f.call("promise.settle", "{\"id\":\"o:child\",\"state\":\"resolved\"}");
    try testing.expectEqual(TaskState.pending, f.doc.task("o:t").?.state);
    try testing.expectEqual(@as(usize, 1), f.doc.task("o:t").?.resumes.len());
    try testing.expectEqual(@as(usize, 1), f.executes());
    try testing.expect(!f.doc.promise("o:child").?.callbacks.contains("o:t"));
}

test "suspending on an already settled promise says carry on" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g","resonate:branch":"o:t"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");
    _ = try f.call("promise.create",
        \\{"id":"o:t.child","timeoutAt":9000000000000,"tags":{"resonate:scope":"global","resonate:branch":"o:t"}}
    );
    _ = try f.call("promise.settle", "{\"id\":\"o:t.child\",\"state\":\"resolved\",\"value\":{\"data\":\"7\"}}");

    const r = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"promise.register_callback","head":{},"data":{"awaited":"o:t.child","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(@as(i32, 300), r.status);
    // The sibling's result rides along, which is what the worker was going to ask for.
    try testing.expect(std.mem.indexOf(u8, r.data, "\"o:t.child\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"data\":\"7\"") != null);
    // Still acquired: it never suspended.
    try testing.expectEqual(TaskState.acquired, f.doc.task("o:t").?.state);
}

test "suspend refuses a malformed action set" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");

    const empty = try f.call("task.suspend", "{\"id\":\"o:t\",\"version\":1,\"actions\":[]}");
    try testing.expectEqual(@as(i32, 400), empty.status);
    try testing.expectEqualStrings("\"Actions array cannot be empty\"", empty.data);

    const dup = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"k","head":{},"data":{"awaited":"o:c","awaiter":"o:t"}},{"kind":"k","head":{},"data":{"awaited":"o:c","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(@as(i32, 400), dup.status);
    try testing.expectEqualStrings("\"Awaited promise IDs must be unique\"", dup.data);

    const self = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"k","head":{},"data":{"awaited":"o:t","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(@as(i32, 400), self.status);
    try testing.expectEqualStrings("\"Awaited and awaiter must be different promises\"", self.data);

    const cross = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"k","head":{},"data":{"awaited":"other:c","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(@as(i32, 400), cross.status);
    try testing.expectEqualStrings("\"Awaiter and awaited must belong to the same origin\"", cross.data);
}

test "a callback on an already settled promise resumes immediately" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("promise.create",
        \\{"id":"o:c","timeoutAt":9000000000000,"tags":{"resonate:scope":"global"}}
    );
    _ = try f.call("promise.settle", "{\"id\":\"o:c\",\"state\":\"resolved\"}");

    const r = try f.call("promise.register_callback", "{\"awaited\":\"o:c\",\"awaiter\":\"o:t\"}");
    try testing.expectEqual(@as(i32, 200), r.status);
    // The answer is the awaited promise.
    try testing.expect(std.mem.indexOf(u8, r.data, "\"id\":\"o:c\"") != null);
    // Pending rather than suspended, so it only records the resume.
    try testing.expectEqual(@as(usize, 1), f.doc.task("o:t").?.resumes.len());
    try testing.expectEqual(@as(usize, 0), f.executes());
}

test "a callback needs an awaiter with somewhere to run and an awaitable awaited" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create", "{\"id\":\"o:plain\",\"timeoutAt\":9000000000000}");
    _ = try f.call("promise.create", "{\"id\":\"o:other\",\"timeoutAt\":9000000000000}");

    const no_awaited = try f.call("promise.register_callback", "{\"awaited\":\"o:gone\",\"awaiter\":\"o:plain\"}");
    try testing.expectEqual(@as(i32, 404), no_awaited.status);
    try testing.expectEqualStrings("\"Awaited promise not found\"", no_awaited.data);

    const no_awaiter = try f.call("promise.register_callback", "{\"awaited\":\"o:plain\",\"awaiter\":\"o:gone\"}");
    try testing.expectEqual(@as(i32, 422), no_awaiter.status);
    try testing.expectEqualStrings("\"Awaiter promise not found\"", no_awaiter.data);

    const no_target = try f.call("promise.register_callback", "{\"awaited\":\"o:other\",\"awaiter\":\"o:plain\"}");
    try testing.expectEqual(@as(i32, 422), no_target.status);
    try testing.expectEqualStrings("\"Awaiter promise has no resonate:target tag\"", no_target.data);

    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    const not_awaitable = try f.call("promise.register_callback", "{\"awaited\":\"o:plain\",\"awaiter\":\"o:t\"}");
    try testing.expectEqual(@as(i32, 422), not_awaitable.status);
    try testing.expectEqualStrings("\"Awaited promise is not awaitable\"", not_awaitable.data);
}

test "a listener is told when the promise settles" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:scope":"global"}}
    );
    _ = try f.call("promise.register_listener", "{\"awaited\":\"o:a\",\"address\":\"http://w:1/cb\"}");
    try testing.expectEqual(@as(usize, 1), f.doc.promise("o:a").?.listeners.len());

    const bad = try f.call("promise.register_listener", "{\"awaited\":\"o:a\",\"address\":\"not a url\"}");
    try testing.expectEqual(@as(i32, 400), bad.status);
    try testing.expectEqualStrings("\"Invalid listener address\"", bad.data);

    _ = try f.call("promise.settle", "{\"id\":\"o:a\",\"state\":\"rejected\",\"value\":{\"data\":\"boom\"}}");
    try testing.expectEqual(@as(usize, 1), f.last_effects.len);
    try testing.expectEqual(Effect.Kind.unblock, f.last_effects[0].kind);
    try testing.expectEqualStrings("http://w:1/cb", f.last_effects[0].address);
    try testing.expect(std.mem.indexOf(u8, f.last_effects[0].promise_json, "\"state\":\"rejected\"") != null);
    // Consumed: a listener is told once.
    try testing.expectEqual(@as(usize, 0), f.doc.promise("o:a").?.listeners.len());
}

test "task.create claims work and never offers it to anyone else" {
    var f = Fixture.init();
    defer f.deinit();
    const r = try f.call("task.create",
        \\{"pid":"w1","ttl":60000,"action":{"kind":"promise.create","head":{},"data":{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    // Acquired at version 1, and no offer: the caller is the worker.
    try testing.expect(std.mem.indexOf(u8, r.data, "\"version\":1") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"state\":\"acquired\"") != null);
    try testing.expectEqual(@as(usize, 0), f.executes());

    // A second caller finds it held.
    const held = try f.call("task.create",
        \\{"pid":"w2","ttl":60000,"action":{"kind":"promise.create","head":{},"data":{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}}}
    );
    try testing.expectEqual(@as(i32, 409), held.status);
    try testing.expectEqualStrings("\"Already exists\"", held.data);

    // The action must name somewhere to run.
    const no_target = try f.call("task.create",
        \\{"pid":"w1","ttl":1,"action":{"kind":"promise.create","head":{},"data":{"id":"o:b","timeoutAt":9000000000000,"tags":{}}}}
    );
    try testing.expectEqual(@as(i32, 400), no_target.status);
    try testing.expectEqualStrings("\"Action must have a resonate:target tag\"", no_target.data);

    // A promise without a task cannot grow one.
    _ = try f.call("promise.create", "{\"id\":\"o:plain\",\"timeoutAt\":9000000000000}");
    const grow = try f.call("task.create",
        \\{"pid":"w1","ttl":1,"action":{"kind":"promise.create","head":{},"data":{"id":"o:plain","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}}}
    );
    try testing.expectEqual(@as(i32, 422), grow.status);
    try testing.expectEqualStrings("\"The promise does not have a resonate:target tag\"", grow.data);
}

test "task.create picks up a pending task instead of conflicting" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    const r = try f.call("task.create",
        \\{"pid":"w1","ttl":60000,"action":{"kind":"promise.create","head":{},"data":{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"version\":1") != null);
    try testing.expectEqual(TaskState.acquired, f.doc.task("o:a").?.state);
}

test "halt stops a task and continue offers it again" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    const halted = try f.call("task.halt", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 200), halted.status);
    try testing.expectEqual(TaskState.halted, f.doc.task("o:a").?.state);
    try testing.expect(f.doc.task("o:a").?.timeout == null);
    // Halting twice is the same request twice.
    try testing.expectEqual(@as(i32, 200), (try f.call("task.halt", "{\"id\":\"o:a\"}")).status);

    const resumed = try f.call("task.continue", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 200), resumed.status);
    try testing.expectEqual(TaskState.pending, f.doc.task("o:a").?.state);
    try testing.expectEqual(@as(usize, 1), f.executes());
    // Continuing something that is not halted is a conflict.
    try testing.expectEqual(@as(i32, 409), (try f.call("task.continue", "{\"id\":\"o:a\"}")).status);

    _ = try f.call("promise.settle", "{\"id\":\"o:a\",\"state\":\"resolved\"}");
    const finished = try f.call("task.halt", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 409), finished.status);
    try testing.expectEqualStrings("\"Task is fulfilled\"", finished.data);
}

test "a fence commits an action only while the lease is still held" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");

    const r = try f.call("task.fence",
        \\{"id":"o:t","version":1,"action":{"kind":"promise.create","head":{},"data":{"id":"o:child","timeoutAt":9000000000000,"tags":{}}}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"corrId\":\"c1\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"status\":200") != null);
    try testing.expect(f.doc.promise("o:child") != null);

    // A superseded holder cannot land its action.
    const stale = try f.call("task.fence",
        \\{"id":"o:t","version":0,"action":{"kind":"promise.create","head":{},"data":{"id":"o:other","timeoutAt":9000000000000,"tags":{}}}}
    );
    try testing.expectEqual(@as(i32, 409), stale.status);
    try testing.expectEqualStrings("\"Version mismatch\"", stale.data);
    try testing.expect(f.doc.promise("o:other") == null);

    // The action cannot be the task itself, nor another origin.
    const self = try f.call("task.fence",
        \\{"id":"o:t","version":1,"action":{"kind":"promise.settle","head":{},"data":{"id":"o:t","state":"resolved"}}}
    );
    try testing.expectEqual(@as(i32, 400), self.status);
    try testing.expectEqualStrings("\"Action ID must not equal the task ID\"", self.data);

    const cross = try f.call("task.fence",
        \\{"id":"o:t","version":1,"action":{"kind":"promise.create","head":{},"data":{"id":"z:x","timeoutAt":9000000000000,"tags":{}}}}
    );
    try testing.expectEqual(@as(i32, 400), cross.status);
    try testing.expectEqualStrings("\"Action must belong to the task's origin\"", cross.data);

    const unknown = try f.call("task.fence",
        \\{"id":"o:t","version":1,"action":{"kind":"promise.nope","head":{},"data":{"id":"o:z"}}}
    );
    try testing.expectEqual(@as(i32, 400), unknown.status);
    try testing.expectEqualStrings("\"Invalid fence action kind\"", unknown.data);
}

test "a fenced settle reports 404 inside a 200" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":60000}");
    const r = try f.call("task.fence",
        \\{"id":"o:t","version":1,"action":{"kind":"promise.settle","head":{},"data":{"id":"o:gone","state":"resolved"}}}
    );
    // The fence succeeded; the action it carried did not.
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"status\":404") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"Promise not found\"") != null);
}

test "an expired promise settles the moment anything names it" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":1000005000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w1\",\"ttl\":600000}");
    f.now = 1_000_005_000;
    const got = try f.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"state\":\"rejected_timedout\"") != null);
    // Not the instant somebody noticed: the instant the deadline fell.
    try testing.expect(std.mem.indexOf(u8, got.data, "\"settledAt\":1000005000") != null);
    try testing.expectEqual(TaskState.fulfilled, f.doc.task("o:a").?.state);
    try testing.expect(f.doc.timer_at == null);
}

test "a heartbeat does not extend a lease past the promise's own deadline" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":1000005000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w1\",\"ttl\":1000}");
    const before = f.doc.task("o:a").?.timeout.?.at;
    f.now = 1_000_005_000;
    _ = try f.call("task.heartbeat", "{\"pid\":\"w1\",\"tasks\":[{\"id\":\"o:a\",\"version\":1}]}");
    try testing.expectEqual(before, f.doc.task("o:a").?.timeout.?.at);
}

test "the sweep re-offers a pending task and moves its deadline" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    const first = f.doc.task("o:a").?.timeout.?.at;
    // Nothing due yet, so nothing happens.
    try f.sweep();
    try testing.expectEqual(@as(usize, 0), f.executes());
    try testing.expectEqual(first, f.doc.task("o:a").?.timeout.?.at);

    f.now = first;
    try f.sweep();
    try testing.expectEqual(@as(usize, 1), f.executes());
    try testing.expectEqual(f.now + protocol.pending_retry_ttl, f.doc.task("o:a").?.timeout.?.at);
}

test "the sweep settles a deadline and resumes what was blocked on it" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":9000000}");
    _ = try f.call("promise.create",
        \\{"id":"o:c","timeoutAt":1000002000,"tags":{"resonate:target":"poll://any@g"}}
    );
    _ = try f.call("task.suspend",
        \\{"id":"o:t","version":1,"actions":[{"kind":"k","head":{},"data":{"awaited":"o:c","awaiter":"o:t"}}]}
    );
    try testing.expectEqual(TaskState.suspended, f.doc.task("o:t").?.state);

    f.now = 1_000_002_000;
    try f.sweep();
    try testing.expectEqual(PromiseState.rejected_timedout, f.doc.promise("o:c").?.state);
    try testing.expectEqual(TaskState.fulfilled, f.doc.task("o:c").?.state);
    // The awaiter was woken and offered again.
    try testing.expectEqual(TaskState.pending, f.doc.task("o:t").?.state);
    try testing.expectEqual(@as(usize, 1), f.doc.task("o:t").?.resumes.len());
    var offers: usize = 0;
    for (f.last_effects) |e| {
        if (e.kind == .execute and std.mem.eql(u8, e.task_id, "o:t")) offers += 1;
    }
    try testing.expectEqual(@as(usize, 1), offers);
}

test "validation refuses what the protocol does not admit" {
    var f = Fixture.init();
    defer f.deinit();
    const cases = [_]struct { kind: []const u8, data: []const u8, message: []const u8 }{
        .{ .kind = "promise.create", .data = "{\"timeoutAt\":1}", .message = "Invalid request: missing field `id`" },
        .{ .kind = "promise.create", .data = "{\"id\":\"\",\"timeoutAt\":1}", .message = "Promise ID is required" },
        .{ .kind = "promise.create", .data = "{\"id\":\"a\",\"timeoutAt\":-1}", .message = "TimeoutAt must be a non-negative integer" },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"a\",\"timeoutAt\":1,\"tags\":{\"resonate:origin\":\"x:y\"}}",
            .message = "resonate:origin must not contain ':'",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"a\",\"timeoutAt\":1,\"tags\":{\"resonate:origin\":\"b\"}}",
            .message = "Promise ID must be prefixed by resonate:origin",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"o:a\",\"timeoutAt\":1,\"tags\":{\"resonate:branch\":\"o:b\"}}",
            .message = "Promise ID must be prefixed by resonate:branch",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"o:a\",\"timeoutAt\":100,\"tags\":{\"resonate:delay\":\"nope\"}}",
            .message = "resonate:delay must be a non-negative integer",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"o:a\",\"timeoutAt\":100,\"tags\":{\"resonate:delay\":\"200\",\"resonate:target\":\"poll://a@g\"}}",
            .message = "resonate:delay must be less than timeoutAt",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"o:a\",\"timeoutAt\":100,\"tags\":{\"resonate:delay\":\"50\"}}",
            .message = "resonate:delay requires a resonate:target tag",
        },
        .{
            .kind = "promise.create",
            .data = "{\"id\":\"o:a\",\"timeoutAt\":9000000000000,\"tags\":{\"resonate:target\":\"not a url\"}}",
            .message = "Invalid resonate:target address",
        },
        .{ .kind = "promise.settle", .data = "{\"id\":\"a\",\"state\":\"nope\"}", .message = "Invalid request: unknown variant `nope`, expected one of `resolved`, `rejected`, `rejected_canceled`" },
        .{ .kind = "task.acquire", .data = "{\"id\":\"a\",\"version\":-1,\"pid\":\"p\",\"ttl\":1}", .message = "Version mismatch" ** 0 ++ "Version must be a non-negative integer" },
        .{ .kind = "task.acquire", .data = "{\"id\":\"a\",\"version\":0,\"pid\":\"p\",\"ttl\":0}", .message = "TTL must be a positive integer" },
        .{ .kind = "task.heartbeat", .data = "{\"pid\":\"p\",\"tasks\":[]}", .message = "Tasks array must not be empty" },
        .{
            .kind = "task.heartbeat",
            .data = "{\"pid\":\"p\",\"tasks\":[{\"id\":\"a:1\",\"version\":0},{\"id\":\"b:1\",\"version\":0}]}",
            .message = "All tasks must belong to the same origin",
        },
        .{ .kind = "task.get", .data = "{}", .message = "Invalid request: missing field `id`" },
    };
    for (cases) |c| {
        const r = try f.call(c.kind, c.data);
        try testing.expectEqual(@as(i32, 400), r.status);
        const expected = try std.fmt.allocPrint(f.arena.allocator(), "\"{s}\"", .{c.message});
        try testing.expectEqualStrings(expected, r.data);
    }
}

test "the branch preload carries the siblings and stops at the limit" {
    var f = Fixture.init();
    defer f.deinit();
    f.cfg.preload_limit = 2;
    _ = try f.call("promise.create",
        \\{"id":"o:t","timeoutAt":9000000000000,"tags":{"resonate:target":"poll://any@g","resonate:branch":"o:t"}}
    );
    for ([_][]const u8{ "o:t.a", "o:t.b", "o:t.c" }) |id| {
        const body = try std.fmt.allocPrint(
            f.arena.allocator(),
            "{{\"id\":\"{s}\",\"timeoutAt\":9000000000000,\"tags\":{{\"resonate:branch\":\"o:t\"}}}}",
            .{id},
        );
        _ = try f.call("promise.create", body);
    }
    const r = try f.call("task.acquire", "{\"id\":\"o:t\",\"version\":0,\"pid\":\"w1\",\"ttl\":1000}");
    try testing.expect(std.mem.indexOf(u8, r.data, "\"o:t.a\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"o:t.b\"") != null);
    // Truncated at the limit, in id order.
    try testing.expect(std.mem.indexOf(u8, r.data, "\"o:t.c\"") == null);
}

test "origin_of_request finds the document each operation belongs to" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { kind: []const u8, data: []const u8, want: []const u8 }{
        .{ .kind = "promise.get", .data = "{\"id\":\"o:a\"}", .want = "o" },
        .{ .kind = "promise.register_callback", .data = "{\"awaited\":\"o:a\",\"awaiter\":\"w:b\"}", .want = "w" },
        .{ .kind = "promise.register_listener", .data = "{\"awaited\":\"o:a\",\"address\":\"x:y\"}", .want = "o" },
        .{ .kind = "task.create", .data = "{\"pid\":\"p\",\"ttl\":1,\"action\":{\"kind\":\"k\",\"head\":{},\"data\":{\"id\":\"z:1\"}}}", .want = "z" },
        .{ .kind = "task.heartbeat", .data = "{\"pid\":\"p\",\"tasks\":[{\"id\":\"h:1\",\"version\":0}]}", .want = "h" },
        .{ .kind = "task.fence", .data = "{\"id\":\"f:1\",\"version\":0,\"action\":{}}", .want = "f" },
    };
    for (cases) |c| {
        const data = try json.parse(a, c.data);
        const op = Op.parse(c.kind).?;
        try testing.expectEqualStrings(c.want, origin_of_request(op, data).?);
    }
    // A request that names no id is routed anywhere and refused once parsed.
    try testing.expect(origin_of_request(.promise_get, try json.parse(a, "{}")) == null);
}

test "far future deadlines never fire" {
    var f = Fixture.init();
    defer f.deinit();
    _ = try f.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    f.now = far_future - 1;
    try f.sweep();
    try testing.expectEqual(PromiseState.pending, f.doc.promise("o:a").?.state);
}
