//! Schedules: a cron expression, a promise template, and the next instant it is
//! due.
//!
//! A schedule is not origin-scoped — it has no promises and no tasks of its own,
//! only a template for the ones it fires — so it gets a key of its own rather
//! than a place in a document.
//!
//! ## Firing is two commits, and that is fine
//!
//! A round fires the promises that are due and *then* moves the schedule
//! forward. Those are separate writes to separate objects, so a crash between
//! them leaves the schedule pointing at an instant it has already fired. It
//! fires again, and creating a promise that exists is a no-op — which is what
//! makes one run one run, however many times the sweep is interrupted. The
//! alternative would be a two-object commit, which this design does not have and
//! does not need.
//!
//! ## Catch-up is bounded
//!
//! A schedule that has not run for a year owes a year of runs. Each round fires
//! at most `max_catch_up` of them and leaves the next instant in the past, so
//! the deadline fires again immediately and the backlog drains in bounded steps
//! rather than in one unbounded one.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const doc_mod = @import("doc.zig");
const cron = @import("cron.zig");
const applier_mod = @import("applier.zig");
const scan = @import("scan.zig");

const assert = stdx.assert;
const ScheduleDoc = doc_mod.ScheduleDoc;
const Store = store_mod.Store;
const KeySpace = store_mod.KeySpace;

/// How many overdue runs one round fires before yielding.
pub const max_catch_up: usize = 256;

pub const Kind = enum {
    get,
    create,
    delete,
    /// Not a protocol request: the sweep reached this schedule's deadline.
    fire,

    pub fn parse(kind: []const u8) ?Kind {
        const table = .{
            .{ "schedule.get", Kind.get },
            .{ "schedule.create", Kind.create },
            .{ "schedule.delete", Kind.delete },
        };
        inline for (table) |entry| {
            if (std.mem.eql(u8, kind, entry[0])) return entry[1];
        }
        return null;
    }
};

pub const Request = struct {
    kind: Kind,
    corr_id: []const u8 = "",
    data: json.Value = json.Value.null_value,
    now: i64 = 0,
    /// `fire` only: the schedule to sweep.
    id: []const u8 = "",

    arena: *std.heap.ArenaAllocator,
    callback: *const fn (*Request) void,
    context: ?*anyopaque = null,

    status: i32 = 0,
    reply_data: []const u8 = "",

    // ── Working state ─────────────────────────────────────────────────────────
    phase: enum {
        start,
        loading,
        arming,
        committing,
        disarming,
        firing,
        done,
    } = .start,
    service: *Service = undefined,
    sched: ?ScheduleDoc = null,
    etag: ?store_mod.Etag = null,
    /// What the schedule pointed at when this round started, and which write
    /// armed it, so the old timer object can be removed once the new one is in
    /// place — and only that one.
    old_next_run_at: i64 = 0,
    old_timer_generation: u64 = 0,
    new_next_run_at: i64 = 0,
    body: []const u8 = &.{},
    op: store_mod.Operation = undefined,
    key_buf: std.ArrayList(u8) = undefined,
    old_key_buf: std.ArrayList(u8) = undefined,
    /// `fire` only: the outstanding promise creations.
    fires: []applier_mod.Work = &.{},
    fires_outstanding: usize = 0,
    /// `fire` only: whether a run could not be created because there was no
    /// answer. See `on_fired`.
    fires_lost: bool = false,
    /// `create` only: what the request asked for, held between the validation
    /// and the read that decides whether there is anything to create.
    create_fields: ?CreateFields = null,
    /// How many times a contended write has been re-decided.
    attempt: u32 = 0,
};

/// How many times a contended schedule is re-read before the caller is told the
/// truth: this did not happen, try again.
pub const max_cas_retries: u32 = 8;

const CreateFields = struct {
    cron: []const u8,
    promise_id: []const u8,
    promise_timeout: i64,
    promise_param: doc_mod.PromiseValue,
    promise_tags: protocol.StringMap,
};

pub const Service = struct {
    allocator: std.mem.Allocator,
    store: Store,
    keys: KeySpace,
    applier: *applier_mod.Applier,
    /// Called when a schedule's next deadline has been written, so whatever
    /// fires deadlines can hold it in memory instead of listing for it.
    deadline_hook: ?*const fn (context: ?*anyopaque, id: []const u8, at: i64, generation: u64) void = null,
    deadline_context: ?*anyopaque = null,

    pub fn init(
        allocator: std.mem.Allocator,
        store: Store,
        keys: KeySpace,
        applier: *applier_mod.Applier,
    ) Service {
        return .{ .allocator = allocator, .store = store, .keys = keys, .applier = applier };
    }

    pub fn submit(self: *Service, req: *Request) void {
        req.service = self;
        const a = req.arena.allocator();
        req.key_buf = std.ArrayList(u8).init(a);
        req.old_key_buf = std.ArrayList(u8).init(a);
        switch (req.kind) {
            .create => self.begin_create(req),
            else => self.load(req),
        }
    }

    /// The id a request names.
    fn id_of(req: *Request) ?[]const u8 {
        if (req.kind == .fire or req.kind == .create) return req.id;
        var p = Parse.init(req.arena.allocator(), req.data);
        const parsed = p.string("id", "Schedule ID is required");
        if (p.failure) |message| {
            fail(req, 400, message);
            return null;
        }
        return parsed;
    }

    // ── Reading ───────────────────────────────────────────────────────────────

    fn load(self: *Service, req: *Request) void {
        const id = id_of(req) orelse return;
        req.id = id;
        const key = self.keys.sched_key(&req.key_buf, id) catch
            return fail(req, 503, "out of memory");
        req.phase = .loading;
        req.op = .{
            .kind = .get,
            .key = key,
            .arena = req.arena.allocator(),
            .callback = on_complete,
            .context = req,
        };
        self.store.submit(&req.op);
    }

    fn on_complete(op: *store_mod.Operation) void {
        const req: *Request = @ptrCast(@alignCast(op.context.?));
        const self = req.service;
        switch (req.phase) {
            .loading => self.on_loaded(req, op.result),
            .arming => self.on_armed(req, op.result),
            .committing => self.on_committed(req, op.result),
            .disarming => self.finish_ok(req),

            else => unreachable,
        }
    }

    fn on_loaded(self: *Service, req: *Request, result: store_mod.Result) void {
        const a = req.arena.allocator();
        switch (result) {
            .not_found => {
                switch (req.kind) {
                    .get, .delete => return fail(req, 404, "Schedule not found"),
                    // The schedule is gone and its deadline outlived it. Nothing
                    // to fire; the stale key is collected by the caller.
                    .fire => return self.finish_empty(req),
                    // Nothing there, so there is something to create.
                    .create => return self.build_and_commit(req),
                }
            },
            .found => |f| {
                req.etag = f.etag;
                req.sched = ScheduleDoc.decode(a, f.body, req.id) catch
                    return fail(req, 500, "the schedule object is corrupt");
                if (req.sched.?.deleted) {
                    // A tombstone: the schedule is gone, and only the object is
                    // still there.
                    switch (req.kind) {
                        .get, .delete => return fail(req, 404, "Schedule not found"),
                        .fire => return self.finish_empty(req),
                        // Reclaimed rather than refused: the id is free again, and
                        // the write that takes it replaces the tombstone.
                        .create => return self.build_and_commit(req),
                    }
                }
            },
            .unavailable => |detail| return fail(req, 503, detail),
            else => return fail(req, 500, "the store answered a read with something else"),
        }
        switch (req.kind) {
            // Creating a schedule that exists is the same request arriving
            // twice, and the answer is the schedule that is there.
            .get, .create => self.reply_schedule(req),
            .delete => self.tombstone(req),
            .fire => self.fire(req),
        }
    }

    fn reply_schedule(self: *Service, req: *Request) void {
        _ = self;
        const a = req.arena.allocator();
        var out = std.ArrayList(u8).init(a);
        var w = json.Writer.init(&out);
        w.object_begin() catch return fail(req, 503, "out of memory");
        w.key("schedule") catch return fail(req, 503, "out of memory");
        scan.write_schedule_record(&w, &req.sched.?) catch return fail(req, 503, "out of memory");
        w.object_end() catch return fail(req, 503, "out of memory");
        req.status = 200;
        req.reply_data = out.items;
        req.phase = .done;
        req.callback(req);
    }

    // ── Creating ──────────────────────────────────────────────────────────────

    fn begin_create(self: *Service, req: *Request) void {
        const a = req.arena.allocator();
        var p = Parse.init(a, req.data);
        const id = p.string("id", "Schedule ID is required");
        const cron_text = p.string("cron", "Cron expression is required");
        const promise_id = p.string("promiseId", "Promise ID template is required");
        const promise_timeout = p.int("promiseTimeout", 0, "Promise timeout must be a non-negative integer");
        const promise_param = p.promise_value("promiseParam");
        const promise_tags = p.map("promiseTags");
        if (!p.failed()) {
            // A schedule id is stamped, through the promise id template, onto the
            // origin of every promise it fires — so it is bound by exactly the
            // rule an origin is: ':' is the origin/lineage separator, and an
            // origin holding one would be unrepresentable.
            if (std.mem.indexOfScalar(u8, id, ':') != null) {
                p.set("Schedule ID must not contain ':'");
            } else if (promise_tags.get(protocol.tag_origin)) |og| {
                if (std.mem.indexOfScalar(u8, og, ':') != null) p.set("resonate:origin must not contain ':'");
            }
            if (!p.failed()) {
                if (promise_tags.get(protocol.tag_prefix)) |px| {
                    if (std.mem.indexOfScalar(u8, px, ':') != null) p.set("resonate:prefix must not contain ':'");
                }
            }
            if (!p.failed() and !promise_tags.has(protocol.tag_target)) {
                p.set("promiseTags must include a resonate:target tag");
            }
        }
        if (p.failure) |message| return fail(req, 400, message);
        if (!cron.is_valid(cron_text)) return fail(req, 400, "Invalid cron expression");

        req.id = id;
        req.create_fields = .{
            .cron = cron_text,
            .promise_id = promise_id,
            .promise_timeout = promise_timeout,
            .promise_param = promise_param,
            .promise_tags = promise_tags,
        };
        // Read before writing. An idempotent create must not arm a deadline:
        // the schedule that is already there owns its own, and a second one at a
        // different instant would be an orphan nobody asked for.
        self.load(req);
    }

    fn build_and_commit(self: *Service, req: *Request) void {
        const fields = req.create_fields.?;
        // The document's arena hangs off the request's, so it goes when the
        // request does and there is nothing to remember to free.
        var sched = ScheduleDoc.init(req.arena.allocator());
        const owned = sched.allocator();
        sched.id = owned.dupe(u8, req.id) catch return fail(req, 503, "out of memory");
        sched.cron = owned.dupe(u8, fields.cron) catch return fail(req, 503, "out of memory");
        sched.promise_id = owned.dupe(u8, fields.promise_id) catch return fail(req, 503, "out of memory");
        sched.promise_timeout = fields.promise_timeout;
        sched.promise_param = fields.promise_param.clone(owned) catch return fail(req, 503, "out of memory");
        sched.promise_tags = fields.promise_tags.clone(owned) catch return fail(req, 503, "out of memory");
        sched.created_at = req.now;
        sched.next_run_at = cron.compute_next(fields.cron, req.now);
        sched.last_run_at = null;
        req.sched = sched;

        // The version read, if any: reclaiming a tombstone replaces an object that
        // is there, and creating a fresh schedule replaces nothing.
        req.old_next_run_at = 0;
        req.new_next_run_at = sched.next_run_at;
        self.encode_and_arm(req);
    }

    /// Write the deadline's object before the schedule that takes it on.
    fn encode_and_arm(self: *Service, req: *Request) void {
        const sched = &req.sched.?;
        // The timer object is named by the arm that wrote it, so a writer only
        // ever removes the object its own predecessor put there — and an attempt
        // that armed and then failed to commit cannot hand its name to the retry
        // that follows it. See `Applier.fresh_arm`, which this mirrors.
        req.old_timer_generation = sched.timer_generation;
        sched.generation += 1;
        const moved = req.new_next_run_at != req.old_next_run_at or req.old_next_run_at == 0;
        if (moved) sched.timer_generation = self.applier.fresh_arm(sched.generation);

        const a = req.arena.allocator();
        var body = std.ArrayList(u8).init(a);
        sched.encode(&body) catch return fail(req, 503, "out of memory");
        req.body = body.items;

        if (!moved) return self.commit(req);
        const key = self.keys.sched_timer_key(&req.key_buf, req.id, req.new_next_run_at, sched.timer_generation) catch
            return fail(req, 503, "out of memory");
        req.phase = .arming;
        req.op = .{
            .kind = .put,
            .key = key,
            .body = &.{},
            .precondition = .none,
            .arena = a,
            .callback = on_complete,
            .context = req,
        };
        self.store.submit(&req.op);
    }

    fn on_armed(self: *Service, req: *Request, result: store_mod.Result) void {
        switch (result) {
            .written => self.commit(req),
            .unavailable => |detail| fail(req, 503, detail),
            else => fail(req, 503, "the schedule's deadline could not be armed"),
        }
    }

    fn commit(self: *Service, req: *Request) void {
        const key = self.keys.sched_key(&req.key_buf, req.id) catch
            return fail(req, 503, "out of memory");
        req.phase = .committing;
        req.op = .{
            .kind = .put,
            .key = key,
            .body = req.body,
            .precondition = if (req.etag) |e| .{ .match = e } else .absent,
            .arena = req.arena.allocator(),
            .callback = on_complete,
            .context = req,
        };
        self.store.submit(&req.op);
    }

    fn on_committed(self: *Service, req: *Request, result: store_mod.Result) void {
        switch (result) {
            .written => {
                if (req.kind == .delete) {
                    // The tombstone landed, so this caller is the one that deleted
                    // it — and that is the whole of a delete. The object stays: it
                    // carries the write counter that names this schedule's timer
                    // objects, and a counter that restarted would let one
                    // incarnation remove another's deadline. The timer object it
                    // leaves behind is collected when it fires into a schedule that
                    // is not there, which is the same repair the origin documents
                    // rely on.
                    return self.finish_empty(req);
                }
                if (self.deadline_hook) |hook| {
                    hook(self.deadline_context, req.id, req.new_next_run_at, req.sched.?.timer_generation);
                }
                self.disarm(req);
            },
            .precondition_failed => {
                req.attempt += 1;
                if (req.attempt > max_cas_retries) {
                    return fail(req, 503, "the schedule was contended for too long");
                }
                switch (req.kind) {
                    // Someone else wrote first, so this decision was made against
                    // state that no longer exists: read again and decide again.
                    // Never turn it into a different operation — a create that
                    // found the schedule deleted in between still has a schedule
                    // to create, and answering "not found" to a create is an
                    // answer no sequential execution could give.
                    // A fire is no different. "Whoever moved it did this round's
                    // work" is only true if they moved it *past* this instant, and
                    // the only way to know is to read again — the promises are
                    // already created, and creating them again is a no-op, so
                    // redoing the round is free and leaving the schedule
                    // un-advanced is not: a tick that answered would have fired a
                    // run nothing recorded.
                    .create, .delete, .fire => {
                        req.sched = null;
                        req.etag = null;
                        self.load(req);
                    },
                    else => fail(req, 409, "the schedule changed while it was being written"),
                }
            },
            .conflict => self.commit(req),
            .unavailable => |detail| fail(req, 503, detail),
            else => fail(req, 500, "the store answered a write with something else"),
        }
    }

    fn disarm(self: *Service, req: *Request) void {
        if (req.old_next_run_at == 0 or req.old_timer_generation == req.sched.?.timer_generation) {
            return self.finish_ok(req);
        }
        const key = self.keys.sched_timer_key(&req.old_key_buf, req.id, req.old_next_run_at, req.old_timer_generation) catch
            return self.finish_ok(req);
        req.phase = .disarming;
        req.op = .{
            .kind = .delete,
            .key = key,
            .arena = req.arena.allocator(),
            .callback = on_complete,
            .context = req,
        };
        self.store.submit(&req.op);
    }

    fn finish_ok(self: *Service, req: *Request) void {
        switch (req.kind) {
            .create => self.reply_schedule(req),
            .fire => self.finish_empty(req),
            else => self.finish_empty(req),
        }
    }

    // ── Deleting ──────────────────────────────────────────────────────────────

    /// Claim the deletion with a compare-and-swap.
    ///
    /// Exactly one caller can win the write, and that caller is the one that
    /// deleted the schedule. A read followed by an unconditional remove would let
    /// two callers both report that they had — and two successful deletes of one
    /// schedule is a history no sequential execution can explain.
    fn tombstone(self: *Service, req: *Request) void {
        const sched = &req.sched.?;
        sched.deleted = true;
        req.old_next_run_at = sched.next_run_at;
        req.old_timer_generation = sched.timer_generation;
        req.new_next_run_at = sched.next_run_at;
        sched.generation += 1;

        const a = req.arena.allocator();
        var body = std.ArrayList(u8).init(a);
        sched.encode(&body) catch return fail(req, 503, "out of memory");
        req.body = body.items;
        self.commit(req);
    }

    // ── Firing ────────────────────────────────────────────────────────────────

    fn fire(self: *Service, req: *Request) void {
        const a = req.arena.allocator();
        const sched = &req.sched.?;
        req.old_next_run_at = sched.next_run_at;

        // Every instant this schedule owes, up to the cap.
        var due = std.ArrayList(i64).init(a);
        var current = sched.next_run_at;
        while (current <= req.now and due.items.len < max_catch_up) {
            due.append(current) catch return fail(req, 503, "out of memory");
            const next = cron.compute_next(sched.cron, current);
            // A cron expression that does not advance would loop forever.
            if (next <= current) break;
            current = next;
        }
        if (due.items.len == 0) {
            // Not due after all: the deadline was stale, or another server got
            // here first.
            return self.finish_empty(req);
        }
        sched.last_run_at = due.items[due.items.len - 1];
        sched.next_run_at = current;
        req.new_next_run_at = current;

        var fires = a.alloc(applier_mod.Work, due.items.len) catch
            return fail(req, 503, "out of memory");
        req.fires = fires;
        req.fires_outstanding = due.items.len;

        for (due.items, 0..) |at, i| {
            const promise_id = render_promise_id(a, sched.promise_id, req.id, at) catch
                return fail(req, 503, "out of memory");
            const tags = stamp_tags(a, sched.promise_tags, req.id, promise_id) catch
                return fail(req, 503, "out of memory");
            fires[i] = .{
                .kind = .{ .schedule_fire = .{
                    .promise_id = promise_id,
                    .fired_at = at,
                    .promise_timeout = sched.promise_timeout,
                    .param = sched.promise_param,
                    .tags = tags,
                } },
                .now = req.now,
                .arena = a,
                .callback = on_fired,
                .context = req,
            };
        }
        req.phase = .firing;
        // Submitted, not drained: the caller drains, so several schedules firing
        // in one round share the commits of any origin they land in.
        for (fires) |*work| {
            const origin = protocol.origin(work.kind.schedule_fire.promise_id);
            self.applier.submit(origin, work);
        }
    }

    fn on_fired(work: *applier_mod.Work) void {
        const req: *Request = @ptrCast(@alignCast(work.context.?));
        assert(req.fires_outstanding > 0);
        req.fires_outstanding -= 1;
        // A run nobody could store is a run that did not happen, and the schedule
        // must not move past it: advancing would retire the deadline and the
        // occurrence would be lost for good, with the answer still 200 and
        // nothing anywhere to say a run was owed. So the deadline stays, this
        // request fails, and whoever fired it tries again — creating a promise
        // that exists is a no-op, so a retry costs nothing and losing a run costs
        // the run.
        //
        // Only where there was no answer. A request the state machine *refused*
        // is refused however often it is retried: a promise id this schedule
        // wants and something else already holds under different terms will not
        // become creatable, and a schedule that never advances again is worse
        // than an occurrence that could not run.
        if (work.status == 0 or work.status >= 500) req.fires_lost = true;
        if (req.fires_outstanding > 0) return;
        if (req.fires_lost) return fail(req, 503, "a run of this schedule could not be created");
        // The promises are durable. Now the schedule can move forward: if this
        // crashes, the schedule refires and creating a promise that exists is a
        // no-op, so one run stays one run.
        req.service.encode_and_arm(req);
    }

    fn finish_empty(self: *Service, req: *Request) void {
        _ = self;
        req.status = 200;
        req.reply_data = "{}";
        req.phase = .done;
        req.callback(req);
    }

    fn fail(req: *Request, status: i32, message: []const u8) void {
        var buf = std.ArrayList(u8).init(req.arena.allocator());
        json.write_string(&buf, message) catch {};
        req.status = status;
        req.reply_data = buf.items;
        req.phase = .done;
        req.callback(req);
    }
};

/// The promise id a schedule fires at an instant.
///
/// Two substitutions, and only two: the schedule's own id and the instant. A
/// template with neither fires the same promise id every time, which is a
/// schedule that runs once — legal, and occasionally what someone means.
pub fn render_promise_id(
    allocator: std.mem.Allocator,
    template: []const u8,
    schedule_id: []const u8,
    at: i64,
) ![]const u8 {
    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();
    var timestamp: [24]u8 = undefined;
    const ts = try std.fmt.bufPrint(&timestamp, "{d}", .{at});
    var i: usize = 0;
    while (i < template.len) {
        if (std.mem.startsWith(u8, template[i..], "{{.id}}")) {
            try out.appendSlice(schedule_id);
            i += "{{.id}}".len;
        } else if (std.mem.startsWith(u8, template[i..], "{{.timestamp}}")) {
            try out.appendSlice(ts);
            i += "{{.timestamp}}".len;
        } else {
            try out.append(template[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice();
}

/// The tags a fired promise carries: the schedule's own, plus which schedule
/// fired it, plus a lineage rooted at the promise itself.
///
/// The four lineage tags all name the promise, because a scheduled run is the
/// root of its own execution: nothing called it.
pub fn stamp_tags(
    allocator: std.mem.Allocator,
    base: protocol.StringMap,
    schedule_id: []const u8,
    promise_id: []const u8,
) !protocol.StringMap {
    var tags = try base.clone(allocator);
    try tags.put(allocator, protocol.tag_schedule, schedule_id);
    for ([_][]const u8{
        protocol.tag_origin,
        protocol.tag_branch,
        protocol.tag_parent,
        protocol.tag_prefix,
    }) |key| {
        try tags.put(allocator, key, promise_id);
    }
    return tags;
}

/// The same accumulating reader the state machine uses, over a schedule's
/// fields. Duplicated rather than shared because the two have no other coupling
/// and a shared reader would be a dependency in the wrong direction.
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

    fn failed(self: *const Parse) bool {
        return self.failure != null;
    }

    fn missing(self: *Parse, name: []const u8) void {
        if (self.failure != null) return;
        self.failure = std.fmt.allocPrint(
            self.scratch,
            "Invalid request: missing field `{s}`",
            .{name},
        ) catch "Validation failed";
    }

    fn wrong_type(self: *Parse, name: []const u8, expected: []const u8) void {
        if (self.failure != null) return;
        self.failure = std.fmt.allocPrint(
            self.scratch,
            "Invalid request: invalid type for field `{s}`, expected {s}",
            .{ name, expected },
        ) catch "Validation failed";
    }

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

    fn map(self: *Parse, name: []const u8) protocol.StringMap {
        if (self.failed()) return .empty;
        const v = self.data.get(name) orelse return .empty;
        if (v.is_null()) return .empty;
        return protocol.StringMap.from_json(self.scratch, v) catch {
            self.wrong_type(name, "an object of strings");
            return .empty;
        };
    }

    fn promise_value(self: *Parse, name: []const u8) doc_mod.PromiseValue {
        if (self.failed()) return .empty;
        return doc_mod.PromiseValue.from_json(self.scratch, self.data.get(name)) catch {
            self.wrong_type(name, "a value with optional headers and data");
            return .empty;
        };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const env = @import("env.zig");
const sender_mod = @import("sender.zig");
const handle = @import("handle.zig");

const Fixture = struct {
    allocator: std.mem.Allocator,
    sim: env.Simulated,
    mem: store_mod.MemoryStore,
    sender: sender_mod.Sender,
    applier: applier_mod.Applier,
    service: Service,
    keys: KeySpace,

    fn create(allocator: std.mem.Allocator) !*Fixture {
        const self = try allocator.create(Fixture);
        self.* = .{
            .allocator = allocator,
            .sim = env.Simulated.init(allocator, 1_000_000_000),
            .mem = store_mod.MemoryStore.init(allocator),
            .sender = undefined,
            .applier = undefined,
            .service = undefined,
            .keys = KeySpace.init("", 4),
        };
        self.sender = sender_mod.Sender.init(allocator, self.bus(), "http://s");
        self.sender.hold = true;
        self.applier = applier_mod.Applier.init(
            allocator,
            self.mem.store(),
            self.keys,
            self.sim.clock(),
            self.sim.timer(),
            &self.sender,
            .{},
        );
        self.service = Service.init(allocator, self.mem.store(), self.keys, &self.applier);
        return self;
    }

    fn destroy(self: *Fixture) void {
        self.applier.deinit();
        self.sender.deinit();
        self.mem.deinit();
        self.sim.deinit();
        self.allocator.destroy(self);
    }

    fn bus(self: *Fixture) env.MessageBus {
        return .{ .ptr = self, .vtable = &.{ .send = send, .serves = serves } };
    }
    fn serves(_: *anyopaque, _: []const u8) bool {
        return true;
    }
    fn send(_: *anyopaque, d: *env.Delivery) void {
        d.complete(.delivered, "");
    }

    const Reply = struct {
        status: i32 = 0,
        data: []const u8 = "",
        done: bool = false,
        fn callback(req: *Request) void {
            const self: *Reply = @ptrCast(@alignCast(req.context.?));
            self.status = req.status;
            self.data = req.reply_data;
            self.done = true;
        }
    };

    fn call(
        self: *Fixture,
        arena: *std.heap.ArenaAllocator,
        kind: []const u8,
        data_json: []const u8,
    ) !Reply {
        // Anything the store is holding back has to land for the answer to arrive.
        var reply = Reply{};
        var req = Request{
            .kind = Kind.parse(kind).?,
            .corr_id = "c1",
            .data = try json.parse(arena.allocator(), data_json),
            .now = self.sim.now,
            .arena = arena,
            .callback = Reply.callback,
            .context = &reply,
        };
        self.service.submit(&req);
        var guard: usize = 0;
        while (!reply.done) {
            guard += 1;
            if (guard > 1_000) return error.NeverAnswered;
            self.mem.drain_delayed();
            self.applier.drain();
        }
        return reply;
    }

    fn fire(self: *Fixture, arena: *std.heap.ArenaAllocator, id: []const u8) !Reply {
        var reply = Reply{};
        var req = Request{
            .kind = .fire,
            .id = id,
            .now = self.sim.now,
            .arena = arena,
            .callback = Reply.callback,
            .context = &reply,
        };
        self.service.submit(&req);
        self.applier.drain();
        try testing.expect(reply.done);
        return reply;
    }

    /// Read a promise back through the state machine, so the test asserts on
    /// what a caller would see.
    fn promise_state(self: *Fixture, arena: *std.heap.ArenaAllocator, id: []const u8) !?[]const u8 {
        const a = arena.allocator();
        const body = try std.fmt.allocPrint(a, "{{\"id\":\"{s}\"}}", .{id});
        const Got = struct {
            status: i32 = 0,
            data: []const u8 = "",
            done: bool = false,
            fn cb(work: *applier_mod.Work) void {
                const g: *@This() = @ptrCast(@alignCast(work.context.?));
                g.status = work.status;
                g.data = work.reply_data;
                g.done = true;
            }
        };
        var got = Got{};
        var work = applier_mod.Work{
            .kind = .{ .request = .promise_get },
            .data = try json.parse(a, body),
            .now = self.sim.now,
            .arena = a,
            .callback = Got.cb,
            .context = &got,
        };
        self.applier.submit(protocol.origin(id), &work);
        self.applier.drain();
        try testing.expect(got.done);
        if (got.status != 200) return null;
        return got.data;
    }
};

test "a schedule is created once, read back, and deleted" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const created = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    try testing.expectEqual(@as(i32, 200), created.status);
    try testing.expect(std.mem.indexOf(u8, created.data, "\"id\":\"s0\"") != null);
    // 1_000_000_000 ms is 1970-01-12T13:46:40Z, so the next minute is at :47:00.
    try testing.expect(std.mem.indexOf(u8, created.data, "\"nextRunAt\":1000020000") != null);
    try testing.expect(std.mem.indexOf(u8, created.data, "lastRunAt") == null);

    // The schedule object and its timer object.
    try testing.expectEqual(@as(usize, 2), f.mem.count());

    // Creating it again is the same request arriving twice.
    const again = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"0 0 * * *","promiseId":"other","promiseTimeout":1,"promiseTags":{"resonate:target":"http://w"}}
    );
    try testing.expectEqual(@as(i32, 200), again.status);
    try testing.expect(std.mem.indexOf(u8, again.data, "\"cron\":\"* * * * *\"") != null);

    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expectEqualStrings(created.data, got.data);

    const missing = try f.call(&arena, "schedule.get", "{\"id\":\"nope\"}");
    try testing.expectEqual(@as(i32, 404), missing.status);
    try testing.expectEqualStrings("\"Schedule not found\"", missing.data);

    const deleted = try f.call(&arena, "schedule.delete", "{\"id\":\"s0\"}");
    try testing.expectEqual(@as(i32, 200), deleted.status);
    try testing.expectEqualStrings("{}", deleted.data);
    // The tombstone stays, and so does the timer object until it fires into a
    // schedule that is not there: the object carries the write counter that names
    // this schedule's timer objects, and a counter that restarted would let one
    // incarnation remove another's deadline. What matters is that the schedule
    // reads as gone, and that only one caller can delete it.
    try testing.expectEqual(@as(usize, 2), f.mem.count());
    try testing.expectEqual(@as(i32, 404), (try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}")).status);
    try testing.expectEqual(@as(i32, 404), (try f.call(&arena, "schedule.delete", "{\"id\":\"s0\"}")).status);
    // A search skips the tombstone; that is `scan`'s to prove.
}

test "a schedule is refused when it does not say what it would run" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const cases = [_]struct { data: []const u8, message: []const u8 }{
        .{ .data = "{\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "Invalid request: missing field `id`" },
        .{ .data = "{\"id\":\"\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "Schedule ID is required" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "Cron expression is required" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"* * * * *\",\"promiseId\":\"\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "Promise ID template is required" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":-1,\"promiseTags\":{}}", .message = "Promise timeout must be a non-negative integer" },
        .{ .data = "{\"id\":\"a:b\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "Schedule ID must not contain ':'" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{\"resonate:origin\":\"x:y\"}}", .message = "resonate:origin must not contain ':'" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{\"resonate:prefix\":\"x:y\",\"resonate:target\":\"http://w\"}}", .message = "resonate:prefix must not contain ':'" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"* * * * *\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{}}", .message = "promiseTags must include a resonate:target tag" },
        .{ .data = "{\"id\":\"s\",\"cron\":\"not a cron\",\"promiseId\":\"p\",\"promiseTimeout\":1,\"promiseTags\":{\"resonate:target\":\"http://w\"}}", .message = "Invalid cron expression" },
    };
    for (cases) |c| {
        const r = try f.call(&arena, "schedule.create", c.data);
        try testing.expectEqual(@as(i32, 400), r.status);
        const expected = try std.fmt.allocPrint(arena.allocator(), "\"{s}\"", .{c.message});
        try testing.expectEqualStrings(expected, r.data);
    }
    try testing.expectEqual(@as(usize, 0), f.mem.count());
}

test "firing creates the promise the template names, with a lineage of its own" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w","k":"v"}}
    );
    f.sim.now = 1_000_020_000;
    const fired = try f.fire(&arena, "s0");
    try testing.expectEqual(@as(i32, 200), fired.status);

    const p = (try f.promise_state(&arena, "s0.1000020000")).?;
    try testing.expect(std.mem.indexOf(u8, p, "\"state\":\"pending\"") != null);
    // Created at the instant it was due, and timing out a minute later.
    try testing.expect(std.mem.indexOf(u8, p, "\"createdAt\":1000020000") != null);
    try testing.expect(std.mem.indexOf(u8, p, "\"timeoutAt\":1000080000") != null);
    // Its own root: nothing called it.
    try testing.expect(std.mem.indexOf(u8, p, "\"resonate:origin\":\"s0.1000020000\"") != null);
    try testing.expect(std.mem.indexOf(u8, p, "\"resonate:branch\":\"s0.1000020000\"") != null);
    try testing.expect(std.mem.indexOf(u8, p, "\"resonate:parent\":\"s0.1000020000\"") != null);
    try testing.expect(std.mem.indexOf(u8, p, "\"resonate:prefix\":\"s0.1000020000\"") != null);
    try testing.expect(std.mem.indexOf(u8, p, "\"resonate:schedule\":\"s0\"") != null);
    // And the schedule's own tags came along.
    try testing.expect(std.mem.indexOf(u8, p, "\"k\":\"v\"") != null);
    // The offer went out.
    try testing.expectEqual(@as(usize, 1), f.sender.pending());

    // The schedule moved on.
    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"lastRunAt\":1000020000") != null);
    try testing.expect(std.mem.indexOf(u8, got.data, "\"nextRunAt\":1000080000") != null);
}

test "firing twice for the same instant is one run" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    f.sim.now = 1_000_020_000;
    _ = try f.fire(&arena, "s0");
    const created_at_first = (try f.promise_state(&arena, "s0.1000020000")).?;

    // Replay the same round: the schedule has moved, so this one finds nothing
    // due and creating the promise again would be a no-op anyway.
    _ = try f.fire(&arena, "s0");
    const created_at_second = (try f.promise_state(&arena, "s0.1000020000")).?;
    try testing.expectEqualStrings(created_at_first, created_at_second);
}

test "a schedule that has fallen behind catches up, one instant per run" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    // Three minutes late.
    f.sim.now = 1_000_020_000 + 3 * 60_000;
    _ = try f.fire(&arena, "s0");

    for ([_][]const u8{
        "s0.1000020000", "s0.1000080000", "s0.1000140000", "s0.1000200000",
    }) |id| {
        try testing.expect((try f.promise_state(&arena, id)) != null);
    }
    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"lastRunAt\":1000200000") != null);
    try testing.expect(std.mem.indexOf(u8, got.data, "\"nextRunAt\":1000260000") != null);
}

test "firing a schedule that is gone is not an error" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const r = try f.fire(&arena, "never-existed");
    try testing.expectEqual(@as(i32, 200), r.status);
}

test "firing before the schedule is due does nothing" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    const writes = f.mem.puts;
    const r = try f.fire(&arena, "s0");
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expectEqual(writes, f.mem.puts);
}

test "a run nobody could store leaves the schedule where it is" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const created = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    try testing.expectEqual(@as(i32, 200), created.status);
    // Past the first occurrence, so a fire has something to do.
    f.sim.now = 1_000_020_000;

    // The schedule's own read is served and the promise's write is not.
    f.mem.faults.unavailable_after = f.mem.gets + f.mem.puts + f.mem.deletes + f.mem.lists + 1;
    const fired = try f.fire(&arena, "s0");
    try testing.expectEqual(@as(i32, 503), fired.status);
    f.mem.faults.unavailable_after = null;

    // The occurrence is still owed: the schedule has not moved, so the deadline
    // its key names is still the one it is waiting for, and whoever fired it
    // retries. Advancing here would have answered 200 and lost the run.
    const after = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expectEqual(@as(i32, 200), after.status);
    try testing.expect(std.mem.indexOf(u8, after.data, "lastRunAt") == null);
    try testing.expect(std.mem.indexOf(u8, after.data, "\"nextRunAt\":1000020000") != null);
    try testing.expect((try f.promise_state(&arena, "s0.1000020000")) == null);

    // And the retry runs it.
    const again = try f.fire(&arena, "s0");
    try testing.expectEqual(@as(i32, 200), again.status);
    const run = (try f.promise_state(&arena, "s0.1000020000")).?;
    try testing.expect(std.mem.indexOf(u8, run, "\"state\":\"pending\"") != null);
}

test "the promise id template substitutes only what it documents" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expectEqualStrings("s0.1234", try render_promise_id(a, "{{.id}}.{{.timestamp}}", "s0", 1234));
    try testing.expectEqualStrings("fixed", try render_promise_id(a, "fixed", "s0", 1234));
    try testing.expectEqualStrings("{{.other}}", try render_promise_id(a, "{{.other}}", "s0", 1234));
    try testing.expectEqualStrings("run-1234-s0", try render_promise_id(a, "run-{{.timestamp}}-{{.id}}", "s0", 1234));
}

test "two callers deleting one schedule: exactly one of them deleted it" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );

    // Both submitted before either is driven, which is what makes them race.
    var first = Fixture.Reply{};
    var second = Fixture.Reply{};
    var a_request = Request{
        .kind = .delete,
        .data = try json.parse(arena.allocator(), "{\"id\":\"s0\"}"),
        .now = f.sim.now,
        .arena = &arena,
        .callback = Fixture.Reply.callback,
        .context = &first,
    };
    var b_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer b_arena.deinit();
    var b_request = Request{
        .kind = .delete,
        .data = try json.parse(b_arena.allocator(), "{\"id\":\"s0\"}"),
        .now = f.sim.now,
        .arena = &b_arena,
        .callback = Fixture.Reply.callback,
        .context = &second,
    };
    // Held back so both read the schedule before either writes.
    var rng = stdx.Random.init(1);
    f.mem.random = &rng;
    f.mem.faults.defer_percent = 100;
    f.service.submit(&a_request);
    f.service.submit(&b_request);
    var guard: usize = 0;
    while (!first.done or !second.done) {
        guard += 1;
        if (guard > 1_000) return error.NeverAnswered;
        f.mem.drain_delayed();
        f.applier.drain();
    }

    // One deleted it; the other found it gone. Two successes would be a history
    // no sequential execution could explain.
    const successes = @as(u32, if (first.status == 200) 1 else 0) + @as(u32, if (second.status == 200) 1 else 0);
    try testing.expectEqual(@as(u32, 1), successes);
    const missing = @as(u32, if (first.status == 404) 1 else 0) + @as(u32, if (second.status == 404) 1 else 0);
    try testing.expectEqual(@as(u32, 1), missing);

    // And it really is gone.
    f.mem.faults.defer_percent = 0;
    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expectEqual(@as(i32, 404), got.status);
}

test "a deleted schedule reads as absent and its id is reclaimed by a create" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A tombstone, which is what a deleted schedule is.
    var sched = doc_mod.ScheduleDoc.init(testing.allocator);
    defer sched.deinit();
    const owned = sched.allocator();
    sched.id = try owned.dupe(u8, "s0");
    sched.cron = try owned.dupe(u8, "* * * * *");
    sched.promise_id = try owned.dupe(u8, "p");
    sched.promise_timeout = 1;
    sched.created_at = 1;
    sched.next_run_at = 60_000;
    sched.deleted = true;
    var body = std.ArrayList(u8).init(a);
    try sched.encode(&body);
    var key_buf = std.ArrayList(u8).init(a);
    const key = try f.keys.sched_key(&key_buf, "s0");
    var op = store_mod.Operation{
        .kind = .put,
        .key = key,
        .body = body.items,
        .arena = a,
        .callback = struct {
            fn cb(_: *store_mod.Operation) void {}
        }.cb,
    };
    f.mem.store().submit(&op);

    // It reads as absent.
    try testing.expectEqual(@as(i32, 404), (try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}")).status);
    try testing.expectEqual(@as(i32, 404), (try f.call(&arena, "schedule.delete", "{\"id\":\"s0\"}")).status);
    // And the id is free again.
    const created = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"0 * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":5000,"promiseTags":{"resonate:target":"http://w"}}
    );
    try testing.expectEqual(@as(i32, 200), created.status);
    try testing.expect(std.mem.indexOf(u8, created.data, "\"cron\":\"0 * * * *\"") != null);
    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"cron\":\"0 * * * *\"") != null);
}

test "a fire that loses its race reads again and still advances the schedule" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    _ = try f.call(&arena, "schedule.create",
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    f.sim.now = 1_000_020_000;

    // Another writer touches the schedule object while the fire is deciding, so
    // the fire's conditional write is refused. It must read again and finish the
    // round rather than leaving the schedule pointing at an instant it has
    // already fired.
    var reply = Fixture.Reply{};
    var req = Request{
        .kind = .fire,
        .id = "s0",
        .now = f.sim.now,
        .arena = &arena,
        .callback = Fixture.Reply.callback,
        .context = &reply,
    };
    var rng = stdx.Random.init(3);
    f.mem.random = &rng;
    // The first conditional write is refused, the retry is not.
    f.mem.faults.conflict_percent = 0;
    f.service.submit(&req);
    // Let the fire read and decide, then move the object under it.
    f.applier.drain();
    {
        var key_buf = std.ArrayList(u8).init(a);
        const key = try f.keys.sched_key(&key_buf, "s0");
        const existing = f.mem.objects.get(key).?;
        var op = store_mod.Operation{
            .kind = .put,
            .key = key,
            .body = existing.body,
            .precondition = .none,
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        f.mem.store().submit(&op);
    }
    var guard: usize = 0;
    while (!reply.done) {
        guard += 1;
        if (guard > 1_000) return error.NeverAnswered;
        f.mem.drain_delayed();
        f.applier.drain();
    }
    try testing.expectEqual(@as(i32, 200), reply.status);

    // The run happened and the schedule says so.
    const got = try f.call(&arena, "schedule.get", "{\"id\":\"s0\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"lastRunAt\":1000020000") != null);
    try testing.expect(std.mem.indexOf(u8, got.data, "\"nextRunAt\":1000080000") != null);
}
