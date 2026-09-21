//! Deadlines: where they live, and what fires them.
//!
//! ## Deadlines live twice
//!
//! **Durably**, as empty objects under a timer prefix, keyed by zero-padded
//! deadline and then target — written *before* the state that takes them on, so
//! no crash window leaves a deadline uncovered. The key carries the whole
//! meaning, which is why the write is unconditional: arming the same deadline
//! twice is the same object, and arming a different one is a different object, so
//! nothing a racing writer does can lose one.
//!
//! **In memory**, in the queue below, which every writer adds to as soon as its
//! object lands. Normal operation fires from the queue alone: sleep until the
//! nearest deadline, wake, sweep. Finding what is due costs no store operations
//! at all.
//!
//! The listing path survives for exactly two callers: seeding the queue at
//! startup, because the last process's queue died with it and the keys did not;
//! and `debug.tick`, where the caller owns the clock and the queue is not being
//! driven.
//!
//! ## A fired key is deleted only once its sweep has landed
//!
//! Firing a deadline means asking the document to sweep. If that commits, the
//! document has armed whatever comes next and the key that fired is spent, so it
//! goes. If it does not commit, the key is the only record that the deadline
//! exists — deleting it would lose the deadline for good. So the delete waits for
//! the answer, and a failure re-arms in memory and tries again.
//!
//! Orphans are expected, not exceptional: a crash between arming and committing
//! leaves a key for a document that never changed. It fires, the sweep finds
//! nothing due, nothing is written, and the key is collected. That is the whole
//! repair mechanism, and it is the same code path as a real deadline.

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const env = @import("env.zig");
const applier_mod = @import("applier.zig");
const schedules = @import("schedules.zig");

const assert = stdx.assert;
const Store = store_mod.Store;
const KeySpace = store_mod.KeySpace;

/// How many deadlines are fired at once. A bound rather than a target: the
/// sweeps land in different documents and each is a round trip, so some
/// concurrency is the difference between a wake-up and a stall.
pub const fire_concurrency: u32 = 16;

/// How long a deadline waits after its sweep failed to commit.
pub const retry_after_ms: i64 = 1_000;

/// `debug.tick` sweeps until nothing is due, but not forever: a capped
/// catch-up can leave a schedule still behind, and this is how many rounds it
/// gets before the answer is "as far as that".
pub const tick_max_rounds: u32 = 64;

/// How many times a sweep's listing is retried before the sweep gives up.
///
/// A sweep fires everything due at one instant, and a listing that fails halfway
/// through leaves it having fired some of it. Retrying is what keeps that from
/// being the outcome: a caller that is told nothing cannot tell a sweep that did
/// nothing from one that did half, and half a sweep is not something any single
/// operation can mean.
pub const tick_max_list_attempts: u32 = 16;

pub const Timerd = struct {
    const Entry = struct {
        at: i64,
        key: []u8,
    };

    allocator: std.mem.Allocator,
    store: Store,
    keys: KeySpace,
    clock: env.Clock,
    timer: env.Timer,
    applier: *applier_mod.Applier,
    schedule_service: *schedules.Service,

    /// Armed keys, nearest deadline first.
    entries: std.ArrayListUnmanaged(Entry) = .{},
    wake: env.Timeout = undefined,
    wake_at: ?i64 = null,

    in_flight: u32 = 0,
    /// Under the debug flag nothing fires on wall time: the clock belongs to the
    /// caller, and a background sweep would settle promises the caller's trace
    /// never asked about.
    paused: bool = false,
    seeded: bool = false,

    fired: u64 = 0,
    collected: u64 = 0,
    failed: u64 = 0,

    key_buf: std.ArrayList(u8),

    pub fn init(
        allocator: std.mem.Allocator,
        store: Store,
        keys: KeySpace,
        clock: env.Clock,
        timer: env.Timer,
        applier: *applier_mod.Applier,
        schedule_service: *schedules.Service,
    ) Timerd {
        return .{
            .allocator = allocator,
            .store = store,
            .keys = keys,
            .clock = clock,
            .timer = timer,
            .applier = applier,
            .schedule_service = schedule_service,
            .key_buf = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: *Timerd) void {
        if (self.wake_at != null) self.timer.cancel(&self.wake);
        for (self.entries.items) |e| self.allocator.free(e.key);
        self.entries.deinit(self.allocator);
        self.key_buf.deinit();
    }

    // ── The queue ─────────────────────────────────────────────────────────────

    /// Hold a deadline whose object has landed.
    ///
    /// Idempotent on the key: the same deadline armed twice is one deadline.
    pub fn arm(self: *Timerd, at: i64, key: []const u8) void {
        for (self.entries.items) |e| {
            if (std.mem.eql(u8, e.key, key)) return;
        }
        const owned = self.allocator.dupe(u8, key) catch return;
        self.entries.append(self.allocator, .{ .at = at, .key = owned }) catch {
            self.allocator.free(owned);
            return;
        };
        self.reschedule();
    }

    /// Arm the deadline an origin's document now carries.
    pub fn arm_origin(self: *Timerd, origin: []const u8, at: i64, generation: u64) void {
        const key = self.keys.timer_key(&self.key_buf, origin, at, generation) catch return;
        self.arm(at, key);
    }

    pub fn arm_schedule(self: *Timerd, id: []const u8, at: i64, generation: u64) void {
        const key = self.keys.sched_timer_key(&self.key_buf, id, at, generation) catch return;
        self.arm(at, key);
    }

    fn forget(self: *Timerd, key: []const u8) void {
        for (self.entries.items, 0..) |e, i| {
            if (std.mem.eql(u8, e.key, key)) {
                self.allocator.free(e.key);
                _ = self.entries.orderedRemove(i);
                return;
            }
        }
    }

    pub fn armed_count(self: *const Timerd) usize {
        return self.entries.items.len;
    }

    /// The nearest armed deadline.
    pub fn next_deadline(self: *const Timerd) ?i64 {
        var best: ?i64 = null;
        for (self.entries.items) |e| {
            if (best == null or e.at < best.?) best = e.at;
        }
        return best;
    }

    /// Point the wake-up at the nearest deadline, if that has moved.
    fn reschedule(self: *Timerd) void {
        if (self.paused) return;
        const next = self.next_deadline() orelse {
            if (self.wake_at != null) {
                self.timer.cancel(&self.wake);
                self.wake_at = null;
            }
            return;
        };
        if (self.wake_at) |current| {
            if (current <= next) return;
            self.timer.cancel(&self.wake);
        }
        self.wake = .{ .at_ms = next };
        self.wake.listen(*Timerd, self, on_wake);
        self.wake_at = next;
        self.timer.arm(&self.wake, next);
    }

    fn on_wake(self: *Timerd, _: *env.Timeout) void {
        self.wake_at = null;
        self.sweep();
    }

    /// Fire everything due now, up to the concurrency bound, then point the
    /// wake-up at whatever is left.
    pub fn sweep(self: *Timerd) void {
        if (self.paused) return;
        const now = self.clock.now_ms();
        while (self.in_flight < fire_concurrency) {
            var due: ?Entry = null;
            for (self.entries.items) |e| {
                if (e.at > now) continue;
                if (due == null or e.at < due.?.at or
                    (e.at == due.?.at and std.mem.lessThan(u8, e.key, due.?.key)))
                {
                    due = e;
                }
            }
            const entry = due orelse break;
            // Taken out of the queue while it fires, so a second sweep does not
            // fire it again. A failure puts it back.
            const key = entry.key;
            for (self.entries.items, 0..) |e, i| {
                if (e.key.ptr == key.ptr) {
                    _ = self.entries.orderedRemove(i);
                    break;
                }
            }
            self.fire(key, entry.at, now, null);
        }
        self.applier.drain();
        self.reschedule();
    }

    // ── Firing one deadline ───────────────────────────────────────────────────

    const Fire = struct {
        timerd: *Timerd,
        arena: std.heap.ArenaAllocator,
        /// Owned by the timer daemon's allocator, because the arena is reset
        /// before the delete that uses it.
        key: []u8,
        at: i64,
        work: applier_mod.Work = undefined,
        request: schedules.Request = undefined,
        op: store_mod.Operation = undefined,
        /// Set when this fire belongs to a `debug.tick` rather than the loop.
        tick: ?*Tick,
    };

    fn fire(self: *Timerd, key: []u8, at: i64, now: i64, owner: ?*Tick) void {
        const parsed = blk: {
            var tmp = std.heap.ArenaAllocator.init(self.allocator);
            defer tmp.deinit();
            const entry = self.keys.parse_timer_key(tmp.allocator(), key) catch null;
            const e = entry orelse break :blk null;
            // Copy out of the temporary arena.
            break :blk switch (e) {
                .origin => |o| KeySpace.TimerEntry{ .origin = .{
                    .deadline = o.deadline,
                    .name = self.allocator.dupe(u8, o.name) catch break :blk null,
                    .generation = o.generation,
                } },
                .schedule => |sc| KeySpace.TimerEntry{ .schedule = .{
                    .deadline = sc.deadline,
                    .id = self.allocator.dupe(u8, sc.id) catch break :blk null,
                    .generation = sc.generation,
                } },
            };
        };
        const entry = parsed orelse {
            // Something else put this key here, or it is malformed. Ignoring it
            // is the only safe answer: refusing to run because of one stray
            // object would make a bucket unusable.
            self.allocator.free(key);
            if (owner) |t| t.on_fire_done();
            return;
        };

        const f = self.allocator.create(Fire) catch {
            self.allocator.free(key);
            free_entry(self.allocator, entry);
            if (owner) |t| t.on_fire_done();
            return;
        };
        f.* = .{
            .timerd = self,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .key = key,
            .at = at,
            .tick = owner,
        };
        const a = f.arena.allocator();
        self.in_flight += 1;
        self.fired += 1;

        switch (entry) {
            .origin => |o| {
                const origin = a.dupe(u8, o.name) catch {
                    free_entry(self.allocator, entry);
                    return self.finish_fire(f, false);
                };
                free_entry(self.allocator, entry);
                f.work = .{
                    .kind = .sweep,
                    .now = now,
                    .arena = a,
                    .callback = on_swept,
                    .context = f,
                };
                self.applier.submit(origin, &f.work);
            },
            .schedule => |sc| {
                const id = a.dupe(u8, sc.id) catch {
                    free_entry(self.allocator, entry);
                    return self.finish_fire(f, false);
                };
                free_entry(self.allocator, entry);
                f.request = .{
                    .kind = .fire,
                    .id = id,
                    .now = now,
                    .arena = &f.arena,
                    .callback = on_schedule_fired,
                    .context = f,
                };
                self.schedule_service.submit(&f.request);
            },
        }
    }

    fn free_entry(allocator: std.mem.Allocator, entry: KeySpace.TimerEntry) void {
        switch (entry) {
            .origin => |o| allocator.free(o.name),
            .schedule => |s| allocator.free(s.id),
        }
    }

    fn on_swept(work: *applier_mod.Work) void {
        const f: *Fire = @ptrCast(@alignCast(work.context.?));
        f.timerd.finish_fire(f, work.status == 200);
    }

    fn on_schedule_fired(req: *schedules.Request) void {
        const f: *Fire = @ptrCast(@alignCast(req.context.?));
        f.timerd.finish_fire(f, req.status == 200);
    }

    /// The sweep landed, so the key that fired it is spent.
    ///
    /// Always right when the sweep committed: either the deadline was real and
    /// the document has armed the next one under a different key, or it was an
    /// orphan and there was never anything to keep.
    fn finish_fire(self: *Timerd, f: *Fire, committed: bool) void {
        if (!committed) {
            self.failed += 1;
            // The key is the only record the deadline exists. Put it back, a
            // little later, and try again.
            self.arm(@max(f.at, self.clock.now_ms() + retry_after_ms), f.key);
            return self.release(f);
        }
        f.op = .{
            .kind = .delete,
            .key = f.key,
            .arena = f.arena.allocator(),
        };
        f.op.listen(*Fire, f, on_collected);
        self.store.submit(&f.op);
    }

    fn on_collected(f: *Fire, op: *store_mod.Operation) void {
        const self = f.timerd;
        // Best effort. A failure leaves a key that fires into a sweep with
        // nothing due, which collects it next time.
        if (op.result == .deleted) self.collected += 1;
        self.release(f);
    }

    fn release(self: *Timerd, f: *Fire) void {
        const owner = f.tick;
        self.allocator.free(f.key);
        f.arena.deinit();
        self.allocator.destroy(f);
        assert(self.in_flight > 0);
        self.in_flight -= 1;
        if (owner) |t| {
            t.on_fire_done();
        } else {
            // More may have come due, or more may fit under the bound.
            self.sweep();
        }
    }

    // ── Seeding ───────────────────────────────────────────────────────────────

    /// Rebuild the queue from one listing: the recovery read.
    ///
    /// The last process's queue died with it; the keys did not. Everything
    /// parseable is re-armed, so what was about to fire fires now and what is far
    /// off waits in memory as it did before.
    ///
    /// Nothing may fire until this has succeeded, which is why the caller retries
    /// it rather than proceeding.
    pub fn seed(self: *Timerd, on_done: *const fn (context: ?*anyopaque, armed: ?usize) void, context: ?*anyopaque) void {
        const seeding = self.allocator.create(Seed) catch {
            on_done(context, null);
            return;
        };
        seeding.* = .{
            .timerd = self,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
            .on_done = on_done,
            .context = context,
        };
        const prefix = self.keys.timer_prefix(&self.key_buf) catch {
            seeding.finish(null);
            return;
        };
        seeding.op = .{
            .kind = .list,
            .key = seeding.arena.allocator().dupe(u8, prefix) catch {
                seeding.finish(null);
                return;
            },
            .max_keys = std.math.maxInt(u32),
            .arena = seeding.arena.allocator(),
        };
        seeding.op.listen(*Seed, seeding, Seed.on_listed);
        self.store.submit(&seeding.op);
    }

    const Seed = struct {
        timerd: *Timerd,
        arena: std.heap.ArenaAllocator,
        op: store_mod.Operation = undefined,
        on_done: *const fn (context: ?*anyopaque, armed: ?usize) void,
        context: ?*anyopaque,

        fn on_listed(self: *Seed, op: *store_mod.Operation) void {
            switch (op.result) {
                .keys => |keys| {
                    var armed: usize = 0;
                    const a = self.arena.allocator();
                    for (keys) |key| {
                        const entry = (self.timerd.keys.parse_timer_key(a, key) catch null) orelse continue;
                        self.timerd.arm(entry.deadline(), key);
                        armed += 1;
                    }
                    self.timerd.seeded = true;
                    self.finish(armed);
                },
                // No answer. The caller retries: firing before the queue is
                // rebuilt would mean firing only what this process happened to
                // arm itself.
                else => self.finish(null),
            }
        }

        fn finish(self: *Seed, armed: ?usize) void {
            const on_done = self.on_done;
            const context = self.context;
            const timerd = self.timerd;
            self.arena.deinit();
            timerd.allocator.destroy(self);
            on_done(context, armed);
            if (armed != null) timerd.sweep();
        }
    };

    // ── debug.tick ────────────────────────────────────────────────────────────

    /// Sweep everything due at an instant the caller names, over the listing
    /// rather than the queue.
    ///
    /// The queue is not being driven under the debug flag, and the caller's
    /// instant may be nowhere near this process's wall clock — so the durable
    /// keys are the only honest account of what is due. It repeats until a pass
    /// finds nothing, because a capped schedule catch-up can leave a schedule
    /// still behind.
    pub const Tick = struct {
        timerd: *Timerd,
        time: i64,
        arena: *std.heap.ArenaAllocator,
        callback: *const fn (*Tick) void,
        context: ?*anyopaque = null,

        status: i32 = 0,
        reply_data: []const u8 = "",

        round: u32 = 0,
        outstanding: usize = 0,
        op: store_mod.Operation = undefined,
        fired_any: bool = false,
        list_attempts: u32 = 0,

        fn on_fire_done(self: *Tick) void {
            assert(self.outstanding > 0);
            self.outstanding -= 1;
            if (self.outstanding > 0) return;
            self.timerd.applier.drain();
            self.next_round();
        }

        fn next_round(self: *Tick) void {
            self.round += 1;
            // Nothing was due, so there is nothing left to be due: this is the
            // sweep finishing.
            if (!self.fired_any) return self.finish();
            // Still firing after this many rounds means the deadlines are not
            // draining. Whatever the reason, the sweep did not finish, and
            // saying it did would be a lie a caller acts on.
            if (self.round > tick_max_rounds) {
                return self.give_up("the sweep did not drain its deadlines");
            }
            self.list();
        }

        fn list(self: *Tick) void {
            const prefix = self.timerd.keys.timer_prefix(&self.timerd.key_buf) catch
                return self.finish();
            const key = self.arena.allocator().dupe(u8, prefix) catch return self.finish();
            self.op = .{
                .kind = .list,
                .key = key,
                .max_keys = std.math.maxInt(u32),
                .arena = self.arena.allocator(),
            };
            self.op.listen(*Tick, self, on_listed);
            self.timerd.store.submit(&self.op);
        }

        fn on_listed(self: *Tick, op: *store_mod.Operation) void {
            const keys = switch (op.result) {
                .keys => |k| k,
                .unavailable => |detail| {
                    // Try again rather than stop: rounds after the first have
                    // already fired something, and a sweep that stops there is
                    // one nobody can describe.
                    self.list_attempts += 1;
                    if (self.list_attempts < tick_max_list_attempts) return self.list();
                    return self.give_up(detail);
                },
                else => return self.finish(),
            };

            const a = self.arena.allocator();
            var due = std.ArrayList([]u8).init(a);
            var ats = std.ArrayList(i64).init(a);
            for (keys) |key| {
                const entry = (self.timerd.keys.parse_timer_key(a, key) catch null) orelse continue;
                if (entry.deadline() > self.time) continue;
                due.append(self.timerd.allocator.dupe(u8, key) catch continue) catch continue;
                ats.append(entry.deadline()) catch {};
            }
            if (due.items.len == 0) {
                self.fired_any = false;
                return self.finish();
            }
            self.fired_any = true;
            // Everything due at once. The state machine's own ordering — settle
            // the deadlines, then run the settlement chains — is inside one
            // document's sweep, and documents do not depend on each other.
            self.outstanding = due.items.len;
            for (due.items, 0..) |key, i| {
                // The queue holds these too if this process armed them; firing
                // from the listing means taking them out so the loop does not
                // fire them again.
                self.timerd.forget(key);
                self.timerd.fire(key, ats.items[i], self.time, self);
            }
        }

        fn finish(self: *Tick) void {
            self.status = 200;
            // An empty array, which is what this operation has always answered.
            self.reply_data = "[]";
            self.callback(self);
        }

        fn give_up(self: *Tick, detail: []const u8) void {
            var buf = std.ArrayList(u8).init(self.arena.allocator());
            json.write_string(&buf, detail) catch {};
            self.status = 503;
            self.reply_data = buf.items;
            self.callback(self);
        }
    };

    pub fn tick(self: *Timerd, t: *Tick) void {
        t.timerd = self;
        t.round = 0;
        t.fired_any = true;
        t.list();
    }

    /// Forget every armed deadline. `debug.reset` needs it: the objects are gone.
    pub fn reset(self: *Timerd) void {
        for (self.entries.items) |e| self.allocator.free(e.key);
        self.entries.clearRetainingCapacity();
        if (self.wake_at != null) {
            self.timer.cancel(&self.wake);
            self.wake_at = null;
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const sender_mod = @import("sender.zig");
const handle = @import("handle.zig");

const Fixture = struct {
    allocator: std.mem.Allocator,
    sim: env.Simulated,
    mem: store_mod.MemoryStore,
    sender: sender_mod.Sender,
    applier: applier_mod.Applier,
    service: schedules.Service,
    timerd: Timerd,
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
            .timerd = undefined,
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
        self.service = schedules.Service.init(allocator, self.mem.store(), self.keys, &self.applier);
        self.timerd = Timerd.init(
            allocator,
            self.mem.store(),
            self.keys,
            self.sim.clock(),
            self.sim.timer(),
            &self.applier,
            &self.service,
        );
        // The two writers tell the daemon what they armed, which is what keeps
        // the loop off the listing path.
        self.applier.deadline_hook = on_origin_deadline;
        self.applier.deadline_context = self;
        self.service.deadline_hook = on_schedule_deadline;
        self.service.deadline_context = self;
        return self;
    }

    fn on_origin_deadline(context: ?*anyopaque, origin: []const u8, at: i64, generation: u64) void {
        const self: *Fixture = @ptrCast(@alignCast(context.?));
        self.timerd.arm_origin(origin, at, generation);
    }

    fn on_schedule_deadline(context: ?*anyopaque, id: []const u8, at: i64, generation: u64) void {
        const self: *Fixture = @ptrCast(@alignCast(context.?));
        self.timerd.arm_schedule(id, at, generation);
    }

    fn destroy(self: *Fixture) void {
        self.timerd.deinit();
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
        fn on_work(work: *applier_mod.Work) void {
            const self: *Reply = @ptrCast(@alignCast(work.context.?));
            self.status = work.status;
            self.data = work.reply_data;
            self.done = true;
        }
        fn on_schedule(req: *schedules.Request) void {
            const self: *Reply = @ptrCast(@alignCast(req.context.?));
            self.status = req.status;
            self.data = req.reply_data;
            self.done = true;
        }
        fn on_tick(t: *Timerd.Tick) void {
            const self: *Reply = @ptrCast(@alignCast(t.context.?));
            self.status = t.status;
            self.data = t.reply_data;
            self.done = true;
        }
    };

    fn call(self: *Fixture, arena: *std.heap.ArenaAllocator, kind: []const u8, body: []const u8) !Reply {
        const a = arena.allocator();
        var reply = Reply{};
        const data = try json.parse(a, body);
        const op = handle.Op.parse(kind).?;
        var work = applier_mod.Work{
            .kind = .{ .request = op },
            .corr_id = "c1",
            .data = data,
            .now = self.sim.now,
            .arena = a,
            .callback = Reply.on_work,
            .context = &reply,
        };
        self.applier.submit(handle.origin_of_request(op, data) orelse "", &work);
        self.applier.drain();
        try testing.expect(reply.done);
        return reply;
    }

    fn create_schedule(self: *Fixture, arena: *std.heap.ArenaAllocator, body: []const u8) !Reply {
        var reply = Reply{};
        var req = schedules.Request{
            .kind = .create,
            .data = try json.parse(arena.allocator(), body),
            .now = self.sim.now,
            .arena = arena,
            .callback = Reply.on_schedule,
            .context = &reply,
        };
        self.service.submit(&req);
        self.applier.drain();
        try testing.expect(reply.done);
        return reply;
    }

    fn tick(self: *Fixture, arena: *std.heap.ArenaAllocator, time: i64) !Reply {
        var reply = Reply{};
        var t = Timerd.Tick{
            .timerd = &self.timerd,
            .time = time,
            .arena = arena,
            .callback = Reply.on_tick,
            .context = &reply,
        };
        self.timerd.tick(&t);
        var guard: usize = 0;
        while (!reply.done) {
            guard += 1;
            if (guard > 1000) return error.NeverAnswered;
            self.applier.drain();
        }
        return reply;
    }
};

test "the queue holds a deadline once and points the wake-up at the nearest" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    f.timerd.arm(5_000, "t/00/x");
    f.timerd.arm(3_000, "t/00/y");
    f.timerd.arm(5_000, "t/00/x");
    try testing.expectEqual(@as(usize, 2), f.timerd.armed_count());
    try testing.expectEqual(@as(i64, 3_000), f.timerd.next_deadline().?);
    try testing.expectEqual(@as(i64, 3_000), f.timerd.wake_at.?);
    // Arming something nearer moves the wake-up; something further does not.
    f.timerd.arm(1_000, "t/00/z");
    try testing.expectEqual(@as(i64, 1_000), f.timerd.wake_at.?);
    f.timerd.arm(9_000, "t/00/w");
    try testing.expectEqual(@as(i64, 1_000), f.timerd.wake_at.?);
}

test "a deadline fires from the queue without listing" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w"}}
    );
    // Creating it armed the retry deadline, and the daemon was told.
    try testing.expectEqual(@as(usize, 1), f.timerd.armed_count());
    const lists_before = f.mem.lists;

    // Move to the promise's own deadline and let the timer fire.
    _ = f.sim.advance_to(1_000_002_000);
    f.applier.drain();

    const got = try f.call(&arena, "promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "rejected_timedout") != null);
    // No listing was needed to find it.
    try testing.expectEqual(lists_before, f.mem.lists);
    try testing.expect(f.timerd.fired >= 1);
    // The key that fired is spent, and the document arms nothing more.
    try testing.expectEqual(@as(usize, 0), f.timerd.armed_count());
    var timers: usize = 0;
    var it = f.mem.objects.keyIterator();
    while (it.next()) |k| {
        if (std.mem.startsWith(u8, k.*, "t/")) timers += 1;
    }
    try testing.expectEqual(@as(usize, 0), timers);
}

test "a pending task is re-offered when its retry deadline falls" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"http://w"}}
    );
    const offers_before = f.sender.pending();
    _ = f.sim.advance_to(f.sim.now + protocol.pending_retry_ttl);
    f.applier.drain();
    // The offer was replaced rather than duplicated, so the outbox still holds
    // one — but the deadline moved, which is what says it fired.
    try testing.expectEqual(offers_before, f.sender.pending());
    try testing.expect(f.timerd.fired >= 1);
    try testing.expectEqual(@as(usize, 1), f.timerd.armed_count());
    try testing.expectEqual(f.sim.now + protocol.pending_retry_ttl, f.timerd.next_deadline().?);
}

test "seeding rebuilds the queue from the keys that survived" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w"}}
    );
    _ = try f.call(&arena, "promise.create",
        \\{"id":"b:a","timeoutAt":1000003000,"tags":{"resonate:target":"http://w"}}
    );
    // A process restart: the queue dies, the keys do not.
    f.timerd.reset();
    try testing.expectEqual(@as(usize, 0), f.timerd.armed_count());

    const Done = struct {
        var armed: ?usize = null;
        fn cb(_: ?*anyopaque, n: ?usize) void {
            armed = n;
        }
    };
    Done.armed = null;
    f.timerd.paused = true; // so seeding does not immediately fire them
    f.timerd.seed(Done.cb, null);
    try testing.expectEqual(@as(usize, 2), Done.armed.?);
    try testing.expectEqual(@as(usize, 2), f.timerd.armed_count());
    try testing.expect(f.timerd.seeded);
}

test "seeding reports failure rather than firing a queue it could not rebuild" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var rng = stdx.Random.init(3);
    f.mem.random = &rng;
    f.mem.faults.unavailable_percent = 100;

    const Done = struct {
        var armed: ?usize = null;
        fn cb(_: ?*anyopaque, n: ?usize) void {
            armed = n;
        }
    };
    Done.armed = 999;
    f.timerd.seed(Done.cb, null);
    try testing.expect(Done.armed == null);
    try testing.expect(!f.timerd.seeded);
}

test "an orphan key fires into a sweep with nothing due and is collected" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Exactly what a crash between arming and committing leaves behind.
    var key_buf = std.ArrayList(u8).init(a);
    const key = try a.dupe(u8, try f.keys.timer_key(&key_buf, "ghost", 1_000_001_000, 1));
    var op = store_mod.Operation{
        .kind = .put,
        .key = key,
        .body = "",
        .arena = a,
        .callback = struct {
            fn cb(_: *store_mod.Operation) void {}
        }.cb,
    };
    f.mem.store().submit(&op);
    f.timerd.arm(1_000_001_000, key);

    _ = f.sim.advance_to(1_000_001_000);
    f.applier.drain();

    try testing.expectEqual(@as(usize, 0), f.timerd.armed_count());
    try testing.expect(f.mem.objects.get(key) == null);
    try testing.expect(f.timerd.collected >= 1);
    // And nothing was written for the document that never existed.
    try testing.expect(f.mem.objects.get("wf/ghost") == null);
}

test "an unparseable key is ignored rather than fatal" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    f.timerd.arm(1_000_000_500, "t/00/not-a-timer-key");
    _ = f.sim.advance_to(1_000_000_500);
    f.applier.drain();
    try testing.expectEqual(@as(usize, 0), f.timerd.armed_count());
}

test "a deadline whose sweep did not commit is put back, not lost" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w"}}
    );
    var rng = stdx.Random.init(9);
    f.mem.random = &rng;
    f.mem.faults.unavailable_percent = 100;

    _ = f.sim.advance_to(1_000_002_000);
    f.applier.drain();
    try testing.expect(f.timerd.failed >= 1);
    // Still armed, a little later.
    try testing.expectEqual(@as(usize, 1), f.timerd.armed_count());
    try testing.expect(f.timerd.next_deadline().? > 1_000_002_000);

    // With the store back, it fires.
    f.mem.faults.unavailable_percent = 0;
    _ = f.sim.advance_to(f.sim.now + 10_000);
    f.applier.drain();
    const got = try f.call(&arena, "promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "rejected_timedout") != null);
}

test "nothing fires while the clock belongs to the caller" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    f.timerd.paused = true;
    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w"}}
    );
    _ = f.sim.advance_to(1_000_100_000);
    f.applier.drain();
    // Stored state is untouched: no background sweep settled anything.
    try testing.expectEqual(@as(u64, 0), f.timerd.fired);
    try testing.expect(f.timerd.wake_at == null);
}

test "debug.tick sweeps from the durable keys at the instant it is given" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    f.timerd.paused = true;

    _ = try f.call(&arena, "promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w"}}
    );
    _ = try f.call(&arena, "promise.create",
        \\{"id":"b:a","timeoutAt":1000003000,"tags":{"resonate:target":"http://w"}}
    );

    // Nothing is due yet.
    const early = try f.tick(&arena, 1_000_001_000);
    try testing.expectEqual(@as(i32, 200), early.status);
    try testing.expectEqualStrings("[]", early.data);

    // Past the first deadline only.
    _ = try f.tick(&arena, 1_000_002_000);
    {
        var probe = std.heap.ArenaAllocator.init(testing.allocator);
        defer probe.deinit();
        f.sim.now = 1_000_002_000;
        const a_got = try f.call(&probe, "promise.get", "{\"id\":\"o:a\"}");
        try testing.expect(std.mem.indexOf(u8, a_got.data, "rejected_timedout") != null);
    }

    // Past both.
    _ = try f.tick(&arena, 1_000_003_000);
    {
        var probe = std.heap.ArenaAllocator.init(testing.allocator);
        defer probe.deinit();
        f.sim.now = 1_000_003_000;
        const b_got = try f.call(&probe, "promise.get", "{\"id\":\"b:a\"}");
        try testing.expect(std.mem.indexOf(u8, b_got.data, "rejected_timedout") != null);
    }
}

test "debug.tick fires a schedule and keeps going until nothing is due" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    f.timerd.paused = true;

    _ = try f.create_schedule(&arena,
        \\{"id":"s0","cron":"* * * * *","promiseId":"{{.id}}.{{.timestamp}}","promiseTimeout":60000,"promiseTags":{"resonate:target":"http://w"}}
    );
    // Two minutes on.
    const r = try f.tick(&arena, 1_000_020_000 + 60_000);
    try testing.expectEqual(@as(i32, 200), r.status);

    f.sim.now = 1_000_080_000;
    for ([_][]const u8{ "s0.1000020000", "s0.1000080000" }) |id| {
        var probe = std.heap.ArenaAllocator.init(testing.allocator);
        defer probe.deinit();
        const body = try std.fmt.allocPrint(probe.allocator(), "{{\"id\":\"{s}\"}}", .{id});
        const got = try f.call(&probe, "promise.get", body);
        try testing.expectEqual(@as(i32, 200), got.status);
    }
}

test "reset forgets every armed deadline" {
    const f = try Fixture.create(testing.allocator);
    defer f.destroy();
    f.timerd.arm(5_000, "t/00/x");
    f.timerd.arm(6_000, "t/00/y");
    f.timerd.reset();
    try testing.expectEqual(@as(usize, 0), f.timerd.armed_count());
    try testing.expect(f.timerd.wake_at == null);
}
