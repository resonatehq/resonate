//! Load, decide, commit — the loop that turns a pure state machine into a
//! durable server.
//!
//! ## The shape of a commit
//!
//! One actor per origin, and one commit in flight per actor. A commit is:
//!
//! 1. **Load** the origin's document — from the cache, or one GET.
//! 2. **Decide**: run the state machine over the whole mailbox, in order. Each
//!    request sees the one before it, which is what makes this a group commit
//!    rather than a batch of independent guesses.
//! 3. **Arm** the new deadline, by writing its timer object. Before the
//!    document, so that a deadline the document is about to take on is already
//!    covered if the process dies here.
//! 4. **Commit** the document with a conditional write. This is the
//!    linearization point of every request in the batch.
//! 5. **Disarm** the old deadline. After, so nothing is uncovered in between.
//! 6. **Send**, then **reply**. Strictly post-commit, so a message is never
//!    about a transition that did not happen.
//!
//! ## The two write failures
//!
//! * A refused precondition means someone else committed first. Everything
//!   decided in step 2 was decided against state that no longer exists, so the
//!   whole batch is **re-decided** against a fresh read. Replaying the effects
//!   would be wrong — the idempotence and version checks have to run again.
//! * A conflict means the store could not order two conditional writes and will
//!   not say whether this one landed. The same write is **retried**; if that is
//!   refused, it falls into the re-decide path.
//!
//! ## The write law
//!
//! If the decision left the document byte for byte as it found it, nothing is
//! written. A read that changes nothing costs zero writes, and on a cache hit
//! zero operations at all. The document's clock is excluded from that
//! comparison on purpose: it is a monotonicity hint, and paying a write to
//! advance it would turn every `promise.get` into a write.
//!
//! ## Crash windows
//!
//! | dies after | what is left | how it recovers |
//! |---|---|---|
//! | the timer write | a deadline for a document that never changed | it fires, the sweep finds nothing due, nothing is written, the key is collected |
//! | the commit | a stale timer object | it fires early; the sweep either does nothing or legitimately advances |
//! | the sends | a lost offer or notification | the offer's retry deadline is already committed and re-sends it; a notification is lost, as it is in every backend |
//! | the replies | an unanswered caller | it retries, and every operation is idempotent |

const std = @import("std");
const stdx = @import("stdx.zig");
const json = @import("json.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const doc_mod = @import("doc.zig");
const handle = @import("handle.zig");
const env = @import("env.zig");
const sender_mod = @import("sender.zig");

const assert = stdx.assert;
const Doc = doc_mod.Doc;
const Store = store_mod.Store;
const Etag = store_mod.Etag;
const KeySpace = store_mod.KeySpace;

pub const Config = struct {
    machine: handle.Config = .{},
    /// How many times a contended origin is re-decided before the caller is
    /// told the truth: this did not happen, try again.
    max_cas_retries: u32 = 8,
    /// Documents held in memory.
    cache_entries: u32 = 4096,
    /// Validate a cached document against the store before deciding against it.
    ///
    /// On by default, and it has to be for reads to be linearizable when more than
    /// one process writes the bucket. A cached document was committed, so it is a
    /// state that really existed — but another process may have committed past it,
    /// and answering a read from it is then a read of a state that is no longer
    /// current. A write does not have this problem: its compare-and-swap is
    /// refused and it re-decides.
    ///
    /// The validation is a conditional read: the store answers "not modified" and
    /// sends no body, so a cache hit costs a round trip rather than a transfer.
    /// Turning it off is sound only where this process is the only writer, and
    /// nothing can check that for you — so it is off only if you say so.
    linearizable_reads: bool = true,
};

/// One unit of work for an origin's document.
pub const Work = struct {
    pub const Kind = union(enum) {
        /// A protocol request.
        request: handle.Op,
        /// Fire everything in this document that is due.
        sweep,
        /// Create the promise a schedule is due for.
        schedule_fire: ScheduleFire,
    };

    pub const ScheduleFire = struct {
        promise_id: []const u8,
        fired_at: i64,
        promise_timeout: i64,
        param: doc_mod.PromiseValue,
        tags: protocol.StringMap,
    };

    kind: Kind,
    corr_id: []const u8 = "",
    data: json.Value = json.Value.null_value,
    now: i64,

    /// Where the reply is allocated. The caller's, so the caller decides when
    /// it goes.
    arena: std.mem.Allocator,
    callback: *const fn (*Work) void,
    context: ?*anyopaque = null,

    /// Filled in before the callback runs.
    status: i32 = 0,
    reply_data: []const u8 = "",

    /// Set while the decision stands but the commit has not landed. Points into
    /// the actor's per-batch memory, and is copied into `arena` at reply time —
    /// a decision that has to be taken again must not leave a reply behind.
    staged_status: i32 = 0,
    staged_data: []const u8 = "",

    next: ?*Work = null,
};

/// An intrusive FIFO of work.
const Queue = struct {
    head: ?*Work = null,
    tail: ?*Work = null,
    len: usize = 0,

    fn push(self: *Queue, work: *Work) void {
        assert(work.next == null);
        if (self.tail) |t| t.next = work else self.head = work;
        self.tail = work;
        self.len += 1;
    }

    fn take(self: *Queue) Queue {
        const taken = self.*;
        self.* = .{};
        return taken;
    }

    fn is_empty(self: *const Queue) bool {
        return self.head == null;
    }
};

/// Documents in memory, keyed by origin, holding the canonical bytes and the
/// version they were read at.
///
/// The bytes rather than a decoded document: a decision mutates what it is
/// given, and a re-decide after a lost race has to start from what the store
/// says, not from what the last attempt left behind. Decoding per commit is the
/// price, and a group commit amortizes it over the whole mailbox.
const Cache = struct {
    const Entry = struct {
        bytes: []u8,
        etag: Etag,
    };

    allocator: std.mem.Allocator,
    map: std.StringHashMapUnmanaged(Entry) = .{},
    /// Insertion order, so eviction is a function of the run rather than of a
    /// hash. A cache that evicted differently in two processes would make a
    /// seeded simulation unreproducible.
    order: std.ArrayListUnmanaged([]const u8) = .{},
    capacity: u32,

    hits: u64 = 0,
    misses: u64 = 0,

    fn init(allocator: std.mem.Allocator, capacity: u32) Cache {
        return .{ .allocator = allocator, .capacity = @max(capacity, 1) };
    }

    fn deinit(self: *Cache) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.bytes);
        }
        self.map.deinit(self.allocator);
        self.order.deinit(self.allocator);
    }

    fn get(self: *Cache, origin: []const u8) ?Entry {
        if (self.map.get(origin)) |e| {
            self.hits += 1;
            return e;
        }
        self.misses += 1;
        return null;
    }

    fn put(self: *Cache, origin: []const u8, bytes: []const u8, etag: Etag) void {
        const copy = self.allocator.dupe(u8, bytes) catch return;
        if (self.map.getPtr(origin)) |e| {
            self.allocator.free(e.bytes);
            e.bytes = copy;
            e.etag = etag;
            return;
        }
        const key = self.allocator.dupe(u8, origin) catch {
            self.allocator.free(copy);
            return;
        };
        self.map.put(self.allocator, key, .{ .bytes = copy, .etag = etag }) catch {
            self.allocator.free(key);
            self.allocator.free(copy);
            return;
        };
        self.order.append(self.allocator, key) catch {};
        while (self.order.items.len > self.capacity) {
            const oldest = self.order.orderedRemove(0);
            self.invalidate(oldest);
        }
    }

    fn invalidate(self: *Cache, origin: []const u8) void {
        if (self.map.fetchRemove(origin)) |kv| {
            for (self.order.items, 0..) |k, i| {
                if (std.mem.eql(u8, k, origin)) {
                    _ = self.order.orderedRemove(i);
                    break;
                }
            }
            self.allocator.free(kv.key);
            self.allocator.free(kv.value.bytes);
        }
    }

    fn clear(self: *Cache) void {
        var it = self.map.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.bytes);
        }
        self.map.clearRetainingCapacity();
        self.order.clearRetainingCapacity();
    }
};

const Phase = enum {
    idle,
    loading,
    arming,
    committing,
    disarming,
    waiting,
};

const Actor = struct {
    applier: *Applier,
    origin: []u8,

    mailbox: Queue = .{},
    batch: Queue = .{},
    phase: Phase = .idle,

    /// Per-batch memory: the loaded bytes, the decoded document's own arena, the
    /// replies and the effects. Released when the batch finishes, and reset when
    /// it is re-decided.
    scratch: std.heap.ArenaAllocator,
    doc: ?Doc = null,
    before_bytes: []const u8 = &.{},
    body: []const u8 = &.{},
    /// The copy held in memory, while the store is being asked whether it is
    /// still current.
    cached_bytes: []const u8 = &.{},
    etag: ?Etag = null,
    old_timer_at: ?i64 = null,
    /// Which commit's key the old deadline is on the store under.
    old_timer_generation: u64 = 0,
    new_timer_at: ?i64 = null,
    new_timer_generation: u64 = 0,
    effects: std.ArrayListUnmanaged(handle.Effect) = .{},
    attempt: u32 = 0,
    /// True when the commit failed with a conflict rather than a refused
    /// precondition: the same write is retried, not re-decided.
    retry_same_write: bool = false,

    op: store_mod.Operation = undefined,
    /// Initialized rather than left undefined: `destroy` has to be able to ask
    /// whether it is armed, and an actor that went away leaving a deadline
    /// pointing at it is a crash on the next tick of the clock.
    backoff: env.Timeout = .{ .at_ms = 0, .callback = on_backoff },
    key: std.ArrayList(u8),
    old_key: std.ArrayList(u8),

    /// On the applier's runnable list.
    queued: bool = false,
    next_runnable: ?*Actor = null,

    fn create(applier: *Applier, origin: []const u8) !*Actor {
        const self = try applier.allocator.create(Actor);
        errdefer applier.allocator.destroy(self);
        self.* = .{
            .applier = applier,
            .origin = try applier.allocator.dupe(u8, origin),
            .scratch = std.heap.ArenaAllocator.init(applier.allocator),
            .key = std.ArrayList(u8).init(applier.allocator),
            .old_key = std.ArrayList(u8).init(applier.allocator),
        };
        return self;
    }

    fn destroy(self: *Actor) void {
        const allocator = self.applier.allocator;
        // Nothing may outlive this actor holding a pointer to it.
        if (self.backoff.armed) self.applier.timer.cancel(&self.backoff);
        if (self.doc) |*d| d.deinit();
        self.scratch.deinit();
        self.key.deinit();
        self.old_key.deinit();
        allocator.free(self.origin);
        allocator.destroy(self);
    }

    fn reset_batch(self: *Actor) void {
        if (self.doc) |*d| {
            d.deinit();
            self.doc = null;
        }
        self.effects = .{};
        self.before_bytes = &.{};
        self.cached_bytes = &.{};
        self.body = &.{};
        _ = self.scratch.reset(.retain_capacity);
    }

    fn advance(self: *Actor) void {
        switch (self.phase) {
            .idle => self.begin(),
            // Everything else is waiting on a completion that will wake it.
            else => {},
        }
    }

    fn begin(self: *Actor) void {
        assert(self.phase == .idle);
        assert(self.batch.is_empty());
        if (self.mailbox.is_empty()) {
            // Nothing left to do. The actor goes; the cache keeps the document,
            // so the next request for this origin costs nothing extra.
            self.applier.retire(self);
            return;
        }
        self.batch = self.mailbox.take();
        self.attempt = 0;
        self.retry_same_write = false;
        self.load();
    }

    fn load(self: *Actor) void {
        self.reset_batch();
        const a = self.scratch.allocator();
        var precondition: store_mod.Precondition = .none;
        if (self.applier.cache.get(self.origin)) |entry| {
            if (!self.applier.cfg.linearizable_reads) {
                // Trusted without asking. Sound only where nothing else writes
                // this bucket; see `Config.linearizable_reads`.
                self.before_bytes = a.dupe(u8, entry.bytes) catch return self.fail_batch(503, "out of memory");
                self.etag = entry.etag;
                self.decide();
                return;
            }
            // Ask whether it is still current. The answer is a status, not a
            // document, unless something else has committed since.
            self.cached_bytes = a.dupe(u8, entry.bytes) catch return self.fail_batch(503, "out of memory");
            self.etag = entry.etag;
            precondition = .{ .unchanged = entry.etag };
        } else {
            self.cached_bytes = &.{};
        }
        const key = self.applier.keys.doc_key(&self.key, self.origin) catch
            return self.fail_batch(503, "out of memory");
        self.phase = .loading;
        self.op = .{
            .kind = .get,
            .key = key,
            .precondition = precondition,
            .arena = a,
            .callback = on_store_complete,
            .context = self,
        };
        self.applier.store.submit(&self.op);
    }

    fn on_store_complete(op: *store_mod.Operation) void {
        const self: *Actor = @ptrCast(@alignCast(op.context.?));
        switch (self.phase) {
            .loading => self.on_loaded(op.result),
            .arming => self.on_armed(op.result),
            .committing => self.on_committed(op.result),
            .disarming => self.on_disarmed(),
            else => unreachable,
        }
    }

    fn on_loaded(self: *Actor, result: store_mod.Result) void {
        switch (result) {
            .found => |f| {
                self.before_bytes = f.body;
                self.etag = f.etag;
            },
            // Still at the version this process holds, so the copy in hand is
            // current and the store sent no body.
            .not_modified => {
                assert(self.cached_bytes.len > 0);
                self.before_bytes = self.cached_bytes;
            },
            .not_found => {
                // An origin with no document is an origin with no promises. The
                // baseline is what an empty document encodes to, so the write
                // law can compare against it.
                self.etag = null;
                var empty = Doc.init(self.applier.allocator);
                defer empty.deinit();
                var buf = std.ArrayList(u8).init(self.scratch.allocator());
                empty.encode(&buf, self.origin) catch
                    return self.fail_batch(503, "out of memory");
                self.before_bytes = buf.items;
            },
            .unavailable => |detail| return self.fail_batch(503, detail),
            else => return self.fail_batch(500, "the store answered a read with a write's result"),
        }
        self.decide();
    }

    fn decide(self: *Actor) void {
        const a = self.scratch.allocator();
        const document = Doc.decode(self.applier.allocator, self.before_bytes, self.origin) catch |e| {
            // A document this build cannot read is not something to guess at.
            // Answering 500 keeps the object intact for an operator to look at.
            const detail = switch (e) {
                error.FormatTooNew => "the document was written by a newer server",
                error.WrongOrigin => "the document does not belong under its key",
                error.Corrupt => "the document is corrupt",
                error.OutOfMemory => "out of memory",
            };
            return self.fail_batch(500, detail);
        };
        self.doc = document;
        const d = &self.doc.?;

        self.old_timer_at = d.timer_at;
        self.old_timer_generation = d.timer_generation;
        const loaded_clock = d.clock;
        const loaded_generation = d.generation;
        var latest: i64 = loaded_clock;

        var it = self.batch.head;
        while (it) |work| : (it = work.next) {
            if (work.now > latest) latest = work.now;
            const outcome = self.apply_one(d, work, a) catch {
                return self.fail_batch(503, "out of memory deciding");
            };
            work.staged_status = outcome.reply.status;
            work.staged_data = outcome.reply.data;
            self.effects.appendSlice(a, outcome.effects) catch {
                return self.fail_batch(503, "out of memory deciding");
            };
        }
        d.reseat_timer();

        // Whether the deadline has to be armed again: only if it moved. When it
        // did not, the object already on the store is still the right one, and it
        // keeps the token that armed it.
        const deadline_moved = blk: {
            if (self.old_timer_at == null) break :blk d.timer_at != null;
            if (d.timer_at == null) break :blk true;
            break :blk self.old_timer_at.? != d.timer_at.?;
        };
        d.timer_generation = if (deadline_moved)
            self.applier.fresh_arm(loaded_generation)
        else
            self.old_timer_generation;

        // The write law: put the header fields back as they were read and see
        // whether the bytes moved at all.
        d.clock = loaded_clock;
        d.generation = loaded_generation;
        var probe = std.ArrayList(u8).init(a);
        d.encode(&probe, self.origin) catch return self.fail_batch(503, "out of memory encoding");
        if (std.mem.eql(u8, probe.items, self.before_bytes)) {
            // Nothing changed, so nothing is written. The linearization point of
            // a batch that writes nothing is the read it decided against, which
            // is sound because it *is* read-only.
            self.new_timer_at = self.old_timer_at;
            self.new_timer_generation = self.old_timer_generation;
            self.post_commit();
            return;
        }

        d.clock = @max(loaded_clock, latest);
        d.generation = loaded_generation + 1;
        var body = std.ArrayList(u8).init(a);
        d.encode(&body, self.origin) catch return self.fail_batch(503, "out of memory encoding");
        self.body = body.items;
        self.new_timer_at = d.timer_at;
        self.new_timer_generation = d.timer_generation;
        self.arm();
    }

    fn apply_one(self: *Actor, d: *Doc, work: *Work, a: std.mem.Allocator) !handle.Outcome {
        return switch (work.kind) {
            .request => |op| try handle.handle(
                d,
                op,
                work.corr_id,
                work.data,
                work.now,
                self.applier.cfg.machine,
                a,
            ),
            .sweep => blk: {
                const outcome = try handle.drain(d, work.now, self.applier.cfg.machine, a);
                d.reseat_timer();
                break :blk outcome;
            },
            .schedule_fire => |f| blk: {
                const outcome = try handle.schedule_fire(
                    d,
                    f.promise_id,
                    f.fired_at,
                    f.promise_timeout,
                    f.param,
                    f.tags,
                    work.now,
                    self.applier.cfg.machine,
                    a,
                );
                d.reseat_timer();
                break :blk outcome;
            },
        };
    }

    /// Write the new deadline's object, before the document that takes it on.
    ///
    /// Unconditional, because the key carries everything the write means:
    /// arming the same deadline twice is the same object, and arming a different
    /// one is a different object. Nothing a racing writer does can lose it.
    fn arm(self: *Actor) void {
        const at = self.new_timer_at orelse return self.commit();
        // The deadline did not move, so the object already there is the right one.
        if (self.new_timer_generation == self.old_timer_generation) return self.commit();

        const key = self.applier.keys.timer_key(&self.key, self.origin, at, self.new_timer_generation) catch
            return self.fail_batch(503, "out of memory");
        self.phase = .arming;
        self.op = .{
            .kind = .put,
            .key = key,
            .body = &.{},
            .precondition = .none,
            .arena = self.scratch.allocator(),
            .callback = on_store_complete,
            .context = self,
        };
        self.applier.store.submit(&self.op);
    }

    fn on_armed(self: *Actor, result: store_mod.Result) void {
        switch (result) {
            .written => self.commit(),
            // Committing now would leave a deadline nothing will fire. The
            // caller is told this did not happen, and it did not.
            .unavailable => |detail| self.fail_batch(503, detail),
            else => self.fail_batch(503, "the deadline could not be armed"),
        }
    }

    fn commit(self: *Actor) void {
        const key = self.applier.keys.doc_key(&self.key, self.origin) catch
            return self.fail_batch(503, "out of memory");
        self.phase = .committing;
        self.op = .{
            .kind = .put,
            .key = key,
            .body = self.body,
            .precondition = if (self.etag) |e| .{ .match = e } else .absent,
            .arena = self.scratch.allocator(),
            .callback = on_store_complete,
            .context = self,
        };
        self.applier.store.submit(&self.op);
    }

    fn on_committed(self: *Actor, result: store_mod.Result) void {
        switch (result) {
            .written => |etag| {
                self.applier.cache.put(self.origin, self.body, etag);
                self.applier.commits += 1;
                self.disarm();
            },
            .precondition_failed => {
                // Someone else got there first. Everything above was decided
                // against state that no longer exists, so drop it and decide
                // again — never replay.
                self.applier.cache.invalidate(self.origin);
                self.applier.contentions += 1;
                self.retry_same_write = false;
                self.retry();
            },
            .conflict => {
                // The store would not say whether this landed. Retry the same
                // conditional write; if it is refused, that is the re-decide
                // path and the version check will catch up.
                self.applier.conflicts += 1;
                self.retry_same_write = true;
                self.retry();
            },
            .unavailable => |detail| {
                // It may have landed. The caller is told nothing happened,
                // which is the honest answer, and every operation is idempotent
                // so the retry reports whatever is true.
                self.applier.cache.invalidate(self.origin);
                self.fail_batch(503, detail);
            },
            else => self.fail_batch(500, "the store answered a write with a read's result"),
        }
    }

    fn retry(self: *Actor) void {
        self.attempt += 1;
        if (self.attempt > self.applier.cfg.max_cas_retries) {
            return self.fail_batch(503, "the origin was contended for too long");
        }
        const at = self.applier.clock.now_ms() + self.applier.backoff_ms(self.attempt);
        self.phase = .waiting;
        self.backoff = .{ .at_ms = at, .callback = on_backoff, .context = self };
        self.applier.timer.arm(&self.backoff, at);
    }

    fn on_backoff(timeout: *env.Timeout) void {
        const self: *Actor = @ptrCast(@alignCast(timeout.context.?));
        // Read before the call: retrying can finish the batch and retire this
        // actor, and then `self` is freed memory. Every place that continues after
        // handing control back to the applier has to hold the applier, not the
        // actor.
        const applier = self.applier;
        if (self.retry_same_write) {
            self.commit();
        } else {
            self.load();
        }
        applier.drain();
    }

    fn disarm(self: *Actor) void {
        const old = self.old_timer_at orelse return self.post_commit();
        // Nothing to remove when the object was not rewritten.
        if (self.new_timer_generation == self.old_timer_generation) return self.post_commit();

        // Keyed by the commit that wrote it, so this removes the object this
        // writer's own predecessor put there and nothing else. Without the
        // generation it would remove whatever is at that deadline, which another
        // writer may have just armed for a state this one knows nothing about.
        const key = self.applier.keys.timer_key(&self.old_key, self.origin, old, self.old_timer_generation) catch
            return self.post_commit();
        self.phase = .disarming;
        self.op = .{
            .kind = .delete,
            .key = key,
            .arena = self.scratch.allocator(),
            .callback = on_store_complete,
            .context = self,
        };
        self.applier.store.submit(&self.op);
    }

    fn on_disarmed(self: *Actor) void {
        // Best effort. A failure leaves an orphan that fires into a sweep with
        // nothing due, and the firing loop collects it.
        self.post_commit();
    }

    fn post_commit(self: *Actor) void {
        // The deadline is durable and the document is committed, so the timer
        // can go into memory where the firing loop reads it.
        if (self.new_timer_at) |at| {
            self.applier.on_deadline_armed(self.origin, at, self.new_timer_generation);
        }

        // Sends first, replies second. A caller that gets its answer and then
        // races the worker is a caller that can observe the effect before the
        // message that caused it.
        self.applier.sender.enqueue(self.effects.items) catch {};

        var it = self.batch.head;
        while (it) |work| {
            const next = work.next;
            work.next = null;
            work.status = work.staged_status;
            work.reply_data = work.arena.dupe(u8, work.staged_data) catch "\"out of memory\"";
            work.callback(work);
            it = next;
        }
        self.batch = .{};
        self.phase = .idle;
        self.reset_batch();
        self.applier.wake(self);
    }

    /// Answer every request in the batch with the same failure.
    ///
    /// Used where the store did not cooperate. Nothing was committed, so nothing
    /// is sent: the whole point of holding the sends until after the write is
    /// that this path exists.
    fn fail_batch(self: *Actor, status: i32, detail: []const u8) void {
        var it = self.batch.head;
        while (it) |work| {
            const next = work.next;
            work.next = null;
            work.status = status;
            var buf = std.ArrayList(u8).init(work.arena);
            json.write_string(&buf, detail) catch {};
            work.reply_data = buf.items;
            work.callback(work);
            it = next;
        }
        self.batch = .{};
        self.phase = .idle;
        self.reset_batch();
        self.applier.wake(self);
    }
};

pub const Applier = struct {
    allocator: std.mem.Allocator,
    store: Store,
    keys: KeySpace,
    clock: env.Clock,
    timer: env.Timer,
    sender: *sender_mod.Sender,
    cfg: Config,
    random: ?*stdx.Random = null,

    cache: Cache,
    actors: std.StringHashMapUnmanaged(*Actor) = .{},

    runnable_head: ?*Actor = null,
    runnable_tail: ?*Actor = null,
    draining: bool = false,

    /// Called after a commit whose document carries a deadline, so whatever
    /// fires deadlines can hold it in memory instead of listing for it.
    deadline_hook: ?*const fn (context: ?*anyopaque, origin: []const u8, at: i64, generation: u64) void = null,
    deadline_context: ?*anyopaque = null,

    commits: u64 = 0,
    contentions: u64 = 0,
    conflicts: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        store: Store,
        keys: KeySpace,
        clock: env.Clock,
        timer: env.Timer,
        sender: *sender_mod.Sender,
        cfg: Config,
    ) Applier {
        return .{
            .allocator = allocator,
            .store = store,
            .keys = keys,
            .clock = clock,
            .timer = timer,
            .sender = sender,
            .cfg = cfg,
            .cache = Cache.init(allocator, cfg.cache_entries),
        };
    }

    pub fn deinit(self: *Applier) void {
        var it = self.actors.valueIterator();
        while (it.next()) |actor| actor.*.destroy();
        self.actors.deinit(self.allocator);
        self.cache.deinit();
    }

    /// Queue work for an origin.
    ///
    /// Queued, not run: the caller submits everything one poll of the event loop
    /// produced and then calls `drain` once. That is what makes the group commit
    /// real — requests that arrive while a write is in flight ride the next one,
    /// and a hot origin costs one write per round rather than one per request.
    pub fn submit(self: *Applier, origin: []const u8, work: *Work) void {
        work.next = null;
        const actor = self.actor_for(origin) catch {
            work.status = 503;
            var buf = std.ArrayList(u8).init(work.arena);
            json.write_string(&buf, "out of memory") catch {};
            work.reply_data = buf.items;
            work.callback(work);
            return;
        };
        actor.mailbox.push(work);
        self.mark_runnable(actor);
    }

    fn actor_for(self: *Applier, origin: []const u8) !*Actor {
        if (self.actors.get(origin)) |a| return a;
        const actor = try Actor.create(self, origin);
        errdefer actor.destroy();
        try self.actors.put(self.allocator, actor.origin, actor);
        return actor;
    }

    /// An idle actor with an empty mailbox has nothing to hold. The cache keeps
    /// the document, so retiring it costs the next request nothing.
    fn retire(self: *Applier, actor: *Actor) void {
        assert(actor.phase == .idle);
        assert(actor.mailbox.is_empty());
        assert(actor.batch.is_empty());
        assert(!actor.queued);
        _ = self.actors.remove(actor.origin);
        actor.destroy();
    }

    fn wake(self: *Applier, actor: *Actor) void {
        self.mark_runnable(actor);
        self.drain();
    }

    fn mark_runnable(self: *Applier, actor: *Actor) void {
        if (!actor.queued) {
            actor.queued = true;
            actor.next_runnable = null;
            if (self.runnable_tail) |t| t.next_runnable = actor else self.runnable_head = actor;
            self.runnable_tail = actor;
        }
    }

    /// Run every actor that has something to do, to completion or to its next
    /// wait.
    ///
    /// Iterative rather than recursive. The in-memory store completes inline, so
    /// a commit's five steps and a batch's replies would otherwise nest — and a
    /// server that overflows its stack under load because its store got faster
    /// is not a server.
    pub fn drain(self: *Applier) void {
        if (self.draining) return;
        self.draining = true;
        defer self.draining = false;
        while (self.runnable_head) |actor| {
            self.runnable_head = actor.next_runnable;
            if (self.runnable_head == null) self.runnable_tail = null;
            actor.next_runnable = null;
            actor.queued = false;
            actor.advance();
        }
    }

    fn on_deadline_armed(self: *Applier, origin: []const u8, at: i64, generation: u64) void {
        if (self.deadline_hook) |hook| hook(self.deadline_context, origin, at, generation);
    }

    /// Doubling with jitter, to a ceiling. Two writers that backed off by the
    /// same amount would collide again on the same schedule forever.
    /// The token that names the timer object a commit is about to arm.
    ///
    /// It has to be unique to the **arm**, not to the commit. An attempt that
    /// armed its object and then failed to commit leaves that object behind, and
    /// a retry that loads the same document version computes the same deadline —
    /// so a token derived from the document's own counter gives the retry the
    /// same *name*. Two arms, one name. The orphan then fires into a sweep with
    /// nothing due, which collects it by name, and the name it deletes is the
    /// live object the retry wrote. That is a deadline lost for good: a promise
    /// that never times out and a task that is never offered again, with nothing
    /// anywhere to say so.
    ///
    /// Without a random source the token *is* the document's next counter, which
    /// is a pure function of the state. That is what the sequential specification
    /// wants — it has no failed commits and no second writer, so no name can
    /// repeat there — and it is why the source is optional rather than required.
    /// Every server that writes to a shared bucket sets one.
    pub fn fresh_arm(self: *Applier, loaded_generation: u64) u64 {
        const rng = self.random orelse return loaded_generation + 1;
        while (true) {
            // 63 bits: a document is JSON, and a JSON integer here is read back as
            // an i64. A token that does not survive its own round trip would read
            // as zero, and a document whose bytes change when nothing happened is
            // a write on every read.
            const token = rng.word() & std.math.maxInt(i64);
            // Zero is "no deadline armed", so it is not a token.
            if (token != 0) return token;
        }
    }

    fn backoff_ms(self: *Applier, attempt: u32) i64 {
        const base: i64 = @as(i64, 5) * (@as(i64, 1) << @intCast(@min(attempt, 6)));
        const jitter: i64 = if (self.random) |rng|
            @intCast(rng.below(@intCast(base + 1)))
        else
            0;
        return base + jitter;
    }

    /// Forget every cached document. `debug.reset` needs it: the objects are
    /// gone, so anything cached is a document about state that no longer exists.
    pub fn reset(self: *Applier) void {
        self.cache.clear();
    }

    pub fn idle(self: *const Applier) bool {
        return self.actors.count() == 0;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// Everything a test needs wired together over an in-memory store.
const Harness = struct {
    allocator: std.mem.Allocator,
    sim: env.Simulated,
    mem: store_mod.MemoryStore,
    bus_sent: std.ArrayList([]u8),
    sender: sender_mod.Sender,
    applier: Applier,
    arena: std.heap.ArenaAllocator,
    rng: stdx.Random,

    fn create(allocator: std.mem.Allocator) !*Harness {
        const self = try allocator.create(Harness);
        self.* = .{
            .allocator = allocator,
            .sim = env.Simulated.init(allocator, 1_000_000_000),
            .mem = store_mod.MemoryStore.init(allocator),
            .bus_sent = std.ArrayList([]u8).init(allocator),
            .sender = undefined,
            .applier = undefined,
            .arena = std.heap.ArenaAllocator.init(allocator),
            .rng = stdx.Random.init(1),
        };
        self.sender = sender_mod.Sender.init(allocator, self.bus(), "http://server:8001");
        self.applier = Applier.init(
            allocator,
            self.mem.store(),
            KeySpace.init("", 4),
            self.sim.clock(),
            self.sim.timer(),
            &self.sender,
            .{},
        );
        self.applier.random = &self.rng;
        return self;
    }

    fn destroy(self: *Harness) void {
        self.applier.deinit();
        self.sender.deinit();
        for (self.bus_sent.items) |b| self.allocator.free(b);
        self.bus_sent.deinit();
        self.mem.deinit();
        self.sim.deinit();
        self.arena.deinit();
        self.allocator.destroy(self);
    }

    fn bus(self: *Harness) env.MessageBus {
        return .{ .ptr = self, .vtable = &.{ .send = send_erased, .serves = serves_erased } };
    }

    fn forget_sent(self: *Harness) void {
        for (self.bus_sent.items) |b| self.allocator.free(b);
        self.bus_sent.clearRetainingCapacity();
    }

    fn serves_erased(_: *anyopaque, _: []const u8) bool {
        return true;
    }

    fn send_erased(ptr: *anyopaque, delivery: *env.Delivery) void {
        const self: *Harness = @ptrCast(@alignCast(ptr));
        self.bus_sent.append(self.allocator.dupe(u8, delivery.body) catch unreachable) catch unreachable;
        delivery.complete(.delivered, "");
    }

    const Reply = struct {
        status: i32 = 0,
        data: []const u8 = "",
        done: bool = false,

        fn callback(work: *Work) void {
            const self: *Reply = @ptrCast(@alignCast(work.context.?));
            self.status = work.status;
            self.data = work.reply_data;
            self.done = true;
        }
    };

    /// Submit one request and run until it is answered.
    fn call(self: *Harness, kind: []const u8, data_json: []const u8) !Reply {
        const a = self.arena.allocator();
        var reply = Reply{};
        const data = try json.parse(a, data_json);
        const op = handle.Op.parse(kind).?;
        var work = Work{
            .kind = .{ .request = op },
            .corr_id = "c1",
            .data = data,
            .now = self.sim.now,
            .arena = a,
            .callback = Reply.callback,
            .context = &reply,
        };
        const origin = handle.origin_of_request(op, data) orelse "";
        self.applier.submit(origin, &work);
        // Anything held back by fault injection, and any backoff, has to run.
        var guard: usize = 0;
        while (!reply.done) {
            guard += 1;
            if (guard > 1000) return error.NeverAnswered;
            self.mem.drain_delayed();
            _ = self.sim.advance_to(self.sim.now + 1_000);
            self.applier.drain();
        }
        return reply;
    }
};

test "a request is committed, cached, and answered" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();

    const r = try h.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"http://w:1"}}
    );
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"state\":\"pending\"") != null);
    try testing.expectEqual(@as(u64, 1), h.applier.commits);

    // The document, and the timer object the deadline needs.
    try testing.expectEqual(@as(usize, 2), h.mem.count());
    // The offer went out after the commit.
    try testing.expectEqual(@as(usize, 1), h.bus_sent.items.len);
    try testing.expect(std.mem.indexOf(u8, h.bus_sent.items[0], "\"kind\":\"execute\"") != null);

    // The actor is gone but the document is cached, so the next read costs one
    // conditional read — a status and no body — and no write at all.
    try testing.expect(h.applier.idle());
    const gets_before = h.mem.gets;
    const puts_before = h.mem.puts;
    const got = try h.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 200), got.status);
    try testing.expectEqual(gets_before + 1, h.mem.gets);
    try testing.expectEqual(puts_before, h.mem.puts);

    // Told that nothing else writes this bucket, it does not even ask.
    h.applier.cfg.linearizable_reads = false;
    const trusted = try h.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expectEqual(@as(i32, 200), trusted.status);
    try testing.expectEqual(gets_before + 1, h.mem.gets);
}

test "a read that changes nothing writes nothing" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    _ = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    const puts = h.mem.puts;
    for (0..5) |_| {
        _ = try h.call("promise.get", "{\"id\":\"o:a\"}");
    }
    try testing.expectEqual(puts, h.mem.puts);
    // A 404 costs nothing either.
    _ = try h.call("promise.get", "{\"id\":\"o:gone\"}");
    try testing.expectEqual(puts, h.mem.puts);
}

test "a read that expires a promise does write" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    _ = try h.call("promise.create",
        \\{"id":"o:a","timeoutAt":1000005000,"tags":{"resonate:target":"http://w:1"}}
    );
    const puts = h.mem.puts;
    h.sim.now = 1_000_005_000;
    const got = try h.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "rejected_timedout") != null);
    // A ghost timeout is a real transition.
    try testing.expect(h.mem.puts > puts);
}

test "the timer object moves with the earliest deadline and the old one goes" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    _ = try h.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"http://w:1"}}
    );
    const a = h.arena.allocator();
    var timer_keys = std.ArrayList([]const u8).init(a);
    {
        var it = h.mem.objects.keyIterator();
        while (it.next()) |k| {
            if (std.mem.startsWith(u8, k.*, "t/")) try timer_keys.append(k.*);
        }
    }
    try testing.expectEqual(@as(usize, 1), timer_keys.items.len);
    const first = try a.dupe(u8, timer_keys.items[0]);

    // Acquiring replaces the retry deadline with a lease, so the object moves.
    _ = try h.call("task.acquire", "{\"id\":\"o:a\",\"version\":0,\"pid\":\"w\",\"ttl\":1000}");
    var after = std.ArrayList([]const u8).init(a);
    {
        var it = h.mem.objects.keyIterator();
        while (it.next()) |k| {
            if (std.mem.startsWith(u8, k.*, "t/")) try after.append(k.*);
        }
    }
    try testing.expectEqual(@as(usize, 1), after.items.len);
    try testing.expect(!std.mem.eql(u8, first, after.items[0]));
}

test "a batch of concurrent requests rides one commit" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    const a = h.arena.allocator();

    var replies: [20]Harness.Reply = undefined;
    var works: [20]Work = undefined;
    for (0..20) |i| {
        replies[i] = .{};
        const body = try std.fmt.allocPrint(
            a,
            "{{\"id\":\"o:p{d}\",\"timeoutAt\":9000000000000}}",
            .{i},
        );
        works[i] = .{
            .kind = .{ .request = .promise_create },
            .corr_id = "c",
            .data = try json.parse(a, body),
            .now = h.sim.now,
            .arena = a,
            .callback = Harness.Reply.callback,
            .context = &replies[i],
        };
    }
    // Everything is submitted before anything is drained, which is exactly what
    // one poll of the event loop does, so they all ride one commit.
    const puts_before = h.mem.puts;
    for (&works) |*w| h.applier.submit("o", w);
    h.applier.drain();

    for (replies) |r| try testing.expectEqual(@as(i32, 200), r.status);
    // One document write for all twenty, not twenty.
    try testing.expect(h.mem.puts - puts_before < 20);
    try testing.expectEqual(@as(u64, 1), h.applier.commits);

    // And all twenty are there.
    for (0..20) |i| {
        const body = try std.fmt.allocPrint(a, "{{\"id\":\"o:p{d}\"}}", .{i});
        const got = try h.call("promise.get", body);
        try testing.expectEqual(@as(i32, 200), got.status);
    }
}

test "each request in a batch sees the one before it" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    const a = h.arena.allocator();

    // Ten identical creates: the first creates, the rest are idempotent and
    // must all report the same createdAt.
    var replies: [10]Harness.Reply = undefined;
    var works: [10]Work = undefined;
    const data = try json.parse(a, "{\"id\":\"o:same\",\"timeoutAt\":9000000000000}");
    for (0..10) |i| {
        replies[i] = .{};
        works[i] = .{
            .kind = .{ .request = .promise_create },
            .corr_id = "c",
            .data = data,
            .now = h.sim.now + @as(i64, @intCast(i)),
            .arena = a,
            .callback = Harness.Reply.callback,
            .context = &replies[i],
        };
    }
    for (&works) |*w| h.applier.submit("o", w);
    h.applier.drain();
    for (replies) |r| {
        try testing.expectEqual(@as(i32, 200), r.status);
        try testing.expectEqualStrings(replies[0].data, r.data);
    }
}

test "a lost race is re-decided, not replayed" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    const a = h.arena.allocator();

    // Trust the cache, so the applier decides against a version the store has
    // moved past and its compare-and-swap is refused. That is the path this test
    // is about; with the cache validated the staleness is caught by the read
    // instead, which is the same correctness and a different code path.
    h.applier.cfg.linearizable_reads = false;

    // Commit once, so the applier holds a cached document and the version it was
    // read at.
    _ = try h.call("promise.create", "{\"id\":\"o:seed\",\"timeoutAt\":9000000000000}");

    // Now move the stored document on behind the applier's back, the way another
    // process would.
    {
        var d = Doc.init(testing.allocator);
        defer d.deinit();
        const owned = d.allocator();
        _ = try d.promise_insert(.{
            .id = try owned.dupe(u8, "o:a"),
            .state = .resolved,
            .param = .empty,
            .value = .empty,
            .tags = .empty,
            .timeout_at = 9_000_000_000_000,
            .created_at = 123,
            .settled_at = 456,
            .timeout_armed = false,
            .callbacks = .empty,
            .listeners = .empty,
        });
        var buf = std.ArrayList(u8).init(a);
        try d.encode(&buf, "o");
        var op = store_mod.Operation{
            .kind = .put,
            .key = "wf/o",
            .body = buf.items,
            .precondition = .none,
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        h.mem.store().submit(&op);
        try testing.expect(op.result == .written);
    }

    // The create races and loses, re-reads, and decides again. The answer is the
    // *stored* promise, which a replay of the first decision could never produce.
    const r = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":1}");
    try testing.expectEqual(@as(i32, 200), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"state\":\"resolved\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.data, "\"createdAt\":123") != null);
    try testing.expect(h.applier.contentions >= 1);
    // And the promise the other writer replaced is gone: nothing was invented.
    const seed = try h.call("promise.get", "{\"id\":\"o:seed\"}");
    try testing.expectEqual(@as(i32, 404), seed.status);
}

test "a validated read catches a write another process made" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    const a = h.arena.allocator();

    // Commit, so the document is cached.
    _ = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");

    // Another process settles it.
    {
        var d = Doc.init(testing.allocator);
        defer d.deinit();
        const owned = d.allocator();
        _ = try d.promise_insert(.{
            .id = try owned.dupe(u8, "o:a"),
            .state = .rejected,
            .param = .empty,
            .value = .empty,
            .tags = .empty,
            .timeout_at = 9_000_000_000_000,
            .created_at = 1_000_000_000,
            .settled_at = 1_000_000_001,
            .timeout_armed = false,
            .callbacks = .empty,
            .listeners = .empty,
        });
        var buf = std.ArrayList(u8).init(a);
        try d.encode(&buf, "o");
        var op = store_mod.Operation{
            .kind = .put,
            .key = "wf/o",
            .body = buf.items,
            .precondition = .none,
            .arena = a,
            .callback = struct {
                fn cb(_: *store_mod.Operation) void {}
            }.cb,
        };
        h.mem.store().submit(&op);
    }

    // A read must see it. Serving the cached copy would be a read of a state the
    // bucket has moved past, which is exactly the violation the simulator found.
    const got = try h.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "\"state\":\"rejected\"") != null);
}

test "contention past the retry limit is reported rather than hidden" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    h.mem.random = &h.rng;
    // Every conditional write is refused, so no attempt can ever land.
    h.mem.faults.conflict_percent = 100;
    const r = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    try testing.expectEqual(@as(i32, 503), r.status);
    try testing.expect(std.mem.indexOf(u8, r.data, "contended") != null);
}

test "a store that will not answer becomes a 503, and nothing is sent" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    h.mem.random = &h.rng;
    h.mem.faults.unavailable_percent = 100;
    const r = try h.call("promise.create",
        \\{"id":"o:a","timeoutAt":9000000000000,"tags":{"resonate:target":"http://w:1"}}
    );
    try testing.expectEqual(@as(i32, 503), r.status);
    try testing.expectEqual(@as(usize, 0), h.bus_sent.items.len);
    try testing.expectEqual(@as(u64, 0), h.applier.commits);
}

test "a write that landed under a lost acknowledgement is safe to retry" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    h.mem.random = &h.rng;
    h.mem.faults.lost_ack_percent = 100;

    const first = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    try testing.expectEqual(@as(i32, 503), first.status);

    // It landed. The retry says so, and says it only once.
    h.mem.faults.lost_ack_percent = 0;
    const retry = try h.call("promise.create", "{\"id\":\"o:a\",\"timeoutAt\":9000000000000}");
    try testing.expectEqual(@as(i32, 200), retry.status);
    try testing.expect(std.mem.indexOf(u8, retry.data, "\"createdAt\":1000000000") != null);
}

test "two appliers over one store converge without losing a transition" {
    // Separate caches and separate actors, one bucket. This is the case the
    // single-writer-per-origin optimisation is *not* allowed to be needed for.
    var mem = store_mod.MemoryStore.init(testing.allocator);
    defer mem.deinit();
    var sim = env.Simulated.init(testing.allocator, 1_000_000_000);
    defer sim.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const Nowhere = struct {
        dummy: u8 = 0,
        fn bus(self: *@This()) env.MessageBus {
            return .{ .ptr = self, .vtable = &.{ .send = send, .serves = serves } };
        }
        fn serves(_: *anyopaque, _: []const u8) bool {
            return true;
        }
        fn send(_: *anyopaque, delivery: *env.Delivery) void {
            delivery.complete(.delivered, "");
        }
    };

    var nowhere = Nowhere{};
    var senders: [2]sender_mod.Sender = undefined;
    var appliers: [2]Applier = undefined;
    var rngs: [2]stdx.Random = .{ stdx.Random.init(11), stdx.Random.init(22) };
    for (0..2) |i| {
        senders[i] = sender_mod.Sender.init(testing.allocator, nowhere.bus(), "http://s");
        appliers[i] = Applier.init(
            testing.allocator,
            mem.store(),
            KeySpace.init("", 4),
            sim.clock(),
            sim.timer(),
            &senders[i],
            .{},
        );
        appliers[i].random = &rngs[i];
    }
    defer for (0..2) |i| {
        appliers[i].deinit();
        senders[i].deinit();
    };

    var replies: [12]Harness.Reply = undefined;
    var works: [12]Work = undefined;
    for (0..12) |i| {
        replies[i] = .{};
        const body = try std.fmt.allocPrint(a, "{{\"id\":\"o:p{d}\",\"timeoutAt\":9000000000000}}", .{i});
        works[i] = .{
            .kind = .{ .request = .promise_create },
            .corr_id = "c",
            .data = try json.parse(a, body),
            .now = sim.now,
            .arena = a,
            .callback = Harness.Reply.callback,
            .context = &replies[i],
        };
        // Alternating, so the two appliers race each other on every write.
        appliers[i % 2].submit("o", &works[i]);
    }
    var guard: usize = 0;
    while (true) {
        guard += 1;
        if (guard > 10_000) return error.NeverSettled;
        var all_done = true;
        for (replies) |r| {
            if (!r.done) all_done = false;
        }
        if (all_done) break;
        _ = sim.advance_to(sim.now + 100);
        for (&appliers) |*ap| ap.drain();
    }
    for (replies) |r| try testing.expectEqual(@as(i32, 200), r.status);

    // All twelve survived, in one document.
    const entry = mem.objects.get("wf/o").?;
    var d = try Doc.decode(testing.allocator, entry.body, "o");
    defer d.deinit();
    try testing.expectEqual(@as(usize, 12), d.promises.items.len);
}

test "a sweep fires a due deadline through the same commit path" {
    const h = try Harness.create(testing.allocator);
    defer h.destroy();
    const a = h.arena.allocator();
    _ = try h.call("promise.create",
        \\{"id":"o:a","timeoutAt":1000002000,"tags":{"resonate:target":"http://w:1"}}
    );
    h.forget_sent();
    h.sim.now = 1_000_002_000;

    var reply = Harness.Reply{};
    var work = Work{
        .kind = .sweep,
        .now = h.sim.now,
        .arena = a,
        .callback = Harness.Reply.callback,
        .context = &reply,
    };
    h.applier.submit("o", &work);
    h.applier.drain();
    try testing.expect(reply.done);

    const got = try h.call("promise.get", "{\"id\":\"o:a\"}");
    try testing.expect(std.mem.indexOf(u8, got.data, "rejected_timedout") != null);
    // The deadline is gone, so the document no longer arms one.
    const entry = h.mem.objects.get("wf/o").?;
    var d = try Doc.decode(testing.allocator, entry.body, "o");
    defer d.deinit();
    try testing.expect(d.timer_at == null);
}
