//! The environment: everything outside the server that the server needs.
//!
//! Three ports — a clock, a timer, and a message bus — beside the object store
//! in `store.zig`. Together they are the whole of this program's contact with
//! the world, and that is the point: production wires them to `io_uring`, and
//! the simulator wires them to a deterministic scheduler it owns. Nothing above
//! this file can tell which, so a bug found under one is a bug in the other.
//!
//! Every port is completion-based and every callback runs on the one thread that
//! runs everything. There are no locks in this program because there is nothing
//! to lock.

const std = @import("std");
const stdx = @import("stdx.zig");

/// Wall time, in Unix milliseconds.
///
/// Never `std.time.milliTimestamp()` at a call site. A clock the program cannot
/// replace is a clock a test cannot control, and a server whose behaviour
/// depends on an uncontrollable input is a server whose behaviour cannot be
/// reproduced.
pub const Clock = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        now_ms: *const fn (ptr: *anyopaque) i64,
    };

    pub fn now_ms(self: Clock) i64 {
        return self.vtable.now_ms(self.ptr);
    }
};

/// A deadline the program set for itself.
///
/// Submitted, then completed once, at or after `at_ms`. Cancelling one that has
/// already fired is harmless, which is what lets a caller cancel without first
/// asking whether it needs to.
pub const Timeout = struct {
    at_ms: i64,
    callback: *const fn (*Timeout) void,
    context: ?*anyopaque = null,
    /// Set while the timer holds it. A caller must not rearm an armed timeout.
    armed: bool = false,
    /// Intrusive link for the timer's own queue.
    next: ?*Timeout = null,

    pub fn fire(self: *Timeout) void {
        self.armed = false;
        self.callback(self);
    }
};

pub const Timer = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        arm: *const fn (ptr: *anyopaque, timeout: *Timeout) void,
        cancel: *const fn (ptr: *anyopaque, timeout: *Timeout) void,
    };

    pub fn arm(self: Timer, timeout: *Timeout, at_ms: i64) void {
        stdx.assert(!timeout.armed);
        timeout.at_ms = at_ms;
        timeout.armed = true;
        self.vtable.arm(self.ptr, timeout);
    }

    pub fn cancel(self: Timer, timeout: *Timeout) void {
        if (!timeout.armed) return;
        self.vtable.cancel(self.ptr, timeout);
        timeout.armed = false;
    }
};

/// One message on its way to a worker.
pub const Delivery = struct {
    pub const Outcome = enum {
        pending,
        /// The worker took it. Nothing more is owed.
        delivered,
        /// It did not arrive. Whether it is retried is the sender's decision,
        /// not the bus's.
        failed,
    };

    /// The worker's address, in full, scheme included. The bus routes on the
    /// scheme and hands the rest to whatever serves it.
    address: []const u8,
    /// The message, already JSON. The bus must not retain it past completion.
    body: []const u8,

    callback: *const fn (*Delivery) void,
    context: ?*anyopaque = null,
    outcome: Outcome = .pending,
    /// Why it failed, for the log. Never crosses the wire.
    detail: []const u8 = "",
    next: ?*Delivery = null,

    pub fn complete(self: *Delivery, outcome: Outcome, detail: []const u8) void {
        stdx.assert(self.outcome == .pending);
        self.outcome = outcome;
        self.detail = detail;
        self.callback(self);
    }
};

/// The message bus: the one way out of this program.
///
/// Outbound only. Inbound requests arrive by the server's own entry point,
/// because a request needs a reply and a message does not — folding both into
/// one port would mean every caller carrying a reply channel it has no use for.
pub const MessageBus = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (ptr: *anyopaque, delivery: *Delivery) void,
        /// Does this bus serve that scheme? Asked before a send, so an address
        /// nothing can route is reported as unroutable rather than queued
        /// forever.
        serves: *const fn (ptr: *anyopaque, scheme: []const u8) bool,
    };

    pub fn send(self: MessageBus, delivery: *Delivery) void {
        self.vtable.send(self.ptr, delivery);
    }

    pub fn serves(self: MessageBus, scheme: []const u8) bool {
        return self.vtable.serves(self.ptr, scheme);
    }
};

// ── A clock and a timer for tests and for the simulator ───────────────────────

/// A clock the caller moves by hand, and a timer that fires from it.
///
/// Used by the simulator and by the tests in this tree. It is not a lesser
/// implementation of the ports: it is the one that makes a run reproducible, and
/// the production pair exists only because the world insists on having its own
/// clock.
pub const Simulated = struct {
    now: i64,
    /// Armed timeouts, unordered. Firing walks the list, which is fine at the
    /// sizes this holds — one per origin actor plus one per subsystem — and
    /// keeps the order a function of the list rather than of a heap's shape.
    armed: std.ArrayListUnmanaged(*Timeout) = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, start_ms: i64) Simulated {
        return .{ .now = start_ms, .allocator = allocator };
    }

    pub fn deinit(self: *Simulated) void {
        self.armed.deinit(self.allocator);
    }

    pub fn clock(self: *Simulated) Clock {
        return .{ .ptr = self, .vtable = &clock_vtable };
    }

    pub fn timer(self: *Simulated) Timer {
        return .{ .ptr = self, .vtable = &timer_vtable };
    }

    const clock_vtable: Clock.VTable = .{ .now_ms = now_ms_erased };
    const timer_vtable: Timer.VTable = .{ .arm = arm_erased, .cancel = cancel_erased };

    fn now_ms_erased(ptr: *anyopaque) i64 {
        const self: *Simulated = @ptrCast(@alignCast(ptr));
        return self.now;
    }

    fn arm_erased(ptr: *anyopaque, timeout: *Timeout) void {
        const self: *Simulated = @ptrCast(@alignCast(ptr));
        self.armed.append(self.allocator, timeout) catch {
            // Nowhere to keep it. Firing now is wrong but losing it is worse:
            // a deadline that never fires stalls the system silently.
            timeout.fire();
        };
    }

    fn cancel_erased(ptr: *anyopaque, timeout: *Timeout) void {
        const self: *Simulated = @ptrCast(@alignCast(ptr));
        for (self.armed.items, 0..) |t, i| {
            if (t == timeout) {
                _ = self.armed.swapRemove(i);
                return;
            }
        }
    }

    /// The earliest armed deadline, or null when nothing is waiting.
    pub fn next_deadline(self: *const Simulated) ?i64 {
        var best: ?i64 = null;
        for (self.armed.items) |t| {
            if (best == null or t.at_ms < best.?) best = t.at_ms;
        }
        return best;
    }

    /// Move to `to_ms` and fire everything that comes due, earliest first.
    ///
    /// Returns how many fired. A timeout that rearms itself while firing is
    /// picked up by the next pass rather than this one, so a self-rearming
    /// deadline cannot spin.
    pub fn advance_to(self: *Simulated, to_ms: i64) usize {
        var fired: usize = 0;
        while (true) {
            var earliest: ?*Timeout = null;
            var earliest_index: usize = 0;
            for (self.armed.items, 0..) |t, i| {
                if (t.at_ms > to_ms) continue;
                if (earliest == null or t.at_ms < earliest.?.at_ms) {
                    earliest = t;
                    earliest_index = i;
                }
            }
            const t = earliest orelse break;
            _ = self.armed.swapRemove(earliest_index);
            // The clock reads the deadline while its own callback runs, so
            // anything the callback asks the clock is answered consistently.
            if (t.at_ms > self.now) self.now = t.at_ms;
            t.fire();
            fired += 1;
        }
        if (to_ms > self.now) self.now = to_ms;
        return fired;
    }

    /// Fire everything due at the current instant, without moving the clock.
    pub fn tick(self: *Simulated) usize {
        return self.advance_to(self.now);
    }

    pub fn armed_count(self: *const Simulated) usize {
        return self.armed.items.len;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "the simulated timer fires in deadline order and moves the clock with it" {
    var sim = Simulated.init(testing.allocator, 1_000);
    defer sim.deinit();
    const t = sim.timer();

    const Recorder = struct {
        order: std.ArrayList(i64),
        fn cb(timeout: *Timeout) void {
            const self: *@This() = @ptrCast(@alignCast(timeout.context.?));
            self.order.append(timeout.at_ms) catch unreachable;
        }
    };
    var rec = Recorder{ .order = std.ArrayList(i64).init(testing.allocator) };
    defer rec.order.deinit();

    var a = Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &rec };
    var b = Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &rec };
    var c = Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &rec };
    t.arm(&b, 3_000);
    t.arm(&a, 2_000);
    t.arm(&c, 9_000);
    try testing.expectEqual(@as(usize, 3), sim.armed_count());
    try testing.expectEqual(@as(i64, 2_000), sim.next_deadline().?);

    try testing.expectEqual(@as(usize, 2), sim.advance_to(5_000));
    try testing.expectEqual(@as(usize, 2), rec.order.items.len);
    try testing.expectEqual(@as(i64, 2_000), rec.order.items[0]);
    try testing.expectEqual(@as(i64, 3_000), rec.order.items[1]);
    try testing.expectEqual(@as(i64, 5_000), sim.now);

    // Cancelling one that is still armed removes it.
    t.cancel(&c);
    try testing.expectEqual(@as(usize, 0), sim.armed_count());
    try testing.expectEqual(@as(usize, 0), sim.advance_to(100_000));

    // Cancelling one that already fired is harmless.
    t.cancel(&a);
}

test "a timeout that rearms itself does not spin" {
    var sim = Simulated.init(testing.allocator, 0);
    defer sim.deinit();
    const t = sim.timer();
    const Rearmer = struct {
        timer: Timer,
        count: usize = 0,
        fn cb(timeout: *Timeout) void {
            const self: *@This() = @ptrCast(@alignCast(timeout.context.?));
            self.count += 1;
            if (self.count < 5) self.timer.arm(timeout, timeout.at_ms + 10);
        }
    };
    var r = Rearmer{ .timer = t };
    var timeout = Timeout{ .at_ms = 0, .callback = Rearmer.cb, .context = &r };
    t.arm(&timeout, 10);
    // One pass reaches every rearm inside the window, and then stops.
    _ = sim.advance_to(1_000);
    try testing.expectEqual(@as(usize, 5), r.count);
}

test "the clock reads the deadline of the callback that is running" {
    var sim = Simulated.init(testing.allocator, 0);
    defer sim.deinit();
    const Reader = struct {
        clock: Clock,
        seen: i64 = -1,
        fn cb(timeout: *Timeout) void {
            const self: *@This() = @ptrCast(@alignCast(timeout.context.?));
            self.seen = self.clock.now_ms();
        }
    };
    var r = Reader{ .clock = sim.clock() };
    var timeout = Timeout{ .at_ms = 0, .callback = Reader.cb, .context = &r };
    sim.timer().arm(&timeout, 700);
    _ = sim.advance_to(5_000);
    try testing.expectEqual(@as(i64, 700), r.seen);
}
