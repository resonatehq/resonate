//! The event loop: io_uring, one thread, and no locks anywhere above it.
//!
//! Every operation is submitted with a pointer to its own completion as the
//! kernel's user data, and every completion runs its callback on the one thread
//! that runs everything else. That is why nothing above this file takes a lock:
//! there is nothing to take one against.
//!
//! The loop also provides the clock and the timer ports from `env.zig`, so the
//! whole server can be handed a production environment or a simulated one and
//! cannot tell the difference.
//!
//! ## Why the sleep is capped
//!
//! The loop sleeps until the nearest deadline it knows about, or
//! `max_sleep_ms`, whichever is sooner. The cap is not a correctness
//! requirement — a deadline armed while the loop is already asleep would
//! otherwise wait for the sleep it was armed during, and the cap bounds that at
//! a few milliseconds instead of adding a cancel-and-rearm dance to every arm.
//! Deadlines in this protocol are measured in seconds.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const posix = std.posix;
const stdx = @import("stdx.zig");
const env = @import("env.zig");

const assert = stdx.assert;

pub const max_sleep_ms: i64 = 20;
/// How many submission slots the ring holds. Each in-flight operation takes one.
pub const ring_entries: u16 = 512;

pub const Error = error{
    /// io_uring is not available, or the kernel refused the ring.
    Unavailable,
    SubmissionFailed,
};

/// One submitted operation.
///
/// Owned by whoever submitted it, and valid until its callback runs. The kernel
/// holds a pointer to it, so it must not move.
pub const Completion = struct {
    callback: *const fn (*Completion) void,
    context: ?*anyopaque = null,
    /// The raw result: bytes transferred, a file descriptor, or a negative
    /// errno. Interpreted by the caller, which knows what it asked for.
    result: i32 = 0,
    /// Storage the kernel reads **at submission time**, which is a later
    /// syscall than the call that prepared it — so it lives here, in the
    /// completion, rather than on the stack of whoever submitted the operation.
    /// Getting this wrong hands the kernel a dangling pointer and produces an
    /// error that looks like the peer's fault.
    timespec: linux.kernel_timespec = .{ .sec = 0, .nsec = 0 },
    address: std.net.Address = undefined,
    address_len: posix.socklen_t = @sizeOf(std.net.Address),

    pub fn err(self: *const Completion) ?posix.E {
        if (self.result >= 0) return null;
        return @enumFromInt(@as(u16, @intCast(-self.result)));
    }
};

pub const IO = struct {
    ring: linux.IoUring,
    in_flight: u32 = 0,

    pub fn init() Error!IO {
        // A ring without SQPOLL or IOPOLL: this is a latency-bound workload
        // talking to a network, not a storage benchmark, and a polling thread
        // would burn a core to save microseconds nobody can measure next to an
        // object store.
        const ring = linux.IoUring.init(ring_entries, 0) catch return error.Unavailable;
        return .{ .ring = ring };
    }

    pub fn deinit(self: *IO) void {
        self.ring.deinit();
    }

    fn sqe(self: *IO) Error!*linux.io_uring_sqe {
        return self.ring.get_sqe() catch {
            // The ring is full. Submitting what is queued frees slots; if that
            // does not, the caller is over its own concurrency bound, which is a
            // bug in the caller rather than a condition to paper over.
            _ = self.ring.submit() catch return error.SubmissionFailed;
            return self.ring.get_sqe() catch error.SubmissionFailed;
        };
    }

    pub fn accept(self: *IO, completion: *Completion, listener: posix.socket_t) Error!void {
        const s = try self.sqe();
        completion.address_len = @sizeOf(std.net.Address);
        s.prep_accept(listener, &completion.address.any, &completion.address_len, posix.SOCK.CLOEXEC);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    pub fn recv(self: *IO, completion: *Completion, socket: posix.socket_t, buffer: []u8) Error!void {
        const s = try self.sqe();
        s.prep_recv(socket, buffer, 0);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    pub fn send(self: *IO, completion: *Completion, socket: posix.socket_t, buffer: []const u8) Error!void {
        const s = try self.sqe();
        // `MSG_NOSIGNAL`, because a peer that hangs up mid-response would
        // otherwise raise `SIGPIPE`, whose default disposition is to end the
        // process. A client closing its connection is not an event a server dies
        // of; it is `EPIPE` on this one send, which the caller already handles.
        s.prep_send(socket, buffer, posix.MSG.NOSIGNAL);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    /// Connect. The address is copied into the completion, because the kernel
    /// reads it when the ring is submitted rather than now.
    pub fn connect(
        self: *IO,
        completion: *Completion,
        socket: posix.socket_t,
        address: std.net.Address,
    ) Error!void {
        const s = try self.sqe();
        completion.address = address;
        completion.address_len = address.getOsSockLen();
        s.prep_connect(socket, &completion.address.any, completion.address_len);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    pub fn close(self: *IO, completion: *Completion, fd: posix.fd_t) Error!void {
        const s = try self.sqe();
        s.prep_close(fd);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    /// A relative timeout. Completes with `-ETIME` when it expires, which is the
    /// success case and not an error.
    pub fn timeout(self: *IO, completion: *Completion, nanoseconds: u64) Error!void {
        const s = try self.sqe();
        completion.timespec = .{
            .sec = @intCast(nanoseconds / std.time.ns_per_s),
            .nsec = @intCast(nanoseconds % std.time.ns_per_s),
        };
        s.prep_timeout(&completion.timespec, 0, 0);
        s.user_data = @intFromPtr(completion);
        self.in_flight += 1;
    }

    /// Submit what is queued and run every completion that is ready.
    ///
    /// `wait` says whether to block for at least one completion. Returns how many
    /// ran.
    pub fn tick(self: *IO, wait: bool) Error!u32 {
        if (wait and self.in_flight > 0) {
            _ = self.ring.submit_and_wait(1) catch |e| switch (e) {
                // A signal arrived. Nothing was lost; the caller loops.
                error.SignalInterrupt => {},
                else => return error.SubmissionFailed,
            };
        } else {
            _ = self.ring.submit() catch |e| switch (e) {
                error.SignalInterrupt => {},
                else => return error.SubmissionFailed,
            };
        }

        var ran: u32 = 0;
        var cqes: [256]linux.io_uring_cqe = undefined;
        while (true) {
            const count = self.ring.copy_cqes(&cqes, 0) catch return error.SubmissionFailed;
            if (count == 0) break;
            for (cqes[0..count]) |cqe| {
                assert(self.in_flight > 0);
                self.in_flight -= 1;
                if (cqe.user_data == 0) continue;
                const completion: *Completion = @ptrFromInt(cqe.user_data);
                completion.result = cqe.res;
                completion.callback(completion);
                ran += 1;
            }
        }
        return ran;
    }
};

// ── The loop ──────────────────────────────────────────────────────────────────

/// The clock, the timer, and the run loop, over one ring.
pub const Loop = struct {
    allocator: std.mem.Allocator,
    io: IO,
    monotonic: stdx.MonotonicMillis = .{},
    timeouts: std.ArrayListUnmanaged(*env.Timeout) = .{},
    sleeper: Completion = undefined,
    sleeping: bool = false,
    running: bool = true,

    /// Called once per iteration, after the completions have run: the place
    /// everything that batches gets to commit what arrived together.
    on_iteration: ?*const fn (context: ?*anyopaque) void = null,
    on_iteration_context: ?*anyopaque = null,

    pub fn init(allocator: std.mem.Allocator) Error!Loop {
        return .{ .allocator = allocator, .io = try IO.init() };
    }

    pub fn deinit(self: *Loop) void {
        self.timeouts.deinit(self.allocator);
        self.io.deinit();
    }

    pub fn clock(self: *Loop) env.Clock {
        return .{ .ptr = self, .vtable = &clock_vtable };
    }

    pub fn timer(self: *Loop) env.Timer {
        return .{ .ptr = self, .vtable = &timer_vtable };
    }

    const clock_vtable: env.Clock.VTable = .{ .now_ms = now_ms_erased };
    const timer_vtable: env.Timer.VTable = .{ .arm = arm_erased, .cancel = cancel_erased };

    fn now_ms_erased(ptr: *anyopaque) i64 {
        const self: *Loop = @ptrCast(@alignCast(ptr));
        return self.monotonic.advance(std.time.milliTimestamp());
    }

    fn arm_erased(ptr: *anyopaque, t: *env.Timeout) void {
        const self: *Loop = @ptrCast(@alignCast(ptr));
        self.timeouts.append(self.allocator, t) catch {
            // Nowhere to keep it. Firing now is wrong; losing it is worse, since
            // a deadline that never fires stalls the system in silence.
            t.fire();
        };
    }

    fn cancel_erased(ptr: *anyopaque, t: *env.Timeout) void {
        const self: *Loop = @ptrCast(@alignCast(ptr));
        for (self.timeouts.items, 0..) |item, i| {
            if (item == t) {
                _ = self.timeouts.swapRemove(i);
                return;
            }
        }
    }

    fn next_deadline(self: *const Loop) ?i64 {
        var best: ?i64 = null;
        for (self.timeouts.items) |t| {
            if (best == null or t.at_ms < best.?) best = t.at_ms;
        }
        return best;
    }

    /// Fire every deadline that has come due, earliest first.
    fn fire_due(self: *Loop) usize {
        var fired: usize = 0;
        const now = self.monotonic.advance(std.time.milliTimestamp());
        while (true) {
            var earliest: ?*env.Timeout = null;
            var index: usize = 0;
            for (self.timeouts.items, 0..) |t, i| {
                if (t.at_ms > now) continue;
                if (earliest == null or t.at_ms < earliest.?.at_ms) {
                    earliest = t;
                    index = i;
                }
            }
            const t = earliest orelse break;
            _ = self.timeouts.swapRemove(index);
            t.fire();
            fired += 1;
        }
        return fired;
    }

    fn on_sleep_done(completion: *Completion) void {
        const self: *Loop = @ptrCast(@alignCast(completion.context.?));
        self.sleeping = false;
    }

    /// One turn: wait for something to happen, run it, fire what is due, and let
    /// the subsystems commit whatever arrived together.
    pub fn tick(self: *Loop) !void {
        // Arm a sleep so the ring has something to wake on even when no socket
        // does. One at a time: an expired one that has not been reaped yet is
        // still a wake-up, so a second would only add a syscall.
        if (!self.sleeping) {
            const now = self.monotonic.advance(std.time.milliTimestamp());
            var sleep_ms: i64 = max_sleep_ms;
            if (self.next_deadline()) |at| {
                const until = at - now;
                if (until < sleep_ms) sleep_ms = until;
            }
            if (sleep_ms < 0) sleep_ms = 0;
            self.sleeper = .{ .callback = on_sleep_done, .context = self };
            try self.io.timeout(&self.sleeper, @intCast(sleep_ms * std.time.ns_per_ms));
            self.sleeping = true;
        }
        _ = try self.io.tick(true);
        _ = self.fire_due();
        if (self.on_iteration) |hook| hook(self.on_iteration_context);
    }

    pub fn run(self: *Loop) !void {
        while (self.running) try self.tick();
    }

    pub fn stop(self: *Loop) void {
        self.running = false;
    }
};

// ── Sockets ───────────────────────────────────────────────────────────────────

/// A listening socket, ready for `IO.accept`.
pub fn listen(address: std.net.Address, backlog: u31) !posix.socket_t {
    const socket = try posix.socket(
        address.any.family,
        posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        posix.IPPROTO.TCP,
    );
    errdefer posix.close(socket);
    // So a restart does not have to wait out TIME_WAIT.
    try posix.setsockopt(socket, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    try posix.bind(socket, &address.any, address.getOsSockLen());
    try posix.listen(socket, backlog);
    return socket;
}

/// A socket for an outbound connection. Not connected yet: `IO.connect` does
/// that, so the wait happens in the ring rather than on this thread.
pub fn connect_socket(family: u32) !posix.socket_t {
    const socket = try posix.socket(
        family,
        posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        posix.IPPROTO.TCP,
    );
    errdefer posix.close(socket);
    // Small writes are a request head followed by a body; waiting to coalesce
    // them costs a round trip.
    posix.setsockopt(socket, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};
    return socket;
}

/// The port a listening socket ended up on. Needed when the caller asked for
/// port 0 and let the kernel choose.
pub fn bound_port(socket: posix.socket_t) !u16 {
    var storage: posix.sockaddr.in = undefined;
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
    try posix.getsockname(socket, @ptrCast(&storage), &len);
    return std.mem.bigToNative(u16, storage.port);
}

pub fn set_nodelay(socket: posix.socket_t) void {
    posix.setsockopt(socket, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "the ring completes a timeout" {
    var io = IO.init() catch return error.SkipZigTest;
    defer io.deinit();

    const Fired = struct {
        var done: bool = false;
        var result: i32 = 0;
        fn cb(c: *Completion) void {
            done = true;
            result = c.result;
        }
    };
    Fired.done = false;
    var completion = Completion{ .callback = Fired.cb };
    try io.timeout(&completion, 1 * std.time.ns_per_ms);
    while (!Fired.done) _ = try io.tick(true);
    // An expired timeout reports ETIME, which is the success case.
    try testing.expectEqual(posix.E.TIME, completion.err().?);
}

test "a loop round trips bytes over a real socket" {
    var loop = Loop.init(testing.allocator) catch return error.SkipZigTest;
    defer loop.deinit();

    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    const listener = try listen(address, 8);
    defer posix.close(listener);

    // Which port the kernel chose.
    const port = try bound_port(listener);

    const State = struct {
        loop: *Loop,
        accepted: posix.socket_t = -1,
        client: posix.socket_t = -1,
        connected: bool = false,
        received: [64]u8 = undefined,
        received_len: usize = 0,
        sent: bool = false,

        accept_c: Completion = undefined,
        connect_c: Completion = undefined,
        send_c: Completion = undefined,
        recv_c: Completion = undefined,

        fn on_accept(c: *Completion) void {
            const self: *@This() = @ptrCast(@alignCast(c.context.?));
            self.accepted = @intCast(c.result);
            self.recv_c = .{ .callback = on_recv, .context = self };
            self.loop.io.recv(&self.recv_c, self.accepted, &self.received) catch unreachable;
        }
        fn on_recv(c: *Completion) void {
            const self: *@This() = @ptrCast(@alignCast(c.context.?));
            self.received_len = @intCast(c.result);
        }
        fn on_connect(c: *Completion) void {
            const self: *@This() = @ptrCast(@alignCast(c.context.?));
            self.connected = c.result == 0;
            self.send_c = .{ .callback = on_send, .context = self };
            self.loop.io.send(&self.send_c, self.client, "hello ring") catch unreachable;
        }
        fn on_send(c: *Completion) void {
            const self: *@This() = @ptrCast(@alignCast(c.context.?));
            self.sent = c.result == 10;
        }
    };
    var state = State{ .loop = &loop };
    state.accept_c = .{ .callback = State.on_accept, .context = &state };
    try loop.io.accept(&state.accept_c, listener);

    state.client = try connect_socket(address.any.family);
    defer posix.close(state.client);
    const target = try std.net.Address.parseIp("127.0.0.1", port);
    state.connect_c = .{ .callback = State.on_connect, .context = &state };
    try loop.io.connect(&state.connect_c, state.client, target);

    var guard: usize = 0;
    while (state.received_len == 0) {
        guard += 1;
        if (guard > 500) return error.NeverArrived;
        try loop.tick();
    }
    try testing.expect(state.connected);
    try testing.expect(state.sent);
    try testing.expectEqualStrings("hello ring", state.received[0..state.received_len]);
    posix.close(state.accepted);
}

test "the loop's timer fires deadlines in order" {
    var loop = Loop.init(testing.allocator) catch return error.SkipZigTest;
    defer loop.deinit();
    const t = loop.timer();

    const Recorder = struct {
        var order: [3]u8 = .{ 0, 0, 0 };
        var count: usize = 0;
        fn cb(timeout: *env.Timeout) void {
            const which: *u8 = @ptrCast(@alignCast(timeout.context.?));
            order[count] = which.*;
            count += 1;
        }
    };
    Recorder.count = 0;
    var ids: [3]u8 = .{ 1, 2, 3 };
    const now = loop.clock().now_ms();
    var a = env.Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &ids[0] };
    var b = env.Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &ids[1] };
    var c = env.Timeout{ .at_ms = 0, .callback = Recorder.cb, .context = &ids[2] };
    t.arm(&c, now + 30);
    t.arm(&a, now + 1);
    t.arm(&b, now + 15);

    var guard: usize = 0;
    while (Recorder.count < 3) {
        guard += 1;
        if (guard > 500) return error.NeverFired;
        try loop.tick();
    }
    try testing.expectEqual(@as(u8, 1), Recorder.order[0]);
    try testing.expectEqual(@as(u8, 2), Recorder.order[1]);
    try testing.expectEqual(@as(u8, 3), Recorder.order[2]);
}

test "the loop calls its iteration hook" {
    var loop = Loop.init(testing.allocator) catch return error.SkipZigTest;
    defer loop.deinit();
    const Hook = struct {
        var calls: usize = 0;
        fn cb(_: ?*anyopaque) void {
            calls += 1;
        }
    };
    Hook.calls = 0;
    loop.on_iteration = Hook.cb;
    try loop.tick();
    try loop.tick();
    try testing.expectEqual(@as(usize, 2), Hook.calls);
}
