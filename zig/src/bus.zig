//! HTTP push: the server calls the worker.
//!
//! One transport, two schemes, and the whole of it is a POST. A worker is a URL
//! that accepts `application/json`, which is what makes this the transport for
//! anything with an address — a container, a function, a laptop behind a proxy.
//!
//! A non-2xx answer is a failure, not a retry: the server is not the worker's
//! supervisor. What makes a lost offer harmless is the task's retry deadline,
//! which is committed to the object store *before* the offer goes out, so the
//! recovery is durable and does not depend on this file being clever.

const std = @import("std");
const stdx = @import("stdx.zig");
const env = @import("env.zig");
const http = @import("http.zig");
const net = @import("net.zig");

const assert = stdx.assert;

pub const HttpPush = struct {
    const Pending = struct {
        bus: *HttpPush,
        delivery: *env.Delivery,
        call: net.Call,
        arena: std.heap.ArenaAllocator,
    };

    allocator: std.mem.Allocator,
    client: *net.Client,

    delivered: u64 = 0,
    rejected: u64 = 0,
    unreachable_count: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, client: *net.Client) HttpPush {
        return .{ .allocator = allocator, .client = client };
    }

    pub fn message_bus(self: *HttpPush) env.MessageBus {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: env.MessageBus.VTable = .{ .send = send_erased, .serves = serves_erased };

    fn serves_erased(_: *anyopaque, scheme: []const u8) bool {
        return std.ascii.eqlIgnoreCase(scheme, "http") or std.ascii.eqlIgnoreCase(scheme, "https");
    }

    fn send_erased(ptr: *anyopaque, delivery: *env.Delivery) void {
        const self: *HttpPush = @ptrCast(@alignCast(ptr));
        self.send(delivery);
    }

    pub fn send(self: *HttpPush, delivery: *env.Delivery) void {
        const pending = self.allocator.create(Pending) catch {
            delivery.complete(.failed, "out of memory");
            return;
        };
        pending.* = .{
            .bus = self,
            .delivery = delivery,
            .call = undefined,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
        };
        const a = pending.arena.allocator();
        const url = a.dupe(u8, delivery.address) catch {
            pending.arena.deinit();
            self.allocator.destroy(pending);
            delivery.complete(.failed, "out of memory");
            return;
        };
        const body = a.dupe(u8, delivery.body) catch {
            pending.arena.deinit();
            self.allocator.destroy(pending);
            delivery.complete(.failed, "out of memory");
            return;
        };
        const headers = a.dupe(http.Header, &.{
            .{ .name = "Content-Type", .value = "application/json" },
        }) catch {
            pending.arena.deinit();
            self.allocator.destroy(pending);
            delivery.complete(.failed, "out of memory");
            return;
        };
        pending.call = .{
            .method = "POST",
            .url = url,
            .headers = headers,
            .body = body,
            .arena = a,
            .callback = on_response,
            .context = pending,
        };
        self.client.send(&pending.call);
    }

    fn on_response(call: *net.Call) void {
        const pending: *Pending = @ptrCast(@alignCast(call.context.?));
        const self = pending.bus;
        const delivery = pending.delivery;
        var outcome: env.Delivery.Outcome = .failed;
        var detail: []const u8 = "";
        if (call.status == 0) {
            self.unreachable_count += 1;
            detail = "the worker could not be reached";
        } else if (call.status >= 200 and call.status < 300) {
            self.delivered += 1;
            outcome = .delivered;
        } else {
            // The worker answered and said no. That is its decision, and the
            // server's recovery is the retry deadline it already committed.
            self.rejected += 1;
            detail = "the worker refused the message";
        }
        pending.arena.deinit();
        self.allocator.destroy(pending);
        delivery.complete(outcome, detail);
    }
};

/// A bus that routes nothing.
///
/// For a build with no transports configured: an address it cannot serve is
/// reported as unroutable straight away rather than queued for a delivery that
/// will never happen.
pub const Nowhere = struct {
    dropped: u64 = 0,

    pub fn message_bus(self: *Nowhere) env.MessageBus {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: env.MessageBus.VTable = .{ .send = send_erased, .serves = serves_erased };

    fn serves_erased(_: *anyopaque, _: []const u8) bool {
        return false;
    }

    fn send_erased(ptr: *anyopaque, delivery: *env.Delivery) void {
        const self: *Nowhere = @ptrCast(@alignCast(ptr));
        self.dropped += 1;
        delivery.complete(.failed, "no transport serves that address");
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const io_mod = @import("io.zig");
const posix = std.posix;

/// A worker: records what it was posted, and can be told to refuse.
const Worker = struct {
    allocator: std.mem.Allocator,
    received: std.ArrayListUnmanaged([]u8) = .{},
    status: u16 = 200,

    fn handler(self: *Worker) net.Handler {
        return .{ .ptr = self, .handle = handle };
    }

    fn deinit(self: *Worker) void {
        for (self.received.items) |r| self.allocator.free(r);
        self.received.deinit(self.allocator);
    }

    fn handle(ptr: ?*anyopaque, exchange: *net.Exchange) void {
        const self: *Worker = @ptrCast(@alignCast(ptr.?));
        self.received.append(
            self.allocator,
            self.allocator.dupe(u8, exchange.body) catch return,
        ) catch {};
        exchange.respond(self.status, "application/json", "{}");
    }
};

const Rig = struct {
    allocator: std.mem.Allocator,
    loop: io_mod.Loop,
    listener: posix.socket_t,
    server: net.Server,
    client: net.Client,
    worker: Worker,
    bus: HttpPush,
    url: []u8,

    fn create(allocator: std.mem.Allocator) !*Rig {
        const self = try allocator.create(Rig);
        self.* = .{
            .allocator = allocator,
            .loop = io_mod.Loop.init(allocator) catch return error.SkipZigTest,
            .listener = undefined,
            .server = undefined,
            .client = undefined,
            .worker = .{ .allocator = allocator },
            .bus = undefined,
            .url = undefined,
        };
        const address = try std.net.Address.parseIp("127.0.0.1", 0);
        self.listener = try io_mod.listen(address, 32);
        const port = try io_mod.bound_port(self.listener);
        self.url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}/work", .{port});
        self.server = try net.Server.init(allocator, &self.loop, self.listener, self.worker.handler(), 8);
        self.client = net.Client.init(allocator, &self.loop);
        self.bus = HttpPush.init(allocator, &self.client);
        self.server.accept_more();
        return self;
    }

    fn destroy(self: *Rig) void {
        self.client.deinit();
        self.server.deinit();
        posix.close(self.listener);
        self.loop.deinit();
        self.worker.deinit();
        self.allocator.free(self.url);
        self.allocator.destroy(self);
    }

    const Sent = struct {
        outcome: env.Delivery.Outcome = .pending,
        detail: []const u8 = "",
        done: bool = false,
        fn cb(d: *env.Delivery) void {
            const self: *Sent = @ptrCast(@alignCast(d.context.?));
            self.outcome = d.outcome;
            self.detail = d.detail;
            self.done = true;
        }
    };

    fn deliver(self: *Rig, address: []const u8, body: []const u8) !Sent {
        var sent = Sent{};
        var delivery = env.Delivery{
            .address = address,
            .body = body,
            .callback = Sent.cb,
            .context = &sent,
        };
        self.bus.message_bus().send(&delivery);
        var guard: usize = 0;
        while (!sent.done) {
            guard += 1;
            if (guard > 4_000) return error.NeverDelivered;
            try self.loop.tick();
        }
        return sent;
    }
};

test "a message is posted to the worker's url as json" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();

    const body = "{\"data\":{\"task\":{\"id\":\"o:t\",\"version\":3}},\"head\":{},\"kind\":\"execute\"}";
    const sent = try rig.deliver(rig.url, body);
    try testing.expectEqual(env.Delivery.Outcome.delivered, sent.outcome);
    try testing.expectEqual(@as(usize, 1), rig.worker.received.items.len);
    try testing.expectEqualStrings(body, rig.worker.received.items[0]);
    try testing.expectEqual(@as(u64, 1), rig.bus.delivered);
}

test "a worker that refuses is a failure, not a retry" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    rig.worker.status = 500;
    const sent = try rig.deliver(rig.url, "{}");
    try testing.expectEqual(env.Delivery.Outcome.failed, sent.outcome);
    try testing.expectEqualStrings("the worker refused the message", sent.detail);
    // It arrived; the worker said no.
    try testing.expectEqual(@as(usize, 1), rig.worker.received.items.len);
    try testing.expectEqual(@as(u64, 1), rig.bus.rejected);
}

test "a worker that is not there is unreachable" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    const sent = try rig.deliver("http://127.0.0.1:1/gone", "{}");
    try testing.expectEqual(env.Delivery.Outcome.failed, sent.outcome);
    try testing.expectEqualStrings("the worker could not be reached", sent.detail);
    try testing.expectEqual(@as(u64, 1), rig.bus.unreachable_count);
}

test "the bus serves the schemes it says it serves" {
    var push = HttpPush.init(testing.allocator, undefined);
    const bus = push.message_bus();
    try testing.expect(bus.serves("http"));
    try testing.expect(bus.serves("HTTPS"));
    try testing.expect(!bus.serves("poll"));
    try testing.expect(!bus.serves("gcps"));
}

test "a bus with no transports says so at once" {
    var nowhere = Nowhere{};
    const bus = nowhere.message_bus();
    try testing.expect(!bus.serves("http"));

    const Sent = struct {
        var outcome: env.Delivery.Outcome = .pending;
        fn cb(d: *env.Delivery) void {
            outcome = d.outcome;
        }
    };
    var delivery = env.Delivery{ .address = "http://w", .body = "{}", .callback = Sent.cb };
    bus.send(&delivery);
    try testing.expectEqual(env.Delivery.Outcome.failed, Sent.outcome);
    try testing.expectEqual(@as(u64, 1), nowhere.dropped);
}

test "several messages go out over one reused connection" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    for (0..5) |i| {
        const body = try std.fmt.allocPrint(testing.allocator, "{{\"n\":{d}}}", .{i});
        defer testing.allocator.free(body);
        const sent = try rig.deliver(rig.url, body);
        try testing.expectEqual(env.Delivery.Outcome.delivered, sent.outcome);
    }
    try testing.expectEqual(@as(usize, 5), rig.worker.received.items.len);
    try testing.expectEqual(@as(u64, 1), rig.server.accepted);
    try testing.expectEqual(@as(u64, 4), rig.client.reused_connections);
}
