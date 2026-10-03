//! The outbound HTTP client, when the program is a wasm module.
//!
//! The same `Call` and `Client` that `s3.zig` and `bus.zig` hold in `net.zig`,
//! with the sockets taken out: `send` hands the request to the host, and the
//! host hands the response back through `complete`, on a later turn. Nothing
//! above this file can tell the difference, which is the point — the store and
//! the bus are the same code under `io_uring` and under `fetch`.
//!
//! The host owns the connection: pooling, TLS, signing, CORS. All of it is a
//! deployment decision, and in a browser none of it is ours to make anyway.
//!
//! The deadline is ours, as in `net.zig`: a call the host has not answered by
//! then is failed here, and the host's answer, if it ever comes, is dropped.
//! The host is told nothing — it may abort its own request to free the socket,
//! but nothing here depends on it doing so.

const std = @import("std");
const http = @import("http.zig");
const env = @import("env.zig");

/// As `net.default_request_timeout_ms`.
pub const default_request_timeout_ms: i64 = 10_000;

/// Start an exchange. The host answers with `complete(id, …)`, never inline:
/// a callback that ran inside `send` would run inside its caller's frame.
extern "env" fn host_fetch(
    id: u32,
    method_ptr: [*]const u8,
    method_len: usize,
    url_ptr: [*]const u8,
    url_len: usize,
    headers_ptr: [*]const u8,
    headers_len: usize,
    body_ptr: [*]const u8,
    body_len: usize,
) void;

pub const Call = struct {
    method: []const u8,
    url: []const u8,
    headers: []const http.Header = &.{},
    body: []const u8 = &.{},

    /// Where the response is allocated, and the call's own working memory.
    arena: std.mem.Allocator,
    callback: *const fn (*Call) void,
    context: ?*anyopaque = null,

    /// Zero when the exchange did not complete at all.
    status: u16 = 0,
    response_headers: http.Headers = .{},
    response_body: []const u8 = &.{},
    /// Why it did not complete. Empty on success.
    failure: []const u8 = "",

    // ── Internals ─────────────────────────────────────────────────────────────
    client: *Client = undefined,
    id: u32 = 0,
    deadline: env.Timeout = .{ .at_ms = 0 },
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    clock: env.Clock,
    timer: env.Timer,
    /// How long one call may take before the caller stops waiting.
    request_timeout_ms: i64 = default_request_timeout_ms,
    /// Calls the host holds, by the id it was given for each.
    pending: std.AutoHashMapUnmanaged(u32, *Call) = .{},
    next_id: u32 = 1,

    sent: u64 = 0,
    failures: u64 = 0,
    /// Calls the deadline ended.
    expirations: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, clock: env.Clock, timer: env.Timer) Client {
        return .{ .allocator = allocator, .clock = clock, .timer = timer };
    }

    pub fn deinit(self: *Client) void {
        self.pending.deinit(self.allocator);
    }

    pub fn send(self: *Client, call: *Call) void {
        // Headers cross as `name: value\n` lines: one buffer, and nothing for the
        // host to learn about this program's structs.
        var head = std.ArrayList(u8).init(call.arena);
        for (call.headers) |h| {
            head.writer().print("{s}: {s}\n", .{ h.name, h.value }) catch return self.fail(call, "out of memory");
        }
        const id = self.next_id;
        self.next_id +%= 1;
        if (self.next_id == 0) self.next_id = 1;
        self.pending.put(self.allocator, id, call) catch return self.fail(call, "out of memory");
        self.sent += 1;
        call.client = self;
        call.id = id;
        call.deadline = .{ .at_ms = 0 };
        call.deadline.listen(*Call, call, on_deadline);
        self.timer.arm(&call.deadline, self.clock.now_ms() + self.request_timeout_ms);
        host_fetch(
            id,
            call.method.ptr,
            call.method.len,
            call.url.ptr,
            call.url.len,
            head.items.ptr,
            head.items.len,
            call.body.ptr,
            call.body.len,
        );
    }

    /// The host's answer. `status == 0` means the exchange did not complete,
    /// and `body` then says why. Everything is copied into the call's arena:
    /// the buffers are the host's and go when this returns.
    pub fn complete(self: *Client, id: u32, status: u16, headers: []const u8, body: []const u8) void {
        // Gone if the deadline already ended it: the answer is too late to mean
        // anything, and the caller has been told so.
        const call = (self.pending.fetchRemove(id) orelse return).value;
        self.timer.cancel(&call.deadline);
        if (status == 0) {
            return self.fail(call, call.arena.dupe(u8, body) catch "the host did not complete the exchange");
        }
        call.status = status;
        call.response_body = call.arena.dupe(u8, body) catch return self.fail(call, "out of memory");
        var lines = std.mem.splitScalar(u8, headers, '\n');
        while (lines.next()) |line| {
            if (call.response_headers.len == http.max_headers) break;
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const name = std.mem.trim(u8, line[0..colon], " \t\r");
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t\r");
            call.response_headers.entries[call.response_headers.len] = .{
                .name = call.arena.dupe(u8, name) catch return self.fail(call, "out of memory"),
                .value = call.arena.dupe(u8, value) catch return self.fail(call, "out of memory"),
            };
            call.response_headers.len += 1;
        }
        call.callback(call);
    }

    fn on_deadline(call: *Call, _: *env.Timeout) void {
        const self = call.client;
        if (self.pending.fetchRemove(call.id) == null) return;
        self.expirations += 1;
        self.fail(call, "the deadline passed");
    }

    fn fail(self: *Client, call: *Call, why: []const u8) void {
        self.failures += 1;
        call.status = 0;
        call.failure = why;
        call.callback(call);
    }
};
