//! The two ends of the wire: an HTTP server that answers requests, and an HTTP
//! client that makes them.
//!
//! Both sit on the one ring in `io.zig` and share its thread. Neither knows
//! anything about Resonate: the server hands a body to a callback and writes back
//! whatever it is given, and the client sends a request and hands back a
//! response. That is what lets the simulator replace both with nothing.
//!
//! No TLS. A proxy in front terminates it, which is also where authentication
//! and authorization belong — this process answers what reaches it.
//!
//! ## Connections are pooled, and a pooled connection may be dead
//!
//! An idle connection can be closed by the far end at any moment, and the close
//! is invisible until the next write fails. So a request that fails *before any
//! response byte arrives* on a reused connection is retried once on a fresh one.
//! A request that fails after that is not retried here: the caller knows whether
//! its operation is idempotent and this layer does not.

const std = @import("std");
const posix = std.posix;
const stdx = @import("stdx.zig");
const io_mod = @import("io.zig");
const http = @import("http.zig");

const assert = stdx.assert;
const Completion = io_mod.Completion;

pub const read_chunk = 32 * 1024;
/// The largest body either end will accept. A document is a whole origin, so it
/// can be large; it cannot be unbounded.
pub const max_body_bytes = 64 * 1024 * 1024;
/// How long a resolved host is reused before it is looked up again.
pub const dns_ttl_ms: i64 = 60_000;

// ── The server ────────────────────────────────────────────────────────────────

/// One request being answered.
///
/// The handler is given this, does whatever it takes — possibly over many
/// completions — and calls `respond` exactly once.
pub const Exchange = struct {
    method: []const u8,
    target: []const u8,
    body: []const u8,
    /// Freed once the response has been written. Anything the handler allocates
    /// for its answer belongs here.
    arena: std.mem.Allocator,
    context: ?*anyopaque = null,

    connection: *Connection,

    pub fn respond(self: *Exchange, status: u16, content_type: []const u8, body: []const u8) void {
        self.connection.respond(status, content_type, body);
    }
};

pub const Handler = struct {
    ptr: ?*anyopaque,
    handle: *const fn (ptr: ?*anyopaque, exchange: *Exchange) void,
};

pub const Connection = struct {
    const State = enum { free, reading, handling, writing, closing };

    server: *Server = undefined,
    fd: posix.socket_t = -1,
    state: State = .free,
    completion: Completion = undefined,
    /// Bytes read and not yet consumed. A pipelined second request lives here
    /// while the first is being answered.
    in: std.ArrayListUnmanaged(u8) = .{},
    out: std.ArrayListUnmanaged(u8) = .{},
    written: usize = 0,
    read_buf: [read_chunk]u8 = undefined,
    arena: std.heap.ArenaAllocator = undefined,
    exchange: Exchange = undefined,
    keep_alive: bool = true,
    /// How many bytes of `in` the request being answered used. Dropped only once
    /// the answer is written, so the handler's view of the body stays valid.
    consumed: usize = 0,

    fn start(self: *Connection, server: *Server, fd: posix.socket_t) void {
        self.server = server;
        self.fd = fd;
        self.state = .reading;
        self.in = .{};
        self.out = .{};
        self.written = 0;
        self.keep_alive = true;
        self.arena = std.heap.ArenaAllocator.init(server.allocator);
        io_mod.set_nodelay(fd);
        self.read();
    }

    fn read(self: *Connection) void {
        self.state = .reading;
        self.completion = .{ .callback = on_read, .context = self };
        self.server.loop.io.recv(&self.completion, self.fd, &self.read_buf) catch {
            self.close();
        };
    }

    fn on_read(completion: *Completion) void {
        const self: *Connection = @ptrCast(@alignCast(completion.context.?));
        if (completion.result <= 0) {
            // Zero is a clean close by the peer; negative is an error. Either
            // way there is no more request coming on this connection.
            return self.close();
        }
        const n: usize = @intCast(completion.result);
        self.in.appendSlice(self.server.allocator, self.read_buf[0..n]) catch return self.close();
        self.try_dispatch();
    }

    /// Answer as soon as a whole request has arrived.
    fn try_dispatch(self: *Connection) void {
        const head = http.parse_request_head(self.in.items) catch {
            return self.fail(400, "malformed request");
        } orelse {
            if (self.in.items.len > max_body_bytes) return self.fail(413, "request too large");
            return self.read();
        };

        const framing = http.framing_of(&head) catch {
            return self.fail(501, "unsupported transfer encoding");
        };
        var body: []const u8 = &.{};
        var consumed = head.len;
        switch (framing) {
            .none => {},
            .length => |len| {
                if (len > max_body_bytes) return self.fail(413, "request too large");
                if (self.in.items.len < head.len + len) return self.read();
                body = self.in.items[head.len..][0..len];
                consumed = head.len + len;
            },
            .chunked => {
                const decoded = http.decode_chunked(self.in.items[head.len..]) catch {
                    return self.fail(400, "malformed chunked body");
                } orelse return self.read();
                body = self.in.items[head.len..][0..decoded.body_len];
                consumed = head.len + decoded.consumed;
            },
            .until_close => return self.fail(400, "a request body must be framed"),
        }

        self.keep_alive = head.keep_alive();
        self.state = .handling;
        // The consumed bytes are dropped only once the answer is written, so
        // `body` stays valid for the whole exchange.
        self.exchange = .{
            .method = head.method,
            .target = head.target,
            .body = body,
            .arena = self.arena.allocator(),
            .connection = self,
        };
        self.consumed = consumed;
        self.server.handler.handle(self.server.handler.ptr, &self.exchange);
    }

    fn respond(self: *Connection, status: u16, content_type: []const u8, body: []const u8) void {
        assert(self.state == .handling);
        self.out = .{};
        var out = std.ArrayList(u8).init(self.server.allocator);
        http.write_response(&out, status, content_type, body, self.keep_alive) catch {
            out.deinit();
            return self.close();
        };
        self.out = .{ .items = out.items, .capacity = out.capacity };
        self.written = 0;
        self.write();
    }

    fn fail(self: *Connection, status: u16, message: []const u8) void {
        self.keep_alive = false;
        self.state = .handling;
        self.consumed = self.in.items.len;
        self.respond(status, "text/plain", message);
    }

    /// Send bytes the caller has already framed as a whole response.
    ///
    /// The one way past `respond`, and it exists for a test's sake: a stand-in
    /// object store has to answer with headers of its own, and giving every
    /// caller an arbitrary header list to reproduce S3's two would be a worse
    /// trade than this. The bytes are copied, because the connection frees what
    /// it writes with its own allocator.
    pub fn respond_raw(self: *Connection, bytes: []const u8) void {
        assert(self.state == .handling);
        var out = std.ArrayList(u8).init(self.server.allocator);
        out.appendSlice(bytes) catch {
            out.deinit();
            return self.close();
        };
        self.out = .{ .items = out.items, .capacity = out.capacity };
        self.written = 0;
        self.write();
    }

    fn write(self: *Connection) void {
        self.state = .writing;
        self.completion = .{ .callback = on_write, .context = self };
        self.server.loop.io.send(
            &self.completion,
            self.fd,
            self.out.items[self.written..],
        ) catch self.close();
    }

    fn on_write(completion: *Completion) void {
        const self: *Connection = @ptrCast(@alignCast(completion.context.?));
        if (completion.result <= 0) return self.close();
        self.written += @intCast(completion.result);
        if (self.written < self.out.items.len) return self.write();

        self.out.deinit(self.server.allocator);
        self.out = .{};
        if (!self.keep_alive) return self.close();

        // Drop what this request used; anything after it is the next request,
        // already in hand.
        const leftover = self.in.items.len - self.consumed;
        if (leftover > 0) {
            std.mem.copyForwards(u8, self.in.items[0..leftover], self.in.items[self.consumed..]);
        }
        self.in.shrinkRetainingCapacity(leftover);
        self.consumed = 0;
        _ = self.arena.reset(.retain_capacity);
        if (leftover > 0) {
            // Pipelined. Answer it without waiting for another read.
            return self.try_dispatch();
        }
        self.read();
    }

    fn close(self: *Connection) void {
        if (self.state == .closing or self.state == .free) return;
        self.state = .closing;
        self.completion = .{ .callback = on_close, .context = self };
        self.server.loop.io.close(&self.completion, self.fd) catch {
            posix.close(self.fd);
            self.release();
        };
    }

    fn on_close(completion: *Completion) void {
        const self: *Connection = @ptrCast(@alignCast(completion.context.?));
        self.release();
    }

    fn release(self: *Connection) void {
        const server = self.server;
        self.in.deinit(server.allocator);
        self.out.deinit(server.allocator);
        self.arena.deinit();
        self.fd = -1;
        self.state = .free;
        server.open -= 1;
        server.accept_more();
    }
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    loop: *io_mod.Loop,
    listener: posix.socket_t,
    handler: Handler,
    connections: []Connection,
    accept_completion: Completion = undefined,
    accepting: bool = false,
    open: u32 = 0,

    accepted: u64 = 0,
    rejected: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        loop: *io_mod.Loop,
        listener: posix.socket_t,
        handler: Handler,
        max_connections: u32,
    ) !Server {
        const connections = try allocator.alloc(Connection, max_connections);
        for (connections) |*c| c.* = .{};
        return .{
            .allocator = allocator,
            .loop = loop,
            .listener = listener,
            .handler = handler,
            .connections = connections,
        };
    }

    pub fn deinit(self: *Server) void {
        for (self.connections) |*c| {
            if (c.state != .free and c.fd >= 0) {
                posix.close(c.fd);
                c.in.deinit(self.allocator);
                c.out.deinit(self.allocator);
                c.arena.deinit();
                c.state = .free;
            }
        }
        self.allocator.free(self.connections);
    }

    pub fn accept_more(self: *Server) void {
        if (self.accepting) return;
        if (self.open >= self.connections.len) return;
        self.accepting = true;
        self.accept_completion = .{ .callback = on_accept, .context = self };
        self.loop.io.accept(&self.accept_completion, self.listener) catch {
            self.accepting = false;
        };
    }

    fn on_accept(completion: *Completion) void {
        const self: *Server = @ptrCast(@alignCast(completion.context.?));
        self.accepting = false;
        defer self.accept_more();
        if (completion.result < 0) return;
        const fd: posix.socket_t = @intCast(completion.result);
        const slot = self.free_slot() orelse {
            // At the connection bound. Closing immediately is honest: a
            // connection accepted and never read from looks like a hang.
            posix.close(fd);
            self.rejected += 1;
            return;
        };
        self.open += 1;
        self.accepted += 1;
        slot.start(self, fd);
    }

    fn free_slot(self: *Server) ?*Connection {
        for (self.connections) |*c| {
            if (c.state == .free) return c;
        }
        return null;
    }
};

// ── The client ────────────────────────────────────────────────────────────────

/// One outbound request.
pub const Call = struct {
    method: []const u8,
    url: []const u8,
    /// Sent verbatim, after `Host`, `Content-Length` and `Connection`.
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
    url_parts: http.Url = undefined,
    fd: posix.socket_t = -1,
    reused: bool = false,
    attempt: u8 = 0,
    state: enum { start, connecting, writing, reading, done } = .start,
    completion: Completion = undefined,
    request_bytes: std.ArrayListUnmanaged(u8) = .{},
    written: usize = 0,
    in: std.ArrayListUnmanaged(u8) = .{},
    read_buf: [read_chunk]u8 = undefined,
    saw_response_bytes: bool = false,
};

pub const Client = struct {
    const Idle = struct {
        authority: []u8,
        fd: posix.socket_t,
    };

    const Resolved = struct {
        address: std.net.Address,
        at_ms: i64,
    };

    allocator: std.mem.Allocator,
    loop: *io_mod.Loop,
    idle: std.ArrayListUnmanaged(Idle) = .{},
    max_idle: u32 = 64,
    resolved: std.StringHashMapUnmanaged(Resolved) = .{},

    sent: u64 = 0,
    reused_connections: u64 = 0,
    failures: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, loop: *io_mod.Loop) Client {
        return .{ .allocator = allocator, .loop = loop };
    }

    pub fn deinit(self: *Client) void {
        for (self.idle.items) |entry| {
            posix.close(entry.fd);
            self.allocator.free(entry.authority);
        }
        self.idle.deinit(self.allocator);
        var it = self.resolved.keyIterator();
        while (it.next()) |k| self.allocator.free(k.*);
        self.resolved.deinit(self.allocator);
    }

    pub fn send(self: *Client, call: *Call) void {
        call.client = self;
        call.attempt = 0;
        self.attempt(call);
    }

    fn attempt(self: *Client, call: *Call) void {
        call.url_parts = http.Url.parse(call.url) catch return self.fail(call, "the url is not one");
        call.saw_response_bytes = false;
        call.written = 0;
        call.in = .{};
        call.request_bytes = .{};
        self.build_request(call) catch return self.fail(call, "out of memory building the request");

        const authority = std.fmt.allocPrint(
            call.arena,
            "{s}:{d}",
            .{ call.url_parts.host, call.url_parts.port },
        ) catch return self.fail(call, "out of memory");

        if (self.take_idle(authority)) |fd| {
            call.fd = fd;
            call.reused = true;
            self.reused_connections += 1;
            return self.write(call);
        }

        const address = self.resolve(call.url_parts.host, call.url_parts.port) catch
            return self.fail(call, "the host did not resolve");
        call.fd = io_mod.connect_socket(address.any.family) catch
            return self.fail(call, "no socket");
        call.reused = false;
        call.state = .connecting;
        call.completion = .{ .callback = on_connect, .context = call };
        self.loop.io.connect(&call.completion, call.fd, address) catch
            return self.fail_and_close(call, "the connection could not be submitted");
    }

    fn build_request(self: *Client, call: *Call) !void {
        _ = self;
        var out = std.ArrayList(u8).init(call.arena);
        const w = out.writer();
        try w.print("{s} {s} HTTP/1.1\r\n", .{ call.method, call.url_parts.path });
        if (call.url_parts.port == 80 or call.url_parts.port == 443) {
            try w.print("Host: {s}\r\n", .{call.url_parts.host});
        } else {
            try w.print("Host: {s}:{d}\r\n", .{ call.url_parts.host, call.url_parts.port });
        }
        for (call.headers) |h| try w.print("{s}: {s}\r\n", .{ h.name, h.value });
        try w.print("Content-Length: {d}\r\n", .{call.body.len});
        try out.appendSlice("Connection: keep-alive\r\n\r\n");
        try out.appendSlice(call.body);
        call.request_bytes = .{ .items = out.items, .capacity = out.capacity };
    }

    /// A host, resolved at most once per TTL.
    ///
    /// The lookup blocks this thread, which is the one thing in this program that
    /// does. It is here rather than in a thread because a thread would mean locks
    /// everywhere above, and it is cached because that makes it rare: a server
    /// talking to one bucket resolves it once a minute.
    fn resolve(self: *Client, host: []const u8, port: u16) !std.net.Address {
        // An address literal needs no lookup at all.
        if (std.net.Address.parseIp(host, port)) |address| return address else |_| {}

        const now = self.loop.clock().now_ms();
        if (self.resolved.getPtr(host)) |entry| {
            if (now - entry.at_ms < dns_ttl_ms) {
                var address = entry.address;
                address.setPort(port);
                return address;
            }
        }
        var list = try std.net.getAddressList(self.allocator, host, port);
        defer list.deinit();
        if (list.addrs.len == 0) return error.HostNotFound;
        const address = list.addrs[0];
        if (self.resolved.getPtr(host)) |entry| {
            entry.* = .{ .address = address, .at_ms = now };
        } else {
            const key = try self.allocator.dupe(u8, host);
            self.resolved.put(self.allocator, key, .{ .address = address, .at_ms = now }) catch {
                self.allocator.free(key);
            };
        }
        return address;
    }

    fn take_idle(self: *Client, authority: []const u8) ?posix.socket_t {
        var i = self.idle.items.len;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.idle.items[i].authority, authority)) {
                const entry = self.idle.swapRemove(i);
                self.allocator.free(entry.authority);
                return entry.fd;
            }
        }
        return null;
    }

    fn keep_idle(self: *Client, call: *Call, fd: posix.socket_t) void {
        if (self.idle.items.len >= self.max_idle) {
            posix.close(fd);
            return;
        }
        const authority = std.fmt.allocPrint(
            self.allocator,
            "{s}:{d}",
            .{ call.url_parts.host, call.url_parts.port },
        ) catch {
            posix.close(fd);
            return;
        };
        self.idle.append(self.allocator, .{ .authority = authority, .fd = fd }) catch {
            self.allocator.free(authority);
            posix.close(fd);
        };
    }

    fn on_connect(completion: *Completion) void {
        const call: *Call = @ptrCast(@alignCast(completion.context.?));
        const self = call.client;
        if (completion.result < 0) return self.fail_and_close(call, "the connection was refused");
        self.write(call);
    }

    fn write(self: *Client, call: *Call) void {
        call.state = .writing;
        call.completion = .{ .callback = on_write, .context = call };
        self.loop.io.send(
            &call.completion,
            call.fd,
            call.request_bytes.items[call.written..],
        ) catch return self.fail_and_close(call, "the request could not be submitted");
    }

    fn on_write(completion: *Completion) void {
        const call: *Call = @ptrCast(@alignCast(completion.context.?));
        const self = call.client;
        if (completion.result <= 0) return self.retry_or_fail(call, "the request was not written");
        call.written += @intCast(completion.result);
        if (call.written < call.request_bytes.items.len) return self.write(call);
        self.read(call);
    }

    fn read(self: *Client, call: *Call) void {
        call.state = .reading;
        call.completion = .{ .callback = on_read, .context = call };
        self.loop.io.recv(&call.completion, call.fd, &call.read_buf) catch
            return self.retry_or_fail(call, "the response could not be read");
    }

    fn on_read(completion: *Completion) void {
        const call: *Call = @ptrCast(@alignCast(completion.context.?));
        const self = call.client;
        if (completion.result < 0) return self.retry_or_fail(call, "the connection failed mid-response");
        if (completion.result == 0) {
            // The peer closed. Without a complete response that is a truncated
            // answer, and a truncated answer must never be read as a short one.
            return self.retry_or_fail(call, "the connection closed before the response was complete");
        }
        const n: usize = @intCast(completion.result);
        call.saw_response_bytes = true;
        call.in.appendSlice(call.arena, call.read_buf[0..n]) catch
            return self.fail_and_close(call, "out of memory reading the response");

        const maybe_head = http.parse_response_head(call.in.items) catch
            return self.fail_and_close(call, "the response is not HTTP");
        const head = maybe_head orelse {
            if (call.in.items.len > max_body_bytes) return self.fail_and_close(call, "the response is too large");
            return self.read(call);
        };
        const framing = http.framing_of(&head) catch
            return self.fail_and_close(call, "the response framing is not supported");

        var body: []const u8 = &.{};
        switch (framing) {
            .none => {},
            .length => |len| {
                if (len > max_body_bytes) return self.fail_and_close(call, "the response is too large");
                if (call.in.items.len < head.len + len) return self.read(call);
                body = call.in.items[head.len..][0..len];
            },
            .chunked => {
                const maybe = http.decode_chunked(call.in.items[head.len..]) catch
                    return self.fail_and_close(call, "the chunked response is malformed");
                const decoded = maybe orelse return self.read(call);
                body = call.in.items[head.len..][0..decoded.body_len];
            },
            .until_close => return self.fail_and_close(call, "an unframed response is a truncated one"),
        }

        call.status = head.status;
        call.response_headers = head.headers;
        call.response_body = body;
        call.state = .done;
        if (head.keep_alive()) {
            self.keep_idle(call, call.fd);
        } else {
            posix.close(call.fd);
        }
        call.fd = -1;
        call.callback(call);
    }

    /// A reused connection that failed before answering is a dead connection, not
    /// a failed request. One retry, on a fresh one.
    fn retry_or_fail(self: *Client, call: *Call, detail: []const u8) void {
        if (call.reused and !call.saw_response_bytes and call.attempt == 0) {
            posix.close(call.fd);
            call.fd = -1;
            call.attempt = 1;
            return self.attempt(call);
        }
        self.fail_and_close(call, detail);
    }

    fn fail_and_close(self: *Client, call: *Call, detail: []const u8) void {
        if (call.fd >= 0) {
            posix.close(call.fd);
            call.fd = -1;
        }
        self.fail(call, detail);
    }

    fn fail(self: *Client, call: *Call, detail: []const u8) void {
        self.failures += 1;
        call.status = 0;
        call.failure = detail;
        call.state = .done;
        call.callback(call);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

/// An echo handler: answers with what it was sent, and says where.
const Echo = struct {
    calls: usize = 0,
    last_target: [64]u8 = undefined,
    last_target_len: usize = 0,

    fn handler(self: *Echo) Handler {
        return .{ .ptr = self, .handle = handle };
    }

    fn handle(ptr: ?*anyopaque, exchange: *Exchange) void {
        const self: *Echo = @ptrCast(@alignCast(ptr.?));
        self.calls += 1;
        const n = @min(exchange.target.len, self.last_target.len);
        @memcpy(self.last_target[0..n], exchange.target[0..n]);
        self.last_target_len = n;
        if (std.mem.eql(u8, exchange.target, "/teapot")) {
            return exchange.respond(418, "text/plain", "short and stout");
        }
        exchange.respond(200, "application/json", exchange.body);
    }
};

const Pair = struct {
    loop: io_mod.Loop,
    listener: posix.socket_t,
    server: Server,
    client: Client,
    echo: Echo,
    port: u16,

    fn create(allocator: std.mem.Allocator) !*Pair {
        const self = try allocator.create(Pair);
        errdefer allocator.destroy(self);
        self.* = .{
            .loop = io_mod.Loop.init(allocator) catch return error.SkipZigTest,
            .listener = undefined,
            .server = undefined,
            .client = undefined,
            .echo = .{},
            .port = 0,
        };
        const address = try std.net.Address.parseIp("127.0.0.1", 0);
        self.listener = try io_mod.listen(address, 32);
        self.port = try io_mod.bound_port(self.listener);
        self.server = try Server.init(allocator, &self.loop, self.listener, self.echo.handler(), 8);
        self.server.handler = self.echo.handler();
        self.client = Client.init(allocator, &self.loop);
        self.server.accept_more();
        return self;
    }

    fn destroy(self: *Pair, allocator: std.mem.Allocator) void {
        self.client.deinit();
        self.server.deinit();
        posix.close(self.listener);
        self.loop.deinit();
        allocator.destroy(self);
    }

    const Answer = struct {
        status: u16 = 0,
        body: []const u8 = "",
        failure: []const u8 = "",
        done: bool = false,
        fn callback(call: *Call) void {
            const self: *Answer = @ptrCast(@alignCast(call.context.?));
            self.status = call.status;
            self.body = call.response_body;
            self.failure = call.failure;
            self.done = true;
        }
    };

    fn request(
        self: *Pair,
        arena: std.mem.Allocator,
        method: []const u8,
        path: []const u8,
        body: []const u8,
    ) !Answer {
        const url = try std.fmt.allocPrint(arena, "http://127.0.0.1:{d}{s}", .{ self.port, path });
        var answer = Answer{};
        var call = Call{
            .method = method,
            .url = url,
            .body = body,
            .arena = arena,
            .callback = Answer.callback,
            .context = &answer,
        };
        self.client.send(&call);
        var guard: usize = 0;
        while (!answer.done) {
            guard += 1;
            if (guard > 2_000) return error.NeverAnswered;
            try self.loop.tick();
        }
        return answer;
    }
};

test "a request goes out, is answered, and the connection is reused" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const first = try p.request(arena.allocator(), "POST", "/", "{\"hello\":\"world\"}");
    try testing.expectEqual(@as(u16, 200), first.status);
    try testing.expectEqualStrings("{\"hello\":\"world\"}", first.body);
    try testing.expectEqual(@as(usize, 1), p.echo.calls);
    // The far end asked to keep it, so the client kept it.
    try testing.expectEqual(@as(usize, 1), p.client.idle.items.len);

    const second = try p.request(arena.allocator(), "POST", "/again", "second");
    try testing.expectEqual(@as(u16, 200), second.status);
    try testing.expectEqualStrings("second", second.body);
    try testing.expectEqual(@as(u64, 1), p.client.reused_connections);
    try testing.expectEqualStrings("/again", p.echo.last_target[0..p.echo.last_target_len]);
    // Still one connection, and the server saw both requests on it.
    try testing.expectEqual(@as(u32, 1), p.server.open);
    try testing.expectEqual(@as(u64, 1), p.server.accepted);
}

test "a status other than 200 comes back intact" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const answer = try p.request(arena.allocator(), "GET", "/teapot", "");
    try testing.expectEqual(@as(u16, 418), answer.status);
    try testing.expectEqualStrings("short and stout", answer.body);
}

test "an empty body and a large body both survive the round trip" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const empty = try p.request(a, "GET", "/", "");
    try testing.expectEqual(@as(u16, 200), empty.status);
    try testing.expectEqual(@as(usize, 0), empty.body.len);

    // Bigger than one read, so both ends have to loop.
    const big = try a.alloc(u8, read_chunk * 3 + 17);
    for (big, 0..) |*b, i| b.* = @intCast('a' + (i % 26));
    const answer = try p.request(a, "POST", "/", big);
    try testing.expectEqual(@as(u16, 200), answer.status);
    try testing.expectEqualStrings(big, answer.body);
}

test "a request to nowhere fails rather than hanging" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var answer = Pair.Answer{};
    var call = Call{
        .method = "POST",
        // Port 1 on loopback: nothing listens, and the refusal is immediate.
        .url = "http://127.0.0.1:1/",
        .arena = arena.allocator(),
        .callback = Pair.Answer.callback,
        .context = &answer,
    };
    p.client.send(&call);
    var guard: usize = 0;
    while (!answer.done) {
        guard += 1;
        if (guard > 2_000) return error.NeverAnswered;
        try p.loop.tick();
    }
    try testing.expectEqual(@as(u16, 0), answer.status);
    try testing.expect(answer.failure.len > 0);
}

test "a malformed url is refused without a syscall" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var answer = Pair.Answer{};
    var call = Call{
        .method = "POST",
        .url = "not a url",
        .arena = arena.allocator(),
        .callback = Pair.Answer.callback,
        .context = &answer,
    };
    p.client.send(&call);
    try testing.expect(answer.done);
    try testing.expectEqual(@as(u16, 0), answer.status);
    try testing.expectEqualStrings("the url is not one", answer.failure);
}

/// Open a raw connection to the pair's server and put the socket in
/// non-blocking mode, so a test can write bytes by hand and read what comes back
/// without ever blocking the loop it also has to drive.
fn raw_connect(port: u16) !posix.socket_t {
    const fd = try io_mod.connect_socket(posix.AF.INET);
    errdefer posix.close(fd);
    const target = try std.net.Address.parseIp("127.0.0.1", port);
    try posix.connect(fd, &target.any, target.getOsSockLen());
    const flags = try posix.fcntl(fd, posix.F.GETFL, 0);
    _ = try posix.fcntl(fd, posix.F.SETFL, flags | @as(u32, 1 << @bitOffsetOf(posix.O, "NONBLOCK")));
    return fd;
}

/// Drive the loop, draining whatever the socket has, until `predicate` holds.
fn drive_until(
    pair: *Pair,
    fd: posix.socket_t,
    received: *std.ArrayList(u8),
    predicate: *const fn (*Pair, []const u8) bool,
) !void {
    var guard: usize = 0;
    while (!predicate(pair, received.items)) {
        guard += 1;
        if (guard > 4_000) return error.NeverHappened;
        try pair.loop.tick();
        var buf: [4096]u8 = undefined;
        if (posix.read(fd, &buf)) |n| {
            if (n > 0) try received.appendSlice(buf[0..n]);
        } else |_| {}
    }
}

test "the server answers a pipelined pair on one connection" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const fd = try raw_connect(p.port);
    defer posix.close(fd);
    // Two requests in one write, which is what pipelining is.
    const both =
        "POST /one HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc" ++
        "POST /two HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\ndef";
    _ = try posix.write(fd, both);

    var received = std.ArrayList(u8).init(arena.allocator());
    try drive_until(p, fd, &received, struct {
        fn both_answered(_: *Pair, seen: []const u8) bool {
            return std.mem.indexOf(u8, seen, "abc") != null and
                std.mem.indexOf(u8, seen, "def") != null;
        }
    }.both_answered);

    try testing.expectEqual(@as(usize, 2), p.echo.calls);
    // Two answers, one connection.
    try testing.expectEqual(@as(usize, 2), std.mem.count(u8, received.items, "HTTP/1.1 200"));
    try testing.expectEqual(@as(u64, 1), p.server.accepted);
}

test "a connection that asks to close is closed" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const fd = try raw_connect(p.port);
    defer posix.close(fd);
    _ = try posix.write(fd, "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");

    var received = std.ArrayList(u8).init(arena.allocator());
    try drive_until(p, fd, &received, struct {
        fn answered_and_closed(pair: *Pair, seen: []const u8) bool {
            return std.mem.indexOf(u8, seen, "HTTP/1.1 200") != null and
                pair.server.accepted == 1 and pair.server.open == 0;
        }
    }.answered_and_closed);

    try testing.expectEqual(@as(usize, 1), p.echo.calls);
    try testing.expect(std.mem.indexOf(u8, received.items, "Connection: close") != null);
    // The slot went back to the pool, so the server can take another.
    try testing.expectEqual(@as(u32, 0), p.server.open);
}

test "a malformed request is refused with a status, not a crash" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const fd = try raw_connect(p.port);
    defer posix.close(fd);
    _ = try posix.write(fd, "GARBAGE\r\n\r\n");

    var received = std.ArrayList(u8).init(arena.allocator());
    try drive_until(p, fd, &received, struct {
        fn refused(_: *Pair, seen: []const u8) bool {
            return std.mem.indexOf(u8, seen, "HTTP/1.1 400") != null;
        }
    }.refused);
    // The handler never saw it: what the protocol admits is decided at the edge.
    try testing.expectEqual(@as(usize, 0), p.echo.calls);
}

test "a request past the body limit is refused rather than buffered" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const fd = try raw_connect(p.port);
    defer posix.close(fd);
    const head = try std.fmt.allocPrint(
        arena.allocator(),
        "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: {d}\r\n\r\n",
        .{max_body_bytes + 1},
    );
    _ = try posix.write(fd, head);

    var received = std.ArrayList(u8).init(arena.allocator());
    try drive_until(p, fd, &received, struct {
        fn refused(_: *Pair, seen: []const u8) bool {
            return std.mem.indexOf(u8, seen, "HTTP/1.1 413") != null;
        }
    }.refused);
    try testing.expectEqual(@as(usize, 0), p.echo.calls);
}

test "the connection bound is enforced rather than exceeded" {
    const p = try Pair.create(testing.allocator);
    defer p.destroy(testing.allocator);
    // Eight slots. The ninth connection sits in the listen backlog, unaccepted,
    // which is the backpressure a bounded server owes the kernel: accepting a
    // connection it cannot serve and then closing it looks like a crash to the
    // peer, and leaving it queued looks like load.
    var sockets: [9]posix.socket_t = undefined;
    var opened: usize = 0;
    defer for (sockets[0..opened]) |fd| posix.close(fd);
    while (opened < sockets.len) {
        sockets[opened] = try raw_connect(p.port);
        opened += 1;
        try p.loop.tick();
    }
    var guard: usize = 0;
    while (p.server.open < 8 and guard < 2_000) {
        guard += 1;
        try p.loop.tick();
    }
    try testing.expectEqual(@as(u32, 8), p.server.open);
    try testing.expectEqual(@as(u64, 8), p.server.accepted);
    try testing.expect(!p.server.accepting);

    // One of them finishing frees a slot, and the queued connection is taken.
    _ = try posix.write(sockets[0], "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
    guard = 0;
    while (p.server.accepted < 9 and guard < 2_000) {
        guard += 1;
        try p.loop.tick();
    }
    try testing.expectEqual(@as(u64, 9), p.server.accepted);
}
