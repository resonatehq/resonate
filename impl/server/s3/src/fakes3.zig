//! A stand-in for S3, as a library and as a process.
//!
//! The store's tests use it in process, over the real HTTP client and the real
//! HTTP server on a loopback socket, so the request shapes and the status mapping
//! are exercised rather than asserted about. `fakes3` serves the same thing on a
//! port, which is what lets the *whole server* be run and checked over its S3
//! path — the wiring, the timer listings, the conditional writes — without an S3
//! to run it against.
//!
//! What it implements is what this server uses and nothing else: GET, PUT,
//! DELETE, `list-type=2` with a prefix and a continuation token, `If-None-Match:
//! *`, `If-Match`, `If-None-Match: <etag>`. No signing, no policies, no
//! multipart, no versioning. It is not S3; it answers the way S3 answers to the
//! six operations the store port has.
//!
//! Nothing here belongs in production, and nothing in production imports it.

const std = @import("std");
const stdx = @import("stdx.zig");
const http = @import("http.zig");
const net = @import("net.zig");
const io_mod = @import("io.zig");

const posix = std.posix;

const usage =
    \\Usage: fakes3 [--bind <address>] [--port <port>]
    \\
    \\  --bind <address>   [default: 127.0.0.1]
    \\  --port <port>      0 asks the kernel for one, which is then printed
    \\                     [default: 9100]
    \\  --page <n>         keys per listing page, so pagination is exercised
    \\                     [default: 1000]
    \\
;

pub fn main() u8 {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = std.process.argsAlloc(allocator) catch return 1;
    defer std.process.argsFree(allocator, argv);

    var bind: []const u8 = "127.0.0.1";
    var port: u16 = 9100;
    var page: usize = 1000;
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        const value = if (i + 1 < argv.len) argv[i + 1] else null;
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            std.io.getStdOut().writeAll(usage) catch {};
            return 0;
        } else if (std.mem.eql(u8, arg, "--bind")) {
            bind = value orelse return complain(arg);
            i += 1;
        } else if (std.mem.eql(u8, arg, "--port")) {
            port = std.fmt.parseInt(u16, value orelse return complain(arg), 10) catch return complain(arg);
            i += 1;
        } else if (std.mem.eql(u8, arg, "--page")) {
            page = std.fmt.parseInt(usize, value orelse return complain(arg), 10) catch return complain(arg);
            i += 1;
        } else {
            std.debug.print("unknown option: {s}\n\n{s}", .{ arg, usage });
            return 1;
        }
    }

    var loop = io_mod.Loop.init(allocator) catch |e| {
        std.debug.print("no io_uring: {s}\n", .{@errorName(e)});
        return 1;
    };
    defer loop.deinit();

    const address = std.net.Address.parseIp(bind, port) catch {
        std.debug.print("not an address: {s}\n", .{bind});
        return 1;
    };
    const listener = io_mod.listen(address, 128) catch |e| {
        std.debug.print("cannot listen on {s}:{d}: {s}\n", .{ bind, port, @errorName(e) });
        return 1;
    };
    defer posix.close(listener);
    const bound = io_mod.bound_port(listener) catch port;

    var fake = FakeS3{ .allocator = allocator, .page_size = page };
    defer fake.deinit();
    var server = net.Server.init(allocator, &loop, listener, fake.handler(), 256) catch |e| {
        std.debug.print("cannot serve: {s}\n", .{@errorName(e)});
        return 1;
    };
    defer server.deinit();
    server.accept_more();

    // The port, on its own line, so a script can wait for it.
    const stdout = std.io.getStdOut().writer();
    stdout.print("listening on http://{s}:{d}\n", .{ bind, bound }) catch {};

    while (true) {
        loop.tick() catch |e| {
            std.debug.print("the loop stopped: {s}\n", .{@errorName(e)});
            return 1;
        };
    }
}

fn complain(arg: []const u8) u8 {
    std.debug.print("{s} needs a value\n\n{s}", .{ arg, usage });
    return 1;
}

/// A stand-in S3 over the real HTTP client and server, so the request shapes and
/// the status mapping are exercised end to end rather than asserted about.
pub const FakeS3 = struct {
    const Object = struct { body: []u8, version: u64 };

    allocator: std.mem.Allocator,
    objects: std.StringHashMapUnmanaged(Object) = .{},
    next_version: u64 = 1,
    /// A status to answer the next request with, instead of serving it.
    inject_status: ?u16 = null,
    /// Answer 200 to a GET but without an ETag.
    omit_etag: bool = false,
    /// Truncate every listing to one key, so pagination is exercised.
    page_size: usize = 1000,
    /// Every request, in order, when `watch` is set. A test asserts on the shapes
    /// the client produced; a long-lived process must not grow a list of them.
    watch: bool = false,
    seen: std.ArrayListUnmanaged([]u8) = .{},

    pub fn handler(self: *FakeS3) net.Handler {
        return .{ .ptr = self, .handle = handle };
    }

    pub fn deinit(self: *FakeS3) void {
        var it = self.objects.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.body);
        }
        self.objects.deinit(self.allocator);
        for (self.seen.items) |s| self.allocator.free(s);
        self.seen.deinit(self.allocator);
    }

    fn handle(ptr: ?*anyopaque, exchange: *net.Exchange) void {
        const self: *FakeS3 = @ptrCast(@alignCast(ptr.?));
        const a = exchange.arena;
        if (self.watch) {
            self.seen.append(
                self.allocator,
                std.fmt.allocPrint(self.allocator, "{s} {s}", .{ exchange.method, exchange.target }) catch return,
            ) catch {};
        }

        if (self.inject_status) |status| {
            self.inject_status = null;
            return exchange.respond(status, "application/xml", "<Error><Code>Boom</Code></Error>\nmore");
        }

        // `/bucket/key` or `/bucket?list-type=2&...`
        const target = exchange.target;
        if (std.mem.indexOfScalar(u8, target, '?') != null) return self.list(exchange);
        const after_bucket = std.mem.indexOfScalarPos(u8, target, 1, '/') orelse
            return exchange.respond(400, "text/plain", "no key");
        const key = decode(a, target[after_bucket + 1 ..]) catch
            return exchange.respond(400, "text/plain", "bad key");

        if (std.mem.eql(u8, exchange.method, "GET")) {
            const object = self.objects.get(key) orelse
                return exchange.respond(404, "application/xml", "<Error><Code>NoSuchKey</Code></Error>");
            if (self.omit_etag) return exchange.respond(200, "application/octet-stream", object.body);
            return self.respond_with_etag(exchange, 200, object.version, object.body);
        }
        if (std.mem.eql(u8, exchange.method, "DELETE")) {
            if (self.objects.fetchRemove(key)) |kv| {
                self.allocator.free(kv.key);
                self.allocator.free(kv.value.body);
            }
            return exchange.respond(204, "", "");
        }
        if (!std.mem.eql(u8, exchange.method, "PUT")) {
            return exchange.respond(405, "text/plain", "no");
        }

        // The conditional headers, which are the whole point.
        const existing = self.objects.getPtr(key);
        const if_none_match = self.header(exchange, "if-none-match");
        const if_match = self.header(exchange, "if-match");
        if (if_none_match != null and existing != null) {
            return exchange.respond(412, "application/xml", "<Error><Code>PreconditionFailed</Code></Error>");
        }
        if (if_match) |want| {
            const object = existing orelse
                return exchange.respond(412, "application/xml", "<Error><Code>PreconditionFailed</Code></Error>");
            const have = std.fmt.allocPrint(a, "\"{x:0>16}\"", .{object.version}) catch return;
            if (!std.mem.eql(u8, want, have)) {
                return exchange.respond(412, "application/xml", "<Error><Code>PreconditionFailed</Code></Error>");
            }
        }

        const body = self.allocator.dupe(u8, exchange.body) catch return;
        const version = self.next_version;
        self.next_version += 1;
        if (existing) |object| {
            self.allocator.free(object.body);
            object.body = body;
            object.version = version;
        } else {
            const owned_key = self.allocator.dupe(u8, key) catch return;
            self.objects.put(self.allocator, owned_key, .{ .body = body, .version = version }) catch return;
        }
        return self.respond_with_etag(exchange, 200, version, "");
    }

    fn header(self: *FakeS3, exchange: *net.Exchange, name: []const u8) ?[]const u8 {
        _ = self;
        // The exchange does not expose headers, so they are read back off the
        // raw request the connection still holds.
        const raw = exchange.connection.in.items;
        const parsed = http.parse_request_head(raw) catch return null;
        const head = parsed orelse return null;
        return head.headers.get(name);
    }

    fn respond_with_etag(self: *FakeS3, exchange: *net.Exchange, status: u16, version: u64, body: []const u8) void {
        _ = self;
        // `write_response` does not take extra headers, so this writes the
        // response by hand — which is also a check that the client parses a head
        // it did not produce.
        var out = std.ArrayList(u8).init(exchange.arena);
        out.writer().print(
            "HTTP/1.1 {d} {s}\r\nETag: \"{x:0>16}\"\r\nContent-Length: {d}\r\nConnection: keep-alive\r\n\r\n",
            .{ status, http.reason_phrase(status), version, body.len },
        ) catch return;
        out.appendSlice(body) catch return;
        exchange.connection.respond_raw(out.items);
    }

    fn list(self: *FakeS3, exchange: *net.Exchange) void {
        const a = exchange.arena;
        const query = exchange.target[std.mem.indexOfScalar(u8, exchange.target, '?').? + 1 ..];
        const prefix = decode(a, param(query, "prefix") orelse "") catch "";
        const token = blk: {
            const raw = param(query, "continuation-token") orelse break :blk null;
            break :blk decode(a, raw) catch null;
        };

        var matching = std.ArrayList([]const u8).init(a);
        var it = self.objects.keyIterator();
        while (it.next()) |k| {
            if (!std.mem.startsWith(u8, k.*, prefix)) continue;
            if (token) |after| {
                if (!std.mem.lessThan(u8, after, k.*)) continue;
            }
            matching.append(k.*) catch return;
        }
        std.mem.sort([]const u8, matching.items, {}, stdx.less_than_bytes);

        const page = @min(matching.items.len, self.page_size);
        const truncated = matching.items.len > page;
        var out = std.ArrayList(u8).init(a);
        out.appendSlice("<ListBucketResult>") catch return;
        out.writer().print("<IsTruncated>{s}</IsTruncated>", .{if (truncated) "true" else "false"}) catch return;
        if (truncated) {
            out.writer().print("<NextContinuationToken>{s}</NextContinuationToken>", .{matching.items[page - 1]}) catch return;
        }
        for (matching.items[0..page]) |key| {
            out.writer().print("<Contents><Key>{s}</Key></Contents>", .{key}) catch return;
        }
        out.appendSlice("</ListBucketResult>") catch return;
        exchange.respond(200, "application/xml", out.items);
    }

    fn param(query: []const u8, name: []const u8) ?[]const u8 {
        var it = std.mem.splitScalar(u8, query, '&');
        while (it.next()) |pair| {
            const eq = std.mem.indexOfScalar(u8, pair, '=') orelse continue;
            if (std.mem.eql(u8, pair[0..eq], name)) return pair[eq + 1 ..];
        }
        return null;
    }

    fn decode(a: std.mem.Allocator, s: []const u8) ![]const u8 {
        var out = std.ArrayList(u8).init(a);
        var i: usize = 0;
        while (i < s.len) {
            if (s[i] == '%' and i + 3 <= s.len) {
                try out.append(try std.fmt.parseInt(u8, s[i + 1 ..][0..2], 16));
                i += 3;
            } else {
                try out.append(s[i]);
                i += 1;
            }
        }
        return out.items;
    }
};
