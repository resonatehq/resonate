//! The object store, over S3's REST API.
//!
//! Four requests and one header's worth of cleverness:
//!
//! | operation | request | precondition |
//! |---|---|---|
//! | read | `GET /<bucket>/<key>` | — |
//! | create | `PUT /<bucket>/<key>` | `If-None-Match: *` |
//! | replace | `PUT /<bucket>/<key>` | `If-Match: <etag>` |
//! | write | `PUT /<bucket>/<key>` | — |
//! | delete | `DELETE /<bucket>/<key>` | — |
//! | list | `GET /<bucket>?list-type=2&prefix=…` | — |
//!
//! Those two conditional headers are the whole of this design's concurrency
//! control. There are no leases, no locks and no log: the ETag of one object
//! totally orders the commits to it, and every operation the protocol admits
//! touches exactly one object.
//!
//! **Not every S3-compatible store qualifies.** A store that accepts a stale
//! `If-Match` loses writes under this design, silently, and no amount of care
//! here can detect it — the write simply succeeds when it should not. S3, R2, GCS
//! and Azure implement them; some others answer 200 and drop the precondition.
//!
//! ## No signing, no TLS
//!
//! Requests go out unsigned over plain HTTP to whatever endpoint is configured,
//! which is expected to be a proxy on the loopback that adds TLS and SigV4. That
//! is a deployment decision, made once, in one place — rather than a credential
//! chain, a clock skew check and an HMAC implementation in here.

const std = @import("std");
const stdx = @import("stdx.zig");
const http = @import("http.zig");
const net = @import("net.zig");
const store_mod = @import("store.zig");

const assert = stdx.assert;
const Etag = store_mod.Etag;

/// How many keys one `list-type=2` request asks for. S3's own ceiling.
pub const list_page = 1000;

pub const S3 = struct {
    const Pending = struct {
        s3: *S3,
        op: *store_mod.Operation,
        call: net.Call,
        arena: std.heap.ArenaAllocator,
        /// `list` only: the keys gathered so far, across pages.
        keys: std.ArrayListUnmanaged([]const u8) = .{},
        continuation: ?[]const u8 = null,
    };

    allocator: std.mem.Allocator,
    client: *net.Client,
    /// `http://host:port` — no trailing slash, no path.
    endpoint: []const u8,
    /// The bucket, addressed path-style. Virtual-hosted addressing needs DNS per
    /// bucket and buys nothing behind a local proxy.
    bucket: []const u8,

    requests: u64 = 0,
    failures: u64 = 0,
    /// The most recent failure's text, kept alive because it goes into a reply
    /// built after the request's own arena is gone. One live string, replaced on
    /// the next failure.
    detail_owned: []const u8 = &.{},

    pub fn init(
        allocator: std.mem.Allocator,
        client: *net.Client,
        endpoint: []const u8,
        bucket: []const u8,
    ) S3 {
        return .{
            .allocator = allocator,
            .client = client,
            .endpoint = std.mem.trimRight(u8, endpoint, "/"),
            .bucket = bucket,
        };
    }

    pub fn store(self: *S3) store_mod.Store {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: store_mod.Store.VTable = .{ .submit = submit_erased };

    fn submit_erased(ptr: *anyopaque, op: *store_mod.Operation) void {
        const self: *S3 = @ptrCast(@alignCast(ptr));
        self.submit(op);
    }

    pub fn submit(self: *S3, op: *store_mod.Operation) void {
        const pending = self.allocator.create(Pending) catch {
            op.complete(.{ .unavailable = "out of memory submitting to the store" });
            return;
        };
        pending.* = .{
            .s3 = self,
            .op = op,
            .call = undefined,
            .arena = std.heap.ArenaAllocator.init(self.allocator),
        };
        self.dispatch(pending);
    }

    fn dispatch(self: *S3, pending: *Pending) void {
        const a = pending.arena.allocator();
        const op = pending.op;
        self.requests += 1;

        const url = self.build_url(a, op, pending.continuation) catch {
            return self.finish(pending, .{ .unavailable = "out of memory building a request" });
        };

        var headers = std.ArrayList(http.Header).init(a);
        switch (op.kind) {
            .put => {
                headers.append(.{ .name = "Content-Type", .value = "application/octet-stream" }) catch {};
                switch (op.precondition) {
                    .none, .unchanged => {},
                    // `*` means "no version at all", which is what create-only is.
                    .absent => headers.append(.{ .name = "If-None-Match", .value = "*" }) catch {},
                    .match => |etag| headers.append(.{ .name = "If-Match", .value = etag.slice() }) catch {},
                }
            },
            .get => switch (op.precondition) {
                // The same header as a create-only write, and for the same reason:
                // "only if it is not at this version". On a read the store answers
                // 304 and sends nothing.
                .unchanged => |etag| headers.append(.{ .name = "If-None-Match", .value = etag.slice() }) catch {},
                else => {},
            },
            else => {},
        }

        pending.call = .{
            .method = switch (op.kind) {
                .get, .list => "GET",
                .put => "PUT",
                .delete => "DELETE",
            },
            .url = url,
            .headers = headers.items,
            .body = if (op.kind == .put) op.body else &.{},
            .arena = a,
            .callback = on_response,
            .context = pending,
        };
        self.client.send(&pending.call);
    }

    fn build_url(
        self: *S3,
        a: std.mem.Allocator,
        op: *store_mod.Operation,
        continuation: ?[]const u8,
    ) ![]const u8 {
        var out = std.ArrayList(u8).init(a);
        try out.appendSlice(self.endpoint);
        try out.append('/');
        try http.encode_path(&out, self.bucket, false);
        if (op.kind == .list) {
            try out.appendSlice("?list-type=2&prefix=");
            try http.encode_path(&out, op.key, false);
            const page = @min(op.max_keys, list_page);
            try out.writer().print("&max-keys={d}", .{@max(page, 1)});
            if (continuation) |token| {
                try out.appendSlice("&continuation-token=");
                try http.encode_path(&out, token, false);
            }
        } else {
            try out.append('/');
            // Slashes are kept: they are the key's own structure, and S3 reads
            // them as path segments.
            try http.encode_path(&out, op.key, true);
        }
        return out.items;
    }

    fn on_response(call: *net.Call) void {
        const pending: *Pending = @ptrCast(@alignCast(call.context.?));
        const self = pending.s3;
        const op = pending.op;

        if (call.status == 0) {
            return self.finish(pending, .{ .unavailable = self.own(call.failure) });
        }

        switch (op.kind) {
            .get => switch (call.status) {
                200 => {
                    const etag = call.response_headers.get("etag") orelse {
                        // Without a version there is no conditional write, and
                        // without conditional writes this design cannot be
                        // correct. Better to stop than to proceed unsafely.
                        return self.finish(pending, .{
                            .unavailable = "the store reported no ETag; conditional writes are impossible without one",
                        });
                    };
                    const body = op.arena.dupe(u8, call.response_body) catch {
                        return self.finish(pending, .{ .unavailable = "out of memory reading an object" });
                    };
                    self.finish(pending, .{ .found = .{ .body = body, .etag = Etag.from(etag) } });
                },
                304 => self.finish(pending, .not_modified),
                404 => self.finish(pending, .not_found),
                else => self.finish(pending, self.error_for(pending, call)),
            },
            .put => switch (call.status) {
                200, 201 => {
                    const etag = call.response_headers.get("etag") orelse {
                        return self.finish(pending, .{
                            .unavailable = "the store reported no ETag; conditional writes are impossible without one",
                        });
                    };
                    self.finish(pending, .{ .written = Etag.from(etag) });
                },
                // The version required is not the version there. Re-read and
                // re-decide; never replay.
                412 => self.finish(pending, .precondition_failed),
                // Two conditional writes the service could not order. Nothing is
                // known about whether this one landed, so retry the same write.
                409 => self.finish(pending, .conflict),
                else => self.finish(pending, self.error_for(pending, call)),
            },
            .delete => switch (call.status) {
                // Deleting what is not there succeeds, which is what makes
                // collecting an orphan free.
                200, 204, 404 => self.finish(pending, .deleted),
                else => self.finish(pending, self.error_for(pending, call)),
            },
            .list => switch (call.status) {
                200 => self.on_listed(pending, call),
                else => self.finish(pending, self.error_for(pending, call)),
            },
        }
    }

    fn on_listed(self: *S3, pending: *Pending, call: *net.Call) void {
        const a = pending.arena.allocator();
        const op = pending.op;
        const page = parse_list(a, call.response_body) catch {
            return self.finish(pending, .{ .unavailable = "the listing was not the XML S3 sends" });
        };
        for (page.keys) |key| {
            if (pending.keys.items.len >= op.max_keys) break;
            pending.keys.append(a, key) catch {
                return self.finish(pending, .{ .unavailable = "out of memory listing" });
            };
        }
        const want_more = pending.keys.items.len < op.max_keys;
        if (page.next) |token| {
            if (want_more) {
                pending.continuation = token;
                return self.dispatch(pending);
            }
        }
        // S3 returns keys in ascending order within a page and across pages, which
        // is what the whole deadline design rests on. Sorting anyway costs
        // nothing at these sizes and makes the guarantee this code's rather than
        // the service's.
        const keys = op.arena.alloc([]const u8, pending.keys.items.len) catch {
            return self.finish(pending, .{ .unavailable = "out of memory listing" });
        };
        for (pending.keys.items, 0..) |key, i| {
            keys[i] = op.arena.dupe(u8, key) catch {
                return self.finish(pending, .{ .unavailable = "out of memory listing" });
            };
        }
        std.mem.sort([]const u8, keys, {}, stdx.less_than_bytes);
        self.finish(pending, .{ .keys = keys });
    }

    /// A status this operation has no meaning for.
    ///
    /// Everything is `unavailable`: the caller must assume its request may
    /// already have been applied, which is the only safe reading of "the store
    /// said something I do not understand".
    fn error_for(self: *S3, pending: *Pending, call: *net.Call) store_mod.Result {
        const detail = std.fmt.allocPrint(
            pending.arena.allocator(),
            "the store answered {d}: {s}",
            .{ call.status, first_line(call.response_body) },
        ) catch "the store answered with a status this operation has no meaning for";
        return .{ .unavailable = self.own(detail) };
    }

    /// Copy a detail string into the store's own allocator.
    ///
    /// The reason lives longer than the request's arena: it goes into the
    /// caller's reply, which is built after this arena is gone. There is one of
    /// these per failure and it is freed on the next, so the cost is one live
    /// string.
    fn own(self: *S3, detail: []const u8) []const u8 {
        if (self.detail_owned.len > 0) self.allocator.free(self.detail_owned);
        self.detail_owned = self.allocator.dupe(u8, detail) catch "";
        return self.detail_owned;
    }

    fn finish(self: *S3, pending: *Pending, result: store_mod.Result) void {
        const op = pending.op;
        if (result.is_error()) self.failures += 1;
        pending.arena.deinit();
        self.allocator.destroy(pending);
        op.complete(result);
    }

    pub fn deinit(self: *S3) void {
        if (self.detail_owned.len > 0) self.allocator.free(self.detail_owned);
        self.detail_owned = &.{};
    }
};

fn first_line(body: []const u8) []const u8 {
    const capped = body[0..@min(body.len, 200)];
    const end = std.mem.indexOfAny(u8, capped, "\r\n") orelse capped.len;
    return capped[0..end];
}

// ── The listing's XML ─────────────────────────────────────────────────────────

pub const Listing = struct {
    keys: []const []const u8,
    /// Set when the listing was truncated.
    next: ?[]const u8,
};

/// Pull the keys out of a `ListObjectsV2` response.
///
/// Not a general XML parser, and deliberately not: the document has a fixed
/// shape, the only elements that matter are `Key`, `IsTruncated` and
/// `NextContinuationToken`, and a general parser would be a much larger surface
/// for a much smaller return.
pub fn parse_list(allocator: std.mem.Allocator, body: []const u8) !Listing {
    var keys = std.ArrayList([]const u8).init(allocator);
    errdefer keys.deinit();
    var truncated = false;
    var next: ?[]const u8 = null;

    var rest = body;
    while (element(&rest, "Key")) |text| {
        try keys.append(try unescape_xml(allocator, text));
    }

    var scan = body;
    if (element(&scan, "IsTruncated")) |text| {
        truncated = std.ascii.eqlIgnoreCase(std.mem.trim(u8, text, " \t\r\n"), "true");
    }
    scan = body;
    if (element(&scan, "NextContinuationToken")) |text| {
        if (truncated) next = try unescape_xml(allocator, text);
    }
    // Truncated with no token is a listing that cannot be continued. Reporting no
    // token means the caller stops rather than looping on the same page.
    return .{ .keys = try keys.toOwnedSlice(), .next = next };
}

/// The text of the next `<name>…</name>`, advancing `rest` past it.
fn element(rest: *[]const u8, comptime name: []const u8) ?[]const u8 {
    const open = "<" ++ name ++ ">";
    const close = "</" ++ name ++ ">";
    const start = std.mem.indexOf(u8, rest.*, open) orelse return null;
    const after_open = start + open.len;
    const end = std.mem.indexOfPos(u8, rest.*, after_open, close) orelse return null;
    const text = rest.*[after_open..end];
    rest.* = rest.*[end + close.len ..];
    return text;
}

/// The five entities XML defines, and numeric references.
fn unescape_xml(allocator: std.mem.Allocator, text: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, text, '&') == null) return allocator.dupe(u8, text);
    var out = std.ArrayList(u8).init(allocator);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < text.len) {
        if (text[i] != '&') {
            try out.append(text[i]);
            i += 1;
            continue;
        }
        const semi = std.mem.indexOfScalarPos(u8, text, i, ';') orelse {
            try out.append(text[i]);
            i += 1;
            continue;
        };
        const entity = text[i + 1 .. semi];
        i = semi + 1;
        if (std.mem.eql(u8, entity, "amp")) {
            try out.append('&');
        } else if (std.mem.eql(u8, entity, "lt")) {
            try out.append('<');
        } else if (std.mem.eql(u8, entity, "gt")) {
            try out.append('>');
        } else if (std.mem.eql(u8, entity, "quot")) {
            try out.append('"');
        } else if (std.mem.eql(u8, entity, "apos")) {
            try out.append('\'');
        } else if (entity.len > 1 and entity[0] == '#') {
            const code = if (entity.len > 2 and (entity[1] == 'x' or entity[1] == 'X'))
                std.fmt.parseInt(u21, entity[2..], 16) catch 0xFFFD
            else
                std.fmt.parseInt(u21, entity[1..], 10) catch 0xFFFD;
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(code, &buf) catch blk: {
                buf[0] = '?';
                break :blk 1;
            };
            try out.appendSlice(buf[0..n]);
        } else {
            // An entity nobody defined. Keeping it verbatim is the only reading
            // that cannot corrupt a key.
            try out.append('&');
            try out.appendSlice(entity);
            try out.append(';');
        }
    }
    return out.toOwnedSlice();
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "a listing yields its keys in order" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        \\<Name>bucket</Name><Prefix>t/</Prefix><KeyCount>2</KeyCount>
        \\<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
        \\<Contents><Key>t/00/00000000000000030000_diff</Key><Size>0</Size></Contents>
        \\<Contents><Key>t/01/00000000000000060000_sched:s0</Key><Size>0</Size></Contents>
        \\</ListBucketResult>
    ;
    const listing = try parse_list(arena.allocator(), body);
    try testing.expectEqual(@as(usize, 2), listing.keys.len);
    try testing.expectEqualStrings("t/00/00000000000000030000_diff", listing.keys[0]);
    try testing.expectEqualStrings("t/01/00000000000000060000_sched:s0", listing.keys[1]);
    try testing.expect(listing.next == null);
}

test "a truncated listing reports its continuation token" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body =
        "<ListBucketResult><IsTruncated>true</IsTruncated>" ++
        "<NextContinuationToken>1ueGcxLPRx1Tr</NextContinuationToken>" ++
        "<Contents><Key>a</Key></Contents></ListBucketResult>";
    const listing = try parse_list(arena.allocator(), body);
    try testing.expectEqualStrings("1ueGcxLPRx1Tr", listing.next.?);

    // Truncated with no token cannot be continued, and says so.
    const stuck = try parse_list(arena.allocator(), "<R><IsTruncated>true</IsTruncated><Contents><Key>a</Key></Contents></R>");
    try testing.expect(stuck.next == null);

    // A token without truncation is not a continuation.
    const done = try parse_list(
        arena.allocator(),
        "<R><IsTruncated>false</IsTruncated><NextContinuationToken>x</NextContinuationToken></R>",
    );
    try testing.expect(done.next == null);
}

test "keys are unescaped" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const listing = try parse_list(
        arena.allocator(),
        "<R><Contents><Key>a&amp;b&lt;c&gt;d&quot;e&apos;f&#65;&#x42;</Key></Contents></R>",
    );
    try testing.expectEqualStrings("a&b<c>d\"e'fAB", listing.keys[0]);

    // An entity nobody defined is kept rather than guessed at.
    const odd = try parse_list(arena.allocator(), "<R><Contents><Key>a&nosuch;b</Key></Contents></R>");
    try testing.expectEqualStrings("a&nosuch;b", odd.keys[0]);
}

test "an empty listing is empty, not an error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const listing = try parse_list(arena.allocator(), "<ListBucketResult><KeyCount>0</KeyCount></ListBucketResult>");
    try testing.expectEqual(@as(usize, 0), listing.keys.len);
    try testing.expect(listing.next == null);
}

/// A stand-in S3 over the real HTTP client and server, so the request shapes and
/// the status mapping are exercised end to end rather than asserted about.
const FakeS3 = struct {
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
    seen: std.ArrayListUnmanaged([]u8) = .{},

    fn handler(self: *FakeS3) net.Handler {
        return .{ .ptr = self, .handle = handle };
    }

    fn deinit(self: *FakeS3) void {
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
        self.seen.append(
            self.allocator,
            std.fmt.allocPrint(self.allocator, "{s} {s}", .{ exchange.method, exchange.target }) catch return,
        ) catch {};

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

const io_mod = @import("io.zig");
const posix = std.posix;

const Rig = struct {
    allocator: std.mem.Allocator,
    loop: io_mod.Loop,
    listener: posix.socket_t,
    server: net.Server,
    client: net.Client,
    fake: FakeS3,
    s3: S3,
    endpoint: []u8,

    fn create(allocator: std.mem.Allocator) !*Rig {
        const self = try allocator.create(Rig);
        self.* = .{
            .allocator = allocator,
            .loop = io_mod.Loop.init(allocator) catch return error.SkipZigTest,
            .listener = undefined,
            .server = undefined,
            .client = undefined,
            .fake = .{ .allocator = allocator },
            .s3 = undefined,
            .endpoint = undefined,
        };
        const address = try std.net.Address.parseIp("127.0.0.1", 0);
        self.listener = try io_mod.listen(address, 32);
        const port = try io_mod.bound_port(self.listener);
        self.endpoint = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
        self.server = try net.Server.init(allocator, &self.loop, self.listener, self.fake.handler(), 8);
        self.client = net.Client.init(allocator, &self.loop);
        self.s3 = S3.init(allocator, &self.client, self.endpoint, "resonate-bucket");
        self.server.accept_more();
        return self;
    }

    fn destroy(self: *Rig) void {
        self.s3.deinit();
        self.client.deinit();
        self.server.deinit();
        posix.close(self.listener);
        self.loop.deinit();
        self.fake.deinit();
        self.allocator.free(self.endpoint);
        self.allocator.destroy(self);
    }

    const Done = struct {
        result: store_mod.Result = .pending,
        done: bool = false,
        fn cb(op: *store_mod.Operation) void {
            const self: *Done = @ptrCast(@alignCast(op.context.?));
            self.result = op.result;
            self.done = true;
        }
    };

    fn run(self: *Rig, arena: std.mem.Allocator, template: store_mod.Operation) !store_mod.Result {
        var done = Done{};
        var op = template;
        op.arena = arena;
        op.callback = Done.cb;
        op.context = &done;
        self.s3.store().submit(&op);
        var guard: usize = 0;
        while (!done.done) {
            guard += 1;
            if (guard > 4_000) return error.NeverAnswered;
            try self.loop.tick();
        }
        return done.result;
    }
};

test "the store reads, creates, replaces and refuses a stale version over HTTP" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expect((try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined })) == .not_found);

    const created = try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "one", .precondition = .absent, .arena = a, .callback = undefined });
    const first = created.written;

    // The request went where it should have.
    try testing.expect(std.mem.indexOf(u8, rig.fake.seen.items[1], "PUT /resonate-bucket/wf/o") != null);

    try testing.expect((try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "two", .precondition = .absent, .arena = a, .callback = undefined })) == .precondition_failed);

    const read = try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined });
    try testing.expectEqualStrings("one", read.found.body);
    try testing.expect(Etag.eql(first, read.found.etag));

    const replaced = try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "two", .precondition = .{ .match = first }, .arena = a, .callback = undefined });
    try testing.expect(!Etag.eql(first, replaced.written));

    // The load-bearing assertion: a stale version is refused.
    try testing.expect((try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "three", .precondition = .{ .match = first }, .arena = a, .callback = undefined })) == .precondition_failed);
    try testing.expectEqualStrings("two", (try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined })).found.body);

    try testing.expect((try rig.run(a, .{ .kind = .delete, .key = "wf/o", .arena = a, .callback = undefined })) == .deleted);
    // Deleting what is not there succeeds.
    try testing.expect((try rig.run(a, .{ .kind = .delete, .key = "wf/o", .arena = a, .callback = undefined })) == .deleted);
    try testing.expect((try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined })) == .not_found);
}

test "a key with characters that need escaping survives the round trip" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key = "wf/with%20space%2Fslash";
    _ = try rig.run(a, .{ .kind = .put, .key = key, .body = "x", .arena = a, .callback = undefined });
    const read = try rig.run(a, .{ .kind = .get, .key = key, .arena = a, .callback = undefined });
    try testing.expectEqualStrings("x", read.found.body);
}

test "a listing pages until it has what was asked for" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    for (0..7) |i| {
        const key = try std.fmt.allocPrint(a, "t/00/{d:0>20}_o", .{i * 1000});
        _ = try rig.run(a, .{ .kind = .put, .key = key, .body = "", .arena = a, .callback = undefined });
    }
    // Two keys a page, so this takes four requests.
    rig.fake.page_size = 2;
    const all = try rig.run(a, .{ .kind = .list, .key = "t/", .max_keys = 100, .arena = a, .callback = undefined });
    try testing.expectEqual(@as(usize, 7), all.keys.len);
    for (all.keys[0 .. all.keys.len - 1], all.keys[1..]) |x, y| {
        try testing.expect(std.mem.lessThan(u8, x, y));
    }

    // A cap stops the paging early.
    const capped = try rig.run(a, .{ .kind = .list, .key = "t/", .max_keys = 3, .arena = a, .callback = undefined });
    try testing.expectEqual(@as(usize, 3), capped.keys.len);
    try testing.expect(std.mem.endsWith(u8, capped.keys[0], "00000000000000000000_o"));
}

test "a status the operation has no meaning for is unavailable, not a guess" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    rig.fake.inject_status = 503;
    const unavailable = try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined });
    try testing.expect(unavailable == .unavailable);
    try testing.expect(std.mem.indexOf(u8, unavailable.unavailable, "503") != null);

    // 409 on a conditional write is the one the caller must retry rather than
    // re-decide.
    rig.fake.inject_status = 409;
    const conflict = try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "x", .precondition = .absent, .arena = a, .callback = undefined });
    try testing.expect(conflict == .conflict);

    // 412 is the re-decide path.
    rig.fake.inject_status = 412;
    const stale = try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "x", .precondition = .absent, .arena = a, .callback = undefined });
    try testing.expect(stale == .precondition_failed);
}

test "a store that reports no version is refused rather than trusted" {
    const rig = try Rig.create(testing.allocator);
    defer rig.destroy();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    _ = try rig.run(a, .{ .kind = .put, .key = "wf/o", .body = "x", .arena = a, .callback = undefined });
    rig.fake.omit_etag = true;
    const result = try rig.run(a, .{ .kind = .get, .key = "wf/o", .arena = a, .callback = undefined });
    try testing.expect(result == .unavailable);
    try testing.expect(std.mem.indexOf(u8, result.unavailable, "no ETag") != null);
}

test "an endpoint that is not there is unavailable, not a hang" {
    var loop = io_mod.Loop.init(testing.allocator) catch return error.SkipZigTest;
    defer loop.deinit();
    var client = net.Client.init(testing.allocator, &loop);
    defer client.deinit();
    var s3 = S3.init(testing.allocator, &client, "http://127.0.0.1:1", "bucket");
    defer s3.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const Done = struct {
        var result: store_mod.Result = .pending;
        var done: bool = false;
        fn cb(op: *store_mod.Operation) void {
            result = op.result;
            done = true;
        }
    };
    Done.done = false;
    var op = store_mod.Operation{
        .kind = .get,
        .key = "wf/o",
        .arena = arena.allocator(),
        .callback = Done.cb,
    };
    s3.store().submit(&op);
    var guard: usize = 0;
    while (!Done.done) {
        guard += 1;
        if (guard > 4_000) return error.NeverAnswered;
        try loop.tick();
    }
    try testing.expect(Done.result == .unavailable);
}
