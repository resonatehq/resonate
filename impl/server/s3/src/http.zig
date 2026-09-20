//! HTTP/1.1, by hand: enough of it to be a server and a client, and no more.
//!
//! Two shapes of message, one framing problem. Everything here is a pure
//! function over bytes — nothing reads a socket, nothing allocates unless it is
//! told to — so the whole of it is testable without a network, which is what the
//! simulator needs.
//!
//! What is deliberately not here: no HTTPS, no HTTP/2, no compression, no
//! authentication. A proxy in front does those. What is here is what an object
//! store and a worker actually require: keep-alive, chunked bodies both ways, the
//! conditional-write headers, and a parser that refuses anything it does not
//! understand rather than guessing.

const std = @import("std");
const stdx = @import("stdx.zig");
const assert = stdx.assert;

pub const max_headers = 48;
/// The longest head this build will parse. A head is metadata; something sending
/// 32 KiB of it is not talking to this server in good faith.
pub const max_head_bytes = 32 * 1024;

pub const Error = error{
    /// Not HTTP, or not a shape this build admits.
    Malformed,
    /// Syntactically fine, but past a limit.
    TooLarge,
    /// A framing this build does not implement.
    Unsupported,
};

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Headers = struct {
    entries: [max_headers]Header = undefined,
    len: usize = 0,

    pub fn get(self: *const Headers, name: []const u8) ?[]const u8 {
        for (self.entries[0..self.len]) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }

    pub fn has_token(self: *const Headers, name: []const u8, token: []const u8) bool {
        const value = self.get(name) orelse return false;
        var it = std.mem.splitScalar(u8, value, ',');
        while (it.next()) |part| {
            if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, part, " \t"), token)) return true;
        }
        return false;
    }
};

/// A parsed head: the start line, the headers, and how many bytes they took.
pub const Head = struct {
    /// Requests only.
    method: []const u8 = "",
    target: []const u8 = "",
    /// Responses only.
    status: u16 = 0,
    reason: []const u8 = "",
    /// The `1` in `HTTP/1.1`. A `1.0` peer does not keep the connection alive
    /// unless it says so.
    minor: u8 = 1,
    headers: Headers = .{},
    /// Bytes consumed, the blank line included.
    len: usize = 0,

    /// Should the connection be reused after this message?
    pub fn keep_alive(self: *const Head) bool {
        if (self.headers.has_token("connection", "close")) return false;
        if (self.minor == 0) return self.headers.has_token("connection", "keep-alive");
        return true;
    }
};

/// How the body that follows a head is framed.
pub const Framing = union(enum) {
    /// No body at all.
    none,
    /// Exactly this many bytes.
    length: usize,
    /// Chunks, until a zero-length one.
    chunked,
    /// Until the connection closes. A response only, and one this build refuses:
    /// an object store that answered this way would be indistinguishable from a
    /// truncated body, and reading a truncated object as a document is the one
    /// mistake this design cannot afford.
    until_close,
};

pub fn framing_of(head: *const Head) Error!Framing {
    if (head.headers.get("transfer-encoding")) |te| {
        // Only `chunked`, and only alone. A stack of codings is a shape nothing
        // this talks to produces.
        if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, te, " \t"), "chunked")) return error.Unsupported;
        return .chunked;
    }
    if (head.headers.get("content-length")) |cl| {
        const n = std.fmt.parseInt(usize, std.mem.trim(u8, cl, " \t"), 10) catch return error.Malformed;
        return if (n == 0) .none else .{ .length = n };
    }
    return .none;
}

/// Where the head ends, or null if it has not arrived yet.
fn head_end(buffer: []const u8) Error!?usize {
    if (std.mem.indexOf(u8, buffer, "\r\n\r\n")) |i| return i + 4;
    // A bare-LF head is not HTTP, but it is what a hand-written client sends, and
    // accepting it costs nothing and confuses no one.
    if (std.mem.indexOf(u8, buffer, "\n\n")) |i| return i + 2;
    if (buffer.len > max_head_bytes) return error.TooLarge;
    return null;
}

fn parse_headers(lines: []const u8, out: *Headers) Error!void {
    var it = std.mem.splitScalar(u8, lines, '\n');
    while (it.next()) |raw| {
        const line = std.mem.trimRight(u8, raw, "\r");
        if (line.len == 0) continue;
        // Obsolete line folding. Nothing this talks to sends it, and joining it
        // correctly is more code than refusing it.
        if (line[0] == ' ' or line[0] == '\t') return error.Unsupported;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.Malformed;
        if (colon == 0) return error.Malformed;
        if (out.len == max_headers) return error.TooLarge;
        out.entries[out.len] = .{
            .name = line[0..colon],
            .value = std.mem.trim(u8, line[colon + 1 ..], " \t"),
        };
        out.len += 1;
    }
}

fn parse_version(text: []const u8) Error!u8 {
    if (!std.mem.startsWith(u8, text, "HTTP/1.")) return error.Malformed;
    if (text.len != 8) return error.Malformed;
    return switch (text[7]) {
        '0' => 0,
        '1' => 1,
        else => error.Unsupported,
    };
}

/// Parse a request head, or null if it has not all arrived.
pub fn parse_request_head(buffer: []const u8) Error!?Head {
    const end = try head_end(buffer) orelse return null;
    const head_bytes = buffer[0..end];
    const first_newline = std.mem.indexOfScalar(u8, head_bytes, '\n') orelse return error.Malformed;
    const start_line = std.mem.trimRight(u8, head_bytes[0..first_newline], "\r");

    var parts = std.mem.tokenizeScalar(u8, start_line, ' ');
    const method = parts.next() orelse return error.Malformed;
    const target = parts.next() orelse return error.Malformed;
    const version = parts.next() orelse return error.Malformed;
    if (parts.next() != null) return error.Malformed;

    var head = Head{
        .method = method,
        .target = target,
        .minor = try parse_version(version),
        .len = end,
    };
    try parse_headers(head_bytes[first_newline + 1 ..], &head.headers);
    return head;
}

/// Parse a response head, or null if it has not all arrived.
pub fn parse_response_head(buffer: []const u8) Error!?Head {
    const end = try head_end(buffer) orelse return null;
    const head_bytes = buffer[0..end];
    const first_newline = std.mem.indexOfScalar(u8, head_bytes, '\n') orelse return error.Malformed;
    const start_line = std.mem.trimRight(u8, head_bytes[0..first_newline], "\r");

    const sp1 = std.mem.indexOfScalar(u8, start_line, ' ') orelse return error.Malformed;
    const minor = try parse_version(start_line[0..sp1]);
    const rest = start_line[sp1 + 1 ..];
    if (rest.len < 3) return error.Malformed;
    const status = std.fmt.parseInt(u16, rest[0..3], 10) catch return error.Malformed;
    const reason = if (rest.len > 4) rest[4..] else "";

    var head = Head{
        .status = status,
        .reason = reason,
        .minor = minor,
        .len = end,
    };
    try parse_headers(head_bytes[first_newline + 1 ..], &head.headers);
    return head;
}

/// Decode a chunked body in place.
///
/// The decoded body is never longer than the encoded one, so it is compacted to
/// the front of `buffer` rather than allocated. Returns null while the terminating
/// zero-length chunk has not arrived.
pub fn decode_chunked(buffer: []u8) Error!?struct { body_len: usize, consumed: usize } {
    var read: usize = 0;
    var write: usize = 0;
    while (true) {
        const line_end = std.mem.indexOfPos(u8, buffer, read, "\r\n") orelse return null;
        const size_line = buffer[read..line_end];
        // A chunk extension after a `;` is legal and ignorable.
        const size_text = blk: {
            const semi = std.mem.indexOfScalar(u8, size_line, ';') orelse break :blk size_line;
            break :blk size_line[0..semi];
        };
        const size = std.fmt.parseInt(usize, std.mem.trim(u8, size_text, " \t"), 16) catch
            return error.Malformed;
        read = line_end + 2;
        if (size == 0) {
            // Trailers, then the final blank line.
            const trailer_end = std.mem.indexOfPos(u8, buffer, read, "\r\n") orelse return null;
            if (trailer_end == read) return .{ .body_len = write, .consumed = read + 2 };
            // A trailer section: skip to its blank line.
            const section_end = std.mem.indexOfPos(u8, buffer, read, "\r\n\r\n") orelse return null;
            return .{ .body_len = write, .consumed = section_end + 4 };
        }
        if (read + size + 2 > buffer.len) return null;
        std.mem.copyForwards(u8, buffer[write..][0..size], buffer[read..][0..size]);
        write += size;
        read += size;
        if (buffer[read] != '\r' or buffer[read + 1] != '\n') return error.Malformed;
        read += 2;
    }
}

// ── Writing ───────────────────────────────────────────────────────────────────

pub fn reason_phrase(status: u16) []const u8 {
    return switch (status) {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        300 => "Multiple Choices",
        304 => "Not Modified",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        412 => "Precondition Failed",
        413 => "Content Too Large",
        422 => "Unprocessable Content",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        else => "Status",
    };
}

/// Write a response: status line, the headers given, `Content-Length`, and the
/// body.
pub fn write_response(
    out: *std.ArrayList(u8),
    status: u16,
    content_type: []const u8,
    body: []const u8,
    keep_alive: bool,
) !void {
    const writer = out.writer();
    try writer.print("HTTP/1.1 {d} {s}\r\n", .{ status, reason_phrase(status) });
    if (content_type.len > 0) try writer.print("Content-Type: {s}\r\n", .{content_type});
    try writer.print("Content-Length: {d}\r\n", .{body.len});
    try writer.print("Connection: {s}\r\n", .{if (keep_alive) "keep-alive" else "close"});
    try out.appendSlice("\r\n");
    try out.appendSlice(body);
}

/// A URL, split into the parts a client needs. No allocation: every field is a
/// slice of the input.
pub const Url = struct {
    scheme: []const u8,
    host: []const u8,
    port: u16,
    /// Always starts with `/`, and carries the query if there was one.
    path: []const u8,

    pub fn parse(url: []const u8) Error!Url {
        const scheme_end = std.mem.indexOf(u8, url, "://") orelse return error.Malformed;
        const scheme = url[0..scheme_end];
        const default_port: u16 = if (std.ascii.eqlIgnoreCase(scheme, "https")) 443 else 80;
        const rest = url[scheme_end + 3 ..];
        const authority_end = std.mem.indexOfAny(u8, rest, "/?#") orelse rest.len;
        var authority = rest[0..authority_end];
        // Userinfo is not a routing decision; anything before an `@` is dropped.
        if (std.mem.lastIndexOfScalar(u8, authority, '@')) |at| authority = authority[at + 1 ..];
        if (authority.len == 0) return error.Malformed;

        var host = authority;
        var port = default_port;
        if (authority[0] == '[') {
            // A bracketed IPv6 literal, with an optional port after the bracket.
            const close = std.mem.indexOfScalar(u8, authority, ']') orelse return error.Malformed;
            host = authority[1..close];
            if (close + 1 < authority.len) {
                if (authority[close + 1] != ':') return error.Malformed;
                port = std.fmt.parseInt(u16, authority[close + 2 ..], 10) catch return error.Malformed;
            }
        } else if (std.mem.lastIndexOfScalar(u8, authority, ':')) |colon| {
            host = authority[0..colon];
            port = std.fmt.parseInt(u16, authority[colon + 1 ..], 10) catch return error.Malformed;
        }
        if (host.len == 0) return error.Malformed;

        const path = if (authority_end == rest.len) "/" else rest[authority_end..];
        return .{ .scheme = scheme, .host = host, .port = port, .path = path };
    }
};

/// Percent-encode a path segment: everything but the unreserved set, and `/`
/// when the caller says the input is a whole path.
pub fn encode_path(out: *std.ArrayList(u8), s: []const u8, keep_slash: bool) !void {
    const hex = "0123456789ABCDEF";
    for (s) |b| {
        switch (b) {
            'A'...'Z', 'a'...'z', '0'...'9', '-', '.', '_', '~' => try out.append(b),
            '/' => {
                if (keep_slash) {
                    try out.append(b);
                } else {
                    try out.appendSlice("%2F");
                }
            },
            else => {
                try out.append('%');
                try out.append(hex[b >> 4]);
                try out.append(hex[b & 0x0f]);
            },
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "a request head is parsed, and an incomplete one is not" {
    const full = "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nContent-Type: application/json\r\n\r\nhello";
    const head = (try parse_request_head(full)).?;
    try testing.expectEqualStrings("POST", head.method);
    try testing.expectEqualStrings("/", head.target);
    try testing.expectEqual(@as(u8, 1), head.minor);
    try testing.expectEqualStrings("application/json", head.headers.get("CONTENT-TYPE").?);
    try testing.expectEqual(Framing{ .length = 5 }, try framing_of(&head));
    try testing.expectEqualStrings("hello", full[head.len..]);
    try testing.expect(head.keep_alive());

    try testing.expect((try parse_request_head("POST / HTTP/1.1\r\nHost: x\r\n")) == null);
    try testing.expect((try parse_request_head("")) == null);
}

test "keep-alive follows the version and the connection header" {
    {
        const h = (try parse_request_head("GET / HTTP/1.1\r\n\r\n")).?;
        try testing.expect(h.keep_alive());
    }
    {
        const h = (try parse_request_head("GET / HTTP/1.1\r\nConnection: close\r\n\r\n")).?;
        try testing.expect(!h.keep_alive());
    }
    {
        const h = (try parse_request_head("GET / HTTP/1.0\r\n\r\n")).?;
        try testing.expect(!h.keep_alive());
    }
    {
        const h = (try parse_request_head("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")).?;
        try testing.expect(h.keep_alive());
    }
    {
        // A list of tokens, as a real client sends.
        const h = (try parse_request_head("GET / HTTP/1.1\r\nConnection: TE, Close\r\n\r\n")).?;
        try testing.expect(!h.keep_alive());
    }
}

test "a response head carries the status and the version" {
    const head = (try parse_response_head("HTTP/1.1 412 Precondition Failed\r\nETag: \"abc\"\r\n\r\n")).?;
    try testing.expectEqual(@as(u16, 412), head.status);
    try testing.expectEqualStrings("Precondition Failed", head.reason);
    try testing.expectEqualStrings("\"abc\"", head.headers.get("etag").?);
    try testing.expectEqual(Framing.none, try framing_of(&head));

    // No reason phrase is legal.
    const terse = (try parse_response_head("HTTP/1.1 204\r\n\r\n")).?;
    try testing.expectEqual(@as(u16, 204), terse.status);
}

test "malformed and unsupported messages are refused, not guessed at" {
    try testing.expectError(error.Malformed, parse_request_head("GET\r\n\r\n"));
    try testing.expectError(error.Malformed, parse_request_head("GET / HTTP/1.1 extra\r\n\r\n"));
    try testing.expectError(error.Malformed, parse_request_head("GET / HTTP/9.9\r\n\r\n"));
    try testing.expectError(error.Unsupported, parse_request_head("GET / HTTP/1.2\r\n\r\n"));
    try testing.expectError(error.Malformed, parse_request_head("GET / HTTP/1.1\r\nnocolon\r\n\r\n"));
    try testing.expectError(error.Malformed, parse_request_head("GET / HTTP/1.1\r\n: empty\r\n\r\n"));
    try testing.expectError(error.Unsupported, parse_request_head("GET / HTTP/1.1\r\nX: a\r\n  folded\r\n\r\n"));
    try testing.expectError(error.Malformed, parse_response_head("HTTP/1.1 xx\r\n\r\n"));

    var head = (try parse_request_head("GET / HTTP/1.1\r\nContent-Length: abc\r\n\r\n")).?;
    try testing.expectError(error.Malformed, framing_of(&head));
    head = (try parse_request_head("GET / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n")).?;
    try testing.expectError(error.Unsupported, framing_of(&head));

    // A head past the limit is refused rather than buffered forever.
    var big = std.ArrayList(u8).init(testing.allocator);
    defer big.deinit();
    try big.appendSlice("GET / HTTP/1.1\r\n");
    while (big.items.len < max_head_bytes + 16) try big.appendSlice("X-Pad: 0123456789\r\n");
    try testing.expectError(error.TooLarge, parse_request_head(big.items));
}

test "a chunked body is decoded in place" {
    var buf = "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\ntrailing".*;
    const decoded = (try decode_chunked(&buf)).?;
    try testing.expectEqual(@as(usize, 11), decoded.body_len);
    try testing.expectEqualStrings("hello world", buf[0..decoded.body_len]);
    try testing.expectEqualStrings("trailing", buf[decoded.consumed..]);

    // Chunk extensions are ignored, and trailers are skipped.
    var ext = "3;name=value\r\nabc\r\n0\r\nX-Trailer: 1\r\n\r\n".*;
    const with_ext = (try decode_chunked(&ext)).?;
    try testing.expectEqualStrings("abc", ext[0..with_ext.body_len]);
    try testing.expectEqual(ext.len, with_ext.consumed);

    // Incomplete stays incomplete rather than becoming a short body.
    var partial = "5\r\nhel".*;
    try testing.expect((try decode_chunked(&partial)) == null);
    var no_terminator = "5\r\nhello\r\n".*;
    try testing.expect((try decode_chunked(&no_terminator)) == null);
    var bad = "zz\r\n".*;
    try testing.expectError(error.Malformed, decode_chunked(&bad));
}

test "a response is written with its framing" {
    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    try write_response(&out, 404, "application/json", "\"nope\"", true);
    try testing.expectEqualStrings(
        "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 6\r\nConnection: keep-alive\r\n\r\n\"nope\"",
        out.items,
    );
    // And it round-trips through the response parser.
    const head = (try parse_response_head(out.items)).?;
    try testing.expectEqual(@as(u16, 404), head.status);
    try testing.expectEqual(Framing{ .length = 6 }, try framing_of(&head));
    try testing.expectEqualStrings("\"nope\"", out.items[head.len..]);
}

test "a url splits into the parts a client needs" {
    const cases = [_]struct { url: []const u8, scheme: []const u8, host: []const u8, port: u16, path: []const u8 }{
        .{ .url = "http://worker:9999", .scheme = "http", .host = "worker", .port = 9999, .path = "/" },
        .{ .url = "http://worker", .scheme = "http", .host = "worker", .port = 80, .path = "/" },
        .{ .url = "https://worker/path", .scheme = "https", .host = "worker", .port = 443, .path = "/path" },
        .{ .url = "http://h:1/a/b?c=d", .scheme = "http", .host = "h", .port = 1, .path = "/a/b?c=d" },
        .{ .url = "http://user:pw@h:2/x", .scheme = "http", .host = "h", .port = 2, .path = "/x" },
        .{ .url = "http://[::1]:8080/x", .scheme = "http", .host = "::1", .port = 8080, .path = "/x" },
        .{ .url = "http://[::1]/x", .scheme = "http", .host = "::1", .port = 80, .path = "/x" },
        .{ .url = "http://h?q", .scheme = "http", .host = "h", .port = 80, .path = "?q" },
    };
    for (cases) |c| {
        const u = try Url.parse(c.url);
        try testing.expectEqualStrings(c.scheme, u.scheme);
        try testing.expectEqualStrings(c.host, u.host);
        try testing.expectEqual(c.port, u.port);
        try testing.expectEqualStrings(c.path, u.path);
    }
    for ([_][]const u8{ "", "notaurl", "http://", "http://h:notaport", "http://[::1" }) |bad| {
        try testing.expectError(error.Malformed, Url.parse(bad));
    }
}

test "a path is percent encoded, with slashes kept or not as asked" {
    var out = std.ArrayList(u8).init(testing.allocator);
    defer out.deinit();
    try encode_path(&out, "res/wf/my%20origin", true);
    try testing.expectEqualStrings("res/wf/my%2520origin", out.items);
    out.clearRetainingCapacity();
    try encode_path(&out, "a/b", false);
    try testing.expectEqualStrings("a%2Fb", out.items);
    out.clearRetainingCapacity();
    try encode_path(&out, "keep-these._~", true);
    try testing.expectEqualStrings("keep-these._~", out.items);
}

test "bare newline heads are accepted" {
    const head = (try parse_request_head("GET / HTTP/1.1\nHost: x\n\nbody")).?;
    try testing.expectEqualStrings("GET", head.method);
    try testing.expectEqualStrings("x", head.headers.get("host").?);
    try testing.expectEqualStrings("body", "GET / HTTP/1.1\nHost: x\n\nbody"[head.len..]);
}
