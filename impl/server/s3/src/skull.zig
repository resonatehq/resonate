//! skulld assertions, in Zig.
//!
//! skulld runs the system in a deterministic VM, injects faults, and judges
//! properties the program reports. This file is how this program reports them,
//! and it is two things:
//!
//! **A static list.** Every property is declared once, as a `pub const` in one
//! namespace (`properties.zig`):
//!
//!     pub const acked_never_lost = skull.Always("an acknowledged write is never lost");
//!
//! `catalog(properties)` walks that namespace at compile time, so the full list
//! is known before anything runs: `zig build skulld-catalog` prints it, and the
//! program announces it at startup, which is what lets skulld report a
//! `sometimes` that never held or a `reachable` never reached.
//!
//! **A call at the site.** Three verbs:
//!
//!     properties.queue_bounded.assert(len <= cap, .{ .len = len });  // the program's own invariant
//!     properties.document_decodes.check(ok, .{ .key = key });        // reported, not enforced
//!     properties.store_never_answered.reached(.{});                  // a reachability site
//!
//! `assert` is `stdx.assert` with a name: it reports, then — if the condition
//! is false — it is `unreachable`, exactly as before. An `Unreachable`'s
//! `reached` is `unreachable` the same way. `check` only reports: for
//! properties about what came from outside, where the program answers rather
//! than stops. `Sometimes` and `Reachable` are evidence across a campaign, which
//! one process cannot violate.
//!
//! Each property sends at most one event per condition value, like `skull.h`.
//! Where skulld cannot be — wasm, anything but Linux, or a root that declares
//! `pub const skull_enabled = false` — reporting compiles to nothing: an
//! `assert` is then exactly `if (!ok) unreachable`, a `Sometimes` is nothing at
//! all. Where it can, the first report looks for the agent's socket; a process
//! not under skulld finds none and every later one is a load and a branch.
//!
//! ## The wire
//!
//! The same protocol `libskull.so` speaks, without the library: this program
//! has no libc to load it with. One `SOCK_SEQPACKET` connection to
//! `/run/skull/libskull.sock`, which the agent (PID 1 in the guest) mounts into
//! every container; one packet per message, a one-byte tag first:
//!
//!     'J' <json>   one event: a catalog entry or an assertion
//!     'R'          ask for 8 bytes of the run's seeded randomness
//!
//! The agent learns which service sent it from the peer's `SKULL_SERVICE`,
//! stamps the guest's virtual time, and forwards it to the host over
//! virtio-serial, where `props.rs` evaluates it per run and per campaign.
//!
//! The JSON is the shape `skull.h` sends: `{"skull_assert":{...}}`.

const std = @import("std");
const builtin = @import("builtin");
const root = @import("root");

/// Whether this build can report to skulld at all. Off, every call is gone.
pub const enabled: bool = blk: {
    if (@hasDecl(root, "skull_enabled") and !root.skull_enabled) break :blk false;
    break :blk builtin.os.tag == .linux and !builtin.cpu.arch.isWasm();
};

pub const Kind = enum {
    always,
    always_or_unreachable,
    sometimes,
    reachable,
    @"unreachable",

    fn assert_type(kind: Kind) []const u8 {
        return switch (kind) {
            .always, .always_or_unreachable => "always",
            .sometimes => "sometimes",
            .reachable, .@"unreachable" => "reachability",
        };
    }

    fn display_type(kind: Kind) []const u8 {
        return switch (kind) {
            .always => "Always",
            .always_or_unreachable => "AlwaysOrUnreachable",
            .sometimes => "Sometimes",
            .reachable => "Reachable",
            .@"unreachable" => "Unreachable",
        };
    }

    /// Whether skulld counts it as failed when no run ever evaluates it.
    fn must_hit(kind: Kind) bool {
        return switch (kind) {
            .always, .sometimes, .reachable => true,
            .always_or_unreachable, .@"unreachable" => false,
        };
    }
};

/// Must hold every time it is evaluated, and must be evaluated.
pub fn Always(comptime message: []const u8) type {
    return Property(.always, message);
}

/// Must hold every time it is evaluated; never evaluating it is fine.
pub fn AlwaysOrUnreachable(comptime message: []const u8) type {
    return Property(.always_or_unreachable, message);
}

/// Must hold at least once across the campaign: evidence a case was exercised.
pub fn Sometimes(comptime message: []const u8) type {
    return Property(.sometimes, message);
}

/// Must be reached at least once across the campaign.
pub fn Reachable(comptime message: []const u8) type {
    return Property(.reachable, message);
}

/// Must never be reached.
pub fn Unreachable(comptime message: []const u8) type {
    return Property(.@"unreachable", message);
}

/// One property: its metadata at compile time, and its own record of which
/// condition values it has already reported.
///
/// A type rather than a value so the record can be a container-level `var` —
/// one per property, with no registry to look it up in. Declaring the same
/// kind and message twice gives the same type, which is the same property.
pub fn Property(comptime kind_: Kind, comptime message_: []const u8) type {
    comptime std.debug.assert(message_.len > 0);
    return struct {
        pub const skull_property = true;
        pub const kind = kind_;
        pub const message = message_;

        /// Bit 0: reported true. Bit 1: reported false.
        var reported: u2 = 0;

        /// The program's own invariant: report it, and stop if it is false —
        /// `stdx.assert` with a name. The report goes out before the stop, so
        /// skulld sees which invariant failed, with `details`.
        pub inline fn assert(condition: bool, details: anytype) void {
            comptime std.debug.assert(kind == .always or kind == .always_or_unreachable);
            if (enabled) report(condition, details);
            if (!condition) unreachable;
        }

        /// Evaluate an `always`, `always_or_unreachable` or `sometimes`
        /// without enforcing it.
        /// `details` is anything `std.json` can write, or `.{}`; it goes with
        /// the first report of each condition value, which for an `always`
        /// means the first violation.
        pub inline fn check(condition: bool, details: anytype) void {
            comptime std.debug.assert(kind != .reachable and kind != .@"unreachable");
            if (!enabled) return;
            report(condition, details);
        }

        /// Reach a `reachable` site, or an `unreachable` one — which then stops,
        /// as `unreachable` does.
        pub inline fn reached(details: anytype) if (kind == .@"unreachable") noreturn else void {
            comptime std.debug.assert(kind == .reachable or kind == .@"unreachable");
            if (enabled) report(true, details);
            if (kind == .@"unreachable") unreachable;
        }

        /// Report without enforcing anything. For the panic handler, which
        /// must not stop twice.
        pub fn record(condition: bool, details: anytype) void {
            if (enabled) report(condition, details);
        }

        fn report(condition: bool, details: anytype) void {
            const bit: u2 = if (condition) 1 else 2;
            if (reported & bit != 0) return;
            reported |= bit;
            send_assert(kind, message, null, true, condition, details);
        }

        /// For tests: forget what was reported.
        pub fn reset() void {
            reported = 0;
        }
    };
}

/// One entry of the static list.
pub const Entry = struct {
    /// The declaration's name in the namespace: where it is defined.
    name: []const u8,
    kind: Kind,
    message: []const u8,
};

/// Every property declared in `Namespace`, at compile time.
pub fn catalog(comptime Namespace: type) []const Entry {
    comptime {
        var entries: []const Entry = &.{};
        for (@typeInfo(Namespace).@"struct".decls) |decl| {
            const value = @field(Namespace, decl.name);
            if (@TypeOf(value) != type) continue;
            if (!@hasDecl(value, "skull_property")) continue;
            entries = entries ++ [_]Entry{.{ .name = decl.name, .kind = value.kind, .message = value.message }};
        }
        return entries;
    }
}

/// Announce the static list to skulld. Call once at startup.
pub fn declare(comptime Namespace: type) void {
    if (!enabled) return;
    inline for (comptime catalog(Namespace)) |entry| {
        send_assert(entry.kind, entry.message, entry.name, false, false, .{});
    }
}

/// A panic handler that tells skulld first. Every `unreachable` the program
/// reaches, and every panic, then becomes a finding with a message and an
/// address rather than a container that exited:
///
///     pub const panic = skull.Panic(properties.panicked);
pub fn Panic(comptime Panicked: type) type {
    comptime std.debug.assert(Panicked.kind == .@"unreachable");
    return std.debug.FullPanic(struct {
        var panicking: bool = false;
        fn call(message: []const u8, first_trace_addr: ?usize) noreturn {
            if (!panicking) {
                panicking = true;
                Panicked.record(true, .{
                    .message = message,
                    .address = first_trace_addr orelse @returnAddress(),
                });
            }
            std.debug.defaultPanic(message, first_trace_addr);
        }
    }.call);
}

/// The run's seeded randomness under skulld, the OS's otherwise.
pub fn random() u64 {
    if (enabled) {
        if (connection()) |fd| {
            if (send_packet(fd, 'R', "")) {
                var buf: [8]u8 = undefined;
                const n = std.posix.recv(fd, &buf, 0) catch 0;
                if (n == 8) return std.mem.readInt(u64, &buf, .little);
            }
            disconnect();
        }
    }
    var buf: [8]u8 = undefined;
    std.crypto.random.bytes(&buf);
    return std.mem.readInt(u64, &buf, .little);
}

// ── The wire ──────────────────────────────────────────────────────────────────

/// Where the agent listens. A variable so a test can stand in for the agent.
pub var socket_path: []const u8 = "/run/skull/libskull.sock";

const State = enum { untried, connected, absent };
var state: State = .untried;
var fd_: std.posix.socket_t = -1;

fn connection() ?std.posix.socket_t {
    switch (state) {
        .connected => return fd_,
        .absent => return null,
        .untried => {},
    }
    state = .absent;
    const fd = std.posix.socket(std.posix.AF.UNIX, std.posix.SOCK.SEQPACKET | std.posix.SOCK.CLOEXEC, 0) catch return null;
    var addr = std.net.Address.initUnix(socket_path) catch {
        std.posix.close(fd);
        return null;
    };
    std.posix.connect(fd, &addr.any, addr.getOsSockLen()) catch {
        std.posix.close(fd);
        return null;
    };
    fd_ = fd;
    state = .connected;
    return fd;
}

fn disconnect() void {
    if (state == .connected) std.posix.close(fd_);
    state = .absent;
}

fn send_packet(fd: std.posix.socket_t, tag: u8, payload: []const u8) bool {
    var iov = [_]std.posix.iovec_const{
        .{ .base = @ptrCast(&tag), .len = 1 },
        .{ .base = payload.ptr, .len = payload.len },
    };
    const msg = std.posix.msghdr_const{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = iov.len,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };
    _ = std.posix.sendmsg(fd, &msg, std.posix.MSG.NOSIGNAL) catch return false;
    return true;
}

fn send_assert(
    kind: Kind,
    message: []const u8,
    name: ?[]const u8,
    hit: bool,
    condition: bool,
    details: anytype,
) void {
    const fd = connection() orelse return;
    var buf: [4096]u8 = undefined;
    var stream = std.io.fixedBufferStream(&buf);
    write_assert(stream.writer(), kind, message, name, hit, condition, details) catch return;
    if (!send_packet(fd, 'J', stream.getWritten())) disconnect();
}

fn write_assert(
    w: anytype,
    kind: Kind,
    message: []const u8,
    name: ?[]const u8,
    hit: bool,
    condition: bool,
    details: anytype,
) !void {
    try w.writeAll("{\"skull_assert\":{\"id\":");
    try std.json.encodeJsonString(message, .{}, w);
    try w.writeAll(",\"message\":");
    try std.json.encodeJsonString(message, .{}, w);
    try w.print(
        ",\"assert_type\":\"{s}\",\"display_type\":\"{s}\",\"hit\":{},\"must_hit\":{},\"condition\":{},",
        .{ kind.assert_type(), kind.display_type(), hit, kind.must_hit(), condition },
    );
    try w.writeAll("\"location\":{\"file\":\"properties.zig\",\"function\":");
    try std.json.encodeJsonString(name orelse "", .{}, w);
    try w.writeAll(",\"class\":\"\",\"begin_line\":0,\"begin_column\":0},\"details\":");
    if (@TypeOf(details) == @TypeOf(.{})) {
        try w.writeAll("null");
    } else {
        try std.json.stringify(details, .{}, w);
    }
    try w.writeAll("}}");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

const TestProperties = struct {
    pub const held = Always("test: this holds");
    pub const seen = Sometimes("test: this happens");
    pub const gone = Unreachable("test: this never happens");
    pub const not_a_property = 42;
};

test "the catalog is the namespace's properties, known at compile time" {
    const entries = comptime catalog(TestProperties);
    comptime std.debug.assert(entries.len == 3);
    try testing.expectEqualStrings("held", entries[0].name);
    try testing.expectEqual(Kind.always, entries[0].kind);
    try testing.expectEqualStrings("test: this happens", entries[1].message);
    try testing.expectEqual(Kind.@"unreachable", entries[2].kind);
}

test "the wire: a catalog entry, then each condition value once, then randomness" {
    if (!enabled) return error.SkipZigTest;

    // Stand in for the agent: listen where the program will look.
    var dir = testing.tmpDir(.{});
    defer dir.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try dir.dir.realpath(".", &path_buf);
    var sock_buf: [std.fs.max_path_bytes]u8 = undefined;
    const sock_path = try std.fmt.bufPrint(&sock_buf, "{s}/libskull.sock", .{dir_path});

    const listener = try std.posix.socket(std.posix.AF.UNIX, std.posix.SOCK.SEQPACKET, 0);
    defer std.posix.close(listener);
    var addr = try std.net.Address.initUnix(sock_path);
    try std.posix.bind(listener, &addr.any, addr.getOsSockLen());
    try std.posix.listen(listener, 1);

    const saved = socket_path;
    defer {
        disconnect();
        state = .untried;
        socket_path = saved;
        TestProperties.held.reset();
    }
    socket_path = sock_path;
    state = .untried;

    declare(struct {
        pub const held = TestProperties.held;
    });
    TestProperties.held.check(true, .{});
    TestProperties.held.check(true, .{}); // already reported: not sent
    TestProperties.held.check(false, .{ .key = "k1", .read = @as(u32, 3) });

    const conn = try std.posix.accept(listener, null, null, 0);
    defer std.posix.close(conn);
    var buf: [4096]u8 = undefined;

    const expected = [_]struct { []const u8, []const u8 }{
        .{ "\"hit\":false", "\"function\":\"held\"" },
        .{ "\"hit\":true", "\"condition\":true" },
        .{ "\"condition\":false", "\"details\":{\"key\":\"k1\",\"read\":3}" },
    };
    for (expected) |want| {
        const n = try std.posix.recv(conn, &buf, 0);
        const packet = buf[0..n];
        try testing.expectEqual(@as(u8, 'J'), packet[0]);
        try testing.expect(std.mem.startsWith(u8, packet[1..], "{\"skull_assert\":{\"id\":\"test: this holds\""));
        try testing.expect(std.mem.indexOf(u8, packet, want[0]) != null);
        try testing.expect(std.mem.indexOf(u8, packet, want[1]) != null);
        // And it is JSON, which is all the agent checks.
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, packet[1..], .{});
        parsed.deinit();
    }

    // Randomness comes from the agent: answer the 'R' from another thread.
    const Agent = struct {
        fn serve(fd: std.posix.socket_t) void {
            var b: [16]u8 = undefined;
            const n = std.posix.recv(fd, &b, 0) catch return;
            if (n != 1 or b[0] != 'R') return;
            var out: [8]u8 = undefined;
            std.mem.writeInt(u64, &out, 0x5eed, .little);
            _ = std.posix.send(fd, &out, 0) catch {};
        }
    };
    const thread = try std.Thread.spawn(.{}, Agent.serve, .{conn});
    try testing.expectEqual(@as(u64, 0x5eed), random());
    thread.join();
}

test "outside skulld every call is a no-op" {
    const saved = socket_path;
    defer {
        state = .untried;
        socket_path = saved;
        TestProperties.held.reset();
    }
    socket_path = "/nonexistent/libskull.sock";
    state = .untried;
    TestProperties.held.check(false, .{});
    try testing.expect(state != .connected);
    _ = random(); // falls back to the OS
}
