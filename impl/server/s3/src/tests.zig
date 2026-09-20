//! Every module's tests, in one binary: `zig build test`.
//!
//! Listed rather than discovered, so that a module which stops being reachable
//! from the server still has its tests run — and so that adding one is a
//! deliberate act.

comptime {
    _ = @import("stdx.zig");
    _ = @import("json.zig");
    _ = @import("protocol.zig");
    _ = @import("cron.zig");
    _ = @import("doc.zig");
    _ = @import("handle.zig");
    _ = @import("store.zig");
    _ = @import("env.zig");
    _ = @import("sender.zig");
    _ = @import("applier.zig");
    _ = @import("scan.zig");
    _ = @import("schedules.zig");
    _ = @import("timerd.zig");
    _ = @import("server.zig");
    _ = @import("http.zig");
    _ = @import("io.zig");
    _ = @import("net.zig");
    _ = @import("s3.zig");
    _ = @import("bus.zig");
    _ = @import("sim/model.zig");
    _ = @import("sim/checker.zig");
    _ = @import("sim/workload.zig");
    _ = @import("sim/simulation.zig");
    _ = @import("sim/history.zig");
}
