//! `zig build skull-catalog`: the properties this server reports to skulld,
//! as JSON, one per line. Nothing here runs the server: the list is computed
//! at compile time from `properties.zig`.

const std = @import("std");
const skull = @import("skull.zig");
const properties = @import("properties.zig");

pub fn main() !void {
    const out = std.io.getStdOut().writer();
    inline for (comptime skull.catalog(properties)) |entry| {
        try std.json.stringify(.{
            .name = entry.name,
            .kind = @tagName(entry.kind),
            .message = entry.message,
        }, .{}, out);
        try out.writeByte('\n');
    }
}
