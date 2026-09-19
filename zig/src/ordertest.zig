const std = @import("std");
const history = @import("sim/history.zig");
const Model = @import("sim/model.zig").Model;
const json = @import("json.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const argv = try std.process.argsAlloc(allocator);
    const loaded = try history.load_file(arena.allocator(), argv[1], 1 << 28);

    // The order to try, as indices, from the remaining arguments.
    var order = std.ArrayList(usize).init(allocator);
    defer order.deinit();
    for (argv[2..]) |a| try order.append(try std.fmt.parseInt(usize, a, 10));

    const model = try Model.create(allocator);
    defer model.destroy();
    var mismatches: usize = 0;
    for (order.items) |i| {
        const op = loaded.operations[i];
        const reply = model.apply(op.envelope);
        defer model.free_reply(reply);
        var scratch = std.heap.ArenaAllocator.init(allocator);
        defer scratch.deinit();
        const same = reply.status == op.status and json.equal_text(scratch.allocator(), reply.data, op.data);
        if (!same) {
            mismatches += 1;
            std.debug.print("MISMATCH at {d} ({s})\n  model    {d} {s}\n  observed {d} {s}\n", .{
                i, op.kind, reply.status, reply.data, op.status, op.data,
            });
        }
    }
    std.debug.print("{d} mismatches over {d} operations\n", .{ mismatches, order.items.len });
}
