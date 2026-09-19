const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── The server ────────────────────────────────────────────────────────────
    const exe = b.addExecutable(.{
        .name = "resonate",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    b.step("run", "Run the server").dependOn(&run_cmd.step);

    // ── The simulator (VOPR) ──────────────────────────────────────────────────
    const sim = b.addExecutable(.{
        .name = "simulator",
        .root_source_file = b.path("src/simulator.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(sim);

    const run_sim = b.addRunArtifact(sim);
    run_sim.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_sim.addArgs(args);
    b.step("simulate", "Run one deterministic simulation").dependOn(&run_sim.step);

    // ── The differential ──────────────────────────────────────────────────────
    const differ = b.addExecutable(.{
        .name = "differ",
        .root_source_file = b.path("src/differ.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(differ);

    const run_differ = b.addRunArtifact(differ);
    run_differ.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_differ.addArgs(args);
    b.step("differ", "Compare this server with another one").dependOn(&run_differ.step);

    // ── A stand-in S3, for running the server over its S3 path ────────────────
    const fakes3 = b.addExecutable(.{
        .name = "fakes3",
        .root_source_file = b.path("src/fakes3.zig"),
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(fakes3);

    const run_fakes3 = b.addRunArtifact(fakes3);
    run_fakes3.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_fakes3.addArgs(args);
    b.step("fakes3", "Serve a stand-in S3").dependOn(&run_fakes3.step);

    // ── Unit tests ────────────────────────────────────────────────────────────
    const unit = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_unit = b.addRunArtifact(unit);
    b.step("test", "Run the unit tests").dependOn(&run_unit.step);
}
