const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // `ReleaseSafe` by default rather than `Debug`, because it is the mode that
    // ships and the only release mode in which this program still checks itself:
    // every `stdx.assert` is an `unreachable`, which `ReleaseFast` and
    // `ReleaseSmall` turn from a check into a promise to the optimizer. A smaller
    // binary from either is not a trade against size, it is a trade against every
    // invariant in here.
    //
    // Spelled out rather than `standardOptimizeOption`'s `preferred_optimize_mode`,
    // which replaces `-Doptimize=` with a `-Drelease` toggle and would break every
    // command that names a mode.
    const optimize_named = b.option(
        std.builtin.OptimizeMode,
        "optimize",
        "Optimization mode [default: ReleaseSafe for the binaries, Debug for the tests]",
    );
    const optimize = optimize_named orelse .ReleaseSafe;

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
    //
    // `Debug` unless a mode is named, even though the artifacts default to
    // `ReleaseSafe`. The two keep `unreachable` as a panic, so nothing about an
    // assertion is lost, and `Debug` compiles in a fifth of the time — which is
    // the whole value of a test step you run every few minutes. `zig build test
    // -Doptimize=ReleaseSafe` runs them in the mode that ships.
    const unit = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize_named orelse .Debug,
    });
    const run_unit = b.addRunArtifact(unit);
    b.step("test", "Run the unit tests").dependOn(&run_unit.step);
}
