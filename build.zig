// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    const exe = b.addExecutable("jakoi", "src/main.zig");
    exe.setTarget(target);
    exe.setBuildMode(mode);
    exe.linkLibC();

    if (b.option(bool, "system-lmdb", "Link to system-provided LMDB") orelse false) {
        exe.linkSystemLibrary("lmdb");
    } else {
        exe.addIncludeDir("deps/lmdb/libraries/liblmdb");
        exe.addCSourceFile("deps/lmdb/libraries/liblmdb/mdb.c", &.{});
        exe.addCSourceFile("deps/lmdb/libraries/liblmdb/midl.c", &.{});
    }

    exe.install();

    const run_cmd = exe.run();
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
}
