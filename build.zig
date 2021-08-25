// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");


const Options = struct {
    systemLmdb: bool,
    blake3Neon: bool,

    fn get(b: *std.build.Builder) Options {
        return Options{
            .systemLmdb = b.option(bool, "system-lmdb", "Link to system-provided LMDB") orelse false,
            .blake3Neon = b.option(bool, "blake3-neon", "Enable BLAKE3 optimizations for NEON architecture") orelse false,
        };
    }
};


fn linkStuff(target: *const std.zig.CrossTarget, exe: *std.build.LibExeObjStep, opt: Options) void {
    exe.linkLibC();

    exe.addIncludeDir("deps/BLAKE3/c");
    exe.addCSourceFile("deps/BLAKE3/c/blake3_dispatch.c", &.{});
    exe.addCSourceFile("deps/BLAKE3/c/blake3_portable.c", &.{});
    if (target.getCpuArch() == .x86_64) {
        if (target.getOsTag() == .windows) {
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_sse2_x86-64_windows_gnu.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_sse41_x86-64_windows_gnu.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_avx2_x86-64_windows_gnu.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_avx512_x86-64_windows_gnu.S");
        } else {
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_sse2_x86-64_unix.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_sse41_x86-64_unix.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_avx2_x86-64_unix.S");
            exe.addAssemblyFile("deps/BLAKE3/c/blake3_avx512_x86-64_unix.S");
        }
    }
    if (target.getCpuArch() == .i386) {
        exe.addCSourceFile("deps/BLAKE3/c/blake3_sse2.c", &.{"-msse2"});
        exe.addCSourceFile("deps/BLAKE3/c/blake3_sse41.c", &.{"-msse4.1"});
        exe.addCSourceFile("deps/BLAKE3/c/blake3_avx2.c", &.{"-mavx2"});
        // zig cc doesn't seem to like -mavx512*
        // exe.addCSourceFile("deps/BLAKE3/c/blake3_avx512.c", &.{"-mavx512f", "-mavx512vl"});
        exe.defineCMacro("BLAKE3_NO_AVX512", null);
    }
    if (opt.blake3Neon) {
        exe.addCSourceFile("deps/BLAKE3/c/blake3_neon.c", &.{});
        exe.defineCMacro("BLAKE3_USE_NEON", null);
    }

    if (opt.systemLmdb) {
        exe.linkSystemLibrary("lmdb");
    } else {
        exe.addIncludeDir("deps/lmdb/libraries/liblmdb");
        exe.addCSourceFile("deps/lmdb/libraries/liblmdb/mdb.c", &.{});
        exe.addCSourceFile("deps/lmdb/libraries/liblmdb/midl.c", &.{});
    }
}

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();
    const opt = Options.get(b);

    const exe = b.addExecutable("jakoi", "src/main.zig");
    exe.setTarget(target);
    exe.setBuildMode(mode);
    linkStuff(&target, exe, opt);
    exe.install();

    const run_cmd = exe.run();
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const tst = b.addTest("src/main.zig");
    linkStuff(&target, tst, opt);
    const tst_step = b.step("test", "Run tests");
    tst_step.dependOn(&tst.step);
}
