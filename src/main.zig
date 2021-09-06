// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const scan = @import("scan.zig");
const config = @import("config.zig");
const util = @import("util.zig");
const repometa = @import("repometa.zig");

pub const log_level: std.log.Level = .debug;

pub fn log(comptime level: std.log.Level, comptime scope: @Type(.EnumLiteral), comptime format: []const u8, args: anytype) void {
    _ = scope;
    var l = log_impl.lock.acquire();
    defer l.release();
    log_impl.write(level, std.fmt.bufPrint(&log_impl.logbuf, format, args) catch return);
}

const log_impl = struct {
    // TODO: CLI option.
    var print_level = std.log.Level.debug;

    var file: ?std.fs.File = null;

    var logbuf: [8192]u8 = undefined;
    var lock = std.Thread.Mutex{};

    var id: u32 = 0; // A way to differentiate between processes working on the same store directory.

    fn write(level: std.log.Level, msg: []const u8) void {
        const level_txt = util.logLevelAsText(level);

        if (@enumToInt(level) <= @enumToInt(print_level)) blk: {
            var wr = std.io.getStdOut().writer();
            var buf = std.io.BufferedWriter(512, @TypeOf(wr)){ .unbuffered_writer = wr };
            buf.writer().print("[{s}] {s}\n", .{ level_txt, msg }) catch break :blk;
            buf.flush() catch break :blk;
        }

        if (@enumToInt(level) <= @enumToInt(config.log_level)) {
            if (file) |f| {
                if (id == 0) id = std.crypto.random.int(u32);
                f.seekFromEnd(0) catch return;
                var buf = std.io.BufferedWriter(512, @TypeOf(f.writer())){ .unbuffered_writer = f.writer() };
                // TODO: formatted timestamp
                buf.writer().print("{d:.3} [{x:08}:{}:{s}] {s}\n",
                    .{ @intToFloat(f64, std.time.milliTimestamp())/1000, id, std.Thread.getCurrentId(), level_txt, msg }
                ) catch return;
                buf.flush() catch return;
            }
        }
    }
};


pub fn main() anyerror!void {
    // Set a strict umask to avoid accidentally leaking sensitive data to other
    // system users.
    if (std.builtin.os.tag != .windows)
        _ = @cImport(@cInclude("sys/stat.h")).umask(0o077);

    config.initStore() catch |e| {
        switch (e) {
            error.NoStorePath => std.log.crit(
                "Unable to find a suitable path, please point the JAKOI_STORE"
                ++ " environment variable to a suitable directory.", .{}),
            error.RelativeStorePath => std.log.crit(
                "Unable to use a relative directory as store path ({s})."
                ++ " You may want to adjust the JAKOI_STORE environment variable to use"
                ++ " an absolute path (e.g. \"$HOME/jakoi\").", .{config.store_path}),
            else =>
                if (config.store_path.len > 0) std.log.crit(
                    "Unable to open store directory ({s}): {}", .{config.store_path, e})
                else std.log.crit("Unable to find store directory: {}", .{e}),
        }
        std.process.exit(0);
    };

    var error_line: ?u32 = null;
    config.initConfig(&error_line) catch |e| {
        switch (e) {
            error.UnknownVariable => std.log.crit(
                "Unable to read config file: unknown variable on line {}", .{ error_line.? }),
            error.InvalidStatement => std.log.crit(
                "Unable to read config file: invalid statement on line {}", .{ error_line.? }),
            else =>
                if (error_line) |l| std.log.crit("Unable to read config file on line {}: {}", .{ l, e })
                else std.log.crit("Unable to read config file: {}", .{ e }),
        }
        std.process.exit(0);
    };

    if (config.store_dir.createFile("log", .{.truncate=false})) |f| log_impl.file = f
    else |e| std.log.warn("Unable to open log file: {}", .{e});

    try db.open();
    try scan.scan();
    try scan.hash();
    try repometa.write();
}


test "" {
    _ = @import("blake3.zig");
    _ = @import("util.zig");
    _ = @import("cbor.zig");
}
