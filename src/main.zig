// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const scan = @import("scan.zig");
const config = @import("config.zig");
const util = @import("util.zig");
const daemon = @import("daemon.zig");
const tor = @import("tor.zig");
const repometa = @import("repometa.zig");

pub const log_level: std.log.Level = .debug;

pub fn log(comptime level: std.log.Level, comptime scope: @Type(.EnumLiteral), comptime format: []const u8, args: anytype) void {
    _ = scope;
    var l = log_impl.lock.acquire();
    defer l.release();
    log_impl.write(level, std.fmt.bufPrint(&log_impl.logbuf, format, args) catch &log_impl.logbuf);
}

const log_impl = struct {
    var file: ?std.fs.File = null;

    var logbuf: [8192]u8 = undefined;
    var lock = std.Thread.Mutex{};

    var id: u32 = 0; // A way to differentiate between processes working on the same store directory.

    fn write(level: std.log.Level, msg: []const u8) void {
        const level_txt = util.logLevelAsText(level);

        if (@enumToInt(level) <= @enumToInt(cli.verbosity)) blk: {
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


const cli = struct {
    var verbosity = std.log.Level.info;
    var allow_init = true;
    var store_path: ?[]const u8 = null;
    var cmd = Cmd{ .help = .{} };

    const Cmd = union(enum) {
        help: struct {
            cmd: ?[]u8 = null,
        },
        daemon: struct {
            background: bool = true,
        },
        refresh: void,
        status: void,
        config: struct {
            key: ?[]const u8 = null,
            value: ?[]const u8 = null,
        },
    };

    const eql = std.mem.eql;

    fn help(c: []u8) noreturn {
        const txt = if (eql(u8, c, ""))
            \\Usage: jakoi [-vqs] command [options]
            \\
            \\Global options:
            \\  -v, --verbose       Be more verbose
            \\  -q, --quiet         Be less verbose
            \\  -s, --store <path>  Use the given store directory
            \\
            \\Commands:
            \\  help       - Display help text
            \\  daemon     - Spawn the background daemon
            \\  refresh    - Initiate a filesystem refresh
            \\  status     - Display status
            \\  config     - Display or update configuration
            else if (eql(u8, c, "help"))
            \\Usage: jakoi help [command]
            \\
            \\Print usage information for the given command.
            else if (eql(u8, c, "daemon"))
            \\Usage: jakoi daemon [options]
            \\
            \\Spawn the background daemon.
            \\
            \\Options:
            \\  --foreground    Run the daemon on the foreground
            else if (eql(u8, c, "refresh"))
            \\Usage: jakoi refresh
            \\
            \\Re-scan the filesystem and refresh repository metadata.
            else if (eql(u8, c, "status"))
            \\Usage: jakoi status
            \\
            \\Display status about your repository and file download queue.
            else if (eql(u8, c, "config"))
            \\Usage: jakoi config [key [value]]
            \\
            \\Get or set configuration variables.
            else err("Unknown command: {s}\n", .{ c });
        std.io.getStdOut().writer().print("{s}\n", .{ txt }) catch {};
        std.process.exit(0);
    }

    fn err(comptime fmt: []const u8, arg: anytype) noreturn {
        std.io.getStdErr().writer().print(fmt, arg) catch {};
        std.process.exit(1);
    }

    fn parse() !void {
        var it = std.process.args();
        _ = it.skip();
        while (it.next(util.allocator)) |opt_| {
            var opt = try opt_;
            defer util.allocator.free(opt);
            if (eql(u8, opt, "help") or eql(u8, opt, "-h") or eql(u8, opt, "--help")) {
                cmd = .{ .help = .{} };
                break;
            } else if (eql(u8, opt, "daemon")) {
                cmd = .{ .daemon = .{} };
                break;
            } else if (eql(u8, opt, "refresh")) {
                cmd = .{ .refresh = .{} };
                break;
            } else if (eql(u8, opt, "status")) {
                cmd = .{ .status = .{} };
                break;
            } else if (eql(u8, opt, "config")) {
                cmd = .{ .config = .{} };
                break;
            } else if (eql(u8, opt, "-v") or eql(u8, opt, "--verbose"))
                verbosity = .debug
            else if (eql(u8, opt, "-q") or eql(u8, opt, "--quiet"))
                verbosity = .warn
            else if (eql(u8, opt, "-s") or eql(u8, opt, "--store"))
                store_path = try (it.next(util.allocator) orelse err("Option {s} requires an argument.\n", .{ opt }))
            else err("Unknown argument: {s}\n", .{ opt });
        }

        switch (cmd) {
            .help => |*o| o.cmd = if (it.next(util.allocator)) |v| try v else null,
            else => {}
        }
        if (it.next(util.allocator)) |v| err("Unknown argument: {s}\n", .{ try v });

        allow_init = switch (cmd) {
            .help => |*o| help(o.cmd orelse ""),
            .daemon => true,
            .config => true, // Should be: |*o| o.value != null,
            else => false,
        };
    }
};


pub fn main() anyerror!void {
    // Set a strict umask to avoid accidentally leaking sensitive data to other
    // system users.
    if (std.builtin.os.tag != .windows)
        _ = @cImport(@cInclude("sys/stat.h")).umask(0o077);

    try cli.parse();

    config.initStore(cli.allow_init, cli.store_path) catch |e| {
        switch (e) {
            error.NoStorePath => std.log.crit(
                "Unable to find a suitable path, please point the JAKOI_STORE"
                ++ " environment variable to a suitable directory.", .{}),
            error.RelativeStorePath => std.log.crit(
                "Unable to use a relative directory as store path ({s})."
                ++ " You may want to adjust the JAKOI_STORE environment variable to use"
                ++ " an absolute path (e.g. \"$HOME/jakoi\").", .{config.store_path}),
            error.FileNotFound => std.log.crit("No store directory has been created yet.", .{}),
            else =>
                if (config.store_path.len > 0) std.log.crit(
                    "Unable to open store directory ({s}): {}", .{config.store_path, e})
                else std.log.crit("Unable to find store directory: {}", .{e}),
        }
        std.process.exit(0);
    };

    config.initConfig() catch |e| {
        std.log.crit("Unable to read config file: {}", .{ e });
        std.process.exit(0);
    };

    if (config.store_dir.createFile("log", .{.truncate=false})) |f| log_impl.file = f
    else |e| std.log.warn("Unable to open log file: {}", .{e});

    try db.open();

    switch (cli.cmd) {
        .help => {},
        .daemon => {
            //if (o.background) cli.err("Running in the background is not supported yet, use --foreground.", .{});
            try daemon.run();
        },
        .status, .config, .refresh => cli.err("Not yet implemented.\n", .{}),
    }
}


test "" {
    _ = config;
    _ = @import("blake3.zig");
    _ = @import("util.zig");
    _ = @import("cbor.zig");
}
