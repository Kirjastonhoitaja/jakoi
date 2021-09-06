// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const util = @import("util.zig");

pub var store_path: []const u8 = "";
pub var store_dir = std.fs.cwd();

pub var hash_threads: usize = 4;
pub var blake3_piece_size: u64 = 1024*1024;
pub var log_level: std.log.Level = .info;

// TODO: Support multiple public dirs, mount-style.
pub var public_dir = "zig-cache";


pub const ConfigReader = struct {
    rd: std.io.BufferedReader(4096, std.fs.File.Reader),
    linenum: u32 = 0,
    linebuf: [4096]u8 = undefined,

    const Self = @This();

    const Line = struct {
        line: []const u8,
        key: []const u8,
        val: []const u8,
    };

    fn readLine(self: *Self) !?Line {
        const line_raw = (try self.rd.reader().readUntilDelimiterOrEof(&self.linebuf, '\n')) orelse return null;
        self.linenum += 1;
        const line = std.mem.trim(u8, line_raw, &std.ascii.spaces);
        if (line.len == 0 or line[0] == '#') return Line{.line = line, .key = "", .val = ""};
        const off = std.mem.indexOfScalar(u8, line, ' ') orelse return error.InvalidStatement;
        return Line{
            .line = line,
            .key = line[0..off],
            .val = std.mem.trimLeft(u8, line[off..], &std.ascii.spaces),
        };
    }

    pub fn read(error_line: *?u32) !void {
        var file = try store_dir.openFile("config", .{});
        defer file.close();
        var self = Self{.rd = std.io.bufferedReader(file.reader())};
        errdefer error_line.* = self.linenum;
        while (try self.readLine()) |line| {
            if (line.key.len == 0) continue;
            const eql = std.mem.eql;
            if (eql(u8, line.key, "blake3_piece_size"))
                blake3_piece_size = try std.fmt.parseUnsigned(u64, line.val, 10)
            else if (eql(u8, line.key, "hash_threads"))
                hash_threads = try std.fmt.parseUnsigned(usize, line.val, 10)
            else if (eql(u8, line.key, "log_level"))
                log_level = util.logLevelFromText(line.val) orelse return error.InvalidLogLevel
            else
                return error.UnknownVariable;
        }
    }
};


fn writeConfig() !void {
    // TODO: use ConfigReader to preserve empty lines, comments and order of statements.
    {
        var file = try store_dir.createFile("config~", .{});
        defer file.close();
        errdefer store_dir.deleteFile("config~") catch {};

        var buf = std.io.bufferedWriter(file.writer());
        var wr = buf.writer();
        try wr.print("hash_threads {}\n", .{ hash_threads });
        try wr.print("blake3_piece_size {}\n", .{ blake3_piece_size });
        try wr.print("log_level {s}\n", .{ util.logLevelAsText(log_level) });
        try buf.flush();
    }
    try store_dir.rename("config~", "config");
}


// Uses XDG_CONFIG_HOME by default, but that's not /quite/ ideal since the
// store_dir is used for more than just config files, it also stores repository
// data, caches and runtime stuff. The data /is/ important, though, and
// something one might want to include in backups to avoid expensive
// re-hashing. The caches and runtime stuff could be moved elsewhere, but I do
// sort-of like having everything together, helps with portability and managing
// multiple stores.
pub fn initStore() !void {
    store_path = blk: {
        if (std.process.getEnvVarOwned(util.allocator, "JAKOI_STORE")) |p| break :blk p
        else |_| {}

        if (std.builtin.os.tag == .windows) break :blk try std.fs.getAppDataDir("jakoi");

        if (std.process.getEnvVarOwned(util.allocator, "XDG_CONFIG_HOME")) |p| {
            defer util.allocator.free(p);
            break :blk try std.fs.path.join(util.allocator, &.{p, "jakoi"});
        } else |_| if (std.process.getEnvVarOwned(util.allocator, "HOME")) |p| {
            defer util.allocator.free(p);
            break :blk try std.fs.path.join(util.allocator, &.{p, ".config", "jakoi"});
        } else |_| return error.NoStorePath;
    };

    if (!std.fs.path.isAbsolute(store_path)) return error.RelativeStorePath;

    if (std.fs.accessAbsolute(store_path, .{}))
        std.log.info("Using store path: {s}", .{store_path})
    else |_| {
        std.log.notice("Initializing a fresh Jakoi setup at {s}", .{store_path});
        try std.fs.cwd().makePath(store_path);
    }
    store_dir = try std.fs.openDirAbsolute(store_path, .{});
}


pub fn initConfig(error_line: *?u32) !void {
    hash_threads = std.math.min(4, std.Thread.getCpuCount() catch 1);

    ConfigReader.read(error_line) catch |e| switch (e) {
        error.FileNotFound => try writeConfig(),
        else => return e,
    };
}


pub fn virtualToFs(vpath: []const u8) !util.Path {
    var p = util.Path{};
    try p.push(public_dir);
    try p.push(vpath);
    return p;
}
