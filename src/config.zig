// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const util = @import("util.zig");

pub var store_path: []const u8 = "";
pub var store_dir: std.fs.Dir = undefined;

// Run-time configuration, defaults are set in ConfigFile.defaults and
// initialized by initConfig().
pub var hash_threads: ?usize = undefined;
pub var blake3_piece_size: u64 = undefined;
pub var log_level: std.log.Level = .info; // may be accessed before initConfig()

// Assumption: Tor is running on localhost, so the address must be localhost or a UNIX path.
// TODO: Spawn/embed our own Tor instance, so we can set it up properly.
pub var tor_control_address = std.net.Address.initIp4(.{127,0,0,1}, 9051);
pub var tor_control_password: []const u8 = "0tqTCwh8ziHQGoBs8f6O"; // HASHEDPASSWORD authentication

pub var mounts = Mounts{};


const ConfigFile = struct {
    hash_threads: ?usize = null,
    blake3_piece_size: ?u64 = null,
    log_level: ?[]const u8 = null,
    published_paths: ?[]PublishedPath = null,

    const PublishedPath = struct {
        virtual: []const u8,
        fs: []const u8,

        fn lessThan(_: void, a: PublishedPath, b: PublishedPath) bool {
            return std.mem.lessThan(u8, a.virtual, b.virtual);
        }
    };

    const opts = std.json.ParseOptions{
        .allocator = util.allocator,
        .ignore_unknown_fields = true,
    };

    // Do NOT call .deinit() on this one!
    const defaults = ConfigFile{
        .blake3_piece_size = 1024*1024,
        .log_level = "info",
    };

    fn apply(self: *const ConfigFile) !void {
        hash_threads = self.hash_threads orelse defaults.hash_threads;
        blake3_piece_size = self.blake3_piece_size orelse defaults.blake3_piece_size.?;
        log_level = util.logLevelFromText(self.log_level orelse defaults.log_level.?).?;

        mounts.deinit();
        mounts = try Mounts.fromConfig(self.published_paths orelse &.{});
    }

    fn verify(self: *ConfigFile) !void {
        if (self.log_level) |v| _ = util.logLevelFromText(v) orelse return error.InvalidLogLevel;
        if (self.blake3_piece_size) |v| if (v < 1024 or !std.math.isPowerOfTwo(v)) return error.InvalidBlake3PieceSize;

        if (self.published_paths) |p| {
            for (p) |e| {
                if (!util.isValidPath(e.virtual)) return error.InvalidMountPath;
                if (!std.fs.path.isAbsolute(e.fs)) return error.InvalidRelativeMountPath;
            }
            std.sort.sort(PublishedPath, p, @as(void, undefined), PublishedPath.lessThan);
            if (p.len > 1) {
                var last_virt = p[0].virtual;
                for (p[1..]) |e| {
                    if (std.mem.eql(u8, last_virt, e.virtual))
                        return error.DuplicateMountPoint;
                    last_virt = e.virtual;
                }
            }
        }
    }

    fn read() !ConfigFile {
        var data = try store_dir.readFileAlloc(util.allocator, "config", 10*1024*1024);
        defer util.allocator.free(data);
        var self = try std.json.parse(ConfigFile, &std.json.TokenStream.init(data), opts);
        try self.verify();
        return self;
    }

    fn write(self: *ConfigFile) !void {
        {
            var file = try store_dir.createFile("config~", .{});
            defer file.close();
            errdefer store_dir.deleteFile("config~") catch {};

            var buf = std.io.bufferedWriter(file.writer());
            try std.json.stringify(self, .{ .whitespace = .{} }, buf.writer());
            try buf.writer().writeByte('\n');
            try buf.flush();
        }
        try store_dir.rename("config~", "config");
    }

    fn deinit(self: ConfigFile) void {
        std.json.parseFree(ConfigFile, self, opts);
    }
};

// Recursive HashMap tree representing published mount points
pub const Mounts = struct {
    sub: std.StringHashMap(Mounts) = std.StringHashMap(Mounts).init(util.allocator),
    fs: ?[]const u8 = null,

    fn fromConfig(conf: []const ConfigFile.PublishedPath) error{OutOfMemory}!Mounts {
        var self = Mounts{};
        for (conf) |dir| {
            var virt = dir.virtual;
            var parent = &self;
            while (virt.len > 0) {
                var ent = try parent.sub.getOrPut(util.pathHead(virt));
                if (!ent.found_existing) {
                    ent.key_ptr.* = try util.allocator.dupe(u8, util.pathHead(virt));
                    ent.value_ptr.* = .{};
                }
                parent = &ent.value_ptr.*;
                virt = util.pathTail(virt);
            }
            std.debug.assert(parent.fs == null);
            parent.fs = try util.allocator.dupe(u8, dir.fs);
        }
        return self;
    }

    pub fn isEmpty(self: *const Mounts) bool {
        return self.fs == null and self.sub.count() == 0;
    }

    pub fn virtualToFs(self: *const Mounts, vpath_: []const u8) error{OutOfMemory}!?[:0]u8 {
        var fs = self.fs;
        var vpath = std.mem.trim(u8, vpath_, "/");
        var virt = vpath;
        var parent = self;
        while (vpath.len > 0) {
            if (parent.sub.getPtr(util.pathHead(vpath))) |e| {
                vpath = util.pathTail(vpath);
                if (e.fs) |v| {
                    fs = v;
                    virt = vpath;
                }
                parent = e;
            } else break;
        }
        if (fs) |p| return try std.fs.path.joinZ(util.allocator, &.{p, virt});
        return null;
    }

    pub fn subdir(self: *const Mounts, vpath_: []const u8) ?*const Mounts {
        var vpath = std.mem.trim(u8, vpath_, "/");
        var ret = self;
        while (vpath.len > 0) {
            ret = ret.sub.getPtr(util.pathHead(vpath)) orelse return null;
            vpath = util.pathTail(vpath);
        }
        return ret;
    }

    fn deinit(self: *Mounts) void {
        if (self.fs) |p| util.allocator.free(p);
        var it = self.sub.iterator();
        while (it.next()) |e| {
            util.allocator.free(e.key_ptr.*);
            e.value_ptr.deinit();
        }
        self.sub.deinit();
    }
};


// This test leaks memory. That's fiiiiiine.
test "virtualToFs" {
    const m = try Mounts.fromConfig(&.{
        .{ .virtual = "foo", .fs = "/_foo_" },
        .{ .virtual = "foo/bar", .fs = "/_bar_" },
        .{ .virtual = "a/b/c", .fs = "/_abc_" },
    });
    const ex = std.testing.expect;
    try ex(null == try m.virtualToFs(""));
    try ex(null == try m.virtualToFs("a"));
    try ex(null == try m.virtualToFs("a/b"));
    try ex(null == try m.virtualToFs("a/b/d"));
    try ex(null == try m.virtualToFs("unknownpath"));

    const eqs = std.testing.expectEqualStrings;
    const sep = std.fs.path.sep_str;
    try eqs("/_foo_", (try m.virtualToFs("foo")).?);
    try eqs("/_foo_", (try m.virtualToFs("foo/")).?);
    try eqs("/_foo_", (try m.virtualToFs("//foo///")).?);
    try eqs("/_foo_" ++ sep ++ "rest", (try m.virtualToFs("foo/rest")).?);
    try eqs("/_bar_", (try m.virtualToFs("foo/bar")).?);
    try eqs("/_bar_" ++ sep ++ "rest", (try m.virtualToFs("foo/bar/rest")).?);
    try eqs("/_abc_", (try m.virtualToFs("/a/b/c")).?);
    try eqs("/_abc_" ++ sep ++ "rest", (try m.virtualToFs("/a/b/c/rest")).?);

    // Has a root, so never returns null
    const r = try Mounts.fromConfig(&.{
        .{ .virtual = "", .fs = "/_root_" },
        .{ .virtual = "foo", .fs = "/_foo_" },
    });
    try eqs("/_root_", (try r.virtualToFs("")).?);
    try eqs("/_root_", (try r.virtualToFs("/")).?);
    try eqs("/_root_" ++ sep ++ "rest", (try r.virtualToFs("rest")).?);
    try eqs("/_foo_", (try r.virtualToFs("foo")).?);
    try eqs("/_foo_" ++ sep ++ "rest", (try r.virtualToFs("foo/rest")).?);
}


// Uses XDG_CONFIG_HOME by default, but that's not /quite/ ideal since the
// store_dir is used for more than just config files, it also stores repository
// data, caches and runtime stuff. The data /is/ important, though, and
// something one might want to include in backups to avoid expensive
// re-hashing. The caches and runtime stuff could be moved elsewhere, but I do
// sort-of like having everything together, helps with portability and managing
// multiple stores.
pub fn initStore(allow_init: bool, cli_path: ?[]const u8) !void {
    store_path = blk: {
        if (cli_path) |p| break :blk p;
        if (std.process.getEnvVarOwned(util.allocator, "JAKOI_STORE")) |p| break :blk p
        else |_| {}

        if (std.builtin.os.tag == .windows) {
            if (std.fs.getAppDataDir(util.allocator, "jakoi")) |p| break :blk p
            else |e| switch (e) {
                error.AppDataDirUnavailable => return error.NoStorePath,
                else => return e,
            }
        }

        if (std.process.getEnvVarOwned(util.allocator, "XDG_CONFIG_HOME")) |p| {
            defer util.allocator.free(p);
            break :blk try std.fs.path.join(util.allocator, &.{p, "jakoi"});
        } else |_| if (std.process.getEnvVarOwned(util.allocator, "HOME")) |p| {
            defer util.allocator.free(p);
            break :blk try std.fs.path.join(util.allocator, &.{p, ".config", "jakoi"});
        } else |_| return error.NoStorePath;
    };

    // cli_path may be relative, but env vars must be absolute.
    if (cli_path != null) {
        if (allow_init) try std.fs.cwd().makePath(store_path);
        store_path = try std.fs.cwd().realpathAlloc(util.allocator, store_path);
    }
    if (!std.fs.path.isAbsolute(store_path)) return error.RelativeStorePath;

    if (std.fs.accessAbsolute(store_path, .{}))
        std.log.info("Using store path: {s}", .{store_path})
    else |_| if (allow_init) {
        std.log.notice("Initializing a fresh Jakoi setup at {s}", .{store_path});
        try std.fs.cwd().makePath(store_path);
    } else return error.FileNotFound;
    store_dir = try std.fs.openDirAbsolute(store_path, .{});
}


pub fn initConfig() !void {
    var conf = ConfigFile.read() catch |e| switch (e) {
        error.FileNotFound => ConfigFile{},
        else => return e,
    };
    defer conf.deinit();
    try conf.apply();
}
