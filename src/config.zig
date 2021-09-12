// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const util = @import("util.zig");

pub var store_path: []const u8 = "";
pub var store_dir = std.fs.cwd();

// Run-time configuration, defaults are set in ConfigFile.defaults and
// initialized by initConfig().
pub var hash_threads: ?usize = undefined;
pub var blake3_piece_size: u64 = undefined;
pub var log_level: std.log.Level = .info; // may be accessed before initConfig()

// Assumption: Tor is running on localhost, so the address must be localhost or a UNIX path.
// TODO: Spawn/embed our own Tor instance, so we can set it up properly.
pub var tor_control_address = std.net.Address.initIp4(.{127,0,0,1}, 9051);
pub var tor_control_password: []const u8 = "0tqTCwh8ziHQGoBs8f6O"; // HASHEDPASSWORD authentication

// Published directories, virtual mount point -> absolute filesystem path.
// Assumption: Currently only holds at most a single directory mounted at the root.
// actual multiple mount points should be implemented at some point.
pub var published_dirs = std.StringHashMap([]const u8).init(util.allocator);


const ConfigFile = struct {
    hash_threads: ?usize = null,
    blake3_piece_size: ?u64 = null,
    log_level: ?[]const u8 = null,
    published_dirs: ?[]PublishedDir = null,

    const PublishedDir = struct {
        virtual: []const u8,
        fs: []const u8,
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

        // TODO: Proper merging so that we can update the existing database on change
        published_dirs.clearRetainingCapacity(); // This leaks memory.
        for (self.published_dirs orelse &[0]PublishedDir{}) |dir| try published_dirs.put(
            try util.allocator.dupe(u8, dir.virtual),
            try util.allocator.dupe(u8, dir.fs)
        );
    }

    fn read() !ConfigFile {
        var data = try store_dir.readFileAlloc(util.allocator, "config", 10*1024*1024);
        defer util.allocator.free(data);
        const self = try std.json.parse(ConfigFile, &std.json.TokenStream.init(data), opts);

        if (self.log_level) |v| _ = util.logLevelFromText(v) orelse return error.InvalidLogLevel;
        if (self.blake3_piece_size) |v| if (v < 1024 or !std.math.isPowerOfTwo(v)) return error.InvalidBlake3PieceSize;
        if (self.published_dirs) |v| {
            if (v.len > 1) return error.MultipleDirsNotYetImplemented;
            if (v.len == 1 and v[0].virtual.len != 0) return error.NonRootVirtualNotYetImplemented;
        }
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

        if (std.builtin.os.tag == .windows) break :blk try std.fs.getAppDataDir("jakoi");

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


pub fn virtualToFs(vpath: []const u8) !util.Path {
    std.debug.assert(published_dirs.count() > 0);
    var p = util.Path{};
    try p.push(published_dirs.get("").?);
    try p.push(vpath);
    return p;
}
