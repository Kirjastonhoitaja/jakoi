// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const main = @import("main.zig");


const DirQueue = struct {
    lst: std.ArrayList(Entry) = std.ArrayList(Entry).init(main.allocator),
    names: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(main.allocator),
    idx: usize = 0,
    dir: std.fs.Dir,

    const Entry = struct { name: []const u8, id: u64 = 0 };

    fn lessThan(_: void, a: Entry, b: Entry) bool {
        return std.mem.lessThan(u8, a.name, b.name);
    }

    fn next(s: *@This()) ?Entry {
        defer s.idx += 1;
        return if (s.idx < s.lst.items.len) s.lst.items[s.idx] else null;
    }

    fn deinit(s: *@This()) void {
        s.lst.deinit();
        s.names.deinit();
        s.dir.close();
    }
};

const Listing = struct {
    dirs: DirQueue,
    files: std.ArrayList(File) = std.ArrayList(File).init(main.allocator),
    names: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(main.allocator),

    const File = struct { name: []const u8, lastmod: u64, size: u64 };

    fn lessThan(_: void, a: File, b: File) bool {
        return std.mem.lessThan(u8, a.name, b.name);
    }

    fn get(dir: std.fs.Dir) !Listing {
        var l = Listing{ .dirs = .{ .dir = dir }};
        errdefer l.deinit();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            // TODO symlink following option
            const stat = std.os.fstatat(dir.fd, entry.name, std.os.AT_SYMLINK_NOFOLLOW) catch |e| {
                std.log.info("Unable to stat {s}: {}, skipping", .{entry.name, e});
                continue;
            };
            const isdir = std.os.system.S_ISDIR(stat.mode);
            if (!isdir and !std.os.system.S_ISREG(stat.mode)) {
                std.log.debug("Skipping non-regular file: {s}", .{entry.name});
                continue;
            }
            if (isdir) {
                const name = try l.dirs.names.allocator.dupe(u8, entry.name);
                try l.dirs.lst.append(.{ .name = name });
            } else {
                const name = try l.names.allocator.dupe(u8, entry.name);
                const mtime = stat.mtime().tv_sec;
                try l.files.append(.{
                    .name = name,
                    .lastmod = if (isdir) std.math.maxInt(u64) else if (mtime < 0) 0 else @intCast(u64, mtime),
                    .size = @intCast(u64, stat.size),
                });
            }
        }
        std.sort.sort(DirQueue.Entry, l.dirs.lst.items, @as(void, undefined), DirQueue.lessThan);
        std.sort.sort(File, l.files.items, @as(void, undefined), lessThan);
        return l;
    }

    fn deinit(s: *@This()) void {
        s.dirs.deinit();
        s.files.deinit();
        s.names.deinit();
    }
};


fn storeListing(t: db.Txn, id: u64, lst: *Listing) !void {
    var dirs = lst.dirs.lst.items;
    var files = lst.files.items;
    var dirIdx: usize = 0;
    var fileIdx: usize = 0;

    var iter = t.dirIter(id);
    defer iter.deinit();
    while (try iter.next()) |ent| {
        // Compare with next dir entry, if that comes earlier than the next file entry
        if (dirIdx < dirs.len and (fileIdx >= files.len or std.mem.lessThan(u8, dirs[dirIdx].name, files[fileIdx].name))) {
            const dir = &dirs[dirIdx];
            const cmp = std.mem.order(u8, dir.*.name, ent.name());
            if (cmp == .lt) { // dir < ent
                dirIdx += 1;
                dir.*.id = try iter.addDir(dir.*.name);
                continue;
            }
            if (cmp == .eq) {
                dirIdx += 1;
                switch (ent.val()) {
                    .dir => |d| dir.*.id = d.*,
                    else => {
                        try iter.del(ent);
                        dir.*.id = try iter.addDir(dir.*.name);
                    }
                }
                continue;
            }

        // Compare with next file entry
        } else if (fileIdx < files.len) {
            const file = files[fileIdx];
            const cmp = std.mem.order(u8, file.name, ent.name());
            if (cmp == .lt) { // file < ent
                fileIdx += 1;
                try iter.addFile(file.name, file.lastmod, file.size);
                continue;
            }
            if (cmp == .eq) {
                fileIdx += 1;
                const readd = switch (ent.val()) {
                    .dir => true,
                    .unhashed => |f| file.lastmod > f.lastmod or f.size != file.size,
                    .hashed   => |f| file.lastmod > f.lastmod or f.size != file.size,
                };
                if (readd) {
                    try iter.del(ent); // technically don't need this if ent is unhashed.
                    try iter.addFile(file.name, file.lastmod, file.size);
                }
                continue;
            }
        }
        try iter.del(ent);
    }

    while (dirIdx < dirs.len) : (dirIdx += 1)
        dirs[dirIdx].id = try iter.addDir(dirs[dirIdx].name);

    while (fileIdx < files.len) : (fileIdx += 1)
        try iter.addFile(files[fileIdx].name, files[fileIdx].lastmod, files[fileIdx].size);
}

fn scanDir(id: u64, parent: std.fs.Dir, name: []const u8) !DirQueue {
    var dir = try parent.openDir(name, .{.iterate=true});

    var lst = try Listing.get(dir);
    errdefer lst.deinit();
    try db.txn(.rw, storeListing, .{id, &lst});

    lst.files.deinit();
    lst.names.deinit();
    return lst.dirs;
}

pub fn scan(path: []const u8) !void {
    var stack = std.ArrayList(DirQueue).init(main.allocator);
    defer stack.deinit();

    try stack.append(try scanDir(0, std.fs.cwd(), path));
    while (stack.items.len > 0) {
        if (stack.items[stack.items.len-1].next()) |e| {
            if (scanDir(e.id, stack.items[stack.items.len-1].dir, e.name)) |q|
                try stack.append(q)
            else |err|
                std.log.info("Error reading {s}: {}, skipping.", .{ e.name, err });
        } else
            stack.pop().deinit();
    }
}
