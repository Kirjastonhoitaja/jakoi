// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const main = @import("main.zig");
const config = @import("config.zig");
const blake3 = @import("blake3.zig");
const util = @import("util.zig");


const DirQueue = struct {
    lst: std.ArrayList(Entry) = std.ArrayList(Entry).init(util.allocator),
    names: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(util.allocator),
    idx: usize = 0,

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
    }
};

const Listing = struct {
    dirs: DirQueue = .{},
    files: std.ArrayList(File) = std.ArrayList(File).init(util.allocator),
    names: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(util.allocator),

    const File = struct { name: []const u8, lastmod: u64, size: u64 };

    fn lessThan(_: void, a: File, b: File) bool {
        return std.mem.lessThan(u8, a.name, b.name);
    }

    // TODO windows
    fn add(self: *Listing, virtual: *const util.Path, parent: std.fs.Dir, fs: ?[]const u8) error{OutOfMemory}!?bool {
        const name = std.fs.path.basenamePosix(virtual.slice());

        // TODO symlink following option
        const stat = (
            if (fs) |p| std.os.fstatat(parent.fd, p, 0)
            else std.os.fstatat(parent.fd, name, std.os.AT.SYMLINK_NOFOLLOW)
        ) catch |e| {
            if (fs == null) std.log.info("Unable to stat {}: {}, skipping", .{virtual, e})
            else std.log.warn("Unable to stat published path '{}': {}", .{virtual, e});
            return null;
        };
        const isdir = std.os.system.S.ISDIR(stat.mode);
        if (!isdir and !std.os.system.S.ISREG(stat.mode)) {
            if (fs == null) std.log.debug("Skipping non-regular file: {}", .{virtual})
            else std.log.warn("Published path '{}' is neither a file nor a directory", .{virtual});
            return null;
        }
        if (isdir)
            try self.dirs.lst.append(.{ .name = try self.dirs.names.allocator.dupe(u8, name) })
        else {
            const mtime = stat.mtime().tv_sec;
            try self.files.append(.{
                .name = try self.names.allocator.dupe(u8, name),
                .lastmod = if (mtime < 0) 0 else @intCast(u64, mtime),
                .size = @intCast(u64, stat.size),
            });
        }
        return isdir;
    }

    fn addFs(self: *Listing, dir: std.fs.Dir, mounts: *const config.Mounts, path: *util.Path) !void {
        var it = dir.iterate();
        while (try it.next()) |entry| {
            try path.push(entry.name);
            defer path.pop();

            if (mounts.sub.contains(entry.name)) {
                std.log.info("Virtual path '{}' also exists on the filesystem, ignoring filesystem entry", .{path});
                continue;
            }

            if (!util.isValidFileName(entry.name)) {
                std.log.info("Invalid file name: {}, skipping", .{path});
                continue;
            }

            _ = try self.add(path, dir, null);
        }
    }

    fn addMounts(self: *Listing, mounts: *const config.Mounts, path: *util.Path) !void {
        var it = mounts.sub.iterator();
        while (it.next()) |entry| {
            try path.push(entry.key_ptr.*);
            defer path.pop();

            if (entry.value_ptr.*.fs) |fs| {
                const isdir = (try self.add(path, std.fs.cwd(), fs)) orelse false;
                if (!isdir and entry.value_ptr.*.sub.count() > 0)
                    std.log.warn("Published path '{}' points to a file, but there are other published paths beneath it. Those sub-paths will not be published.", .{path});
            } else
                try self.dirs.lst.append(.{ .name = try self.dirs.names.allocator.dupe(u8, entry.key_ptr.*) });
        }
    }

    fn deinit(s: *@This()) void {
        s.dirs.deinit();
        s.files.deinit();
        s.names.deinit();
    }
};


fn storeListing(t: db.Txn, id: u64, lst: *Listing, path: *util.Path) !void {
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
                        try iter.del(ent, path);
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
                    try iter.del(ent, path);
                    try iter.addFile(file.name, file.lastmod, file.size);
                }
                continue;
            }
        }
        try iter.del(ent, path);
    }

    while (dirIdx < dirs.len) : (dirIdx += 1)
        dirs[dirIdx].id = try iter.addDir(dirs[dirIdx].name);

    while (fileIdx < files.len) : (fileIdx += 1)
        try iter.addFile(files[fileIdx].name, files[fileIdx].lastmod, files[fileIdx].size);
}

// Pushes the given name to 'path' on success
fn scanDir(id: u64, path: *util.Path, name: []const u8) !DirQueue {
    try path.push(name);
    errdefer path.pop();

    var l = Listing{};
    errdefer l.deinit();

    var sub = config.mounts.subdir(path.slice());
    if (sub) |s| try l.addMounts(s, path);
    if (try config.mounts.virtualToFs(path.slice())) |fs| {
        defer util.allocator.free(fs);
        var dir = try std.fs.cwd().openDir(fs, .{.iterate=true});
        defer dir.close();
        try l.addFs(dir, sub orelse &.{}, path);
    }

    std.sort.sort(DirQueue.Entry, l.dirs.lst.items, @as(void, undefined), DirQueue.lessThan);
    std.sort.sort(Listing.File, l.files.items, @as(void, undefined), Listing.lessThan);
    try db.txn(.rw, storeListing, .{id, &l, path});

    l.files.deinit();
    l.names.deinit();
    return l.dirs;
}

pub fn scan() !void {
    var stack = std.ArrayList(DirQueue).init(util.allocator);
    defer stack.deinit();

    var path = util.Path{};
    try stack.append(try scanDir(0, &path, ""));
    while (stack.items.len > 0) {
        if (stack.items[stack.items.len-1].next()) |e| {
            if (scanDir(e.id, &path, e.name)) |q|
                try stack.append(q)
            else |err|
                std.log.info("Error reading {}{s}{s}: {}, skipping.",
                    .{ path, @as([]const u8, if (path.len > 0) "/" else ""), e.name, err });
        } else {
            path.pop();
            stack.pop().deinit();
        }
    }
}


fn hashPopulate(t: db.Txn) !void {
    db.hash_queue.reset();
    try db.hash_queue.populate(t);
}

fn hashNext(t: db.Txn) !?db.hash_queue.Entry {
    return db.hash_queue.next(t);
}

// TODO: It may be worth batching these into fewer transactions, especially for small files.
fn hashStore(t: db.Txn, e: db.hash_queue.Entry, b3: [32]u8, pieces: ?[]const u8) !void {
    return db.hash_queue.store(t, e, b3, pieces);
}

fn hashFile(ent: db.hash_queue.Entry) !void {
    std.log.debug("Hashing {s}", .{ent.path});

    // Short-circuit empty files, mmap() doesn't like those.
    if (ent.size == 0)
        return db.txn(.rw, hashStore, .{ ent, blake3.hashPiece(0, "").root(), null });

    var fspath = (try config.mounts.virtualToFs(ent.path)) orelse unreachable;
    defer util.allocator.free(fspath);
    var fd = try std.fs.cwd().openFileZ(fspath, .{});
    defer fd.close();

    // TODO: Validate if ent.size is still correct.
    // (Still subject to an unavoidable race condition, but may handle a few cases)
    // TODO: Make this work on Windows.
    // TODO: Handle large files on 32bit systems.
    // TODO: Non-mmap fallback? Even fixing the above, mmap /is/ slightly fragile.
    var map = try std.os.mmap(null, ent.size, std.os.PROT.READ, std.os.MAP.PRIVATE, fd.handle, 0);
    defer std.os.munmap(map);

    const piece_size = config.blake3_piece_size;
    const num_pieces = std.math.divCeil(u64, ent.size, piece_size) catch unreachable;
    if (num_pieces == 1) {
        const b3 = blake3.hashPiece(0, map).root();
        return db.txn(.rw, hashStore, .{ ent, b3, null });
    }

    var piecedata = try std.ArrayList(u8).initCapacity(util.allocator, 8+32*num_pieces);
    defer piecedata.deinit();
    piecedata.appendSlice(std.mem.asBytes(&ent.size)) catch unreachable;
    var i: u64 = 0;
    while (i < num_pieces) : (i += 1)
        piecedata.appendSlice(&blake3.hashPiece(
            i*piece_size/blake3.CHUNK_LEN,
            map[i*piece_size..std.math.min(ent.size, (i+1)*piece_size)]
        ).chainingValue()) catch unreachable;
    const b3 = blake3.mergePieces(piecedata.items[8..]).root();
    return db.txn(.rw, hashStore, .{ ent, b3, piecedata.items });
}

// TODO: For large files, we can maintain a separate "piece queue", consulted
// before db.hash_queue, so that multiple threads can work on the same file.
fn hashThread() void {
    while (true) {
        var ent = db.txn(.ro, hashNext, .{}) catch |e| {
            std.log.warn("Hash thread exited with error: {}", .{e});
            return;
        } orelse return;
        defer ent.deinit();
        hashFile(ent) catch |e|
            std.log.warn("Error hashing {s}: {}", .{ent.path, e});
    }
}

pub fn hash() !void {
    try db.txn(.ro, hashPopulate, .{});
    var threads = std.ArrayList(std.Thread).init(util.allocator);
    defer threads.deinit();
    var i = config.hash_threads orelse std.math.min(4, std.Thread.getCpuCount() catch 1);

    while (i > 0) : (i -= 1) {
        // We do call a few recursive functions, but 1M should be more than enough.
        var t = try std.Thread.spawn(.{ .stack_size = 1024*1024 }, hashThread, .{});
        var buf: [32]u8 = undefined;
        if (std.fmt.bufPrint(&buf, "Hasher #{}", .{i})) |name| t.setName(name) catch {}
        else |_| {}
        try threads.append(t);
    }
    for (threads.items) |*t| t.join();
}
