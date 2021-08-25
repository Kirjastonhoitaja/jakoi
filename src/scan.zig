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
    files: std.ArrayList(File) = std.ArrayList(File).init(util.allocator),
    names: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(util.allocator),

    const File = struct { name: []const u8, lastmod: u64, size: u64 };

    fn lessThan(_: void, a: File, b: File) bool {
        return std.mem.lessThan(u8, a.name, b.name);
    }

    fn get(dir: std.fs.Dir, path: *util.Path) !Listing {
        var l = Listing{ .dirs = .{ .dir = dir }};
        errdefer l.deinit();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            try path.push(entry.name);
            defer path.pop();

            // TODO symlink following option
            const stat = std.os.fstatat(dir.fd, entry.name, std.os.AT_SYMLINK_NOFOLLOW) catch |e| {
                std.log.info("Unable to stat {}: {}, skipping", .{path, e});
                continue;
            };
            const isdir = std.os.system.S_ISDIR(stat.mode);
            if (!isdir and !std.os.system.S_ISREG(stat.mode)) {
                std.log.debug("Skipping non-regular file: {}", .{path});
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

fn scanDir(id: u64, parent: ?std.fs.Dir, path: *util.Path, name: []const u8) !DirQueue {
    if (parent != null) try path.push(name);
    errdefer if (parent != null) path.pop();
    var dir = try (parent orelse std.fs.cwd()).openDir(name, .{.iterate=true});

    var lst = try Listing.get(dir, path);
    errdefer lst.deinit();
    try db.txn(.rw, storeListing, .{id, &lst, path});

    lst.files.deinit();
    lst.names.deinit();
    return lst.dirs;
}

pub fn scan() !void {
    var stack = std.ArrayList(DirQueue).init(util.allocator);
    defer stack.deinit();

    var path = util.Path{};

    try stack.append(try scanDir(0, null, &path, config.public_dir));
    while (stack.items.len > 0) {
        if (stack.items[stack.items.len-1].next()) |e| {
            if (scanDir(e.id, stack.items[stack.items.len-1].dir, &path, e.name)) |q|
                try stack.append(q)
            else |err|
                std.log.info("Error reading {}: {}, skipping.", .{ path, err });
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

    var fspath = try config.virtualToFs(ent.path);
    var fd = try std.fs.cwd().openFileZ(try fspath.sliceZ(), .{});
    defer fd.close();

    // TODO: Validate if ent.size is still correct.
    // (Still subject to an unavoidable race condition, but may handle a few cases)
    // TODO: Make this work on Windows.
    // TODO: Handle large files on 32bit systems.
    // TODO: Non-mmap fallback? Even fixing the above, mmap /is/ slightly fragile.
    var map = try std.os.mmap(null, ent.size, std.os.PROT_READ, std.os.MAP_PRIVATE, fd.handle, 0);
    defer std.os.munmap(map);

    const piece_size = config.min_piece_size;
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
    var i = config.hash_threads;
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
