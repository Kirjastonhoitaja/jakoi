// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1
//
// LMDB key format:
//
// 0 0
//   u8: database major version (1)
//   u8: database minor version (0)
// 0 1
//   u64: monotonically increasing sequence for internal id generation
// 0 2
//   u256: directory listing hash
// 0 3
//   u256: blake3 listing hash
// 0 4
//   u64: number of entries in the blake3 listing
//
// 1 + <u64: dir_id> + <string: name>
//   Directory entry; top-level dir has dir_id=0, others are generated from a sequence.
//   For files:
//     u64: lastmod
//     u64: size
//     u256: blake3 hash (only present if known)
//   For dirs:
//     u64: dir_id
//   (Yes, the difference between files and dirs is the size of the value)
//   TODO: Splitting this up to have separate namespaces for dirs and files may
//   simplify and improve performance for both the filesystem scanning code and
//   the repo metadata writing.
//
// 2 + <u256: file_hash>
//   u64: size
//   rest: blake3 hash data
//
// 3 + <u256: file_hash>
//   CBOR-encoded file metadata.
//
// 4 + <u256: file_hash> + <u64: path_hash>
//   Value: path string
//   path_hash is the prefix of blake3(path)
//   For hash -> path lookups.

const std = @import("std");
const config = @import("config.zig");
const util = @import("util.zig");
const blake3 = @import("blake3.zig");
const main = @import("main.zig");
const c = @cImport({ @cInclude("errno.h"); @cInclude("lmdb.h"); });

var db_env: ?*c.MDB_env = null;
var db_dbi: c.MDB_dbi = undefined;


// Bit of constness sloppyness here, but practically all the MDB_val pointers
// we pass to lmdb will not be modified by lmdb, so we can safely encode that
// in the type system.
fn toVal(v: []const u8) c.MDB_val {
    return c.MDB_val{ .mv_size = v.len, .mv_data = @intToPtr(*c_void, @ptrToInt(v.ptr)) };
}

// We don't use MDB_WRITEMAP, so values returned from lmdb must be treated as const.
fn fromVal(v: c.MDB_val) []const u8 {
    return (@ptrCast([*c]const u8, v.mv_data.?))[0..v.mv_size];
}

fn rcErr(rc: c_int) !void {
    return switch (rc) {
        c.MDB_SUCCESS => {},
        c.MDB_INVALID => error.MdbInvalid,
        c.MDB_MAP_FULL => error.MdbMapFull,
        c.MDB_MAP_RESIZED => error.MdbMapResized,
        c.MDB_PANIC => error.MdbPanic,
        c.MDB_TXN_FULL => error.MdbTxnFull,
        c.MDB_VERSION_MISMATCH => error.MdbVersionMismatch,
        c.EACCES => error.AccessDenied,
        c.EIO => error.InputOutput,
        c.ENOMEM => error.OutOfMemory,
        c.ENOSPC => error.NoSpaceLeft,
        else => error.MdbUnexpected,
    };
}

pub const MdbError = @typeInfo(@typeInfo(@TypeOf(rcErr)).Fn.return_type.?).ErrorUnion.error_set;


pub const Mode = enum { ro, rw };

// txn(mode, cb: fn(Txn, args) !ret, args) !ret
//
// Run the callback within a new transaction. The transaction is aborted if the
// callback returns an error. If the callback returned MdbMapResized or
// MdbMapFull, the mapsize is adjusted and the callback is run again.
//
// The return value of the callback must not contain pointers into memory
// returned by database functions, as these may be invalidated when the
// transaction is closed.
// If committing the transaction fails, the return value of the callback is
// discarded. This may result in a resource leak.
//
// Nested transactions are not supported. An async callback that suspends is
// kinda supported, but only if it is resumed on the same OS thread and no
// other concurrent transactions are started on the same OS thread.
//
// Type system sloppiness: cb must have the full rcErr() in its error set,
// otherwise this won't compile. The return type of this function also includes
// MdbMapResized and MdbMapFull, even though those will never be returned.
pub fn txn(comptime mode: Mode, cb: anytype, args: anytype) @typeInfo(@TypeOf(cb)).Fn.return_type.? {
    while (true) {
        var t = try txn_impl.start(if (mode == .ro) c.MDB_RDONLY else 0);
        const r = @call(.{}, cb, .{t} ++ args);

        if (txn_impl.final(t, if (r) null else |e| e)) |v| {
            if (v) return r;
        } else |e|
            return @errSetCast(@typeInfo(@typeInfo(@TypeOf(cb)).Fn.return_type.?).ErrorUnion.error_set, e);
    }
}

const txn_impl = struct {
    // Complex machinery to ensure we can safely resize the LMDB map. :(
    var resizing: bool = false;
    var active_txn: usize = 0;
    var lock = std.Thread.Mutex{};
    var canresize = std.Thread.Condition{}; // active_txn == 1
    var resized = std.Thread.Condition{}; // resizing == false

    // decrement active_txn
    fn release() void {
        const l = lock.acquire();
        active_txn -= 1;
        const signal = active_txn == 1;
        l.release();
        if (signal) canresize.signal();
    }

    // Try to resize the map, assumes we still count as an active_txn.
    fn resize(new_size: u64) !void {
        const l = lock.acquire();
        if (resizing) { // Let the other thread do its thing.
            l.release();
            return;
        }
        resizing = true;
        while (active_txn > 1) canresize.wait(&lock);
        const rc = c.mdb_env_set_mapsize(db_env, new_size);
        resizing = false;
        l.release();
        resized.broadcast();
        return rcErr(rc);
    }

    fn start(flags: c_uint) !Txn {
        const l = lock.acquire();
        while (resizing) resized.wait(&lock);
        active_txn += 1;
        l.release();

        errdefer release();
        var t = Txn{};
        try rcErr(c.mdb_txn_begin(db_env, null, flags, &t.t));
        return t;
    }

    fn final(t: Txn, err: ?anyerror) anyerror!bool {
        defer release();

        const e = blk: {
            if (err) |e| {
                c.mdb_txn_abort(t.t);
                break :blk e;
            } else {
                if (rcErr(c.mdb_txn_commit(t.t))) return true
                else |e| break :blk e;
            }
        };

        switch (e) {
            error.MdbMapResized => {
                std.log.debug("LMDB map has been resized by an external process", .{});
                try resize(0);
                return false;
            },
            error.MdbMapFull => {
                var nfo: c.MDB_envinfo = undefined;
                _ = c.mdb_env_info(db_env, &nfo);
                const new = nfo.me_mapsize + nfo.me_mapsize/2;
                std.log.debug("LMDB map resized from {} to {}", .{nfo.me_mapsize, new});
                try resize(new);
                return false;
            },
            else => return e
        }
    }
};


pub const Txn = struct {
    t: ?*c.MDB_txn = null,

    fn put(t: Txn, key: []const u8, val: []const u8) !void {
        return rcErr(c.mdb_put(t.t, db_dbi, &toVal(key), &toVal(val), 0));
    }

    fn get(t: Txn, key: []const u8) !?[]const u8 {
        var val: c.MDB_val = undefined;
        const rc = c.mdb_get(t.t, db_dbi, &toVal(key), &val);
        if (rc == c.MDB_NOTFOUND) return null;
        try rcErr(rc);
        return fromVal(val);
    }

    fn del(t: Txn, key: []const u8) !bool {
        const rc = c.mdb_del(t.t, db_dbi, &toVal(key), null);
        if (rc == c.MDB_NOTFOUND) return false;
        try rcErr(rc);
        return true;
    }

    const Entry = struct { key: []const u8, value: []const u8 };

    // Open the cursor at the given key if it's null, otherwise get the next item.
    fn curs_next(t: Txn, cursor: *?*c.MDB_cursor, key_slice: []const u8) !?Entry {
        var key: c.MDB_val = toVal(key_slice);
        var value: c.MDB_val = undefined;
        const op: c_uint = if (cursor.* == null) blk: {
            try rcErr(c.mdb_cursor_open(t.t, db_dbi, cursor));
            break :blk c.MDB_SET_RANGE;
        } else c.MDB_NEXT;

        const rc = c.mdb_cursor_get(cursor.*, &key, &value, op);
        if (rc == c.MDB_NOTFOUND) return null;
        try rcErr(rc);
        return Entry{ .key = fromVal(key), .value = fromVal(value) };
    }

    // Beware: Identifiers given out in a transaction that has not been
    // (successfully) committed may be re-used later on.
    pub fn nextSequence(t: Txn) !u64 {
        const v = if (try t.get(&.{0,1})) |v| 1+@bitCast(u64, v[0..8].*) else 1;
        try t.put(&.{0,1}, std.mem.asBytes(&v));
        return v;
    }

    pub fn getDirList(t: Txn) !?[32]u8 {
        return if (try t.get(&[_]u8{0,2})) |v| v[0..32].* else null;
    }

    pub fn getHashList(t: Txn) !?[32]u8 {
        return if (try t.get(&[_]u8{0,3})) |v| v[0..32].* else null;
    }

    pub fn setDirList(t: Txn, v: [32]u8) !void {
        try t.put(&[_]u8{0,2}, &v);
    }

    pub fn setHashList(t: Txn, v: [32]u8, num: u64) !void {
        if (num > 0) {
            try t.put(&[_]u8{0,3}, &v);
            try t.put(&[_]u8{0,4}, std.mem.asBytes(&num));
        } else {
            _ = try t.del(&[_]u8{0,3});
            _ = try t.del(&[_]u8{0,4});
        }
    }

    pub fn dirIter(t: Txn, id: u64) DirIter {
        return DirIter{ .t = t, .id = id };
    }

    // Iterate through all file paths that have the given blake3 hash. Order is semi-random.
    pub fn hashPathIter(t: Txn, b3: [32]u8) HashPathIter {
        var it = HashPathIter{ .t = t };
        it.prefix[0] = 4;
        it.prefix[1..33].* = b3;
        return it;
    }

    // Iterate through the blake3 hashes of all public files, ordered by hash.
    pub fn hashIter(t: Txn) HashIter {
        return HashIter{ .t = t };
    }
};


fn dirKey(out: []u8, id: u64, name: []const u8) []const u8 {
    std.debug.assert(out.len >= 9 + name.len);
    out[0] = 1;
    std.mem.copy(u8, out[1..9], std.mem.asBytes(&id));
    std.mem.copy(u8, out[9..], name);
    return out[0..9+name.len];
}

pub const DirEntry = struct {
    key: []const u8,
    value: []const u8,

    pub const Unhashed = packed struct { lastmod: u64, size: u64 };
    pub const Hashed = packed struct { lastmod: u64, size: u64, b3: [32]u8 };
    pub const Dir = u64;
    pub const Value = union(enum) {
        unhashed: *const Unhashed,
        hashed: *const Hashed,
        dir: *const align(1) Dir,

        fn bytes(self: Value) []const u8 {
            return switch (self) {
                .unhashed => |p| std.mem.asBytes(p),
                .hashed => |p| std.mem.asBytes(p),
                .dir => |p| std.mem.asBytes(p),
            };
        }
    };

    pub fn id(s: @This()) u64 {
        return @bitCast(u64, s.key[1..9].*);
    }

    pub fn name(s: @This()) []const u8 {
        return s.key[9..];
    }

    pub fn val(s: @This()) Value {
        return switch (s.value.len) {
            @sizeOf(Unhashed) => .{ .unhashed = @ptrCast(*const Unhashed, s.value) },
            @sizeOf(Hashed)   => .{ .hashed   = @ptrCast(*const Hashed, s.value) },
            @sizeOf(Dir)      => .{ .dir      = @ptrCast(*const align(1) Dir, s.value) },
            else => unreachable,
        };
    }

};

pub const DirIter = struct {
    t: Txn,
    cursor: ?*c.MDB_cursor = null,
    id: u64,

    pub fn next(self: *DirIter) !?DirEntry {
        var buf: [9]u8 = undefined;
        const v = (try self.t.curs_next(&self.cursor, dirKey(&buf, self.id, ""))) orelse return null;
        const ent = DirEntry{ .key = v.key, .value = v.value };
        if (ent.id() != self.id) return null;
        return ent;
    }

    // Set the cursor such that the following call to next() will return the
    // entry with the given name, or the entry after that if there is no exact
    // match..
    pub fn skipTo(self: *DirIter, name: []const u8) !void {
        var buf: [500]u8 = undefined;
        var seek = dirKey(&buf, self.id, name);
        var key = toVal(seek);
        var value: c.MDB_val = undefined;

        if (self.cursor == null) try rcErr(c.mdb_cursor_open(self.t.t, db_dbi, &self.cursor));
        const rc = c.mdb_cursor_get(self.cursor, &key, &value, c.MDB_SET_RANGE);
        if (rc == c.MDB_NOTFOUND) return;
        try rcErr(rc);
        try rcErr(c.mdb_cursor_get(self.cursor, &key, &value, c.MDB_PREV));
    }

    pub fn reset(self: *DirIter) !void {
        return self.skipTo("");
    }

    // Add a new subdir, returns a newly allocated dir_id.
    // Cursor will be positioned on the new dir.
    pub fn addDir(self: *DirIter, name: []const u8) !u64 {
        const newid = try self.t.nextSequence();
        std.log.debug("Adding to dir#{}: {s} -> dir#{}", .{ self.id, name, newid });
        var buf: [500]u8 = undefined;
        try rcErr(c.mdb_cursor_put(
                self.cursor,
                &toVal(dirKey(&buf, self.id, name)),
                &toVal(std.mem.asBytes(&newid)),
                c.MDB_NOOVERWRITE));
        return newid;
    }

    // Add a new (unhashed) file.
    // Cursor will be positioned on the new file.
    pub fn addFile(self: *DirIter, name: []const u8, lastmod: u64, size: u64) !void {
        std.log.debug("Adding to dir#{}: {s}", .{ self.id, name });
        var buf: [500]u8 = undefined;
        try rcErr(c.mdb_cursor_put(
                self.cursor,
                &toVal(dirKey(&buf, self.id, name)),
                &toVal(std.mem.asBytes(&DirEntry.Unhashed{ .lastmod = lastmod, .size = size })),
                c.MDB_NOOVERWRITE));
    }

    // Delete the entry last returned by next().
    pub fn del(self: *DirIter, ent: DirEntry, parent: *util.Path) (MdbError||util.Path.Error)!void {
        try parent.push(ent.name());
        defer parent.pop();
        std.log.debug("Deleting from dir#{}: {}", .{ self.id, parent });

        switch (ent.val()) {
            .unhashed => hash_queue.reset(),
            .hashed => |f| {
                var buf: [41]u8 = undefined;
                buf[0] = 4;
                buf[1..33].* = f.b3;
                buf[33..41].* = blake3.hashPiece(0, parent.slice()).root()[0..8].*;
                if(!try self.t.del(buf[0..41])) unreachable;

                // Eagerly delete metadata associated with this blake3 hash if we don't have any other paths with the same hash.
                var it = self.t.hashPathIter(f.b3);
                defer it.deinit();
                if (null == try it.next()) {
                    buf[0] = 2;
                    _ = try self.t.del(buf[0..33]);
                    buf[0] = 3;
                    _ = try self.t.del(buf[0..33]);
                }
            },
            .dir => |dirid| {
                var it = self.t.dirIter(dirid.*);
                defer it.deinit();
                while (try it.next()) |e| try it.del(e, parent);
            },
        }
        try rcErr(c.mdb_cursor_del(self.cursor, 0));
    }

    pub fn deinit(self: *DirIter) void {
        c.mdb_cursor_close(self.cursor);
    }
};


pub const HashPathIter = struct {
    t: Txn,
    prefix: [33]u8 = undefined,
    cursor: ?*c.MDB_cursor = null,

    pub fn next(self: *HashPathIter) !?[]const u8 {
        const v = (try self.t.curs_next(&self.cursor, &self.prefix)) orelse return null;
        if (!std.mem.startsWith(u8, v.key, &self.prefix)) return null;
        return v.value;
    }

    pub fn deinit(self: *HashPathIter) void {
        c.mdb_cursor_close(self.cursor);
    }
};


pub const HashIter = struct {
    t: Txn,
    last: [32]u8 = [_]u8{0} ** 32,
    cursor: ?*c.MDB_cursor = null,

    pub fn next(self: *HashIter) !?[32]u8 {
        while (true) {
            const v = (try self.t.curs_next(&self.cursor, &[_]u8{4})) orelse return null;
            if (v.key.len < 33 or v.key[0] != 4) return null;
            if (std.mem.eql(u8, &self.last, v.key[1..33])) continue;
            self.last = v.key[1..33].*;
            return v.key[1..33].*;
        }
    }

    pub fn deinit(self: *HashIter) void {
        c.mdb_cursor_close(self.cursor);
    }
};


pub const hash_queue = struct {
    var total_size: u64 = 0;
    var total_files: u32 = 0;
    var cache = std.ArrayList(Entry).init(util.allocator);
    var last_path: ?[]u8 = null;
    var mutex = std.Thread.Mutex{}; // Protects the variables above. Functions in this struct will acquire the mutex on their own.

    const MAX_CACHE: usize = 100;

    pub const Entry = struct {
        dir_id: u64,
        size: u64,
        path: []u8,

        pub fn deinit(self: Entry) void {
            util.allocator.free(self.path);
        }
    };

    // Reset the in-memory hash queue, next() will return null until populate() is called again.
    pub fn reset() void {
        var lock = mutex.acquire();
        defer lock.release();
        total_size = 0;
        total_files = 0;
        for (cache.items) |e| e.deinit();
        if (last_path) |l| util.allocator.free(l);
        last_path = null;
        cache.clearAndFree();
    }

    // Each unique to-be-hashed path is only returned once in a single run.
    // Caller is given ownership of the returned Entry and must call deinit().
    pub fn next(t: Txn) !?Entry {
        var lock = mutex.acquire();
        defer lock.release();
        if (cache.items.len == 0 and last_path != null) try populateLocked(t);
        return cache.popOrNull();
    }

    pub fn store(t: Txn, e: Entry, b3: [32]u8, pieces: ?[]const u8) !void {
        // Could be a stray thread calling done() after this file has been
        // removed/modified, so make sure it still exists and we still want its
        // hash.
        var buf: [500]u8 = undefined;
        const key = dirKey(&buf, e.dir_id, std.fs.path.basenamePosix(e.path));
        const lastmod = switch ((DirEntry{ .key = key, .value = (try t.get(key)) orelse return }).val()) {
            .unhashed => |f| if (f.size == e.size) f.lastmod else return,
            else => return
        };
        const hashed = DirEntry.Hashed{ .lastmod = lastmod, .size = e.size, .b3 = b3 };
        try t.put(key, (DirEntry.Value{ .hashed = &hashed }).bytes());

        buf[0] = 2;
        buf[1..33].* = b3;
        if (pieces) |p| try t.put(buf[0..33], p);
        buf[0] = 4;
        buf[33..41].* = blake3.hashPiece(0, e.path).root()[0..8].*;
        try t.put(buf[0..41], e.path);

        // TODO: CBOR metadata

        var lock = mutex.acquire();
        defer lock.release();
        if (total_size > e.size) total_size -= e.size;
        if (total_files > 0) total_files -= 1;
    }

    fn populateRec(t: Txn, id: u64, path: *util.Path, last: []const u8) (MdbError || util.Path.Error)!void {
        var it = t.dirIter(id);
        defer it.deinit();
        if (last.len > 0) {
            try it.skipTo(util.pathHead(last));
            // If "last" refers to a file, make sure we grab the entry *after* that.
            if (util.pathTail(last).len == 0) _ = try it.next();
        }
        while (try it.next()) |e| {
            try path.push(e.name());
            defer path.pop();
            switch (e.val()) {
                .unhashed => |f| {
                    if (last_path == null) {
                        total_files += 1;
                        total_size = std.math.add(u64, total_size, f.size) catch total_size;
                    }
                    if (cache.items.len < MAX_CACHE)
                        try cache.append(Entry{ .dir_id = it.id, .size = f.size, .path = try util.allocator.dupe(u8, path.slice())});
                },
                .dir => |d| try populateRec(t, d.*, path,
                    if (std.mem.eql(u8, e.name(), util.pathHead(last))) util.pathTail(last) else ""),
                else => {}
            }
            if (cache.items.len >= MAX_CACHE and last_path != null) return;
        }
    }

    fn populateLocked(t: Txn) !void {
        var path = util.Path{};
        if (last_path == null) {
            total_files = 0;
            total_size = 0;
        }
        try populateRec(t, 0, &path, last_path orelse "");
        // The queue is processed starting from the last entry, so reversing
        // the queue makes it go in a nicer order.
        std.mem.reverse(Entry, cache.items);
        if (last_path) |l| util.allocator.free(l);
        last_path = if (cache.items.len == MAX_CACHE) try util.allocator.dupe(u8, cache.items[0].path) else null;
        std.log.debug("Hash queue length: {} files, {:.2}", .{ total_files, std.fmt.fmtIntSizeBin(total_size) });
    }

    // Has two modes:
    // - If last_path = null, will fill 'cache' and scan the entire database to get the total_* stats.
    // - Otherwise, will continue from last_path and only fill 'cache'.
    pub fn populate(t: Txn) !void {
        var lock = mutex.acquire();
        defer lock.release();
        return populateLocked(t);
    }
};


fn openDb(t: Txn) !void {
    try rcErr(c.mdb_dbi_open(t.t, null, c.MDB_CREATE, &db_dbi));

    if (try t.get(&.{0,0})) |version| {
        if (version.len < 2) return error.InvalidDatabase;
        if (version[0] > 1) return error.IncompatibleDatabaseVersion;
        // Minor version is currently ignored.
    } else {
        try t.put(&.{0,0}, &.{1,0});
    }
}

pub fn open() !void {
    var path = util.Path{};
    try path.push(config.store_dir);
    try path.push("db");
    try std.fs.cwd().makePath(path.slice());

    try rcErr(c.mdb_env_create(&db_env));
    errdefer c.mdb_env_close(db_env);

    // LMDB docs advise an "as large as possible" map size, but not all systems
    // handle sparse files well so that can result in a lot of wasted disk
    // space. Our approach is to instead handle dynamic map resizes, so we can
    // safely start with a small 32M.
    // (Docs say that the default is 10M but it really is 1M)
    try rcErr(c.mdb_env_set_mapsize(db_env, 32*1024*1024));

    // NOSYNC may cause database corruption on hard reset or OS-level crash.
    // NOMETASYNC is safer and should probably be used instead, but I *really*
    // don't like having to force-sync anything at all. We totally don't need
    // that kind of durability and it completely kills performance of small
    // transactions, which we need in order to handle dynamic map resizes.
    // Maybe add a config option for improved durability on unreliable devices?
    try rcErr(c.mdb_env_open(db_env, path.slice().ptr, c.MDB_NOSYNC, 0o600));

    return txn(.rw, openDb, .{});
}
