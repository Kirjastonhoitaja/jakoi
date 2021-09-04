// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const config = @import("config.zig");
const util = @import("util.zig");
const blake3 = @import("blake3.zig");
const cbor = @import("cbor.zig");
const db = @import("db.zig");


const MetaFile = struct { b3: [32]u8, size: u64 };

fn hashAndRename(old: [:0]const u8, size: u64, fd: std.fs.File, parent: std.fs.Dir) !MetaFile {
    // TODO: Windows.
    // Arguably it would be nicer to do a streaming hash while writing the
    // contents, so we don't have to read back what we just wrote. But eh, my
    // blake3.zig can't do that. Could use the std implementation for that.
    const b3 = blk: {
        var map = try std.os.mmap(null, size, std.os.PROT_READ, std.os.MAP_PRIVATE, fd.handle, 0);
        defer std.os.munmap(map);
        break :blk blake3.hashPiece(0, map).root();
    };

    var hex: [65]u8 = undefined;
    try parent.renameZ(old, std.fmt.bufPrintZ(&hex, "{}", .{ std.fmt.fmtSliceHexLower(&b3) }) catch unreachable);
    return MetaFile{ .b3 = b3, .size = size };
}


// Dirlist format: CBOR, with the following structure:
//
//   directory    = [ subdir_names, files, subdirs ]
//   surdir_names = [ name, .. ]        -- names of subdirectories, ordered by strcmp()
//   files        = [ file, .. ]        -- file entries, ordered by strcmp(name)
//   subdirs      = [ directory, .. ]   -- contents of each subdirectory, same order as subdir_names
//   file         = { 0: name, 1: size, 2: b3 }
//   name         = utf-8 string
//   size         = file size as a positive integer
//   b3           = blake3 root hash encoded as a 32-length byte string
//
// One advantage of this dirlist format is that it can be used to synchronize
// either a single directory listing ('subdir_names' + 'files'), a recursive
// subdirectory ('directory') or the full repository (top-level 'directory').
// The first two require an additional lookup table to quickly get to the right
// byte offsets, but that should be fairly cheap.
//
// Another advantage is that, with said lookup table, this dirlist can be used
// directly as the backend store of a browser interface. Downside is that the
// order of listed files would have to be the same as they are in the dirlist
// (for large dirs, at least, because a sort would be too expensive or because
// the full list may not have been fetched yet). The current order is not super
// user friendly, but at least it does order dirs before files. Should the
// protocol define a more user-friendly order, and what would that look like?
// Case-insensitive file name order? We don't really want full unicode
// collation, as that is locale dependent and not stable between Unicode
// versions. But then you get into the whole MySQL utf8mb4_general_ci vs.
// _unicode_ci vs. _0900_ai_ci situation. :(
//
// This format is totally not final, it's just an initial attempt. Things to
// figure out:
// - Additional file metadata. Current idea: add a metadata hash to file
//   entries for which additional metadata is available. 128bits should provide
//   a good compromise between security and dirlist size (collisions are a
//   minor annoyance at most but not a critical attack).
// - Full repo dirlist synchronization is O(n) in the number of published
//   files, regardless of how many files/dirs have changed in between two syncs:
//   1. Fetch new dirlist (using rsync-like updates to minimize bandwidth)
//   2. Compare new dirlist with old one to find all changes (since the file
//      order is strictly defined, this can be done without maintaining any
//      lookup tables, but still requires an O(n) walk through the dirlist)
//   A full CAS approach of giving each dir its own listing and hash could
//   improve syncing performance if only few dirs have been changed, but comes
//   with a few downsides:
//   - Extra hash for each dir means more metadata and protocol overhead.
//   - Listings of individual dirs can be pretty large (like 1M files), so this
//     doesn't remove the need for an rsync-like transfer mechanism.
//   - Extra hash for each dir also significantly reduces rsync efficiency
//   - Requires more network round-trips.
//   So I'll be sticking with the current approach until I have benchmarks
//   showing that O(n) sync is really too slow.
fn writeDirList(t: db.Txn, obj_dir: std.fs.Dir) !MetaFile {
    var fd = try obj_dir.createFileZ(".tmp-dirlist", .{.read=true});
    defer fd.close();
    errdefer obj_dir.deleteFileZ(".tmp-dirlist") catch {};

    var buf = std.io.bufferedWriter(fd.writer());
    var cnt = std.io.countingWriter(buf.writer());
    var wr = cbor.writer(cnt.writer());
    try writeDirListDir(t, 0, &wr);
    try buf.flush();
    return hashAndRename(".tmp-dirlist", cnt.bytes_written, fd, obj_dir);
}


fn writeDirListDir(t: db.Txn, dir_id: u64, wr: anytype) (std.fs.File.WriteError || db.MdbError)!void {
    var it = t.dirIter(dir_id);
    defer it.deinit();
    try wr.writeArray(null);

    try wr.writeArray(null);
    while (try it.next()) |ent| {
        switch (ent.val()) {
            .dir => try wr.writeStr(ent.name()),
            else => {},
        }
    }
    try wr.writeBreak();

    try wr.writeArray(null);
    try it.reset();
    while (try it.next()) |ent| {
        switch (ent.val()) {
            .hashed => |h| {
                try wr.writeMap(3);
                try wr.writePos(0);
                try wr.writeStr(ent.name());
                try wr.writePos(1);
                try wr.writePos(h.size);
                try wr.writePos(2);
                try wr.writeBytes(&h.b3);
            },
            else => {},
        }
    }
    try wr.writeBreak();

    try wr.writeArray(null);
    try it.reset();
    while (try it.next()) |ent| {
        switch (ent.val()) {
            .dir => |id| try writeDirListDir(t, id.*, wr),
            else => {},
        }
    }
    try wr.writeBreak();

    try wr.writeBreak();
}


fn writeHashList(t: db.Txn, obj_dir: std.fs.Dir) !?MetaFile {
    var fd = try obj_dir.createFileZ(".tmp-hashlist", .{.read=true});
    defer fd.close();
    errdefer obj_dir.deleteFileZ(".tmp-hashlist") catch {};

    var buf = std.io.bufferedWriter(fd.writer());
    var cnt = std.io.countingWriter(buf.writer());
    var wr = cnt.writer();
    var it = t.hashIter();
    defer it.deinit();
    while (try it.next()) |h| try wr.writeAll(&h);
    try buf.flush();
    if (cnt.bytes_written == 0) {
        obj_dir.deleteFileZ(".tmp-hashlist") catch {};
        return null;
    }
    return try hashAndRename(".tmp-hashlist", cnt.bytes_written, fd, obj_dir);
}


const OldFiles = struct { dirlist: ?[32]u8, hashlist: ?[32]u8 };

fn storeMetaFiles(t: db.Txn, dirlist: MetaFile, hashlist: ?MetaFile) !OldFiles {
    var old = OldFiles{
        .dirlist = try t.getDirList(),
        .hashlist = try t.getHashList(),
    };

    if (if (old.dirlist) |v| std.mem.eql(u8, &v, &dirlist.b3) else false) old.dirlist = null
    else try t.setDirList(dirlist.b3);

    if (hashlist) |h| {
        if (if (old.hashlist) |v| std.mem.eql(u8, &v, &h.b3) else false) old.hashlist = null
        else try t.setHashList(h.b3, h.size/32);
    } else try t.setHashList(undefined, 0);

    return old;
}

pub fn write() !void {
    var path = util.Path{};
    try path.push(config.store_dir);
    try path.push("obj");
    try std.fs.cwd().makePath(path.slice());
    var obj_dir = try std.fs.cwd().openDir(path.slice(), .{});
    defer obj_dir.close();

    const dirlist = try db.txn(.ro, writeDirList, .{ obj_dir });
    const hashlist = try db.txn(.ro, writeHashList, .{ obj_dir });

    const old = try db.txn(.rw, storeMetaFiles, .{ dirlist, hashlist });
    var hex: [65]u8 = undefined;
    if (old.dirlist)  |f| obj_dir.deleteFileZ(std.fmt.bufPrintZ(&hex, "{}", .{ std.fmt.fmtSliceHexLower(&f) }) catch unreachable) catch {};
    if (old.hashlist) |f| obj_dir.deleteFileZ(std.fmt.bufPrintZ(&hex, "{}", .{ std.fmt.fmtSliceHexLower(&f) }) catch unreachable) catch {};
}
