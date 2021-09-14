// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

// Dumb little utility to read dirlists.
// Primarily useful for testing and benchmarking.

const std = @import("std");
const cbor = @import("cbor.zig");
const util = @import("util.zig");

const Stream = std.io.FixedBufferStream([]const u8);

var quiet = false;

fn usage() noreturn {
    std.log.warn("Usage: jakoi-dirlist [-q] <file>", .{});
    std.process.exit(1);
}

const ListError = error{
    InvalidDirList,
    DuplicateKey,
    InvalidFileName,
    InvalidBlake3,
    MissingBlake3,
    MissingFileName,
    MissingSize,
} || cbor.ParseError;


const File = struct {
    namebuf: [256]u8,
    namelen: usize,
    size: u64,
    b3: [32]u8,

    fn read(val: cbor.Value, in: anytype) !File {
        var map = try val.readMap();
        var ent: File = undefined;
        var hasName = false;
        var hasSize = false;
        var hasB3 = false;
        var stack: [100]cbor.ValReader = undefined;

        while (try map.next(in)) |k_val| {
            var k = try k_val.readInt(i32);
            var v = (try map.next(in)).?;
            switch (k) {
                0 => {
                    if (hasName) return error.DuplicateKey;
                    hasName = true;
                    ent.namelen = try (try v.readStr(in)).read(&ent.namebuf);
                    if (ent.namelen == ent.namebuf.len or !util.isValidFileName(ent.namebuf[0..ent.namelen]))
                        return error.InvalidFileName;
                },
                1 => {
                    if (hasSize) return error.DuplicateKey;
                    hasSize = true;
                    ent.size = try v.readInt(u64);
                },
                2 => {
                    if (hasB3) return error.DuplicateKey;
                    hasB3 = true;
                    var str = try v.readStr(in);
                    if (32 != try str.read(&ent.b3)) return error.InvalidBlake3;
                    if (0  != try str.read(&ent.b3)) return error.InvalidBlake3;
                },
                else => try v.skip(&stack, in),
            }
        }

        if (!hasName) return error.MissingFileName;
        if (!hasB3) return error.MissingBlake3;
        if (!hasSize) return error.MissingSize;
        return ent;
    }

    fn name(self: *const File) []const u8 {
        return self.namebuf[0..self.namelen];
    }
};


fn list(indent: usize, stream: *Stream) (std.os.WriteError || ListError)!void {
    var out = std.io.getStdOut().writer();
    var dir = try (try cbor.Value.read(stream.reader())).readArray();

    // Skip over the dirnames, will be printed after the file listing.
    var dirnames_stream = stream.*;
    var stack: [1]cbor.ValReader = undefined;
    try ((try dir.next(stream.reader())) orelse return error.InvalidDirList).skip(&stack, stream.reader());

    // Print all file entries
    var files = try ((try dir.next(stream.reader())) orelse return error.InvalidDirList).readArray();
    while (try files.next(stream.reader())) |ent| {
        const e = try File.read(ent, stream.reader());
        if (!quiet) {
            try out.writeByteNTimes(' ', indent*2);
            try out.print("- {s} {:>10.1} {}\n", .{e.name(), std.fmt.fmtIntSizeBin(e.size), std.fmt.fmtSliceHexLower(&e.b3)});
        }
    }

    // Print all dirs, recursively
    var dirnames = try (try cbor.Value.read(dirnames_stream.reader())).readArray();
    var subdirs = try ((try dir.next(stream.reader())) orelse return error.InvalidDirList).readArray();
    var pos = stream.pos;
    while (try subdirs.next(stream.reader())) |_| {
        var nextname = (try dirnames.next(dirnames_stream.reader())) orelse return error.InvalidDirList;
        var name: [256]u8 = undefined;
        var namelen = try (try nextname.readStr(dirnames_stream.reader())).read(&name);
        if (namelen == name.len or !util.isValidFileName(name[0..namelen]))
            return error.InvalidDirList;
        if (!quiet) {
            try out.writeByteNTimes(' ', indent*2);
            try out.print("- {s}/\n", .{name[0..namelen]});
        }
        stream.seekTo(pos) catch unreachable; // un-consume the Value
        try list(indent+1, stream);
        pos = stream.pos;
    }
    if (null != try dir.next(stream.reader())) return error.InvalidDirList;
}


pub fn main() !void {
    var file: ?[]const u8 = null;
    var it = std.process.args();
    _ = it.skip();
    while (it.next(util.allocator)) |arge| {
        var arg = try arge;
        if (std.mem.eql(u8, arg, "-q")) quiet = true
        else if (file == null) {
            file = arg;
            continue;
        } else usage();
        util.allocator.free(arg);
    }

    var fd = if (file) |f| try std.fs.cwd().openFile(f, .{}) else usage();
    defer fd.close();
    var map = try util.mapFile(fd, 0, try fd.getEndPos());
    defer util.unmapFile(map);
    var stream = std.io.fixedBufferStream(map);
    list(0, &stream) catch |e| {
        std.log.err("Read error at byte offset {}: {}", .{ stream.pos, e });
        return;
    };
    if (stream.pos != map.len) return error.TrailingGarbage;
}
