// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");

pub fn Writer(comptime Out: anytype) type {
    return struct {
        out: Out,

        const Self = @This();

        fn writeNum(self: *Self, major: u8, v: u64) !void {
            if (v <= 0x17) return self.out.writeByte(major | @intCast(u8, v));
            if (v <= 0xff) {
                try self.out.writeByte(major | 0x18);
                return self.out.writeByte(@intCast(u8, v));
            }
            if (v <= 0xffff) {
                try self.out.writeByte(major | 0x19);
                return self.out.writeIntBig(u16, @intCast(u16, v));
            }
            if (v <= 0xffff_ffff) {
                try self.out.writeByte(major | 0x1a);
                return self.out.writeIntBig(u32, @intCast(u32, v));
            }
            try self.out.writeByte(major | 0x1b);
            return self.out.writeIntBig(u64, v);
        }

        pub fn writePos(self: *Self, v: u64) !void {
            return self.writeNum(0, v);
        }

        pub fn writeInt(self: *Self, v: i64) !void {
            if (v >= 0) return self.writePos(@intCast(u64, v));
            return self.writeNum(0x20, @intCast(u64, -(v+1)));
        }

        pub fn writeBytes(self: *Self, str: []const u8) !void {
            try self.writeNum(0x40, str.len);
            return self.out.writeAll(str);
        }

        // str must be valid UTF-8
        pub fn writeStr(self: *Self, str: []const u8) !void {
            try self.writeNum(0x60, str.len);
            return self.out.writeAll(str);
        }

        // Must be followed 'len' array elements. If len is null, must be terminated by a writeBreak().
        pub fn writeArray(self: *Self, len: ?u64) !void {
            if (len) |l| try self.writeNum(0x80, l)
            else try self.out.writeByte(0x9f);
        }

        // Must be followed 'len' pairs. If len is null, must be terminated by a writeBreak().
        pub fn writeMap(self: *Self, len: ?u64) !void {
            if (len) |l| try self.writeNum(0xa0, l)
            else try self.out.writeByte(0x9f);
        }

        pub fn writeBool(self: *Self, v: bool) !void {
            try self.out.writeByte(0xf4 + @as(u8, @boolToInt(v)));
        }

        pub fn writeNull(self: *Self) !void {
            try self.out.writeByte(0xf6);
        }

        pub fn writeBreak(self: *Self) !void {
            try self.out.writeByte(0xff);
        }
    };
}

pub fn writer(out: anytype) Writer(@TypeOf(out)) {
    return .{ .out = out };
}


test "Writing Ints" {
    var buf: [64]u8 = undefined;
    var bufwr = std.io.fixedBufferStream(&buf);
    var wr = writer(bufwr.writer());

    const eqs = std.testing.expectEqualSlices;
    inline for(.{
        .{ .v = 0, .e = "\x00" },
        .{ .v = 1, .e = "\x01" },
        .{ .v = 0x18, .e = "\x18\x18" },
        .{ .v = 0xff, .e = "\x18\xff" },
        .{ .v = 0x7ff, .e = "\x19\x07\xff" },
        .{ .v = 0xffff, .e = "\x19\xff\xff" },
        .{ .v = 0x10000, .e = "\x1a\x00\x01\x00\x00" },
        .{ .v = 0xfedc_ba98, .e = "\x1a\xfe\xdc\xba\x98" },
        .{ .v = std.math.maxInt(i64), .e = "\x1b\x7f\xff\xff\xff\xff\xff\xff\xff" },
        .{ .v = -1, .e = "\x20" },
        .{ .v = -0x19, .e = "\x38\x18" },
        .{ .v = -500, .e = "\x39\x01\xf3" },
        .{ .v = -0xfedc_ba98, .e = "\x3a\xfe\xdc\xba\x97" },
        .{ .v = std.math.minInt(i64), .e = "\x3b\x7f\xff\xff\xff\xff\xff\xff\xff" },
    }) |t| {
        bufwr.reset();
        try wr.writeInt(t.v);
        try eqs(u8, bufwr.getWritten(), t.e);
    }

    bufwr.reset();
    try wr.writePos(std.math.maxInt(u64));
    try eqs(u8, bufwr.getWritten(), "\x1b\xff\xff\xff\xff\xff\xff\xff\xff");
}


pub const ParseError = error{Invalid, Unexpected, EndOfStream};


// Streaming recursive descent parser. Not the most convenient API, but
// flexible enough for both streaming and mmap() use.
//
// Error reporting isn't great.
pub const Value = struct {
    n: u64,
    mt: Major,
    ai: u5, // "additional information"

    pub const Major = enum(u3) {
        pos = 0,
        neg = 1,
        bytes = 2,
        str = 3,
        array = 4,
        map = 5,
        tag = 6,
        simple = 7,
    };

    // The 'break' code is considered an error.
    fn readVal(in: anytype, first: u8) (@TypeOf(in).Error || ParseError)!Value {
        var v = Value{
            .n = 0,
            .mt = @intToEnum(Major, first >> 5),
            .ai = @truncate(u5, first),
        };
        switch (v.ai) {
            0x00...0x17 => v.n = v.ai,
            0x18 => v.n = try in.readByte(),
            0x19 => v.n = try in.readIntBig(u16),
            0x1a => v.n = try in.readIntBig(u32),
            0x1b => v.n = try in.readIntBig(u64),
            0x1f => switch (v.mt) {
                .bytes, .str, .array, .map => {},
                else => return error.Invalid,
            },
            else => return error.Invalid,
        }
        return v;
    }

    pub fn read(in: anytype) (@TypeOf(in).Error || ParseError)!Value {
        while (true) {
            const v = try readVal(in, try in.readByte());
            if (v.mt == .tag) continue; // Ignore tags, for now
            return v;
        }
    }

    pub fn readInt(v: Value, comptime T: type) ParseError!T {
        switch (v.mt) {
            .pos => {
                if (v.n > std.math.maxInt(T)) return error.Unexpected;
                return @intCast(T, v.n);
            },
            .neg => {
                if (std.math.minInt(T) == 0) return error.Unexpected;
                if (v.n > std.math.maxInt(T)) return error.Unexpected; // Pretty safe assumption: ints are two's complement.
                return -@intCast(T, v.n) + (-1); // i1 can't represent '1', heh
            },
            else => return error.Unexpected,
        }
    }

    // Reads either a text or byte string, doesn't distinguish between the two.
    // Doesn't validate UTF-8, that would be nice to implement, but it's a bit
    // annoying as the reader API may break up UTF-8 sequences. :(
    pub fn readStr(v: Value, in: anytype) (@TypeOf(in).Error || ParseError)!StrReader(@TypeOf(in)) {
        if (v.mt != .str and v.mt != .bytes) return error.Unexpected;
        return StrReader(@TypeOf(in)){
            .in = in,
            .indef = v.ai == 0x1f,
            .rem = if (v.ai == 0x1f) 0 else v.n,
            .utf8 = v.mt == .str
        };
    }

    pub fn readArray(v: Value) ParseError!ValReader {
        if (v.mt != .array) return error.Unexpected;
        return ValReader{ .rem = if (v.ai == 0x1f) std.math.maxInt(u64) else v.n };
    }

    // Does not verify that map keys have a consistent or even a sane type.
    // Does not detect duplicate keys.
    // Pairs are returned in the order they are found in the CBOR stream.
    pub fn readMap(v: Value) ParseError!ValReader {
        if (v.mt != .map) return error.Unexpected;
        if (v.n >= std.math.maxInt(u64)/2) return error.Invalid;
        return ValReader{ .rem = if (v.ai == 0x1f) std.math.maxInt(u64) else v.n*2 };
    }

    // Consume but throw away the current Value.
    // TODO: Potentially faster version that takes a FixedBufferStream and can skip over strings?
    pub fn skip(v: Value, stack: []ValReader, in: anytype) (@TypeOf(in).Error || ParseError)!void {
        var first = true;
        var depth: usize = 0;
        while (blk: {
            while (depth > 0) {
                if (try stack[depth-1].next(in)) |val| break :blk @as(?Value, val);
                depth -= 1;
            }
            if (first) {
                first = false;
                break :blk @as(?Value, v);
            }
            break :blk @as(?Value, null);
        }) |val| switch (val.mt) {
            .str, .bytes => {
                var rd = try val.readStr(in);
                var buf: [256]u8 = undefined;
                while (0 != try rd.read(&buf)) {}
            },
            .map => {
                if (depth == stack.len) return error.Unexpected;
                stack[depth] = try val.readMap();
                depth += 1;
            },
            .array => {
                if (depth == stack.len) return error.Unexpected;
                stack[depth] = try val.readArray();
                depth += 1;
            },
            else => {},
        };
    }
};


pub fn StrReader(comptime In: type) type {
    return struct {
        in: In,
        indef: bool,
        utf8: bool,
        rem: u64,

        const Self = @This();
        pub const Error = In.Error || ParseError;
        pub const Reader = std.io.Reader(*Self, Error, read);

        pub fn read(self: *Self, buf: []u8) Error!usize {
            var written: usize = 0;
            while (buf.len > written) {
                if (self.rem > 0) {
                    const req = std.math.min(buf.len-written, self.rem);
                    const l = try self.in.read(buf[written..written+req]);
                    if (l == 0) return error.EndOfStream;
                    self.rem -= l;
                    written += l;
                    continue;
                }
                if (!self.indef) break;

                const first = try self.in.readByte();
                if (first == 0xff) {
                    self.indef = false;
                    break;
                }
                const v = try Value.readVal(self.in, first);
                if (v.mt != (if (self.utf8) Value.Major.str else .bytes) or v.ai == 0x1f) return error.Invalid;
                self.rem = v.n;
            }
            return written;
        }

        pub fn reader(self: *Self) Reader {
            return .{ .context = self };
        }
    };
}


// Reads an array or map.
pub const ValReader = struct {
    rem: u64, // maxint meaning 'indefinite'

    // The returned Value must be consumed in full before calling next() again.
    pub fn next(self: *@This(), in: anytype) (@TypeOf(in).Error || ParseError)!?Value {
        if (self.rem == std.math.maxInt(u64)) {
            var first = try in.readByte();
            if (first == 0xff) {
                self.rem = 0;
                return null;
            }
            while (true) {
                const v = try Value.readVal(in, first);
                if (v.mt == .tag) {
                    first = try in.readByte();
                    continue;
                }
                return v;
            }
        } else if (self.rem > 0) {
            self.rem -= 1;
            return try Value.read(in);
        }
        return null;
    }
};


test "Reading Ints" {
    const eq = std.testing.expectEqual;
    const rd = Value.read;
    const buf = std.io.fixedBufferStream;
    try eq(try (try rd(buf("\x00").reader())).readInt(u1), 0);
    try eq(try (try rd(buf("\x01").reader())).readInt(u1), 1);
    try eq(try (try rd(buf("\x18\x18").reader())).readInt(u8), 0x18);
    try eq(try (try rd(buf("\x18\xff").reader())).readInt(u8), 0xff);
    try eq(try (try rd(buf("\x19\x07\xff").reader())).readInt(u64), 0x7ff);
    try eq(try (try rd(buf("\x19\xff\xff").reader())).readInt(u64), 0xffff);
    try eq(try (try rd(buf("\x1a\x00\x01\x00\x00").reader())).readInt(u64), 0x10000);
    try eq(try (try rd(buf("\x1b\x7f\xff\xff\xff\xff\xff\xff\xff").reader())).readInt(i64), std.math.maxInt(i64));
    try eq(try (try rd(buf("\x1b\xff\xff\xff\xff\xff\xff\xff\xff").reader())).readInt(u64), std.math.maxInt(u64));
    try eq(try (try rd(buf("\x1b\xff\xff\xff\xff\xff\xff\xff\xff").reader())).readInt(i65), std.math.maxInt(u64));
    try eq(try (try rd(buf("\x20").reader())).readInt(i1), -1);
    try eq(try (try rd(buf("\x38\x18").reader())).readInt(i8), -0x19);
    try eq(try (try rd(buf("\x39\x01\xf3").reader())).readInt(i16), -500);
    try eq(try (try rd(buf("\x3a\xfe\xdc\xba\x97").reader())).readInt(i33), -0xfedc_ba98);
    try eq(try (try rd(buf("\x3b\x7f\xff\xff\xff\xff\xff\xff\xff").reader())).readInt(i64), std.math.minInt(i64));
    try eq(try (try rd(buf("\x3b\xff\xff\xff\xff\xff\xff\xff\xff").reader())).readInt(i65), std.math.minInt(i65));

    try eq((try rd(buf("\x02").reader())).readInt(u1), error.Unexpected);
    try eq((try rd(buf("\x22").reader())).readInt(i2), error.Unexpected);
    try eq((try rd(buf("\x40").reader())).readInt(u8), error.Unexpected);

    try eq(rd(buf("\x18").reader()), error.EndOfStream);
    try eq(rd(buf("\x1e").reader()), error.Invalid);
    try eq(rd(buf("\x1f").reader()), error.Invalid);
    try eq(rd(buf("\xff").reader()), error.Invalid);
}


test "Reading Strings" {
    const eq = std.testing.expectEqual;
    const eqs = std.testing.expectEqualStrings;
    const rd = Value.read;
    const buf = std.io.fixedBufferStream;
    var out: [128]u8 = undefined;

    inline for (.{
        .{ .in = "\x40", .out = "" },
        .{ .in = "\x5f\xff", .out = "" },
        .{ .in = "\x45\x00\x01\x02\x03\x04", .out = "\x00\x01\x02\x03\x04" },
        .{ .in = "\x61\x61", .out = "a" },
        .{ .in = "\x64\x49\x45\x54\x46", .out = "IETF" },
        .{ .in = "\x62\x22\x5c", .out = "\"\\" },
        .{ .in = "\x62\xc3\xbc", .out = "\u{00fc}" },
        .{ .in = "\x63\xe6\xb0\xb4", .out = "\u{6c34}" },
        //.{ .in = "\x64\xf0\x90\x85\x91", .out = "\u{d800}\u{dd51}" },  // looks like a bug in RFC8949
        .{ .in = "\x66\xed\xa0\x80\xed\xb5\x91", .out = "\u{d800}\u{dd51}" },
        .{ .in = "\x5f\x42\x01\x02\x43\x03\x04\x05\xff", .out = "\x01\x02\x03\x04\x05" },
        .{ .in = "\x7f\x65\x73\x74\x72\x65\x61\x64\x6d\x69\x6e\x67\xff", .out = "streaming" },
        .{ .in = "\x5f\x40\x41x\xff", .out = "x" }, // empty nested string
    }) |t| {
        var in = buf(t.in).reader();
        try eq((try (try rd(in)).readStr(in)).read(&out), t.out.len);
        try eqs(out[0..t.out.len], t.out);
    }

    var in = buf("\x00").reader();
    try eq((try rd(in)).readStr(in), error.Unexpected);

    in = buf("\x5f\x00\xff").reader(); // integer inside byte string
    try eq((try (try rd(in)).readStr(in)).read(&out), error.Invalid);

    in = buf("\x5f\x60\xff").reader(); // utf8 string inside byte string
    try eq((try (try rd(in)).readStr(in)).read(&out), error.Invalid);

    in = buf("\x5f\xc0\x40\xff").reader(); // tagged nested byte string
    try eq((try (try rd(in)).readStr(in)).read(&out), error.Invalid);

    in = buf("\x7f\x40\xff").reader(); // byte string inside utf8 string
    try eq((try (try rd(in)).readStr(in)).read(&out), error.Invalid);

    in = buf("\x7f\x7f\xff\xff").reader(); // nested indefinite-length string
    try eq((try (try rd(in)).readStr(in)).read(&out), error.Invalid);

    in = buf("\x7f").reader();
    try eq((try (try rd(in)).readStr(in)).read(&out), error.EndOfStream);

    in = buf("\x42a").reader();
    try eq((try (try rd(in)).readStr(in)).read(&out), error.EndOfStream);
}


test "Skip reader" {
    var stack: [3]ValReader = undefined;
    inline for (.{
        .{ .in = "\x00" },
        .{ .in = "\x40" },
        .{ .in = "\x41a" },
        .{ .in = "\x5f\xff" },
        .{ .in = "\x5f\x41a\xff" },
        .{ .in = "\x80" },
        .{ .in = "\x81\x00" },
        .{ .in = "\x9f\xff" },
        .{ .in = "\x9f\x9f\xff\xff" },
        .{ .in = "\x9f\x9f\x81\x00\xff\xff" },
        .{ .in = "\xa0" },
        .{ .in = "\xa1\x00\x01" },
        .{ .in = "\xbf\xff" },
        .{ .in = "\xbf\xc0\x00\x9f\xff\xff" },
    }) |t| {
        var buf = std.io.fixedBufferStream(t.in);
        const v = Value.read(buf.reader()) catch unreachable;
        v.skip(&stack, buf.reader()) catch unreachable;
        try std.testing.expectEqual(buf.pos, t.in.len);
    }

    // stack overflow
    var buf = std.io.fixedBufferStream("\x9f\x9f\x9f\x9f");
    const v = Value.read(buf.reader()) catch unreachable;
    try std.testing.expectEqual(v.skip(&stack, buf.reader()), error.Unexpected);
}
