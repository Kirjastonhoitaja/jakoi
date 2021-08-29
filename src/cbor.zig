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


test "Ints" {
    var buf: [64]u8 = undefined;
    var bufwr = std.io.fixedBufferStream(&buf);
    var wr = writer(bufwr.writer());

    const eqs = std.testing.expectEqualSlices;
    try wr.writeInt(0);
    try eqs(u8, bufwr.getWritten(), "\x00");
    bufwr.reset();

    try wr.writeInt(1);
    try eqs(u8, bufwr.getWritten(), "\x01");
    bufwr.reset();

    try wr.writeInt(0x18);
    try eqs(u8, bufwr.getWritten(), "\x18\x18");
    bufwr.reset();

    try wr.writeInt(0xff);
    try eqs(u8, bufwr.getWritten(), "\x18\xff");
    bufwr.reset();

    try wr.writeInt(0x7ff);
    try eqs(u8, bufwr.getWritten(), "\x19\x07\xff");
    bufwr.reset();

    try wr.writeInt(0xffff);
    try eqs(u8, bufwr.getWritten(), "\x19\xff\xff");
    bufwr.reset();

    try wr.writeInt(0x10000);
    try eqs(u8, bufwr.getWritten(), "\x1a\x00\x01\x00\x00");
    bufwr.reset();

    try wr.writeInt(0xfedc_ba98);
    try eqs(u8, bufwr.getWritten(), "\x1a\xfe\xdc\xba\x98");
    bufwr.reset();

    try wr.writeInt(std.math.maxInt(i64));
    try eqs(u8, bufwr.getWritten(), "\x1b\x7f\xff\xff\xff\xff\xff\xff\xff");
    bufwr.reset();

    try wr.writePos(std.math.maxInt(u64));
    try eqs(u8, bufwr.getWritten(), "\x1b\xff\xff\xff\xff\xff\xff\xff\xff");
    bufwr.reset();

    try wr.writeInt(-1);
    try eqs(u8, bufwr.getWritten(), "\x20");
    bufwr.reset();

    try wr.writeInt(-0x19);
    try eqs(u8, bufwr.getWritten(), "\x38\x18");
    bufwr.reset();

    try wr.writeInt(-500);
    try eqs(u8, bufwr.getWritten(), "\x39\x01\xf3");
    bufwr.reset();

    try wr.writeInt(-0xfedc_ba98);
    try eqs(u8, bufwr.getWritten(), "\x3a\xfe\xdc\xba\x97");
    bufwr.reset();

    try wr.writeInt(std.math.minInt(i64));
    try eqs(u8, bufwr.getWritten(), "\x3b\x7f\xff\xff\xff\xff\xff\xff\xff");
    bufwr.reset();
}
