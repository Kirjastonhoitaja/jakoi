// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const allocator = @import("util.zig").allocator;
const config = @import("config.zig");


// QuotedString formatter
const QS = struct {
    str: []const u8,

    pub fn format(self: QS, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) @TypeOf(writer).Error!void {
        _ = fmt;
        _ = options;
        try writer.writeByte('"');
        for (self.str) |c| switch (c) {
            '\\' => try writer.writeAll("\\\\"),
            '"' => try writer.writeAll("\\\""),
            else => try writer.writeByte(c),
        };
        try writer.writeByte('"');
    }
};

fn qs(str: []const u8) QS { return .{.str=str}; }


// TODO: Better error reporting
pub const Control = struct {
    // https://gitweb.torproject.org/torspec.git/tree/control-spec.txt
    // The Tor control protocol is quite a pain to work with, every message
    // requires its own parser. :(

    conn: std.net.Stream,
    wr: std.io.BufferedWriter(4096, std.net.Stream.Writer),
    rd: std.io.BufferedReader(4096, std.net.Stream.Reader),

    status: u10 = 1023,
    msgtype: u8 = 0,

    const Self = @This();

    // Write a single-line command
    fn writeCmd(self: *Self, comptime fmt: []const u8, args: anytype) !void {
        std.log.debug("Tor-control> " ++ fmt, args);
        try self.wr.writer().print(fmt ++ "\r\n", args);
        try self.wr.flush();
    }

    // Read a full response (possibly multiple lines) into a newly allocated buffer.
    fn readResponse(self: *Self) ![]u8 {
        var rd = self.rd.reader();
        var out = std.ArrayList(u8).init(allocator);
        while (true) {
            const line = try rd.readBytesNoEof(4);
            if (!std.ascii.isDigit(line[0]) or
                !std.ascii.isDigit(line[1]) or
                !std.ascii.isDigit(line[2])) return error.InvalidResponse;
            try out.appendSlice(&line);
            while (true) {
                try out.append(try rd.readByte());
                if (std.mem.endsWith(u8, out.items, "\r\n")) break;
            }
            if (line[3] == ' ') break; // EndReplyLine
            if (line[3] == '-') continue; // MidReplyLine
            if (line[3] != '+') return error.InvalidResponse;
            // DataReplyLine
            while (true) {
                try out.append(try rd.readByte());
                if (std.mem.endsWith(u8, out.items, "\r\n.\r\n")) break;
            }
        }
        std.log.debug("Tor-control< {s}", .{ std.mem.trim(u8, out.items, &std.ascii.spaces) });
        return out.toOwnedSlice();
    }

    // Start our onion service, creating a new key if we don't have one yet.
    pub fn setupOnion(self: *Self, http_port: u16, jakoi_port: u16) !void {
        var buf: [256]u8 = undefined;
        const key = config.store_dir.readFile("onion-key", &buf) catch |e| switch (e) {
            error.FileNotFound => "NEW:ED25519-V3",
            else => return e,
        };
        try self.writeCmd("ADD_ONION {s} Port=80,127.0.0.1:{} Port=2931,127.0.0.1:{}", .{
            std.mem.trim(u8, key, &std.ascii.spaces), http_port, jakoi_port });

        var msg_alloc = try self.readResponse();
        defer allocator.free(msg_alloc);
        var msg = msg_alloc;

        if (!std.mem.startsWith(u8, msg, "250-ServiceID=")) return error.UnknownResponse;
        msg = msg[14..];
        const hostend = std.mem.indexOfScalar(u8, msg, '\r') orelse return error.UnknownResponse;
        if (hostend > buf.len-10) return error.UnknownResponse;
        std.mem.copy(u8, &buf, msg[0..hostend]);
        std.mem.copy(u8, buf[hostend..hostend+7], ".onion\n");
        try config.store_dir.writeFile("onion-hostname", buf[0..hostend+7]);

        msg = msg[hostend+1..];
        if (std.mem.startsWith(u8, msg, "\n250-PrivateKey=")) {
            const keyend = std.mem.indexOfScalar(u8, msg, '\r') orelse return error.UnknownResponse;
            msg[keyend] = '\n';
            try config.store_dir.writeFile("onion-key", msg[16..keyend+1]);
        }
    }

    pub fn removeOnion(self: *Self) !void {
        var buf: [256]u8 = undefined;
        const addr = try config.store_dir.readFile("onion-hostname", &buf);
        try self.writeCmd("DEL_ONION {s}", .{ std.mem.sliceTo(addr, '.') });
        var msg = try self.readResponse();
        defer allocator.free(msg);
        if (std.mem.startsWith(u8, msg, "552")) return error.UnknownOnionService;
        if (!std.mem.startsWith(u8, msg, "250")) return error.UnexpectedResponse;
    }

    fn connect() !Control {
        const conn = try std.net.tcpConnectToAddress(config.tor_control_address);
        errdefer conn.close();
        var self = Control{
            .conn = conn,
            .wr = std.io.bufferedWriter(conn.writer()),
            .rd = std.io.bufferedReader(conn.reader()),
        };
        if (config.tor_control_password.len == 0)
            try self.writeCmd("AUTHENTICATE", .{})
        else
            try self.writeCmd("AUTHENTICATE {}", .{ qs(config.tor_control_password) });
        var msg = try self.readResponse();
        defer allocator.free(msg);
        if (std.mem.startsWith(u8, msg, "515")) return error.BadAuthentication;
        if (!std.mem.startsWith(u8, msg, "250")) return error.UnexpectedResponse;
        return self;
    }

    fn close(self: *Self) void {
        self.conn.close();
    }
};


// Return a lazily-initialzed control connection to our Tor instance.
// (Thread-safe, but the Control connection itself isn't, yet!)
pub fn control() !*Control {
    const c = struct {
        var conn: ?*Control = null;
        var mutex = std.Thread.Mutex{};
    };
    var l = c.mutex.acquire();
    defer l.release();
    if (c.conn) |r| return r;
    c.conn = try allocator.create(Control);
    c.conn.?.* = try Control.connect();
    return c.conn.?;
}
