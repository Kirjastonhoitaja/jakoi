// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");

pub const allocator = std.heap.c_allocator;

// Convenient non-allocating path construction helper.
// We simply don't support infinitely long paths.
pub const Path = struct {
    len: usize = 0,
    buf: [4096]u8 = undefined,

    pub const Error = error{NameTooLong};

    pub fn push(self: *Path, path: []const u8) Error!void {
        if (self.len + path.len + 1 > self.buf.len) return error.NameTooLong;
        if (self.len == 0) {
            std.mem.copy(u8, &self.buf, path);
            self.len = path.len;
            while (self.len > 1 and self.buf[self.len-1] == '/') self.len -= 1;
        } else {
            const trimmed = std.mem.trim(u8, path, "/");
            std.debug.assert(trimmed.len > 0);
            if (self.buf[self.len-1] != '/') {
                self.buf[self.len] = '/';
                self.len += 1;
            }
            std.mem.copy(u8, self.buf[self.len..], trimmed);
            self.len += trimmed.len;
        }
    }

    // Remove the last path component. Does not necessarily match with push(),
    // as that may add more than one component.
    pub fn pop(self: *Path) void {
        while (self.len > 0 and self.buf[self.len-1] == '/') self.len -= 1;
        while (self.len > 0 and self.buf[self.len-1] != '/') self.len -= 1;
        while (self.len > 1 and self.buf[self.len-1] == '/') self.len -= 1;
    }

    pub fn slice(self: *const Path) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn sliceZ(self: *Path) ![:0]u8 {
        if (self.len == self.buf.len) return error.NameTooLong;
        self.buf[self.len] = 0;
        return self.buf[0..(self.len+1):0];
    }

    pub fn format(self: *const Path, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;
        try writer.writeAll(self.slice());
    }
};


test "Path" {
    const eqs = std.testing.expectEqualStrings;
    var p = Path{};
    try eqs(p.slice(), "");
    try p.push("/abc");
    try eqs(p.slice(), "/abc");
    try p.push("foo/");
    try eqs(p.slice(), "/abc/foo");
    p.pop();
    try eqs(p.slice(), "/abc");
    p.pop();
    try eqs(p.slice(), "/");
    try p.push("//xyz");
    try eqs(p.slice(), "/xyz");
    p.pop();
    try eqs(p.slice(), "/");
    try p.push("def");
    try eqs(p.slice(), "/def");

    p = Path{};
    p.pop();
    try eqs(p.slice(), "");
    try p.push("bar/ty//");
    try eqs(p.slice(), "bar/ty");
    p.pop();
    try eqs(p.slice(), "bar");
    p.pop();
    try eqs(p.slice(), "");
    p.pop();
    try eqs(p.slice(), "");
    try p.push("");
    try eqs(p.slice(), "");
}


// Get the first component in a path.
pub fn pathHead(path: []const u8) []const u8 {
    var first = std.mem.trimLeft(u8, path, "/");
    var i: usize = 0;
    while (i < first.len and first[i] != '/') i += 1;
    return first[0..i];
}


test "pathHead" {
    const eqs = std.testing.expectEqualStrings;
    try eqs("", pathHead(""));
    try eqs("", pathHead("/"));
    try eqs("foo", pathHead("foo"));
    try eqs("foo", pathHead("//foo"));
    try eqs("foo", pathHead("//foo//bar"));
    try eqs("foo", pathHead("//foo/bar"));
    try eqs("foo", pathHead("/foo/bar"));
    try eqs("foo", pathHead("foo/bar"));
}


// Remove the first component from a path
pub fn pathTail(path: []const u8) []const u8 {
    var first = std.mem.trimLeft(u8, path, "/");
    var i: usize = 0;
    while (i < first.len and first[i] != '/') i += 1;
    return std.mem.trimLeft(u8, first[i..], "/");
}

test "pathTail" {
    const eqs = std.testing.expectEqualStrings;
    try eqs("", pathTail(""));
    try eqs("", pathTail("/"));
    try eqs("", pathTail("foo"));
    try eqs("", pathTail("foo/"));
    try eqs("bar", pathTail("foo/bar"));
    try eqs("bar/etc", pathTail("foo/bar/etc"));
    try eqs("bar", pathTail("/foo/bar"));
}
