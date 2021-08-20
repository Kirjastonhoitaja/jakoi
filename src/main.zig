// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const scan = @import("scan.zig");

pub const allocator = std.heap.c_allocator;

pub fn main() anyerror!void {
    try db.open("test");
    try scan.scan("zig-cache");

    std.log.info("All your codebase are belong to us.", .{});
}
