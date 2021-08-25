// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const util = @import("util.zig");
const scan = @import("scan.zig");
const blake3 = @import("blake3.zig");


pub fn main() anyerror!void {
    try db.open();
    try scan.scan();
    try scan.hash();
    std.log.info("All your codebase are belong to us.", .{});
}


test "" {
    _ = blake3;
    _ = util;
}
