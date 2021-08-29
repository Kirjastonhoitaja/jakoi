// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const db = @import("db.zig");
const scan = @import("scan.zig");
const repometa = @import("repometa.zig");


pub fn main() anyerror!void {
    // Set a strict umask to avoid accidentally leaking sensitive data to other
    // system users.
    if (std.builtin.os.tag != .windows)
        _ = @cImport(@cInclude("sys/stat.h")).umask(0o077);

    try db.open();
    try scan.scan();
    try scan.hash();
    try repometa.write();
    std.log.info("All your codebase are belong to us.", .{});
}


test "" {
    _ = @import("blake3.zig");
    _ = @import("util.zig");
    _ = @import("cbor.zig");
}
