// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-License-Identifier: copyleft-next-0.3.1

const std = @import("std");
const util = @import("util.zig");

pub var hash_threads: usize = 4;

pub var min_piece_size: u64 = 1024*1024;

// Should obviously be configurable.
pub var store_dir = "test";

// TODO: Support multiple public dirs, mount-style.
pub var public_dir = "zig-cache";


pub fn virtualToFs(vpath: []const u8) !util.Path {
    var p = util.Path{};
    try p.push(public_dir);
    try p.push(vpath);
    return p;
}
