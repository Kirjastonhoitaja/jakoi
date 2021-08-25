// SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
// SPDX-FileCopyrightText: 2015-2021 Zig Contributors
// SPDX-License-Identifier: copyleft-next-0.3.1 and MIT
//
// This implementation combines some aspects of the Zig implementation in the
// standard library and the more optimized blake3.c from the official
// repository. It uses the optimized primitives from the C version.
//
// This module does not support keyed hashing or key derivation, but it does
// provide access to intermediate hash tree nodes for incremental verification.
//
// "The Zig standard library has a blake3 implementation, why not use that!?",
// you ask? Well, the Zig implementation is (currently) about 4 times slower
// than the optimized C primitives and it doesn't provide access to internals
// for manual tree construction. I will get rid of this abomination as soon
// as the Zig implementation can compete in performance.

const std = @import("std");
const c = @cImport(@cInclude("blake3_impl.h"));

const BLOCK_LEN: usize = 64;
pub const CHUNK_LEN: usize = 1024;

const CHUNK_START: u8 = 1 << 0;
const CHUNK_END: u8 = 1 << 1;
const PARENT: u8 = 1 << 2;
const ROOT: u8 = 1 << 3;


pub const Output = struct {
    input_chaining_value: [8]u32 align(16),
    block: [BLOCK_LEN]u8 align(16),
    block_len: u8,
    counter: u64,
    flags: u8,

    pub fn chainingValue(self: *Output) [32]u8 {
        c.blake3_compress_in_place(
            &self.input_chaining_value,
            &self.block,
            self.block_len,
            self.counter,
            self.flags
        );
        var out: [32]u8 = undefined;
        c.store_cv_words(&out, &self.input_chaining_value);
        return out;
    }

    fn rootOutputBytes(self: *const Output, output_slice: []u8) void {
        var output = output_slice;
        var output_block_counter: u64 = 0;
        var wide_buf: [64]u8 = undefined;
        while (output.len > 0) {
            c.blake3_compress_xof(
                &self.input_chaining_value,
                &self.block,
                self.block_len,
                output_block_counter,
                self.flags | ROOT,
                &wide_buf
            );
            const len: usize = std.math.min(output.len, 64);
            std.mem.copy(u8, output, wide_buf[0..len]);
            output = output[len..];
            output_block_counter += 1;
        }
    }

    pub fn root(self: *const Output) [32]u8 {
        var out: [32]u8 = undefined;
        self.rootOutputBytes(&out);
        return out;
    }
};

const ChunkState = struct {
    chaining_value: [8]u32 align(16),
    chunk_counter: u64,
    block: [BLOCK_LEN]u8 align(16) = [_]u8{0} ** BLOCK_LEN,
    block_len: u8 = 0,
    blocks_compressed: u8 = 0,
    flags: u8,

    fn init(key: [8]u32, chunk_counter: u64, flags: u8) ChunkState {
        return ChunkState{
            .chaining_value = key,
            .chunk_counter = chunk_counter,
            .flags = flags,
        };
    }

    fn len(self: *const ChunkState) usize {
        return BLOCK_LEN * @as(usize, self.blocks_compressed) + @as(usize, self.block_len);
    }

    fn fillBlockBuf(self: *ChunkState, input: []const u8) []const u8 {
        const want = BLOCK_LEN - self.block_len;
        const take = std.math.min(want, input.len);
        std.mem.copy(u8, self.block[self.block_len..][0..take], input[0..take]);
        self.block_len += @truncate(u8, take);
        return input[take..];
    }

    fn startFlag(self: *const ChunkState) u8 {
        return if (self.blocks_compressed == 0) CHUNK_START else 0;
    }

    fn update(self: *ChunkState, input_slice: []const u8) void {
        var input = input_slice;
        while (input.len > 0) {
            // If the block buffer is full, compress it and clear it. More
            // input is coming, so this compression is not CHUNK_END.
            if (self.block_len == BLOCK_LEN) {
                c.blake3_compress_in_place(
                    &self.chaining_value,
                    &self.block,
                    BLOCK_LEN,
                    self.chunk_counter,
                    self.flags | self.startFlag()
                );
                self.blocks_compressed += 1;
                self.block = [_]u8{0} ** BLOCK_LEN;
                self.block_len = 0;
            }

            // Copy input bytes into the block buffer.
            input = self.fillBlockBuf(input);
        }
    }

    fn output(self: *const ChunkState) Output {
        return Output{
            .input_chaining_value = self.chaining_value,
            .block = self.block,
            .block_len = self.block_len,
            .counter = self.chunk_counter,
            .flags = self.flags | self.startFlag() | CHUNK_END,
        };
    }
};


fn compressParents(left: [32]u8, right: [32]u8) Output {
    var out = Output{
        .input_chaining_value = c.IV,
        .block = undefined,
        .block_len = BLOCK_LEN,
        .counter = 0,
        .flags = PARENT,
    };
    out.block[0..32].* = left;
    out.block[32..64].* = right;
    return out;
}

// Wrapper around blake3_hash_many for contiguous inputs.
fn hashMany(comptime inputs: usize, comptime blocksperinput: usize,
            chunk_start: u64, chunk_counter: bool, flags: u8, start_flags: u8, end_flags: u8,
            in: *const [inputs*blocksperinput*BLOCK_LEN]u8) [inputs*32]u8
{
    var out: [inputs*32]u8 = undefined;
    var chunks: [inputs][*]const u8 = undefined;
    var i: usize = 0;
    while (i < inputs) : (i += 1) chunks[i] = @as([*]const u8, in) + i*blocksperinput*BLOCK_LEN;
    c.blake3_hash_many(&chunks, inputs, blocksperinput, &c.IV, chunk_start, chunk_counter, flags, start_flags, end_flags, &out);
    return out;
}


// Hashes a power-of-two aligned piece of a file and outputs its hash. Can
// output either a root hash if a whole file has been given, or a chaining
// value when given a power-of-two sized file piece (or the last piece).
//
// This function is utterly stupid, because I don't trust myself to implement a
// fully optimized streaming hasher like the one in blake3.c without
// introducing a shitton of bugs. The specializations for 16k and 4k chunks
// still allows this function to get within 99% of the performance of the
// official C and rust implementations, at least in my limited benchmarks. It's
// possible to do get even closer by specializing larger chunks, but that gets
// into diminishing returns territory.
pub fn hashPiece(chunk_start: u64, input: []const u8) Output {
    if (c.MAX_SIMD_DEGREE >= 16 and input.len == 16*CHUNK_LEN) {
        const l3 = hashMany(16, CHUNK_LEN/BLOCK_LEN, chunk_start, true, 0, CHUNK_START, CHUNK_END, input[0..16*CHUNK_LEN]);
        const l2 = hashMany(8, 1, 0, false, PARENT, 0, 0, &l3);
        const l1 = hashMany(4, 1, 0, false, PARENT, 0, 0, &l2);
        const l0 = hashMany(2, 1, 0, false, PARENT, 0, 0, &l1);
        return compressParents(l0[0..32].*, l0[32..64].*);

    } else if (c.MAX_SIMD_DEGREE >= 4 and input.len == 4*CHUNK_LEN) {
        const l1 = hashMany(4, CHUNK_LEN/BLOCK_LEN, chunk_start, true, 0, CHUNK_START, CHUNK_END, input[0..4*CHUNK_LEN]);
        const l0 = hashMany(2, 1, 0, false, PARENT, 0, 0, &l1);
        return compressParents(l0[0..32].*, l0[32..64].*);

    } else if (input.len <= CHUNK_LEN) {
        var chunk = ChunkState.init(c.IV, chunk_start, 0);
        chunk.update(input);
        return chunk.output();

    } else {
        const left = std.math.ceilPowerOfTwo(usize, std.math.divCeil(usize, input.len, 2) catch unreachable) catch unreachable;
        return compressParents(
            hashPiece(chunk_start, input[0..left]).chainingValue(),
            hashPiece(chunk_start+left/1024, input[left..]).chainingValue()
        );
    }
}


// Similar to hashPiece(), except it takes a list of chaining values and
// compresses them up to a single parent.
pub fn mergePieces(input: []const u8) Output {
    std.debug.assert(input.len >= 64 and input.len % 32 == 0);
    const left = std.math.ceilPowerOfTwo(usize, std.math.divCeil(usize, input.len, 2) catch unreachable) catch unreachable;
    return compressParents(
        if (left == 32) input[0..32].* else mergePieces(input[0..left]).chainingValue(),
        if (input.len - left == 32) input[left..][0..32].* else mergePieces(input[left..]).chainingValue()
    );
}


test "BLAKE3" {
    inline for (.{
        .{ .len =      0, .hash = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262" },
        .{ .len =      1, .hash = "2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213" },
        .{ .len =   1023, .hash = "10108970eeda3eb932baac1428c7a2163b0e924c9a9e25b35bba72b28f70bd11" },
        .{ .len =   1024, .hash = "42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7" },
        .{ .len =   1025, .hash = "d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444" },
        .{ .len =   2048, .hash = "e776b6028c7cd22a4d0ba182a8bf62205d2ef576467e838ed6f2529b85fba24a" },
        .{ .len =   2049, .hash = "5f4d72f40d7a5f82b15ca2b2e44b1de3c2ef86c426c95c1af0b6879522563030" },
        .{ .len =   3072, .hash = "b98cb0ff3623be03326b373de6b9095218513e64f1ee2edd2525c7ad1e5cffd2" },
        .{ .len =   3073, .hash = "7124b49501012f81cc7f11ca069ec9226cecb8a2c850cfe644e327d22d3e1cd3" },
        .{ .len =   4096, .hash = "015094013f57a5277b59d8475c0501042c0b642e531b0a1c8f58d2163229e969" },
        .{ .len =   4097, .hash = "9b4052b38f1c5fc8b1f9ff7ac7b27cd242487b3d890d15c96a1c25b8aa0fb995" },
        .{ .len =   5120, .hash = "9cadc15fed8b5d854562b26a9536d9707cadeda9b143978f319ab34230535833" },
        .{ .len =   5121, .hash = "628bd2cb2004694adaab7bbd778a25df25c47b9d4155a55f8fbd79f2fe154cff" },
        .{ .len =   6144, .hash = "3e2e5b74e048f3add6d21faab3f83aa44d3b2278afb83b80b3c35164ebeca205" },
        .{ .len =   6145, .hash = "f1323a8631446cc50536a9f705ee5cb619424d46887f3c376c695b70e0f0507f" },
        .{ .len =   7168, .hash = "61da957ec2499a95d6b8023e2b0e604ec7f6b50e80a9678b89d2628e99ada77a" },
        .{ .len =   7169, .hash = "a003fc7a51754a9b3c7fae0367ab3d782dccf28855a03d435f8cfe74605e7817" },
        .{ .len =   8192, .hash = "aae792484c8efe4f19e2ca7d371d8c467ffb10748d8a5a1ae579948f718a2a63" },
        .{ .len =   8193, .hash = "bab6c09cb8ce8cf459261398d2e7aef35700bf488116ceb94a36d0f5f1b7bc3b" },
        .{ .len =  16384, .hash = "f875d6646de28985646f34ee13be9a576fd515f76b5b0a26bb324735041ddde4" },
        .{ .len =  31744, .hash = "62b6960e1a44bcc1eb1a611a8d6235b6b4b78f32e7abc4fb4c6cdcce94895c47" },
        .{ .len = 102400, .hash = "bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085" },
    }) |t| {
        var input: [t.len]u8 = undefined;
        for (input) |*e, i| e.* = @truncate(u8, i % 251);

        var exp: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&exp, t.hash) catch unreachable;

        // Single-pass
        const root = hashPiece(0, &input).root();
        try std.testing.expectEqual(root, exp);

        // Multi-pass, hashing each chunk individually and then using mergePieces()
        if (t.len > 1024) {
            const chunknum = comptime std.math.divCeil(usize, t.len, 1024) catch unreachable;
            var chunks: [chunknum*32]u8 = undefined;
            var i: usize = 0;
            while (i < chunknum) : (i += 1) chunks[i*32..][0..32].* = hashPiece(i, input[i*1024..std.math.min(t.len, (i+1)*1024)]).chainingValue();
            const root2 = mergePieces(&chunks).root();
            try std.testing.expectEqual(root2, exp);
        }
    }
}
