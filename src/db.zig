// LMDB key format:
//
// 00
//   u8: database major version (1)
//   u8: database minor version (0)
// 01
//   u64: top-level dir_id.
// 02
//   u64: last created dir_id.
// 03
//   u256: directory listing hash
// 04
//   u256: blake3 listing hash
// 05
//   u64: number of entries in the blake3 listing
//
// 1 + <u64: dir_id> + <string: name>
//   Directory entry.
//   For files:
//     u64: lastmod
//     u64: size
//     u256: blake3 hash (all zeros if not yet known)
//   For dirs:
//     u64: dir_id
//   (Yes, the difference between files and dirs is the size of the value)
//
// 2 + <u256: file_hash>
//   u64: size
//   rest: blake3 hash data
//
// 3 + <u256: file_hash>
//   CBOR-encoded file metadata.
//
// 4 + <u256: file_hash> + <u64: path_hash>
//   Value: path string
//   path_hash is the prefix of blake3(path)
//   For hash -> path lookups.
