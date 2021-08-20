<!--
SPDX-FileCopyrightText: 2021 Kirjastonhoitaja <thekirjastonhoitaja@protonmail.com>
SPDX-License-Identifier: copyleft-next-0.3.1
-->

# Jakoi - An experimental file sharing application

Work in progress, don't use.

## Building

Requires Zig master (0.9.0-dev-something, as of writing).

Simply type `zig build`.

If you prefer to use your system libraries instead of statically compiling and
linking the source included in `deps/`, compile with:

```
zig build -Dsystem-lmdb=true
```
