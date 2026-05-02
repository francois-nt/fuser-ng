# fuser-ng

`fuser-ng` is a higher-level, path-oriented FUSE filesystem library for Rust,
built on top of [`fuser`](https://github.com/cberner/fuser) 0.17.

It started as a fork of `fuse-mt`. Version 0.7 updates the crate for `fuser`
0.17, uses fuser's native threading instead of an internal thread pool, and
adds a new inode table that keeps descendant paths correct when a parent
directory is renamed.

## Overview

`fuser` exposes low-level FUSE kernel operations. `fuser-ng` wraps those
operations with an API that is closer to the FUSE C API and simpler to
implement for path-based filesystems.

The crate:

* translates FUSE inodes into paths;
* lets `Filesystem` methods return `std::io::Result` values instead of using
  fuser reply objects directly;
* provides default `ENOSYS` implementations for operations you do not support;
* simplifies `readdir` by handling FUSE pagination internally;
* uses fuser's threaded event loop, configurable with `ThreadCount`;
* adds broader unit and integration test coverage than the original `fuse-mt`
  codebase, including inode-table rename cases and passthrough FUSE operations.

## Path API

Filesystem methods receive path-oriented types instead of raw inode numbers:

* `EntryName` is a child name resolved relative to a parent directory. It is
  used for operations such as `mkdir`, `create`, `unlink`, and `rename`.
* `ResolvedPath` is an existing entry path with its inode attached. It is used
  for operations such as `open`, `read`, `write`, and `getattr`.

The inode table stores complete paths for directories and derives leaf paths
from their parent directories. This keeps descendants consistent after a
directory subtree is renamed.

## Usage

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
fuser-ng = "0.7"
```

Implement `fuser_ng::Filesystem`, then wrap it before mounting:

```rust
let options = [fuser_ng::MountOption::FSName("myfs".into())];

fuser_ng::mount(
    fuser_ng::FuserNG::new(filesystem),
    mountpoint,
    &options,
    fuser_ng::ThreadCount::Default,
)?;
