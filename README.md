# fuser-ng

![Cargo Checks](https://github.com/francois-nt/fuser-ng/actions/workflows/cargo-test.yml/badge.svg)

`fuser-ng` is a higher-level FUSE filesystem library for Rust with a hybrid
path/inode API, built on top of [`fuser`](https://github.com/cberner/fuser) 0.18.

It started as a fork of `fuse-mt`. The 0.7 series moved the crate to `fuser`
0.17, uses fuser's native threading instead of an internal thread pool, and
adds a new inode table that keeps descendant paths correct when a parent
directory is renamed. Version 0.8 refines the public path API for inode-aware
`getattr` and `create` callbacks and adds an optional asynchronous filesystem
interface. Version 0.9 adds streaming directory reads.

## Overview

`fuser` exposes low-level FUSE kernel operations. `fuser-ng` wraps those
operations with an API that combines path resolution with access to inode
identities. It handles low-level replies and simplifies filesystem implementation.

The crate:

* resolves FUSE inodes to paths and exposes inode numbers to callbacks that
  receive a resolved entry;
* lets `Filesystem` methods return `std::io::Result` values instead of using
  fuser reply objects directly;
* provides default `ENOSYS` implementations for operations you do not support;
* streams directory entries in batches while handling FUSE pagination
  internally;
* uses fuser's threaded event loop, configurable with `ThreadCount`;
* optionally supports asynchronous filesystem implementations through Tokio;
* adds broader unit and integration test coverage than the original `fuse-mt`
  codebase, including inode-table rename cases and passthrough FUSE operations.

## Hybrid path/inode API

Filesystem methods receive entry types that carry paths and, when available,
the inode assigned by the adapter. Both synchronous and asynchronous callbacks
use these types:

* `EntryName` is a child name resolved relative to a parent directory, with no
  inode attached. It is used for operations such as `mkdir`, `mknod`, `symlink`,
  `unlink`, and `rename`.
* `ResolvedPath` combines an entry path with its inode, accessible through
  `ino()`. It is used for operations such as `open`, `read`, and `write`.
  `create` also receives this type after the adapter has selected an inode
  for the entry.
* `EntryRef` is used by `getattr`, which may run either while resolving a
  parent/name lookup or after an inode has already been resolved. Match
  `EntryRef::Resolved(path)` to access `path.ino()`; `EntryRef::Lookup` carries
  an `EntryName` without an inode.

Unlike the path-only callback arguments in
[`fuse-mt`](https://docs.rs/fuse_mt/0.6.4/fuse_mt/trait.FilesystemMT.html),
`ResolvedPath` exposes the adapter's inode number alongside the path.

These inode numbers are assigned by FuserNG for the mount. They are independent
of any underlying filesystem's inode numbers and can be reused after the
corresponding inode-table entry is freed.

These path wrapper types implement `Clone`. Cloning them is cheap because the
stored path components are shared internally.

The inode table stores complete paths for directories and derives leaf paths
from their parent directories. This keeps descendants consistent after a
directory subtree is renamed.

## Usage

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
fuser-ng = "0.9"
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
```

## Passthrough example

The workspace includes a passthrough filesystem that forwards operations to
another directory:

```sh
cargo run -p passthrufs -- <target> <mountpoint>
fusermount3 -u <mountpoint>
```

## Testing

```sh
cargo test --workspace
cargo clippy --workspace --tests
```

## Status

This crate is a work in progress. Version 0.9 introduces a streaming directory
API for synchronous and asynchronous filesystem implementations. Bug reports,
pull requests, and feedback are welcome.

## License

Licensed under either MIT or Apache-2.0.
