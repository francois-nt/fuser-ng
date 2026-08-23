# fuser-ng

`fuser-ng` is a higher-level, path-oriented FUSE filesystem library for Rust,
built on top of [`fuser`](https://github.com/cberner/fuser) 0.17.

It started as a fork of `fuse-mt`. The 0.7 series moved the crate to `fuser`
0.17, uses fuser's native threading instead of an internal thread pool, and
adds a new inode table that keeps descendant paths correct when a parent
directory is renamed. Version 0.8 refines the public path API for inode-aware
`getattr` and `create` callbacks and adds an optional asynchronous filesystem
interface.

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
* optionally adapts futures returned by `AsyncFilesystem` through a caller-owned
  Tokio runtime;
* adds broader unit and integration test coverage than the original `fuse-mt`
  codebase, including inode-table rename cases and passthrough FUSE operations.

## Path API

Filesystem methods receive path-oriented types instead of raw inode numbers:

* `EntryName` is a child name resolved relative to a parent directory. It is
  used for operations such as `mkdir`, `mknod`, `symlink`, `unlink`, and
  `rename`.
* `ResolvedPath` is an entry path with its inode attached. It is used when
  `FuserNG` already has an inode for the entry, including operations such as
  `open`, `read`, `write`, and `create`.
* `EntryRef` is used by `getattr`, which may run either while resolving a
  parent/name lookup or after an inode has already been resolved.

These path wrapper types implement `Clone`. Cloning them is cheap because the
stored path components are shared internally.

The inode table stores complete paths for directories and derives leaf paths
from their parent directories. This keeps descendants consistent after a
directory subtree is renamed.

## Usage

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
fuser-ng = "0.8"
```

Implement `fuser_ng::Filesystem`, then wrap it before mounting:

```rust,ignore
let options = [fuser_ng::MountOption::FSName("myfs".into())];

fuser_ng::mount(
    fuser_ng::FuserNG::new(filesystem),
    mountpoint,
    &options,
    fuser_ng::ThreadCount::Default,
)?;
```

## Asynchronous filesystems

Asynchronous support is opt-in. Enable the `async` feature and provide Tokio
with a multithreaded runtime:

```toml
[dependencies]
fuser-ng = { version = "0.8.2", features = ["async"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
```

Implement `fuser_ng::AsyncFilesystem`, then pass a cloned runtime handle to the
adapter:

```rust,ignore
let runtime = tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()?;
let options = [fuser_ng::MountOption::FSName("my-async-fs".into())];

fuser_ng::mount(
    fuser_ng::AsyncFuserNG::new(filesystem, runtime.handle().clone()),
    mountpoint,
    &options,
    fuser_ng::ThreadCount::Default,
)?;
```

The asynchronous trait receives owned path and data arguments and returns
`Send` futures. `init` and `destroy` remain synchronous because they are
lifecycle callbacks.

`AsyncFuserNG` only stores the Tokio `Handle`; it does not own or shut down the
runtime. The caller must keep the runtime alive while the filesystem is
mounted and decides how outstanding tasks are handled during shutdown.
`destroy` is forwarded directly to the target filesystem.

The same APIs are also available as `fuser_ng::asynchronous::Filesystem` and
`fuser_ng::asynchronous::FuserNG`.
