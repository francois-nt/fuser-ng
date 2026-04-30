//! FuserNG -- A higher-level FUSE (Filesystem in Userspace) interface and wrapper around the
//! low-level `fuser` library that makes implementing a filesystem a bit easier.
//!
//! FuserNG translates inodes to paths and simplifies some details of filesystem implementation,
//! for example: splitting the `setattr` call
//! into multiple separate operations, and simplifying the `readdir` call so that filesystems don't
//! need to deal with pagination.
//!
//! To implement a filesystem, implement the `Filesystem` trait. Not all functions in it need to
//! be implemented -- the default behavior is to return `ENOSYS` ("Function not implemented"). For
//! example, a read-only filesystem can skip implementing the `write` call and many others.

//
// Copyright (c) 2016-2022 by William R. Fraser
//

#![deny(rust_2018_idioms)]

#[macro_use]
extern crate log;

mod directory_cache;
mod fuserng;
mod inode_table;
mod types;

/// Crate version from Cargo package metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use crate::fuserng::*;
pub use crate::types::*;
pub use fuser::FileType;
pub use fuser::KernelConfig;
pub use fuser::MountOption;
// Forward to similarly-named fuser functions to work around deprecation for now.
// When these are removed, we'll have to either reimplement or break reverse compat.
// Keep the doc comments in sync with those in fuser.

use std::io;
use std::path::Path;

/// Number of fuser event-loop threads used to serve a mount.
/// This configures fuser session threading, not an internal worker pool.
#[derive(Debug, Default)]
pub enum ThreadCount {
    /// Use the current machine parallelism as reported by the standard library.
    #[default]
    Default,
    /// Use an explicit number of fuser event-loop threads.
    NumThreads(usize),
}

impl ThreadCount {
    fn value(&self) -> usize {
        match self {
            Self::Default => std::thread::available_parallelism().unwrap().into(),
            Self::NumThreads(num_threads) => *num_threads,
        }
    }
}

impl From<usize> for ThreadCount {
    fn from(value: usize) -> Self {
        ThreadCount::NumThreads(value)
    }
}
/// Mount the given filesystem to the given mountpoint. This function will not return until the
/// filesystem is unmounted.
#[inline(always)]
pub fn mount<FS: fuser::Filesystem, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[MountOption],
    num_threads: ThreadCount,
) -> io::Result<()> {
    let mut config = fuser::Config::default();
    config.mount_options = options.to_vec();
    config.n_threads = Some(num_threads.value());
    fuser::mount2(fs, mountpoint, &config)
}

/// Mount the given filesystem to the given mountpoint. This function spawns a background thread to
/// handle filesystem operations while being mounted and therefore returns immediately. The
/// returned handle should be stored to reference the mounted filesystem. If it's dropped, the
/// filesystem will be unmounted.
#[inline(always)]
pub fn spawn_mount<FS: fuser::Filesystem + Send + 'static, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[MountOption],
    num_threads: ThreadCount,
) -> io::Result<fuser::BackgroundSession> {
    let mut config = fuser::Config::default();
    config.mount_options = options.to_vec();
    config.n_threads = Some(num_threads.value());
    fuser::spawn_mount2(fs, mountpoint, &config)
}
