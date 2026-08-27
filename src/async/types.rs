//! Asynchronous filesystem interface and its FUSE adapter.

use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;

use futures_core::Stream;

use crate::{
    EntryName, EntryRef, KernelConfig, RequestInfo, ResolvedPath, ResultCreate, ResultData,
    ResultEmpty, ResultEntry, ResultOpen, ResultReaddirBatch, ResultStatfs, ResultWrite,
    ResultXattr,
};

#[cfg(feature = "legacy_readdir")]
use crate::ResultLegacyReaddirBatch;

#[cfg(target_os = "macos")]
use crate::ResultXTimes;

/// A stream that yields one value.
struct Once<T>(Option<T>);

impl<T: Unpin> Stream for Once<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take())
    }
}

/// Returns an immediately ready unsupported-operation result.
fn enosys_future<T>() -> std::future::Ready<std::io::Result<T>> {
    std::future::ready(Err(std::io::Error::from_raw_os_error(libc::ENOSYS)))
}

/// Filesystem operations that may complete asynchronously.
///
/// Operation arguments are owned so their futures can outlive the FUSE request callback.
#[allow(unused_variables)]
pub trait AsyncFilesystem: Send + Sync + 'static {
    /// Configures the FUSE connection before requests are dispatched.
    fn init(&self, req: RequestInfo, config: &mut KernelConfig) -> ResultEmpty {
        Ok(())
    }

    /// Cleans up the filesystem during unmount.
    fn destroy(&self) {}

    /// Gets the attributes of a filesystem entry.
    fn getattr(
        &self,
        req: RequestInfo,
        path: EntryRef,
        fh: Option<u64>,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Changes the mode of a filesystem entry.
    fn chmod(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        mode: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Changes the owner UID and/or group GID of a filesystem entry.
    fn chown(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Sets the length of a file.
    fn truncate(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        size: u64,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Sets the access and modification timestamps of an entry.
    fn utimens(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Sets the macOS-specific timestamps and flags of an entry.
    #[allow(clippy::too_many_arguments)]
    fn utimens_macos(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Reads a symbolic link.
    fn readlink(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultData> + Send {
        enosys_future()
    }

    /// Creates a special file.
    fn mknod(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
        rdev: u32,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Creates a directory.
    fn mkdir(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Removes a file.
    fn unlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Removes a directory.
    fn rmdir(
        &self,
        req: RequestInfo,
        entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Creates a symbolic link.
    fn symlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
        target: PathBuf,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Renames a filesystem entry.
    fn rename(
        &self,
        req: RequestInfo,
        entry: EntryName,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Creates a hard link.
    fn link(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Opens a file.
    fn open(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send {
        enosys_future()
    }

    /// Reads data from a file.
    fn read(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = ResultData> + Send {
        enosys_future()
    }

    /// Writes data to a file.
    fn write(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        flags: u32,
    ) -> impl Future<Output = ResultWrite> + Send {
        enosys_future()
    }

    /// Flushes pending data for an open file.
    fn flush(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        lock_owner: u64,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Releases an open file.
    #[allow(clippy::too_many_arguments)]
    fn release(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Synchronizes an open file with its backing storage.
    fn fsync(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Opens a directory.
    fn opendir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send {
        enosys_future()
    }

    /// Gets directory entries and attributes as an asynchronous stream of batches.
    fn readdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
    ) -> impl Stream<Item = ResultReaddirBatch> + Send + 'static {
        Once(Some(Err(std::io::Error::from_raw_os_error(libc::ENOSYS))))
    }

    /// Gets directory entries without attributes for the legacy FUSE readdir operation.
    #[cfg(feature = "legacy_readdir")]
    fn legacy_readdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
    ) -> impl Stream<Item = ResultLegacyReaddirBatch> + Send + 'static {
        Once(Some(Err(std::io::Error::from_raw_os_error(libc::ENOSYS))))
    }

    /// Releases an open directory.
    fn releasedir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        flags: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Synchronizes an open directory with its backing storage.
    fn fsyncdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Gets filesystem statistics.
    fn statfs(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultStatfs> + Send {
        enosys_future()
    }

    /// Sets an extended attribute.
    #[allow(clippy::too_many_arguments)]
    fn setxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
        value: Vec<u8>,
        flags: u32,
        position: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Gets an extended attribute.
    fn getxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send {
        enosys_future()
    }

    /// Lists the extended attributes of an entry.
    fn listxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send {
        enosys_future()
    }

    /// Removes an extended attribute.
    fn removexattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Checks whether an entry permits the requested access.
    fn access(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mask: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Creates and opens a file.
    fn create(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mode: u32,
        flags: u32,
    ) -> impl Future<Output = ResultCreate> + Send {
        enosys_future()
    }

    /// Renames the volume on macOS.
    #[cfg(target_os = "macos")]
    fn setvolname(
        &self,
        req: RequestInfo,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Gets the extended timestamps of an entry on macOS.
    #[cfg(target_os = "macos")]
    fn getxtimes(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultXTimes> + Send {
        enosys_future()
    }
}
