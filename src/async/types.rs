//! Asynchronous filesystem interface and its FUSE adapter.

use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::{
    EntryName, EntryRef, KernelConfig, RequestInfo, ResolvedPath, ResultCreate, ResultData,
    ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultStatfs, ResultWrite, ResultXattr,
};

#[cfg(target_os = "macos")]
use crate::ResultXTimes;

/// Filesystem operations that may complete asynchronously.
///
/// Operation arguments are owned so their futures can outlive the FUSE request callback.
pub trait AsyncFilesystem: Send + Sync + 'static {
    /// Configures the FUSE connection before requests are dispatched.
    fn init(&self, req: RequestInfo, config: &mut KernelConfig) -> ResultEmpty;

    /// Cleans up the filesystem during unmount.
    fn destroy(&self);

    /// Gets the attributes of a filesystem entry.
    fn getattr(
        &self,
        req: RequestInfo,
        path: EntryRef,
        fh: Option<u64>,
    ) -> impl Future<Output = ResultEntry> + Send;

    /// Changes the mode of a filesystem entry.
    fn chmod(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        mode: u32,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Changes the owner UID and/or group GID of a filesystem entry.
    fn chown(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Sets the length of a file.
    fn truncate(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        size: u64,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Sets the access and modification timestamps of an entry.
    fn utimens(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> impl Future<Output = ResultEmpty> + Send;

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
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Reads a symbolic link.
    fn readlink(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultData> + Send;

    /// Creates a special file.
    fn mknod(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
        rdev: u32,
    ) -> impl Future<Output = ResultEntry> + Send;

    /// Creates a directory.
    fn mkdir(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
    ) -> impl Future<Output = ResultEntry> + Send;

    /// Removes a file.
    fn unlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Removes a directory.
    fn rmdir(&self, req: RequestInfo, entry: EntryName)
    -> impl Future<Output = ResultEmpty> + Send;

    /// Creates a symbolic link.
    fn symlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
        target: PathBuf,
    ) -> impl Future<Output = ResultEntry> + Send;

    /// Renames a filesystem entry.
    fn rename(
        &self,
        req: RequestInfo,
        entry: EntryName,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Creates a hard link.
    fn link(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEntry> + Send;

    /// Opens a file.
    fn open(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send;

    /// Reads data from a file.
    fn read(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = ResultData> + Send;

    /// Writes data to a file.
    fn write(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        flags: u32,
    ) -> impl Future<Output = ResultWrite> + Send;

    /// Flushes pending data for an open file.
    fn flush(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        lock_owner: u64,
    ) -> impl Future<Output = ResultEmpty> + Send;

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
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Synchronizes an open file with its backing storage.
    fn fsync(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Opens a directory.
    fn opendir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send;

    /// Gets all entries of a directory.
    fn readdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
    ) -> impl Future<Output = ResultReaddir> + Send;

    /// Releases an open directory.
    fn releasedir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        flags: u32,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Synchronizes an open directory with its backing storage.
    fn fsyncdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Gets filesystem statistics.
    fn statfs(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultStatfs> + Send;

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
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Gets an extended attribute.
    fn getxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send;

    /// Lists the extended attributes of an entry.
    fn listxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send;

    /// Removes an extended attribute.
    fn removexattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Checks whether an entry permits the requested access.
    fn access(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mask: u32,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Creates and opens a file.
    fn create(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mode: u32,
        flags: u32,
    ) -> impl Future<Output = ResultCreate> + Send;

    /// Renames the volume on macOS.
    #[cfg(target_os = "macos")]
    fn setvolname(
        &self,
        req: RequestInfo,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send;

    /// Gets the extended timestamps of an entry on macOS.
    #[cfg(target_os = "macos")]
    fn getxtimes(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultXTimes> + Send;
}
