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
    /// Called when the filesystem is mounted, before any other operation.
    ///
    /// * req: information about the FUSE request.
    /// * config: kernel configuration that may be adjusted before mounting.
    fn init(&self, req: RequestInfo, config: &mut KernelConfig) -> ResultEmpty {
        Ok(())
    }

    /// Called when the filesystem is unmounted.
    fn destroy(&self) {}

    /// Get the attributes of a filesystem entry.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the entry.
    /// * fh: a file handle if this is called on an open file.
    fn getattr(
        &self,
        req: RequestInfo,
        path: EntryRef,
        fh: Option<u64>,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Change the mode of a filesystem entry.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the entry.
    /// * fh: a file handle if this is called on an open file.
    /// * mode: the mode to change the file to.
    fn chmod(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        mode: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Change the owner UID and/or group GID of a filesystem entry.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the entry.
    /// * fh: a file handle if this is called on an open file.
    /// * uid: new user ID for the file owner. If None, leave the UID unchanged.
    /// * gid: new group ID for the file. If None, leave the GID unchanged.
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

    /// Set the length of a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: a file handle if this is called on an open file.
    /// * size: new file length in bytes.
    fn truncate(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: Option<u64>,
        size: u64,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Set the timestamps of a filesystem entry.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the entry.
    /// * fh: a file handle if this is called on an open file.
    /// * atime: new access time, or None to leave it unchanged.
    /// * mtime: new modification time, or None to leave it unchanged.
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

    /// Set the timestamps of a filesystem entry using the additional macOS options.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the entry.
    /// * fh: a file handle if this is called on an open file.
    /// * crtime: new creation time, or None to leave it unchanged.
    /// * chgtime: new metadata change time, or None to leave it unchanged.
    /// * bkuptime: new backup time, or None to leave it unchanged.
    /// * flags: new macOS file flags, or None to leave them unchanged.
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

    /// Read a symbolic link.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the symbolic link.
    fn readlink(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultData> + Send {
        enosys_future()
    }

    /// Create a special file.
    ///
    /// * req: information about the FUSE request.
    /// * entry: entry name resolved relative to its parent directory.
    /// * mode: mode for the new entry.
    /// * rdev: device number when mode contains S_IFCHR or S_IFBLK; ignored otherwise.
    fn mknod(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
        rdev: u32,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Create a directory.
    ///
    /// * req: information about the FUSE request.
    /// * entry: directory name resolved relative to its parent directory.
    /// * mode: permissions for the new directory.
    fn mkdir(
        &self,
        req: RequestInfo,
        entry: EntryName,
        mode: u32,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Remove a file.
    ///
    /// * req: information about the FUSE request.
    /// * entry: file name resolved relative to its parent directory.
    fn unlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Remove a directory.
    ///
    /// * req: information about the FUSE request.
    /// * entry: directory name resolved relative to its parent directory.
    fn rmdir(
        &self,
        req: RequestInfo,
        entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Create a symbolic link.
    ///
    /// * req: information about the FUSE request.
    /// * entry: symbolic link name resolved relative to its parent directory.
    /// * target: path (may be relative or absolute) to the target of the link.
    fn symlink(
        &self,
        req: RequestInfo,
        entry: EntryName,
        target: PathBuf,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Rename a filesystem entry.
    ///
    /// * req: information about the FUSE request.
    /// * entry: current entry name resolved relative to its parent directory.
    /// * new_entry: new entry name resolved relative to its parent directory.
    fn rename(
        &self,
        req: RequestInfo,
        entry: EntryName,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Create a hard link.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to an existing file.
    /// * new_entry: new link name resolved relative to its parent directory.
    fn link(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        new_entry: EntryName,
    ) -> impl Future<Output = ResultEntry> + Send {
        enosys_future()
    }

    /// Open a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * flags: one of O_RDONLY, O_WRONLY, or O_RDWR, plus any additional flags.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the file, and can be any value you choose, though it should allow
    /// your filesystem to identify the open file even without path information.
    fn open(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send {
        enosys_future()
    }

    /// Read from a file.
    ///
    /// Reading past the end of the file is not an error. Return only the available data up to the
    /// end of the file, which may be fewer bytes than requested or even zero bytes. Do not extend
    /// the file in this case.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: file handle returned from the open call.
    /// * offset: offset into the file to start reading.
    /// * size: number of bytes to read.
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

    /// Write to a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: file handle returned from the open call.
    /// * offset: offset into the file to start writing.
    /// * data: data to write.
    /// * flags: FUSE write flags.
    ///
    /// Return the number of bytes written.
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

    /// Called each time a program calls close on an open file.
    ///
    /// Note that because file descriptors can be duplicated (by dup, dup2, fork) this may be
    /// called multiple times for a given file handle. The main use of this function is if the
    /// filesystem would like to return an error to the close call. Note that most programs
    /// ignore the return value of close, though.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: file handle returned from the open call.
    /// * lock_owner: if the filesystem supports locking (setlk, getlk), remove all locks
    ///   belonging to this lock owner.
    fn flush(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        lock_owner: u64,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Called when an open file is closed.
    ///
    /// There will be one of these for each open call. After release, no more calls will be
    /// made with the given file handle.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: file handle returned from the open call.
    /// * flags: the flags passed when the file was opened.
    /// * lock_owner: if the filesystem supports locking (setlk, getlk), remove all locks
    ///   belonging to this lock owner.
    /// * flush: whether pending data must be flushed or not.
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

    /// Write out any pending changes to a file.
    ///
    /// When this returns, data should be written to persistent storage.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * fh: file handle returned from the open call.
    /// * datasync: if false, also write metadata; otherwise, write only file data.
    fn fsync(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Open a directory.
    ///
    /// Analogous to the opendir call.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the directory.
    /// * flags: file access flags. Will contain O_DIRECTORY at least.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the directory, and can be any value you choose, though it should
    /// allow your filesystem to identify the open directory even without path information.
    fn opendir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        flags: u32,
    ) -> impl Future<Output = ResultOpen> + Send {
        enosys_future()
    }

    /// Gets directory entries together with their attributes.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the directory.
    /// * fh: file handle returned from the opendir call.
    ///
    /// Results are produced in batches and retained by FuserNG until releasedir, allowing
    /// offsets previously returned to FUSE to be revisited.
    fn readdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
    ) -> impl Stream<Item = ResultReaddirBatch> + Send + 'static {
        Once(Some(Err(std::io::Error::from_raw_os_error(libc::ENOSYS))))
    }

    /// Gets directory entries without attributes for the legacy FUSE readdir operation.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the directory.
    /// * fh: file handle returned from the opendir call.
    ///
    /// Results are produced in batches and retained by FuserNG until releasedir, allowing
    /// offsets previously returned to FUSE to be revisited.
    #[cfg(feature = "legacy_readdir")]
    fn legacy_readdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
    ) -> impl Stream<Item = ResultLegacyReaddirBatch> + Send + 'static {
        Once(Some(Err(std::io::Error::from_raw_os_error(libc::ENOSYS))))
    }

    /// Close an open directory.
    ///
    /// This will be called exactly once for each opendir call.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the directory.
    /// * fh: file handle returned from the opendir call.
    /// * flags: the file access flags passed to the opendir call.
    fn releasedir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        flags: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Write out any pending changes to a directory.
    ///
    /// Analogous to the fsync call.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the directory.
    /// * fh: file handle returned from the opendir call.
    /// * datasync: if false, also write metadata; otherwise, write only directory data.
    fn fsyncdir(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Get filesystem statistics.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to a directory in the filesystem.
    ///
    /// See the Statfs struct for more details.
    fn statfs(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultStatfs> + Send {
        enosys_future()
    }

    /// Set an extended attribute on a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * name: attribute name.
    /// * value: the data to set the value to.
    /// * flags: can be either XATTR_CREATE or XATTR_REPLACE.
    /// * position: offset into the attribute value to write data.
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

    /// Get an extended attribute from a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * name: attribute name.
    /// * size: the maximum number of bytes to read.
    ///
    /// If size is 0, return Xattr::Size(n) where n is the size of the attribute data.
    /// Otherwise, return Xattr::Data(data) with the requested data.
    fn getxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send {
        enosys_future()
    }

    /// List extended attributes for a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * size: maximum number of bytes to return.
    ///
    /// If size is 0, return Xattr::Size(n) where n is the size required for the list of attribute
    /// names. Otherwise, return Xattr::Data(data) where data is all the null-terminated attribute
    /// names.
    fn listxattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        size: u32,
    ) -> impl Future<Output = ResultXattr> + Send {
        enosys_future()
    }

    /// Remove an extended attribute from a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * name: name of the attribute to remove.
    fn removexattr(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Check for access to a file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file.
    /// * mask: mode bits to check for access to.
    ///
    /// Return Ok(()) if all requested permissions are allowed, otherwise return Err(EACCES)
    /// or another appropriate error code (e.g. ENOENT if the file doesn't exist).
    fn access(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mask: u32,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// Create and open a new file.
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the new file.
    /// * mode: the mode to set on the new file.
    /// * flags: flags that would be passed to open.
    ///
    /// Return a CreatedEntry containing the new file's attributes and a file handle. See the
    /// documentation for open for more information about file handles.
    fn create(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
        mode: u32,
        flags: u32,
    ) -> impl Future<Output = ResultCreate> + Send {
        enosys_future()
    }

    /// macOS only: Rename the volume.
    ///
    /// * req: information about the FUSE request.
    /// * name: new name for the volume.
    #[cfg(target_os = "macos")]
    fn setvolname(
        &self,
        req: RequestInfo,
        name: OsString,
    ) -> impl Future<Output = ResultEmpty> + Send {
        enosys_future()
    }

    /// macOS only: Query extended times (bkuptime and crtime).
    ///
    /// * req: information about the FUSE request.
    /// * path: path to the file to get the times for.
    ///
    /// Return an XTimes struct containing the times, or an appropriate error.
    #[cfg(target_os = "macos")]
    fn getxtimes(
        &self,
        req: RequestInfo,
        path: ResolvedPath,
    ) -> impl Future<Output = ResultXTimes> + Send {
        enosys_future()
    }
}
