// Public types exported by FuserNG.
//
// Copyright (c) 2016-2022 by William R. Fraser, 2026 by François NT
//

use crate::KernelConfig;
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub type Inode = u64;

/// Info about a request.
#[derive(Clone, Copy, Debug)]
pub struct RequestInfo {
    /// The unique ID assigned to this request by FUSE.
    pub unique: u64,
    /// The user ID of the process making the request.
    pub uid: u32,
    /// The group ID of the process making the request.
    pub gid: u32,
    /// The process ID of the process making the request.
    pub pid: u32,
}

/// A directory entry.
#[derive(Clone, Debug)]
pub struct DirectoryEntry {
    /// Name of the entry
    pub name: OsString,
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: crate::FileType,
}

/// Filesystem statistics.
#[derive(Clone, Copy, Debug)]
pub struct Statfs {
    /// Total data blocks in the filesystem
    pub blocks: u64,
    /// Free blocks in filesystem
    pub bfree: u64,
    /// Free blocks available to unprivileged user
    pub bavail: u64,
    /// Total file nodes in filesystem
    pub files: u64,
    /// Free file nodes in filesystem
    pub ffree: u64,
    /// Optimal transfer block size
    pub bsize: u32,
    /// Maximum length of filenames
    pub namelen: u32,
    /// Fragment size
    pub frsize: u32,
}

/// File attributes.
#[derive(Clone, Copy, Debug)]
pub struct FileAttr {
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last metadata change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: crate::FileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Device ID (if special file)
    pub rdev: u32,
    /// block size
    pub blksize: u32,
    /// Flags (macOS only; see chflags(2))
    pub flags: u32,
}

/// The return value for `create`: contains info on the newly-created file, as well as a handle to
/// the opened file.
#[derive(Clone, Debug)]
pub struct CreatedEntry {
    /// Entry cache time-to-live.
    pub ttl: Duration,
    /// Attributes returned for the created file.
    pub attr: FileAttr,
    /// File handle returned for the opened file.
    pub fh: u64,
    /// Open response flags returned to fuser.
    pub flags: u32,
}

/// Represents the return value from the `listxattr` and `getxattr` calls, which can be either a
/// size or contain data, depending on how they are called.
#[derive(Clone, Debug)]
pub enum Xattr {
    /// Size needed to read the attribute value or name list.
    Size(u32),
    /// Attribute data or null-terminated attribute name list.
    Data(Vec<u8>),
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug)]
pub struct XTimes {
    pub bkuptime: SystemTime,
    pub crtime: SystemTime,
}

pub type ResultEmpty = std::io::Result<()>;
pub type ResultEntry = std::io::Result<(Duration, FileAttr)>;
pub type ResultOpen = std::io::Result<(u64, u32)>;
pub type ResultReaddir = std::io::Result<Vec<DirectoryEntry>>;
pub type ResultData = std::io::Result<Vec<u8>>;
pub type ResultSlice<'a> = std::io::Result<&'a [u8]>;
pub type ResultWrite = std::io::Result<u32>;
pub type ResultStatfs = std::io::Result<Statfs>;
pub type ResultCreate = std::io::Result<CreatedEntry>;
pub type ResultXattr = std::io::Result<Xattr>;

#[cfg(target_os = "macos")]
pub type ResultXTimes = std::io::Result<XTimes>;

/// Dummy struct returned by the callback in the `read()` method. Cannot be constructed outside
/// this crate, `read()` requires you to return it, thus ensuring that you don't forget to call the
/// callback.
pub struct CallbackResult {
    pub(crate) _private: std::marker::PhantomData<()>,
}

/// Path resolved from an inode with the current inode number attached.
#[derive(Debug)]
pub struct ResolvedPath {
    parent: Arc<PathBuf>,
    name: OsString,
    ino: Inode,
}

impl ResolvedPath {
    /// Returns the full path by joining the parent path and entry name.
    pub fn full_path(&self) -> PathBuf {
        self.parent.join(&self.name)
    }
    /// Returns the final path component.
    pub fn name(&self) -> &OsStr {
        &self.name
    }
    /// Returns the inode associated with this resolved path.
    pub fn ino(&self) -> Inode {
        self.ino
    }
    /// Returns the parent directory path.
    pub fn parent_path(&self) -> Arc<PathBuf> {
        self.parent.clone()
    }
    /// Returns the same path without the inode context.
    pub fn entry_name(&self) -> EntryName {
        EntryName {
            parent: self.parent.clone(),
            name: self.name.clone(),
        }
    }
}

/// Entry name resolved relative to a parent directory path.
#[derive(Debug)]
pub struct EntryName {
    parent: Arc<PathBuf>,
    //parent_ino: Inode,
    name: OsString,
}

impl EntryName {
    /// Attaches an inode to this entry name.
    pub(crate) fn with(self, ino: Inode) -> ResolvedPath {
        ResolvedPath {
            parent: self.parent,
            name: self.name,
            ino,
        }
    }
    /// Creates an entry name from a parent folder path and child name.
    pub(crate) fn new(parent: FolderPath, name: OsString) -> Self {
        Self {
            parent: parent.0,
            name,
        }
    }
    /// Returns the full path by joining the parent path and entry name.
    pub fn full_path(&self) -> PathBuf {
        self.parent.join(&self.name)
    }
    /// Returns the final path component.
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    /// Returns the parent directory path.
    pub fn parent_path(&self) -> Arc<PathBuf> {
        self.parent.clone()
    }
}

/// Shared path for a directory entry in the inode table.
#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct FolderPath(Arc<PathBuf>);

impl From<Arc<PathBuf>> for FolderPath {
    fn from(value: Arc<PathBuf>) -> Self {
        Self(value)
    }
}

impl From<&OsStr> for FolderPath {
    fn from(value: &OsStr) -> Self {
        Self(Arc::new(value.into()))
    }
}

impl Deref for FolderPath {
    type Target = Arc<PathBuf>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn enosys_error<T>() -> std::io::Result<T> {
    Err(std::io::Error::from_raw_os_error(libc::ENOSYS))
}

/// This trait must be implemented to implement a filesystem with FuserNG.
///
/// Methods have default ENOSYS implementations, so a filesystem can implement only the operations
/// it supports.
///
/// ```no_run
/// use std::io;
/// use std::time::{Duration, SystemTime};
///
/// use fuser_ng::{EntryName, FileAttr, FileType, Filesystem, RequestInfo, ResultEntry};
///
/// struct RootOnly;
///
/// impl Filesystem for RootOnly {
///     fn getattr(&self, _req: RequestInfo, path: &EntryName, _fh: Option<u64>) -> ResultEntry {
///         if path.full_path() != std::path::Path::new("/") {
///             return Err(io::Error::from_raw_os_error(libc::ENOENT));
///         }
///
///         let now = SystemTime::now();
///
///         Ok((
///             Duration::from_secs(1),
///             FileAttr {
///                 size: 0,
///                 blocks: 0,
///                 atime: now,
///                 mtime: now,
///                 ctime: now,
///                 crtime: now,
///                 kind: FileType::Directory,
///                 perm: 0o755,
///                 nlink: 2,
///                 uid: 0,
///                 gid: 0,
///                 rdev: 0,
///                 blksize: 512,
///                 flags: 0,
///             },
///         ))
///     }
/// }
/// ```
#[allow(unused_variables)]
pub trait Filesystem {
    /// Called on mount, before any other function.
    fn init(&self, req: RequestInfo, config: &mut KernelConfig) -> ResultEmpty {
        Ok(())
    }

    /// Called on filesystem unmount.
    fn destroy(&self) {
        // Nothing.
    }

    /// Get the attributes of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    fn getattr(&self, req: RequestInfo, path: &EntryName, fh: Option<u64>) -> ResultEntry {
        enosys_error()
    }

    // The following operations in the FUSE C API are all one kernel call: setattr
    // We split them out to match the C API's behavior.

    /// Change the mode of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `mode`: the mode to change the file to.
    fn chmod(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        mode: u32,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Change the owner UID and/or group GID of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `uid`: user ID to change the file's owner to. If `None`, leave the UID unchanged.
    /// * `gid`: group ID to change the file's group to. If `None`, leave the GID unchanged.
    fn chown(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Set the length of a file.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `size`: size in bytes to set as the file's length.
    fn truncate(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        size: u64,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Set timestamps of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `atime`: the time of last access.
    /// * `mtime`: the time of last modification.
    fn utimens(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Set timestamps of a filesystem entry (with extra options only used on MacOS).
    #[allow(clippy::too_many_arguments)]
    fn utimens_macos(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> ResultEmpty {
        enosys_error()
    }

    // END OF SETATTR FUNCTIONS

    /// Read a symbolic link.
    fn readlink(&self, req: RequestInfo, path: &ResolvedPath) -> ResultData {
        enosys_error()
    }

    /// Create a special file.
    ///
    /// * `parent`: path to the directory to make the entry under.
    /// * `name`: name of the entry.
    /// * `mode`: mode for the new entry.
    /// * `rdev`: if mode has the bits `S_IFCHR` or `S_IFBLK` set, this is the major and minor numbers for the device file. Otherwise it should be ignored.
    fn mknod(&self, req: RequestInfo, entry: &EntryName, mode: u32, rdev: u32) -> ResultEntry {
        enosys_error()
    }

    /// Create a directory.
    ///
    /// * `parent`: path to the directory to make the directory under.
    /// * `name`: name of the directory.
    /// * `mode`: permissions for the new directory.
    fn mkdir(&self, req: RequestInfo, entry: &EntryName, mode: u32) -> ResultEntry {
        enosys_error()
    }

    /// Remove a file.
    ///
    /// * `parent`: path to the directory containing the file to delete.
    /// * `name`: name of the file to delete.
    fn unlink(&self, req: RequestInfo, entry: &EntryName) -> ResultEmpty {
        enosys_error()
    }

    /// Remove a directory.
    ///
    /// * `parent`: path to the directory containing the directory to delete.
    /// * `name`: name of the directory to delete.
    fn rmdir(&self, req: RequestInfo, entry: &EntryName) -> ResultEmpty {
        enosys_error()
    }

    /// Create a symbolic link.
    ///
    /// * `parent`: path to the directory to make the link in.
    /// * `name`: name of the symbolic link.
    /// * `target`: path (may be relative or absolute) to the target of the link.
    fn symlink(&self, req: RequestInfo, entry: &EntryName, target: &Path) -> ResultEntry {
        enosys_error()
    }

    /// Rename a filesystem entry.
    ///
    /// * `parent`: path to the directory containing the existing entry.
    /// * `name`: name of the existing entry.
    /// * `newparent`: path to the directory it should be renamed into (may be the same as `parent`).
    /// * `newname`: name of the new entry.
    fn rename(&self, req: RequestInfo, entry: &EntryName, new_entry: &EntryName) -> ResultEmpty {
        enosys_error()
    }

    /// Create a hard link.
    ///
    /// * `path`: path to an existing file.
    /// * `newparent`: path to the directory for the new link.
    /// * `newname`: name for the new link.
    fn link(&self, req: RequestInfo, path: &ResolvedPath, new_entry: &EntryName) -> ResultEntry {
        enosys_error()
    }

    /// Open a file.
    ///
    /// * `path`: path to the file.
    /// * `flags`: one of `O_RDONLY`, `O_WRONLY`, or `O_RDWR`, plus maybe additional flags.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the file, and can be any value you choose, though it should allow
    /// your filesystem to identify the file opened even without any path info.
    fn open(&self, req: RequestInfo, path: &ResolvedPath, flags: u32) -> ResultOpen {
        enosys_error()
    }

    /// Read from a file.
    ///
    /// Note that it is not an error for this call to request to read past the end of the file, and
    /// you should only return data up to the end of the file (i.e. the number of bytes returned
    /// will be fewer than requested; possibly even zero). Do not extend the file in this case.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `offset`: offset into the file to start reading.
    /// * `size`: number of bytes to read.
    /// * `callback`: a callback that must be invoked to return the result of the operation: either
    ///   the result data as a slice, or an error code.
    ///
    /// Return the return value from the `callback` function.
    fn read(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        offset: u64,
        size: u32,
        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        callback(enosys_error())
    }

    /// Write to a file.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `offset`: offset into the file to start writing.
    /// * `data`: the data to write
    /// * `flags`:
    ///
    /// Return the number of bytes written.
    fn write(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        flags: u32,
    ) -> ResultWrite {
        enosys_error()
    }

    /// Called each time a program calls `close` on an open file.
    ///
    /// Note that because file descriptors can be duplicated (by `dup`, `dup2`, `fork`) this may be
    /// called multiple times for a given file handle. The main use of this function is if the
    /// filesystem would like to return an error to the `close` call. Note that most programs
    /// ignore the return value of `close`, though.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `lock_owner`: if the filesystem supports locking (`setlk`, `getlk`), remove all locks
    ///   belonging to this lock owner.
    fn flush(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        lock_owner: u64,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Called when an open file is closed.
    ///
    /// There will be one of these for each `open` call. After `release`, no more calls will be
    /// made with the given file handle.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `flags`: the flags passed when the file was opened.
    /// * `lock_owner`: if the filesystem supports locking (`setlk`, `getlk`), remove all locks
    ///   belonging to this lock owner.
    /// * `flush`: whether pending data must be flushed or not.
    fn release(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Write out any pending changes of a file.
    ///
    /// When this returns, data should be written to persistent storage.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `datasync`: if `false`, also write metadata, otherwise just write file data.
    fn fsync(&self, req: RequestInfo, path: &ResolvedPath, fh: u64, datasync: bool) -> ResultEmpty {
        enosys_error()
    }

    /// Open a directory.
    ///
    /// Analogous to the `opend` call.
    ///
    /// * `path`: path to the directory.
    /// * `flags`: file access flags. Will contain `O_DIRECTORY` at least.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the directory, and can be any value you choose, though it should
    /// allow your filesystem to identify the directory opened even without any path info.
    fn opendir(&self, req: RequestInfo, path: &ResolvedPath, flags: u32) -> ResultOpen {
        enosys_error()
    }

    /// Get the entries of a directory.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    ///
    /// Return all the entries of the directory.
    fn readdir(&self, req: RequestInfo, path: &ResolvedPath, fh: u64) -> ResultReaddir {
        enosys_error()
    }

    /// Close an open directory.
    ///
    /// This will be called exactly once for each `opendir` call.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    /// * `flags`: the file access flags passed to the `opendir` call.
    fn releasedir(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        flags: u32,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Write out any pending changes to a directory.
    ///
    /// Analogous to the `fsync` call.
    fn fsyncdir(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Get filesystem statistics.
    ///
    /// * `path`: path to some folder in the filesystem.
    ///
    /// See the `Statfs` struct for more details.
    fn statfs(&self, req: RequestInfo, path: &ResolvedPath) -> ResultStatfs {
        enosys_error()
    }

    /// Set a file extended attribute.
    ///
    /// * `path`: path to the file.
    /// * `name`: attribute name.
    /// * `value`: the data to set the value to.
    /// * `flags`: can be either `XATTR_CREATE` or `XATTR_REPLACE`.
    /// * `position`: offset into the attribute value to write data.
    fn setxattr(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> ResultEmpty {
        enosys_error()
    }

    /// Get a file extended attribute.
    ///
    /// * `path`: path to the file
    /// * `name`: attribute name.
    /// * `size`: the maximum number of bytes to read.
    ///
    /// If `size` is 0, return `Xattr::Size(n)` where `n` is the size of the attribute data.
    /// Otherwise, return `Xattr::Data(data)` with the requested data.
    fn getxattr(
        &self,
        req: RequestInfo,
        path: &ResolvedPath,
        name: &OsStr,
        size: u32,
    ) -> ResultXattr {
        enosys_error()
    }

    /// List extended attributes for a file.
    ///
    /// * `path`: path to the file.
    /// * `size`: maximum number of bytes to return.
    ///
    /// If `size` is 0, return `Xattr::Size(n)` where `n` is the size required for the list of
    /// attribute names.
    /// Otherwise, return `Xattr::Data(data)` where `data` is all the null-terminated attribute
    /// names.
    fn listxattr(&self, req: RequestInfo, path: &ResolvedPath, size: u32) -> ResultXattr {
        enosys_error()
    }

    /// Remove an extended attribute for a file.
    ///
    /// * `path`: path to the file.
    /// * `name`: name of the attribute to remove.
    fn removexattr(&self, req: RequestInfo, path: &ResolvedPath, name: &OsStr) -> ResultEmpty {
        enosys_error()
    }

    /// Check for access to a file.
    ///
    /// * `path`: path to the file.
    /// * `mask`: mode bits to check for access to.
    ///
    /// Return `Ok(())` if all requested permissions are allowed, otherwise return `Err(EACCES)`
    /// or other error code as appropriate (e.g. `ENOENT` if the file doesn't exist).
    fn access(&self, req: RequestInfo, path: &ResolvedPath, mask: u32) -> ResultEmpty {
        enosys_error()
    }

    /// Create and open a new file.
    ///
    /// * `parent`: path to the directory to create the file in.
    /// * `name`: name of the file to be created.
    /// * `mode`: the mode to set on the new file.
    /// * `flags`: flags like would be passed to `open`.
    ///
    /// Return a `CreatedEntry` (which contains the new file's attributes as well as a file handle
    /// -- see documentation on `open` for more info on that).
    fn create(&self, req: RequestInfo, entry: &EntryName, mode: u32, flags: u32) -> ResultCreate {
        enosys_error()
    }

    // getlk

    // setlk

    // bmap

    /// macOS only: Rename the volume.
    ///
    /// * `name`: new name for the volume
    #[cfg(target_os = "macos")]
    fn setvolname(&self, req: RequestInfo, name: &OsStr) -> ResultEmpty {
        enosys_error()
    }

    // exchange (macOS only, undocumented)

    /// macOS only: Query extended times (bkuptime and crtime).
    ///
    /// * `path`: path to the file to get the times for.
    ///
    /// Return an `XTimes` struct with the times, or other error code as appropriate.
    #[cfg(target_os = "macos")]
    fn getxtimes(&self, req: RequestInfo, path: &ResolvedPath) -> ResultXTimes {
        Err(libc::ENOSYS)
    }
}
