// PassthroughFS :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using fuser_ng::Filesystem.
//
// Copyright (c) 2016-2022 by William R. Fraser, 2026 by François NT
//

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::libc_extras::libc;
use crate::libc_wrappers;

use fuser_ng::*;

pub struct PassthroughFS {
    pub target: OsString,
}

fn mode_to_filetype(mode: libc::mode_t) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => {
            panic!("unknown file type");
        }
    }
}

fn stat_to_fuse(stat: libc::stat64) -> FileAttr {
    // st_mode encodes both the kind and the permissions
    let kind = mode_to_filetype(stat.st_mode);
    let perm = (stat.st_mode & 0o7777) as u16;

    let time =
        |secs: i64, nanos: i64| SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos as u32);

    // libc::nlink_t is wildly different sizes on different platforms:
    // linux amd64: u64
    // linux x86:   u32
    // macOS amd64: u16
    #[allow(clippy::cast_lossless)]
    let nlink = stat.st_nlink as u32;

    FileAttr {
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: time(stat.st_atime, stat.st_atime_nsec),
        mtime: time(stat.st_mtime, stat.st_mtime_nsec),
        ctime: time(stat.st_ctime, stat.st_ctime_nsec),
        crtime: SystemTime::UNIX_EPOCH,
        kind,
        perm,
        nlink,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        blksize: stat.st_blksize as u32,
        flags: 0,
    }
}

#[cfg(target_os = "macos")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: 0, // TODO
        frsize: 0,  // TODO
    }
}

#[cfg(target_os = "linux")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: statfs.f_namelen as u32,
        frsize: statfs.f_frsize as u32,
    }
}

impl PassthroughFS {
    fn real_path(&self, partial: &Path) -> OsString {
        PathBuf::from(&self.target)
            .join(partial.strip_prefix("/").unwrap())
            .into_os_string()
    }

    fn stat_real(&self, path: &Path) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(path);
        debug!("stat_real: {:?}", real);

        match libc_wrappers::lstat(real) {
            Ok(stat) => Ok(stat_to_fuse(stat)),
            Err(e) => {
                let err = io::Error::from_raw_os_error(e);
                error!("lstat({:?}): {}", path, err);
                Err(err)
            }
        }
    }
}

trait ToIoResult<T> {
    fn io_result(self) -> std::io::Result<T>;
}

trait ToIoError {
    fn io_error(self) -> std::io::Error;
}

impl ToIoError for libc::c_int {
    fn io_error(self) -> std::io::Error {
        io::Error::from_raw_os_error(self)
    }
}

impl<T> ToIoResult<T> for Result<T, libc::c_int> {
    fn io_result(self) -> std::io::Result<T> {
        self.map_err(io::Error::from_raw_os_error)
    }
}

const TTL: Duration = Duration::from_secs(1);

impl Filesystem for PassthroughFS {
    fn init(&self, _req: RequestInfo, _config: &mut KernelConfig) -> ResultEmpty {
        debug!("init");
        Ok(())
    }

    fn destroy(&self) {
        debug!("destroy");
    }

    fn getattr(&self, _req: RequestInfo, path: &EntryName, fh: Option<u64>) -> ResultEntry {
        debug!("getattr: {:?}", path);

        if let Some(fh) = fh {
            match libc_wrappers::fstat(fh) {
                Ok(stat) => Ok((TTL, stat_to_fuse(stat))),
                Err(e) => Err(std::io::Error::from_raw_os_error(e)),
            }
        } else {
            match self.stat_real(&path.full_path()) {
                Ok(attr) => Ok((TTL, attr)),
                Err(e) => Err(e),
            }
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &ResolvedPath, _flags: u32) -> ResultOpen {
        let real = self.real_path(&path.full_path());
        debug!("opendir: {:?} (flags = {:#o})", real, _flags);
        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh, 0)),
            Err(e) => {
                let ioerr = io::Error::from_raw_os_error(e);
                error!("opendir({:?}): {}", path, ioerr);
                Err(e.io_error())
            }
        }
    }

    fn releasedir(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        _flags: u32,
    ) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        libc_wrappers::closedir(fh).io_result()
    }

    fn readdir(&self, _req: RequestInfo, path: &ResolvedPath, fh: u64) -> ResultReaddir {
        debug!("readdir: {:?}", path);
        let mut entries: Vec<DirectoryEntry> = vec![];

        if fh == 0 {
            error!("readdir: missing fh");
            return Err(libc::EINVAL.io_error());
        }
        let path = path.full_path();
        loop {
            match libc_wrappers::readdir(fh) {
                Ok(Some(entry)) => {
                    let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                    let name = OsStr::from_bytes(name_c.to_bytes()).to_owned();

                    let filetype = match entry.d_type {
                        libc::DT_DIR => FileType::Directory,
                        libc::DT_REG => FileType::RegularFile,
                        libc::DT_LNK => FileType::Symlink,
                        libc::DT_BLK => FileType::BlockDevice,
                        libc::DT_CHR => FileType::CharDevice,
                        libc::DT_FIFO => FileType::NamedPipe,
                        libc::DT_SOCK => {
                            warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
                            FileType::NamedPipe
                        }
                        _ => {
                            let entry_path = path.join(&name);
                            let real_path = self.real_path(&entry_path);
                            match libc_wrappers::lstat(real_path) {
                                Ok(stat64) => mode_to_filetype(stat64.st_mode),
                                Err(errno) => {
                                    let ioerr = io::Error::from_raw_os_error(errno);
                                    panic!("lstat failed after readdir_r gave no file type for {:?}: {}",
                                           entry_path, ioerr);
                                }
                            }
                        }
                    };

                    entries.push(DirectoryEntry {
                        name,
                        kind: filetype,
                    })
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    error!("readdir: {:?}: {}", path, e);
                    return Err(e.io_error());
                }
            }
        }

        Ok(entries)
    }

    fn open(&self, _req: RequestInfo, path: &ResolvedPath, flags: u32) -> ResultOpen {
        debug!("open: {:?} flags={:#x}", path, flags);

        let real = self.real_path(&path.full_path());
        match libc_wrappers::open(real, flags as libc::c_int) {
            Ok(fh) => Ok((fh, 0)),
            Err(e) => {
                error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e.io_error())
            }
        }
    }

    fn release(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        debug!("release: {:?}", path);
        libc_wrappers::close(fh).io_result()
    }

    fn read(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        offset: u64,
        size: u32,
        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        let path = path.full_path();
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        let mut data = Vec::<u8>::with_capacity(size as usize);

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return callback(Err(e));
        }
        match file.read(unsafe {
            mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(data.spare_capacity_mut())
        }) {
            Ok(n) => {
                unsafe { data.set_len(n) };
            }
            Err(e) => {
                error!("read {:?}, {:#x} @ {:#x}: {}", path, size, offset, e);
                return callback(Err(e));
            }
        }

        callback(Ok(&data))
    }

    fn write(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        _flags: u32,
    ) -> ResultWrite {
        let path = path.full_path();
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return Err(e);
        }
        let nwritten: u32 = match file.write(&data) {
            Ok(n) => n as u32,
            Err(e) => {
                error!("write {:?}, {:#x} @ {:#x}: {}", path, data.len(), offset, e);
                return Err(e);
            }
        };

        Ok(nwritten)
    }

    fn flush(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        _lock_owner: u64,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("flush: {:?}", path);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.flush() {
            error!("flush({:?}): {}", path, e);
            return Err(e);
        }

        Ok(())
    }

    fn fsync(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("fsync: {:?}, data={:?}", path, datasync);
        let file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = if datasync {
            file.sync_data()
        } else {
            file.sync_all()
        } {
            error!("fsync({:?}, {:?}): {}", path, datasync, e);
            return Err(e);
        }

        Ok(())
    }

    fn chmod(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        mode: u32,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("chmod: {:?} to {:#o}", path, mode);

        let result = if let Some(fh) = fh {
            unsafe { libc::fchmod(fh as libc::c_int, mode as libc::mode_t) }
        } else {
            let real = self.real_path(&path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chmod(path_c.as_ptr(), mode as libc::mode_t)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chmod({:?}, {:#o}): {}", path, mode, e);
            Err(e)
        } else {
            Ok(())
        }
    }

    fn chown(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ResultEmpty {
        let path = path.full_path();
        let uid = uid.unwrap_or(u32::MAX); // docs say "-1", but uid_t is unsigned
        let gid = gid.unwrap_or(u32::MAX); // ditto for gid_t
        debug!("chown: {:?} to {}:{}", path, uid, gid);

        let result = if let Some(fd) = fh {
            unsafe { libc::fchown(fd as libc::c_int, uid, gid) }
        } else {
            let real = self.real_path(&path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chown(path_c.as_ptr(), uid, gid)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chown({:?}, {}, {}): {}", path, uid, gid, e);
            Err(e)
        } else {
            Ok(())
        }
    }

    fn truncate(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        size: u64,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("truncate: {:?} to {:#x}", path, size);

        let result = if let Some(fd) = fh {
            unsafe { libc::ftruncate64(fd as libc::c_int, size as i64) }
        } else {
            let real = self.real_path(&path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::truncate64(path_c.as_ptr(), size as i64)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("truncate({:?}, {}): {}", path, size, e);
            Err(e)
        } else {
            Ok(())
        }
    }

    fn utimens(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("utimens: {:?}: {:?}, {:?}", path, atime, mtime);

        let systemtime_to_libc = |time: Option<SystemTime>| -> libc::timespec {
            if let Some(time) = time {
                let (secs, nanos) = match time.duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos()),
                    Err(in_past) => {
                        let duration = in_past.duration();
                        (-(duration.as_secs() as i64), duration.subsec_nanos())
                    }
                };

                libc::timespec {
                    tv_sec: secs,
                    tv_nsec: i64::from(nanos),
                }
            } else {
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                }
            }
        };

        let times = [systemtime_to_libc(atime), systemtime_to_libc(mtime)];

        let result = if let Some(fd) = fh {
            unsafe { libc::futimens(fd as libc::c_int, &times as *const libc::timespec) }
        } else {
            let real = self.real_path(&path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::utimensat(
                    libc::AT_FDCWD,
                    path_c.as_ptr(),
                    &times as *const libc::timespec,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("utimens({:?}, {:?}, {:?}): {}", path, atime, mtime, e);
            Err(e)
        } else {
            Ok(())
        }
    }

    fn readlink(&self, _req: RequestInfo, path: &ResolvedPath) -> ResultData {
        let path = path.full_path();
        debug!("readlink: {:?}", path);

        let real = self.real_path(&path);
        match ::std::fs::read_link(real) {
            Ok(target) => Ok(target.into_os_string().into_vec()),
            Err(e) => Err(e),
        }
    }

    fn statfs(&self, _req: RequestInfo, path: &ResolvedPath) -> ResultStatfs {
        let path = path.full_path();
        debug!("statfs: {:?}", path);

        let real = self.real_path(&path);
        let mut buf: libc::statfs = unsafe { ::std::mem::zeroed() };
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.into_vec());
            libc::statfs(path_c.as_ptr(), &mut buf)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("statfs({:?}): {}", path, e);
            Err(e)
        } else {
            Ok(statfs_to_fuse(buf))
        }
    }

    fn fsyncdir(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        fh: u64,
        datasync: bool,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!("fsyncdir: {:?} (datasync = {:?})", path, datasync);

        // TODO: what does datasync mean with regards to a directory handle?
        let dir = fh as usize as *mut libc::DIR;
        let fd = unsafe { libc::dirfd(dir) };
        let result = unsafe { libc::fsync(fd) };
        if -1 == result {
            let e = io::Error::last_os_error();
            error!("fsyncdir({:?}): {}", path, e);
            Err(e)
        } else {
            Ok(())
        }
    }

    fn mknod(&self, _req: RequestInfo, entry: &EntryName, mode: u32, rdev: u32) -> ResultEntry {
        debug!("mknod: {:?} (mode={:#o}, rdev={})", entry, mode, rdev);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mknod(path_c.as_ptr(), mode as libc::mode_t, rdev as libc::dev_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mknod({:?}, {}, {}): {}", real, mode, rdev, e);
            Err(e)
        } else {
            match libc_wrappers::lstat(real.into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => Err(e.io_error()), // if this happens, yikes
            }
        }
    }

    fn mkdir(&self, _req: RequestInfo, entry: &EntryName, mode: u32) -> ResultEntry {
        debug!("mkdir {:?} (mode={:#o})", entry, mode);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mkdir(path_c.as_ptr(), mode as libc::mode_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mkdir({:?}, {:#o}): {}", real, mode, e);
            Err(e)
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => {
                    error!("lstat after mkdir({:?}, {:#o}): {}", real, mode, e);
                    Err(e.io_error()) // if this happens, yikes
                }
            }
        }
    }

    fn unlink(&self, _req: RequestInfo, entry: &EntryName) -> ResultEmpty {
        debug!("unlink {:?}", entry);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        fs::remove_file(&real).map_err(|ioerr| {
            error!("unlink({:?}): {}", real, ioerr);
            ioerr
        })
    }

    fn rmdir(&self, _req: RequestInfo, entry: &EntryName) -> ResultEmpty {
        debug!("rmdir: {:?}", entry);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        fs::remove_dir(&real).map_err(|ioerr| {
            error!("rmdir({:?}): {}", real, ioerr);
            ioerr
        })
    }

    fn symlink(&self, _req: RequestInfo, entry: &EntryName, target: &Path) -> ResultEntry {
        debug!("symlink: {:?} -> {:?}", entry, target);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        match ::std::os::unix::fs::symlink(target, &real) {
            Ok(()) => match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => {
                    error!("lstat after symlink({:?}, {:?}): {}", real, target, e);
                    Err(e.io_error())
                }
            },
            Err(e) => {
                error!("symlink({:?}, {:?}): {}", real, target, e);
                Err(e)
            }
        }
    }

    fn rename(&self, _req: RequestInfo, entry: &EntryName, new_entry: &EntryName) -> ResultEmpty {
        debug!("rename: {:?} -> {:?}", entry, new_entry);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        let newreal = PathBuf::from(self.real_path(&new_entry.full_path()));
        fs::rename(&real, &newreal).map_err(|ioerr| {
            error!("rename({:?}, {:?}): {}", real, newreal, ioerr);
            ioerr
        })
    }

    fn link(&self, _req: RequestInfo, path: &ResolvedPath, new_entry: &EntryName) -> ResultEntry {
        debug!("link: {:?} -> {:?}", path, new_entry);

        let real = self.real_path(&path.full_path());
        let newreal = PathBuf::from(self.real_path(&new_entry.full_path()));
        match fs::hard_link(&real, &newreal) {
            Ok(()) => match libc_wrappers::lstat(real.clone()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => {
                    error!("lstat after link({:?}, {:?}): {}", real, newreal, e);
                    Err(e.io_error())
                }
            },
            Err(e) => {
                error!("link({:?}, {:?}): {}", real, newreal, e);
                Err(e)
            }
        }
    }

    fn create(&self, _req: RequestInfo, entry: &EntryName, mode: u32, flags: u32) -> ResultCreate {
        debug!("create: {:?} (mode={:#o}, flags={:#x})", entry, mode, flags);

        let real = PathBuf::from(self.real_path(&entry.full_path()));
        let fd = unsafe {
            let real_c = CString::from_vec_unchecked(real.clone().into_os_string().into_vec());
            libc::open(
                real_c.as_ptr(),
                flags as i32 | libc::O_CREAT | libc::O_EXCL,
                mode,
            )
        };

        if -1 == fd {
            let ioerr = io::Error::last_os_error();
            error!("create({:?}): {}", real, ioerr);
            Err(ioerr)
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok(CreatedEntry {
                    ttl: TTL,
                    attr: stat_to_fuse(attr),
                    fh: fd as u64,
                    flags: 0,
                }),
                Err(e) => {
                    error!(
                        "lstat after create({:?}): {}",
                        real,
                        io::Error::from_raw_os_error(e)
                    );
                    Err(e.io_error())
                }
            }
        }
    }

    fn listxattr(&self, _req: RequestInfo, path: &ResolvedPath, size: u32) -> ResultXattr {
        let path = path.full_path();
        debug!("listxattr: {:?}", path);

        let real = self.real_path(&path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            let nread = libc_wrappers::llistxattr(real, unsafe {
                mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(
                    data.spare_capacity_mut(),
                )
            })
            .io_result()?;
            unsafe { data.set_len(nread) };
            Ok(Xattr::Data(data))
        } else {
            let nbytes = libc_wrappers::llistxattr(real, &mut []).io_result()?;
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn getxattr(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        name: &OsStr,
        size: u32,
    ) -> ResultXattr {
        let path = path.full_path();
        debug!("getxattr: {:?} {:?} {}", path, name, size);

        let real = self.real_path(&path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            let nread = libc_wrappers::lgetxattr(real, name.to_owned(), unsafe {
                mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(
                    data.spare_capacity_mut(),
                )
            })
            .io_result()?;
            unsafe { data.set_len(nread) };
            Ok(Xattr::Data(data))
        } else {
            let nbytes = libc_wrappers::lgetxattr(real, name.to_owned(), &mut []).io_result()?;
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn setxattr(
        &self,
        _req: RequestInfo,
        path: &ResolvedPath,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> ResultEmpty {
        let path = path.full_path();
        debug!(
            "setxattr: {:?} {:?} {} bytes, flags = {:#x}, pos = {}",
            path,
            name,
            value.len(),
            flags,
            position
        );
        let real = self.real_path(&path);
        libc_wrappers::lsetxattr(real, name.to_owned(), value, flags, position).io_result()
    }

    fn removexattr(&self, _req: RequestInfo, path: &ResolvedPath, name: &OsStr) -> ResultEmpty {
        let path = path.full_path();
        debug!("removexattr: {:?} {:?}", path, name);
        let real = self.real_path(&path);
        libc_wrappers::lremovexattr(real, name.to_owned()).io_result()
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: RequestInfo, name: &OsStr) -> ResultEmpty {
        info!("setvolname: {:?}", name);
        Err(libc::ENOTSUP.io_error())
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: RequestInfo, path: &ResolvedPath) -> ResultXTimes {
        debug!("getxtimes: {:?}", path);
        let xtimes = XTimes {
            bkuptime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
        };
        Ok(xtimes)
    }
}

/// A file that is not closed upon leaving scope.
struct UnmanagedFile {
    inner: Option<File>,
}

impl UnmanagedFile {
    unsafe fn new(fd: u64) -> UnmanagedFile {
        UnmanagedFile {
            inner: Some(File::from_raw_fd(fd as i32)),
        }
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_all()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_data()
    }
}

impl Drop for UnmanagedFile {
    fn drop(&mut self) {
        // Intentionally leak the file descriptor so it is not closed.
        _ = self.inner.take().unwrap().into_raw_fd();
    }
}

impl Read for UnmanagedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read(buf)
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read_to_end(buf)
    }
}

impl Write for UnmanagedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.as_ref().unwrap().flush()
    }
}

impl Seek for UnmanagedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.as_ref().unwrap().seek(pos)
    }
}
