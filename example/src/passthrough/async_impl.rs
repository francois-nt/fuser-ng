use super::*;
use std::future::Future;

/// Runs a blocking passthrough operation outside Tokio worker threads.
async fn run_blocking<T, F>(operation: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(io::Error::other)?
}

macro_rules! forward_operation {
    (
        $name:ident($($argument:ident: $argument_type:ty),* $(,)?)
        -> $result:ty => |$filesystem:ident| $operation:expr
    ) => {
        #[allow(clippy::too_many_arguments)]
        fn $name(
            &self,
            $($argument: $argument_type),*
        ) -> impl Future<Output = $result> + Send {
            let $filesystem = self.clone();
            async move { run_blocking(move || $operation).await }
        }
    };
}

impl AsyncFilesystem for PassthroughFS {
    fn init(&self, req: RequestInfo, config: &mut KernelConfig) -> ResultEmpty {
        Filesystem::init(self, req, config)
    }

    fn destroy(&self) {
        Filesystem::destroy(self);
    }

    forward_operation!(
        getattr(req: RequestInfo, path: EntryRef, fh: Option<u64>)
        -> ResultEntry => |filesystem| Filesystem::getattr(&filesystem, req, &path, fh)
    );

    forward_operation!(
        chmod(req: RequestInfo, path: ResolvedPath, fh: Option<u64>, mode: u32)
        -> ResultEmpty => |filesystem| Filesystem::chmod(&filesystem, req, &path, fh, mode)
    );

    forward_operation!(
        chown(
            req: RequestInfo,
            path: ResolvedPath,
            fh: Option<u64>,
            uid: Option<u32>,
            gid: Option<u32>,
        ) -> ResultEmpty => |filesystem| {
            Filesystem::chown(&filesystem, req, &path, fh, uid, gid)
        }
    );

    forward_operation!(
        truncate(req: RequestInfo, path: ResolvedPath, fh: Option<u64>, size: u64)
        -> ResultEmpty => |filesystem| Filesystem::truncate(&filesystem, req, &path, fh, size)
    );

    forward_operation!(
        utimens(
            req: RequestInfo,
            path: ResolvedPath,
            fh: Option<u64>,
            atime: Option<SystemTime>,
            mtime: Option<SystemTime>,
        ) -> ResultEmpty => |filesystem| {
            Filesystem::utimens(&filesystem, req, &path, fh, atime, mtime)
        }
    );

    forward_operation!(
        utimens_macos(
            req: RequestInfo,
            path: ResolvedPath,
            fh: Option<u64>,
            crtime: Option<SystemTime>,
            chgtime: Option<SystemTime>,
            bkuptime: Option<SystemTime>,
            flags: Option<u32>,
        ) -> ResultEmpty => |filesystem| {
            Filesystem::utimens_macos(
                &filesystem,
                req,
                &path,
                fh,
                crtime,
                chgtime,
                bkuptime,
                flags,
            )
        }
    );

    forward_operation!(
        readlink(req: RequestInfo, path: ResolvedPath)
        -> ResultData => |filesystem| Filesystem::readlink(&filesystem, req, &path)
    );

    forward_operation!(
        mknod(req: RequestInfo, entry: EntryName, mode: u32, rdev: u32)
        -> ResultEntry => |filesystem| Filesystem::mknod(&filesystem, req, &entry, mode, rdev)
    );

    forward_operation!(
        mkdir(req: RequestInfo, entry: EntryName, mode: u32)
        -> ResultEntry => |filesystem| Filesystem::mkdir(&filesystem, req, &entry, mode)
    );

    forward_operation!(
        unlink(req: RequestInfo, entry: EntryName)
        -> ResultEmpty => |filesystem| Filesystem::unlink(&filesystem, req, &entry)
    );

    forward_operation!(
        rmdir(req: RequestInfo, entry: EntryName)
        -> ResultEmpty => |filesystem| Filesystem::rmdir(&filesystem, req, &entry)
    );

    forward_operation!(
        symlink(req: RequestInfo, entry: EntryName, target: PathBuf)
        -> ResultEntry => |filesystem| Filesystem::symlink(&filesystem, req, &entry, &target)
    );

    forward_operation!(
        rename(req: RequestInfo, entry: EntryName, new_entry: EntryName)
        -> ResultEmpty => |filesystem| {
            Filesystem::rename(&filesystem, req, &entry, &new_entry)
        }
    );

    forward_operation!(
        link(req: RequestInfo, path: ResolvedPath, new_entry: EntryName)
        -> ResultEntry => |filesystem| Filesystem::link(&filesystem, req, &path, &new_entry)
    );

    forward_operation!(
        open(req: RequestInfo, path: ResolvedPath, flags: u32)
        -> ResultOpen => |filesystem| Filesystem::open(&filesystem, req, &path, flags)
    );

    forward_operation!(
        read(_req: RequestInfo, path: ResolvedPath, fh: u64, offset: u64, size: u32)
        -> ResultData => |filesystem| filesystem.read_data(&path, fh, offset, size)
    );

    forward_operation!(
        write(
            req: RequestInfo,
            path: ResolvedPath,
            fh: u64,
            offset: u64,
            data: Vec<u8>,
            flags: u32,
        ) -> ResultWrite => |filesystem| {
            Filesystem::write(&filesystem, req, &path, fh, offset, data, flags)
        }
    );

    forward_operation!(
        flush(req: RequestInfo, path: ResolvedPath, fh: u64, lock_owner: u64)
        -> ResultEmpty => |filesystem| {
            Filesystem::flush(&filesystem, req, &path, fh, lock_owner)
        }
    );

    forward_operation!(
        release(
            req: RequestInfo,
            path: ResolvedPath,
            fh: u64,
            flags: u32,
            lock_owner: u64,
            flush: bool,
        ) -> ResultEmpty => |filesystem| {
            Filesystem::release(&filesystem, req, &path, fh, flags, lock_owner, flush)
        }
    );

    forward_operation!(
        fsync(req: RequestInfo, path: ResolvedPath, fh: u64, datasync: bool)
        -> ResultEmpty => |filesystem| {
            Filesystem::fsync(&filesystem, req, &path, fh, datasync)
        }
    );

    forward_operation!(
        opendir(req: RequestInfo, path: ResolvedPath, flags: u32)
        -> ResultOpen => |filesystem| Filesystem::opendir(&filesystem, req, &path, flags)
    );

    forward_operation!(
        readdir(req: RequestInfo, path: ResolvedPath, fh: u64)
        -> ResultReaddir => |filesystem| Filesystem::readdir(&filesystem, req, &path, fh)
    );

    forward_operation!(
        releasedir(req: RequestInfo, path: ResolvedPath, fh: u64, flags: u32)
        -> ResultEmpty => |filesystem| {
            Filesystem::releasedir(&filesystem, req, &path, fh, flags)
        }
    );

    forward_operation!(
        fsyncdir(req: RequestInfo, path: ResolvedPath, fh: u64, datasync: bool)
        -> ResultEmpty => |filesystem| {
            Filesystem::fsyncdir(&filesystem, req, &path, fh, datasync)
        }
    );

    forward_operation!(
        statfs(req: RequestInfo, path: ResolvedPath)
        -> ResultStatfs => |filesystem| Filesystem::statfs(&filesystem, req, &path)
    );

    forward_operation!(
        setxattr(
            req: RequestInfo,
            path: ResolvedPath,
            name: OsString,
            value: Vec<u8>,
            flags: u32,
            position: u32,
        ) -> ResultEmpty => |filesystem| {
            Filesystem::setxattr(&filesystem, req, &path, &name, &value, flags, position)
        }
    );

    forward_operation!(
        getxattr(req: RequestInfo, path: ResolvedPath, name: OsString, size: u32)
        -> ResultXattr => |filesystem| {
            Filesystem::getxattr(&filesystem, req, &path, &name, size)
        }
    );

    forward_operation!(
        listxattr(req: RequestInfo, path: ResolvedPath, size: u32)
        -> ResultXattr => |filesystem| Filesystem::listxattr(&filesystem, req, &path, size)
    );

    forward_operation!(
        removexattr(req: RequestInfo, path: ResolvedPath, name: OsString)
        -> ResultEmpty => |filesystem| {
            Filesystem::removexattr(&filesystem, req, &path, &name)
        }
    );

    forward_operation!(
        access(req: RequestInfo, path: ResolvedPath, mask: u32)
        -> ResultEmpty => |filesystem| Filesystem::access(&filesystem, req, &path, mask)
    );

    forward_operation!(
        create(req: RequestInfo, path: ResolvedPath, mode: u32, flags: u32)
        -> ResultCreate => |filesystem| Filesystem::create(&filesystem, req, &path, mode, flags)
    );

    #[cfg(target_os = "macos")]
    forward_operation!(
        setvolname(req: RequestInfo, name: OsString)
        -> ResultEmpty => |filesystem| Filesystem::setvolname(&filesystem, req, &name)
    );

    #[cfg(target_os = "macos")]
    forward_operation!(
        getxtimes(req: RequestInfo, path: ResolvedPath)
        -> ResultXTimes => |filesystem| Filesystem::getxtimes(&filesystem, req, &path)
    );
}
