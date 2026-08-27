use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

#[cfg(not(feature = "legacy_readdir"))]
use fuser::InitFlags;
use fuser::{
    AccessFlags, Errno, FileHandle, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags,
    RenameFlags, TimeOrNow, WriteFlags,
};
use futures_core::Stream;

use super::AsyncFilesystem;
use crate::FileType;
use crate::directory_cache::{DirectoryCache, ReaddirCache, ReaddirState, real_fh_or_reply_error};
use crate::inode_table::{InodeTable, InodeToPath};
use crate::types::*;

type ReaddirStream = Pin<Box<dyn Stream<Item = std::io::Result<Vec<DirectoryEntry>>> + Send>>;
type ReaddirSlot = tokio::sync::Mutex<Option<ReaddirState<ReaddirStream, DirectoryEntry>>>;

#[cfg(feature = "legacy_readdir")]
type LegacyReaddirStream =
    Pin<Box<dyn Stream<Item = std::io::Result<Vec<LegacyDirectoryEntry>>> + Send>>;
#[cfg(feature = "legacy_readdir")]
type LegacyReaddirSlot =
    tokio::sync::Mutex<Option<ReaddirState<LegacyReaddirStream, LegacyDirectoryEntry>>>;

/// FUSE directory reply backed by the modern readdir stream.
enum ReaddirReply {
    #[cfg(not(feature = "legacy_readdir"))]
    Plain(fuser::ReplyDirectory),
    Plus(fuser::ReplyDirectoryPlus),
}

impl ReaddirReply {
    /// Completes the reply successfully.
    fn ok(self) {
        match self {
            #[cfg(not(feature = "legacy_readdir"))]
            Self::Plain(reply) => reply.ok(),
            Self::Plus(reply) => reply.ok(),
        }
    }

    /// Completes the reply with an error.
    fn error(self, error: Errno) {
        match self {
            #[cfg(not(feature = "legacy_readdir"))]
            Self::Plain(reply) => reply.error(error),
            Self::Plus(reply) => reply.error(error),
        }
    }
}

trait IntoRequestInfo {
    fn info(&self) -> RequestInfo;
}

impl IntoRequestInfo for fuser::Request {
    fn info(&self) -> RequestInfo {
        RequestInfo {
            unique: self.unique().0,
            uid: self.uid(),
            gid: self.gid(),
            pid: self.pid(),
        }
    }
}

/// Converts path-oriented attributes to their inode-oriented FUSE representation.
fn fuse_fileattr(attr: FileAttr, ino: INodeNo) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: attr.kind,
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: attr.blksize,
        flags: attr.flags,
    }
}

trait TimeOrNowExt {
    fn time(self) -> SystemTime;
}

impl TimeOrNowExt for TimeOrNow {
    fn time(self) -> SystemTime {
        match self {
            TimeOrNow::SpecificTime(time) => time,
            TimeOrNow::Now => SystemTime::now(),
        }
    }
}

/// State shared by all spawned filesystem operations.
#[derive(Debug)]
struct AsyncFuserNGInner<T> {
    target: T,
    table: InodeTable,
    directory_cache: RwLock<DirectoryCache>,
    readdir_cache: RwLock<ReaddirCache<ReaddirSlot>>,
    #[cfg(feature = "legacy_readdir")]
    legacy_readdir_cache: RwLock<ReaddirCache<LegacyReaddirSlot>>,
}

impl<T> AsyncFuserNGInner<T> {
    fn get_path(&self, ino: INodeNo) -> Option<EntryName> {
        self.table.get_path(ino.0)
    }

    fn add_or_get_dir(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.add_or_get_dir(parent.0, name)
    }

    fn add_or_get_leaf(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.add_or_get_leaf(parent.0, name)
    }

    fn create_or_get_leaf(&self, parent: INodeNo, name: &OsStr) -> Option<(bool, u64, u64)> {
        self.table.create_or_get_leaf(parent.0, name)
    }

    fn lookup(&self, ino: u64) {
        self.table.lookup(ino);
    }

    fn forget(&self, ino: INodeNo, count: u64) -> Option<u64> {
        self.table.forget(ino.0, count)
    }

    fn add_leaf(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.add_leaf(parent.0, name)
    }

    fn add_dir(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.add_dir(parent.0, name)
    }

    fn inode_unlink(&self, parent: INodeNo, name: &OsStr) {
        self.table.unlink(parent.0, name)
    }

    fn get_parent_inode(&self, ino: INodeNo) -> Option<u64> {
        self.table.get_parent_inode(ino.0)
    }

    fn inode_rename(
        &self,
        oldparent: INodeNo,
        oldname: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
    ) -> Option<()> {
        self.table
            .rename(oldparent.0, oldname, newparent.0, newname)
    }

    /// Adds an entry to a plain FUSE readdir reply without creating a lookup reference.
    #[cfg(not(feature = "legacy_readdir"))]
    fn add_plain_readdir_entry(
        &self,
        reply: &mut fuser::ReplyDirectory,
        entry: &DirectoryEntry,
        ino: INodeNo,
        parent_inode: INodeNo,
        entry_offset: u64,
    ) -> bool {
        let entry_inode = if entry.name == Path::new(".") {
            ino
        } else if entry.name == Path::new("..") {
            parent_inode
        } else {
            INodeNo(!1u64)
        };
        reply.add(
            entry_inode,
            entry_offset,
            entry.attr.kind,
            entry.name.as_os_str(),
        )
    }

    /// Adds an entry to a FUSE readdirplus reply and records its lookup on success.
    fn add_readdirplus_entry(
        &self,
        reply: &mut fuser::ReplyDirectoryPlus,
        entry: &DirectoryEntry,
        ino: INodeNo,
        parent_inode: INodeNo,
        entry_offset: u64,
    ) -> Result<bool, Errno> {
        let (entry_inode, generation, count_lookup) = if entry.name == Path::new(".") {
            (ino, Generation(0), false)
        } else if entry.name == Path::new("..") {
            (parent_inode, Generation(0), false)
        } else {
            let inode = if entry.attr.kind == FileType::Directory {
                self.add_or_get_dir(ino, &entry.name)
            } else {
                self.add_or_get_leaf(ino, &entry.name)
            };
            let Some((inode, generation)) = inode else {
                return Err(Errno::EINVAL);
            };
            (INodeNo(inode), Generation(generation), true)
        };

        let attr = fuse_fileattr(entry.attr, entry_inode);
        let buffer_full = reply.add(
            entry_inode,
            entry_offset,
            entry.name.as_os_str(),
            &entry.ttl,
            &attr,
            generation,
        );
        if !buffer_full && count_lookup {
            self.lookup(entry_inode.0);
        }
        Ok(buffer_full)
    }

    /// Adds a modern entry to either form of FUSE directory reply.
    fn add_readdir_entry(
        &self,
        reply: &mut ReaddirReply,
        entry: &DirectoryEntry,
        ino: INodeNo,
        parent_inode: INodeNo,
        entry_offset: u64,
    ) -> Result<bool, Errno> {
        match reply {
            #[cfg(not(feature = "legacy_readdir"))]
            ReaddirReply::Plain(reply) => {
                Ok(self.add_plain_readdir_entry(reply, entry, ino, parent_inode, entry_offset))
            }
            ReaddirReply::Plus(reply) => {
                self.add_readdirplus_entry(reply, entry, ino, parent_inode, entry_offset)
            }
        }
    }

    /// Fills one FUSE directory reply from the modern asynchronous stream.
    async fn fill_readdir(
        &self,
        state: &mut ReaddirState<ReaddirStream, DirectoryEntry>,
        ino: INodeNo,
        parent_inode: INodeNo,
        offset: u64,
        mut reply: ReaddirReply,
    ) {
        let Ok(mut index) = usize::try_from(offset) else {
            reply.error(Errno::EINVAL);
            return;
        };
        if index > state.entries.len() {
            reply.error(Errno::EINVAL);
            return;
        }

        let mut added = false;
        loop {
            if index < state.entries.len() {
                let entry = &state.entries[index];
                let entry_offset = match u64::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                {
                    Some(offset) => offset,
                    None => {
                        if added {
                            reply.ok();
                        } else {
                            reply.error(Errno::EOVERFLOW);
                        }
                        return;
                    }
                };
                let buffer_full = match self.add_readdir_entry(
                    &mut reply,
                    entry,
                    ino,
                    parent_inode,
                    entry_offset,
                ) {
                    Ok(buffer_full) => buffer_full,
                    Err(error) => {
                        if added {
                            reply.ok();
                        } else {
                            reply.error(error);
                        }
                        return;
                    }
                };
                if buffer_full {
                    if added {
                        reply.ok();
                    } else {
                        reply.error(Errno::EOVERFLOW);
                    }
                    return;
                }
                added = true;
                index += 1;
                continue;
            }

            if let Some(error) = state.pending_error {
                if added {
                    reply.ok();
                } else {
                    reply.error(error);
                }
                return;
            }

            let Some(producer) = state.producer.as_mut() else {
                reply.ok();
                return;
            };
            match std::future::poll_fn(|context| producer.as_mut().poll_next(context)).await {
                Some(Ok(entries)) => state.entries.extend(entries),
                Some(Err(error)) => {
                    state.producer = None;
                    state.pending_error = Some(error.into());
                }
                None => state.producer = None,
            }
        }
    }

    /// Fills one legacy FUSE readdir reply from its independent asynchronous stream.
    #[cfg(feature = "legacy_readdir")]
    async fn fill_legacy_readdir(
        &self,
        state: &mut ReaddirState<LegacyReaddirStream, LegacyDirectoryEntry>,
        ino: INodeNo,
        parent_inode: INodeNo,
        offset: u64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let Ok(mut index) = usize::try_from(offset) else {
            reply.error(Errno::EINVAL);
            return;
        };
        if index > state.entries.len() {
            reply.error(Errno::EINVAL);
            return;
        }

        let mut added = false;
        loop {
            if index < state.entries.len() {
                let entry = &state.entries[index];
                let entry_offset = match u64::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                {
                    Some(offset) => offset,
                    None => {
                        if added {
                            reply.ok();
                        } else {
                            reply.error(Errno::EOVERFLOW);
                        }
                        return;
                    }
                };
                let entry_inode = if entry.name == Path::new(".") {
                    ino
                } else if entry.name == Path::new("..") {
                    parent_inode
                } else {
                    INodeNo(!1u64)
                };
                if reply.add(
                    entry_inode,
                    entry_offset,
                    entry.kind,
                    entry.name.as_os_str(),
                ) {
                    if added {
                        reply.ok();
                    } else {
                        reply.error(Errno::EOVERFLOW);
                    }
                    return;
                }
                added = true;
                index += 1;
                continue;
            }

            if let Some(error) = state.pending_error {
                if added {
                    reply.ok();
                } else {
                    reply.error(error);
                }
                return;
            }

            let Some(producer) = state.producer.as_mut() else {
                reply.ok();
                return;
            };
            match std::future::poll_fn(|context| producer.as_mut().poll_next(context)).await {
                Some(Ok(entries)) => state.entries.extend(entries),
                Some(Err(error)) => {
                    state.producer = None;
                    state.pending_error = Some(error.into());
                }
                None => state.producer = None,
            }
        }
    }
}

mod executor {
    use std::future::Future;

    pub(super) type Executor = tokio::runtime::Handle;

    /// Spawns a detached filesystem operation.
    pub(super) fn spawn(executor: &Executor, future: impl Future<Output = ()> + Send + 'static) {
        drop(executor.spawn(future));
    }
}

/// Adapts an asynchronous path-oriented filesystem to the synchronous FUSE callback interface.
#[derive(Debug)]
pub struct AsyncFuserNG<T> {
    inner: Arc<AsyncFuserNGInner<T>>,
    executor: executor::Executor,
}

impl<T: AsyncFilesystem + Sync + Send + 'static> AsyncFuserNG<T> {
    /// Creates an adapter using the handle of an existing Tokio runtime.
    pub fn new(target_fs: T, executor: tokio::runtime::Handle) -> Self {
        Self {
            inner: Arc::new(AsyncFuserNGInner {
                target: target_fs,
                table: InodeTable::new(),
                directory_cache: RwLock::new(DirectoryCache::new()),
                readdir_cache: RwLock::new(ReaddirCache::new()),
                #[cfg(feature = "legacy_readdir")]
                legacy_readdir_cache: RwLock::new(ReaddirCache::new()),
            }),
            executor,
        }
    }

    /// Spawns a detached filesystem operation on the configured executor.
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        executor::spawn(&self.executor, future);
    }
}

macro_rules! get_entry_name {
    ($inner:expr, $ino:expr, $reply:expr) => {
        if let Some(path) = $inner.get_path($ino) {
            path
        } else {
            $reply.error(Errno::EINVAL);
            return;
        }
    };
}

macro_rules! resolve_from_parent {
    ($inner:expr, $ino:expr, $name:expr, $reply:expr) => {
        if let Some(path) = $inner.table.resolve_from_parent($ino.0, $name.into()) {
            path
        } else {
            $reply.error(Errno::EINVAL);
            return;
        }
    };
}

macro_rules! get_resolved_path {
    ($inner:expr, $ino:expr, $reply:expr) => {{ get_entry_name!($inner, $ino, $reply).with($ino.0) }};
}

impl<T: AsyncFilesystem + Sync + Send + 'static> fuser::Filesystem for AsyncFuserNG<T> {
    fn init(
        &mut self,
        req: &fuser::Request,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), std::io::Error> {
        debug!("init");
        self.inner.target.init(req.info(), config)?;
        #[cfg(not(feature = "legacy_readdir"))]
        if let Err(unsupported) = config.add_capabilities(InitFlags::FUSE_DO_READDIRPLUS) {
            warn!("kernel does not support FUSE_READDIRPLUS: {unsupported:?}");
        }
        Ok(())
    }

    fn destroy(&mut self) {
        debug!("destroy");
        self.inner.target.destroy();
    }

    fn lookup(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("lookup: {:?}", path);

        self.spawn(async move {
            match inner
                .target
                .getattr(req, EntryRef::Lookup(path), None)
                .await
            {
                Ok((ttl, attr)) => {
                    let value = if attr.kind == FileType::Directory {
                        inner.add_or_get_dir(parent, &name)
                    } else {
                        inner.add_or_get_leaf(parent, &name)
                    };
                    if let Some((ino, generation)) = value {
                        inner.lookup(ino);
                        reply.entry(
                            &ttl,
                            &fuse_fileattr(attr, INodeNo(ino)),
                            Generation(generation),
                        );
                    } else {
                        reply.error(Errno::EINVAL);
                    }
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn forget(&self, _req: &fuser::Request, ino: INodeNo, nlookup: u64) {
        let lookups = match self.inner.forget(ino, nlookup) {
            Some(value) => value,
            _ => {
                log::error!("catastrophic error in forget");
                return;
            }
        };

        let path = self.inner.get_path(ino).unwrap_or_else(|| {
            EntryName::new(
                Arc::new(PathBuf::from(OsStr::new(""))).into(),
                OsString::from("[unknown]").into(),
            )
        });
        debug!(
            "forget: inode {} ({:?}) now at {} lookups",
            ino, path, lookups
        );
    }

    fn getattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: Option<FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("getattr: {:?}", path);

        self.spawn(async move {
            match inner
                .target
                .getattr(req, EntryRef::Resolved(path), fh.map(|fh| fh.0))
                .await
            {
                Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<fuser::BsdFileFlags>,
        reply: fuser::ReplyAttr,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        let fh = fh.map(|fh| fh.0);
        debug!("setattr: {:?}", path);

        self.spawn(async move {
            if let Some(mode) = mode
                && let Err(error) = inner.target.chmod(req, path.clone(), fh, mode).await
            {
                reply.error(error.into());
                return;
            }

            if (uid.is_some() || gid.is_some())
                && let Err(error) = inner.target.chown(req, path.clone(), fh, uid, gid).await
            {
                reply.error(error.into());
                return;
            }

            if let Some(size) = size
                && let Err(error) = inner.target.truncate(req, path.clone(), fh, size).await
            {
                reply.error(error.into());
                return;
            }

            if atime.is_some() || mtime.is_some() {
                let atime = atime.map(TimeOrNowExt::time);
                let mtime = mtime.map(TimeOrNowExt::time);
                if let Err(error) = inner
                    .target
                    .utimens(req, path.clone(), fh, atime, mtime)
                    .await
                {
                    reply.error(error.into());
                    return;
                }
            }

            if (crtime.is_some() || chgtime.is_some() || bkuptime.is_some() || flags.is_some())
                && let Err(error) = inner
                    .target
                    .utimens_macos(
                        req,
                        path.clone(),
                        fh,
                        crtime,
                        chgtime,
                        bkuptime,
                        flags.map(|flags| flags.bits()),
                    )
                    .await
            {
                reply.error(error.into());
                return;
            }

            match inner
                .target
                .getattr(req, EntryRef::Resolved(path), fh)
                .await
            {
                Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn readlink(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyData) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("readlink: {:?}", path);

        self.spawn(async move {
            match inner.target.readlink(req, path).await {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn mknod(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("mknod: {:?}", entry);

        self.spawn(async move {
            match inner.target.mknod(req, entry, mode, rdev).await {
                Ok((ttl, attr)) => {
                    if let Some((ino, generation)) = inner.add_leaf(parent, &name) {
                        reply.entry(
                            &ttl,
                            &fuse_fileattr(attr, INodeNo(ino)),
                            Generation(generation),
                        );
                    } else {
                        reply.error(Errno::EINVAL);
                    }
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn mkdir(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("mkdir: {:?} (mode={:#o})", entry, mode);

        self.spawn(async move {
            match inner.target.mkdir(req, entry, mode).await {
                Ok((ttl, attr)) => {
                    if let Some((ino, generation)) = inner.add_dir(parent, &name) {
                        reply.entry(
                            &ttl,
                            &fuse_fileattr(attr, INodeNo(ino)),
                            Generation(generation),
                        );
                    } else {
                        reply.error(Errno::EINVAL);
                    }
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn unlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("unlink: {:?}", entry);

        self.spawn(async move {
            match inner.target.unlink(req, entry).await {
                Ok(()) => {
                    inner.inode_unlink(parent, &name);
                    reply.ok();
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn rmdir(&self, req: &fuser::Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("rmdir: {:?}", entry);

        self.spawn(async move {
            match inner.target.rmdir(req, entry).await {
                Ok(()) => {
                    inner.inode_unlink(parent, &name);
                    reply.ok();
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn symlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        link: &Path,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let name = name.to_os_string();
        let link = link.to_path_buf();
        let req = req.info();
        debug!("symlink: {:?} -> {:?}", entry, link);

        self.spawn(async move {
            match inner.target.symlink(req, entry, link).await {
                Ok((ttl, attr)) => {
                    if let Some((ino, generation)) = inner.add_leaf(parent, &name) {
                        reply.entry(
                            &ttl,
                            &fuse_fileattr(attr, INodeNo(ino)),
                            Generation(generation),
                        );
                    } else {
                        reply.error(Errno::EINVAL);
                    }
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn rename(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let entry = resolve_from_parent!(inner, parent, name, reply);
        let new_entry = resolve_from_parent!(inner, newparent, newname, reply);
        let name = name.to_os_string();
        let newname = newname.to_os_string();
        let req = req.info();
        debug!("rename: {:?} -> {:?}", entry, new_entry);

        self.spawn(async move {
            match inner.target.rename(req, entry, new_entry).await {
                Ok(()) => {
                    if inner
                        .inode_rename(parent, &name, newparent, &newname)
                        .is_none()
                    {
                        log::error!("inode rename {parent} {:?} {newparent} {:?}", name, newname);
                    }
                    reply.ok();
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn link(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let new_entry = resolve_from_parent!(inner, newparent, newname, reply);
        let newname = newname.to_os_string();
        let req = req.info();
        debug!("link: {:?} -> {:?}", path, new_entry);

        self.spawn(async move {
            match inner.target.link(req, path, new_entry).await {
                Ok((ttl, attr)) => {
                    if let Some((new_ino, generation)) = inner.add_leaf(newparent, &newname) {
                        reply.entry(
                            &ttl,
                            &fuse_fileattr(attr, INodeNo(new_ino)),
                            Generation(generation),
                        );
                    } else {
                        reply.error(Errno::EINVAL);
                    }
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn open(&self, req: &fuser::Request, ino: INodeNo, flags: OpenFlags, reply: fuser::ReplyOpen) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("open: {:?}", path);

        self.spawn(async move {
            match inner.target.open(req, path, flags.0 as u32).await {
                Ok((fh, flags)) => {
                    reply.opened(FileHandle(fh), FopenFlags::from_bits_retain(flags));
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn read(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyData,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);

        self.spawn(async move {
            match inner.target.read(req, path, fh.0, offset, size).await {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn write(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let data = data.to_vec();
        let req = req.info();
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);

        self.spawn(async move {
            match inner
                .target
                .write(req, path, fh.0, offset, data, flags.0 as u32)
                .await
            {
                Ok(written) => reply.written(written),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn flush(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("flush: {:?}", path);

        self.spawn(async move {
            match inner.target.flush(req, path, fh.0, lock_owner.0).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn release(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("release: {:?}", path);

        self.spawn(async move {
            match inner
                .target
                .release(
                    req,
                    path,
                    fh.0,
                    flags.0 as u32,
                    lock_owner.map(|owner| owner.0).unwrap_or(0),
                    flush,
                )
                .await
            {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn fsync(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("fsync: {:?}", path);

        self.spawn(async move {
            match inner.target.fsync(req, path, fh.0, datasync).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn opendir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        flags: OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("opendir: {:?}", path);

        self.spawn(async move {
            match inner.target.opendir(req, path, flags.0 as u32).await {
                Ok((fh, flags)) => {
                    let cache_key = inner.directory_cache.write().unwrap().new_entry(fh);
                    inner
                        .readdir_cache
                        .write()
                        .unwrap()
                        .insert(cache_key, tokio::sync::Mutex::new(None));
                    #[cfg(feature = "legacy_readdir")]
                    inner
                        .legacy_readdir_cache
                        .write()
                        .unwrap()
                        .insert(cache_key, tokio::sync::Mutex::new(None));
                    reply.opened(FileHandle(cache_key), FopenFlags::from_bits_retain(flags));
                }
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn readdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: fuser::ReplyDirectory,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("readdir: {:?} @ {}", path, offset);

        let parent_inode = if ino == INodeNo::ROOT {
            ino
        } else {
            match inner.get_parent_inode(ino) {
                Some(inode) => INodeNo(inode),
                None => {
                    error!("readdir: unable to get parent inode for {:?}", path);
                    reply.error(Errno::EIO);
                    return;
                }
            }
        };

        self.spawn(async move {
            #[cfg(feature = "legacy_readdir")]
            {
                let Some(slot) = inner.legacy_readdir_cache.read().unwrap().get(fh.0) else {
                    reply.error(Errno::EINVAL);
                    return;
                };
                let mut slot = slot.lock().await;
                if slot.is_none() {
                    let real_fh = real_fh_or_reply_error!(
                        inner.directory_cache.read().unwrap().real_fh(fh.0),
                        reply
                    );
                    let producer = inner.target.legacy_readdir(req, path, real_fh);
                    *slot = Some(ReaddirState::new(Box::pin(producer)));
                }
                inner
                    .fill_legacy_readdir(slot.as_mut().unwrap(), ino, parent_inode, offset, reply)
                    .await;
            }

            #[cfg(not(feature = "legacy_readdir"))]
            {
                let Some(slot) = inner.readdir_cache.read().unwrap().get(fh.0) else {
                    reply.error(Errno::EINVAL);
                    return;
                };
                let mut slot = slot.lock().await;
                if slot.is_none() {
                    let real_fh = real_fh_or_reply_error!(
                        inner.directory_cache.read().unwrap().real_fh(fh.0),
                        reply
                    );
                    let producer = inner.target.readdir(req, path, real_fh);
                    *slot = Some(ReaddirState::new(Box::pin(producer)));
                }
                inner
                    .fill_readdir(
                        slot.as_mut().unwrap(),
                        ino,
                        parent_inode,
                        offset,
                        ReaddirReply::Plain(reply),
                    )
                    .await;
            }
        });
    }

    fn readdirplus(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("readdirplus: {:?} @ {}", path, offset);

        let parent_inode = if ino == INodeNo::ROOT {
            ino
        } else {
            match inner.get_parent_inode(ino) {
                Some(inode) => INodeNo(inode),
                None => {
                    error!("readdirplus: unable to get parent inode for {:?}", path);
                    reply.error(Errno::EIO);
                    return;
                }
            }
        };

        let Some(slot) = inner.readdir_cache.read().unwrap().get(fh.0) else {
            reply.error(Errno::EINVAL);
            return;
        };

        self.spawn(async move {
            let mut slot = slot.lock().await;
            if slot.is_none() {
                let real_fh = real_fh_or_reply_error!(
                    inner.directory_cache.read().unwrap().real_fh(fh.0),
                    reply
                );
                let producer = inner.target.readdir(req, path, real_fh);
                *slot = Some(ReaddirState::new(Box::pin(producer)));
            }
            inner
                .fill_readdir(
                    slot.as_mut().unwrap(),
                    ino,
                    parent_inode,
                    offset,
                    ReaddirReply::Plus(reply),
                )
                .await;
        });
    }

    fn releasedir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let real_fh =
            real_fh_or_reply_error!(inner.directory_cache.read().unwrap().real_fh(fh.0), reply);
        let req = req.info();
        debug!("releasedir: {:?}", path);

        self.spawn(async move {
            match inner
                .target
                .releasedir(req, path, real_fh, flags.0 as u32)
                .await
            {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
            inner.directory_cache.write().unwrap().delete(fh.0);
            inner.readdir_cache.write().unwrap().delete(fh.0);
            #[cfg(feature = "legacy_readdir")]
            inner.legacy_readdir_cache.write().unwrap().delete(fh.0);
        });
    }

    fn fsyncdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let real_fh =
            real_fh_or_reply_error!(inner.directory_cache.read().unwrap().real_fh(fh.0), reply);
        let req = req.info();
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);

        self.spawn(async move {
            match inner.target.fsyncdir(req, path, real_fh, datasync).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn statfs(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyStatfs) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("statfs: {:?}", path);

        self.spawn(async move {
            match inner.target.statfs(req, path).await {
                Ok(statfs) => reply.statfs(
                    statfs.blocks,
                    statfs.bfree,
                    statfs.bavail,
                    statfs.files,
                    statfs.ffree,
                    statfs.bsize,
                    statfs.namelen,
                    statfs.frsize,
                ),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let name = name.to_os_string();
        let value = value.to_vec();
        let req = req.info();
        debug!("setxattr: {:?} {:?}", path, name);

        self.spawn(async move {
            match inner
                .target
                .setxattr(req, path, name, value, flags as u32, position)
                .await
            {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn getxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("getxattr: {:?} {:?}", path, name);

        self.spawn(async move {
            match inner.target.getxattr(req, path, name, size).await {
                Ok(Xattr::Size(size)) => reply.size(size),
                Ok(Xattr::Data(data)) => reply.data(&data),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn listxattr(&self, req: &fuser::Request, ino: INodeNo, size: u32, reply: fuser::ReplyXattr) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("listxattr: {:?}", path);

        self.spawn(async move {
            match inner.target.listxattr(req, path, size).await {
                Ok(Xattr::Size(size)) => reply.size(size),
                Ok(Xattr::Data(data)) => reply.data(&data),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn removexattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let name = name.to_os_string();
        let req = req.info();
        debug!("removexattr: {:?}, {:?}", path, name);

        self.spawn(async move {
            match inner.target.removexattr(req, path, name).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn access(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        mask: AccessFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("access: {:?}, mask={:#o}", path, mask.bits());

        self.spawn(async move {
            match inner.target.access(req, path, mask.bits() as u32).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    fn create(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let inner = Arc::clone(&self.inner);
        let (created, path, generation) = match inner.create_or_get_leaf(parent, name) {
            Some((created, ino, generation)) => (
                created,
                get_resolved_path!(inner, INodeNo(ino), reply),
                generation,
            ),
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };
        let name = name.to_os_string();
        let req = req.info();
        debug!("create: {:?} (mode={:#o}, flags={:#x})", path, mode, flags);

        self.spawn(async move {
            match inner
                .target
                .create(req, path.clone(), mode, flags as u32)
                .await
            {
                Ok(create) => {
                    if !created {
                        inner.lookup(path.ino());
                    }
                    let attr = fuse_fileattr(create.attr, INodeNo(path.ino()));
                    reply.created(
                        &create.ttl,
                        &attr,
                        Generation(generation),
                        FileHandle(create.fh),
                        FopenFlags::from_bits_retain(create.flags),
                    );
                }
                Err(error) => {
                    if created {
                        inner.inode_unlink(parent, &name);
                        inner.forget(INodeNo(path.ino()), 1);
                    }
                    reply.error(error.into());
                }
            }
        });
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, req: &fuser::Request, name: &OsStr, reply: fuser::ReplyEmpty) {
        let inner = Arc::clone(&self.inner);
        let name = name.to_os_string();
        let req = req.info();
        debug!("setvolname: {:?}", name);

        self.spawn(async move {
            match inner.target.setvolname(req, name).await {
                Ok(()) => reply.ok(),
                Err(error) => reply.error(error.into()),
            }
        });
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyXTimes) {
        let inner = Arc::clone(&self.inner);
        let path = get_resolved_path!(inner, ino, reply);
        let req = req.info();
        debug!("getxtimes: {:?}", path);

        self.spawn(async move {
            match inner.target.getxtimes(req, path).await {
                Ok(xtimes) => reply.xtimes(xtimes.bkuptime, xtimes.crtime),
                Err(error) => reply.error(error.into()),
            }
        });
    }
}
