// FuserNG :: A wrapper around FUSE that presents paths instead of inodes.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use fuser::{
    AccessFlags, Errno, FileHandle, FopenFlags, Generation, INodeNo, LockOwner, OpenFlags,
    RenameFlags, TimeOrNow, WriteFlags,
};

use crate::FileType;
use crate::directory_cache::*;
use crate::inode_table::{InodeTable, InodeToPath};
use crate::types::*;

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
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => SystemTime::now(),
        }
    }
}

#[derive(Debug)]
pub struct FuserNG<T> {
    target: Arc<T>,
    table: RwLock<InodeTable>,
    directory_cache: RwLock<DirectoryCache>,
}

impl<T: Filesystem + Sync + Send + 'static> FuserNG<T> {
    pub fn new(target_fs: T) -> FuserNG<T> {
        FuserNG {
            target: Arc::new(target_fs),
            table: InodeTable::new().into(),
            directory_cache: DirectoryCache::new().into(),
        }
    }
    fn add_or_get_dir(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table
            .try_write()
            .unwrap()
            .add_or_get_dir(parent.0, name)
    }
    fn add_or_get_leaf(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table
            .try_write()
            .unwrap()
            .add_or_get_leaf(parent.0, name)
    }
    fn lookup(&self, ino: u64) {
        self.table.try_write().unwrap().lookup(ino);
    }
    fn forget(&self, ino: INodeNo, n: u64) -> u64 {
        self.table.try_write().unwrap().forget(ino.0, n)
    }
    fn add_leaf(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.try_write().unwrap().add_leaf(parent.0, name)
    }
    fn add_dir(&self, parent: INodeNo, name: &OsStr) -> Option<(u64, u64)> {
        self.table.try_write().unwrap().add_dir(parent.0, name)
    }
}

macro_rules! get_entry_name {
    ($s:expr, $ino:expr, $reply:expr) => {
        if let Some(path) = $s.table.try_read().unwrap().get_path($ino.0) {
            path
        } else {
            $reply.error(Errno::EINVAL);
            return;
        }
    };
}

macro_rules! resolve_from_parent {
    ($s:expr, $ino:expr, $name:expr, $reply:expr) => {
        if let Some(path) = $s
            .table
            .try_read()
            .unwrap()
            .resolve_from_parent($ino.0, $name.into())
        {
            path
        } else {
            $reply.error(Errno::EINVAL);
            return;
        }
    };
}

macro_rules! get_resolved_path {
    ($s:expr, $ino:expr, $reply:expr) => {{ get_entry_name!($s, $ino, $reply).with($ino.0) }};
}

impl<T: Filesystem + Sync + Send + 'static> fuser::Filesystem for FuserNG<T> {
    fn init(
        &mut self,
        req: &fuser::Request,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), std::io::Error> {
        debug!("init");
        self.target.init(req.info(), config)
    }

    fn destroy(&mut self) {
        debug!("destroy");
        self.target.destroy();
    }

    fn lookup(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let path = resolve_from_parent!(self, parent, name, reply);
        debug!("lookup: {:?}", path);
        //let parent_path = get_folder_path!(self, parent, reply);
        //debug!("lookup: {:?}, {:?}", parent_path, name);
        //let path = Arc::new((*parent_path).clone().join(name));
        match self.target.getattr(req.info(), &path, None) {
            Ok((ttl, attr)) => {
                let value = if attr.kind == FileType::Directory {
                    self.add_or_get_dir(parent, name)
                } else {
                    self.add_or_get_leaf(parent, name)
                };
                if let Some((ino, generation)) = value {
                    self.lookup(ino);
                    reply.entry(
                        &ttl,
                        &fuse_fileattr(attr, INodeNo(ino)),
                        Generation(generation),
                    );
                } else {
                    reply.error(Errno::EINVAL)
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn forget(&self, _req: &fuser::Request, ino: INodeNo, nlookup: u64) {
        let lookups = self.forget(ino, nlookup);
        let path = self
            .table
            .try_read()
            .unwrap()
            .get_path(ino.0)
            .unwrap_or_else(|| EntryName::new(OsStr::new("").into(), OsString::from("[unknown]")));
        debug!(
            "forget: inode {} ({:?}) now at {} lookups",
            ino, path, lookups
        );
    }

    fn getattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: Option<fuser::FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        let path = get_entry_name!(self, ino, reply);
        debug!("getattr: {:?}", path);
        match self.target.getattr(req.info(), &path, fh.map(|fh| fh.0)) {
            Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn setattr(
        &self,
        req: &fuser::Request,               // passed to all
        ino: INodeNo,                       // translated to path; passed to all
        mode: Option<u32>,                  // chmod
        uid: Option<u32>,                   // chown
        gid: Option<u32>,                   // chown
        size: Option<u64>,                  // truncate
        atime: Option<TimeOrNow>,           // utimens
        mtime: Option<TimeOrNow>,           // utimens
        _ctime: Option<SystemTime>,         // ? TODO
        fh: Option<fuser::FileHandle>,      // passed to all
        crtime: Option<SystemTime>,         // utimens_osx  (OS X only)
        chgtime: Option<SystemTime>,        // utimens_osx  (OS X only)
        bkuptime: Option<SystemTime>,       // utimens_osx  (OS X only)
        flags: Option<fuser::BsdFileFlags>, // utimens_osx  (OS X only)
        reply: fuser::ReplyAttr,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("setattr: {:?}", path);

        debug!("\tino:\t{:?}", ino);
        debug!("\tmode:\t{:?}", mode);
        debug!("\tuid:\t{:?}", uid);
        debug!("\tgid:\t{:?}", gid);
        debug!("\tsize:\t{:?}", size);
        debug!("\tatime:\t{:?}", atime);
        debug!("\tmtime:\t{:?}", mtime);
        debug!("\tfh:\t{:?}", fh);

        // TODO: figure out what C FUSE does when only some of these are implemented.

        if let Some(mode) = mode
            && let Err(e) = self
                .target
                .chmod(req.info(), &path, fh.map(|fh| fh.0), mode)
        {
            reply.error(e.into());
            return;
        }

        if (uid.is_some() || gid.is_some())
            && let Err(e) = self
                .target
                .chown(req.info(), &path, fh.map(|fh| fh.0), uid, gid)
        {
            reply.error(e.into());
            return;
        }

        if let Some(size) = size
            && let Err(e) = self
                .target
                .truncate(req.info(), &path, fh.map(|fh| fh.0), size)
        {
            reply.error(e.into());
            return;
        }

        if atime.is_some() || mtime.is_some() {
            let atime = atime.map(TimeOrNowExt::time);
            let mtime = mtime.map(TimeOrNowExt::time);
            if let Err(e) = self
                .target
                .utimens(req.info(), &path, fh.map(|fh| fh.0), atime, mtime)
            {
                reply.error(e.into());
                return;
            }
        }

        if (crtime.is_some() || chgtime.is_some() || bkuptime.is_some() || flags.is_some())
            && let Err(e) = self.target.utimens_macos(
                req.info(),
                &path,
                fh.map(|fh| fh.0),
                crtime,
                chgtime,
                bkuptime,
                flags.map(|flags| flags.bits()),
            )
        {
            reply.error(e.into());
            return;
        }

        match self
            .target
            .getattr(req.info(), &path.entry_name(), fh.map(|fh| fh.0))
        {
            Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readlink(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyData) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("readlink: {:?}", path);
        match self.target.readlink(req.info(), &path) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e.into()),
        }
    }

    fn mknod(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("mknod: {:?}", entry);
        match self.target.mknod(req.info(), &entry, mode, rdev) {
            Ok((ttl, attr)) => {
                if let Some((ino, generation)) = self.add_leaf(parent, name) {
                    reply.entry(
                        &ttl,
                        &fuse_fileattr(attr, INodeNo(ino)),
                        Generation(generation),
                    )
                } else {
                    reply.error(Errno::from_i32(libc::EINVAL))
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn mkdir(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        reply: fuser::ReplyEntry,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("mkdir: {:?} (mode={:#o})", entry, mode);
        match self.target.mkdir(req.info(), &entry, mode) {
            Ok((ttl, attr)) => {
                if let Some((ino, generation)) = self.add_dir(parent, name) {
                    reply.entry(
                        &ttl,
                        &fuse_fileattr(attr, INodeNo(ino)),
                        Generation(generation),
                    )
                } else {
                    reply.error(Errno::from_i32(libc::EINVAL))
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("unlink: {:?}", entry);
        match self.target.unlink(req.info(), &entry) {
            Ok(()) => {
                self.table.try_write().unwrap().unlink(parent.0, name);
                reply.ok()
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(&self, req: &fuser::Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("rmdir: {:?}", entry);
        match self.target.rmdir(req.info(), &entry) {
            Ok(()) => {
                self.table.try_write().unwrap().unlink(parent.0, name);
                reply.ok()
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn symlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        link: &Path,
        reply: fuser::ReplyEntry,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("symlink: {:?} -> {:?}", entry, link);
        match self.target.symlink(req.info(), &entry, link) {
            Ok((ttl, attr)) => {
                if let Some((ino, generation)) = self.add_leaf(parent, name) {
                    reply.entry(
                        &ttl,
                        &fuse_fileattr(attr, INodeNo(ino)),
                        Generation(generation),
                    )
                } else {
                    reply.error(Errno::EINVAL)
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn rename(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags, // TODO
        reply: fuser::ReplyEmpty,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        let new_entry = resolve_from_parent!(self, newparent, newname, reply);
        debug!("rename: {:?} -> {:?}", entry, new_entry);
        match self.target.rename(req.info(), &entry, &new_entry) {
            Ok(()) => {
                self.table
                    .try_write()
                    .unwrap()
                    .rename(parent.0, name, newparent.0, newname);
                reply.ok()
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn link(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        let new_entry = resolve_from_parent!(self, newparent, newname, reply);
        debug!("link: {:?} -> {:?}", path, new_entry);
        match self.target.link(req.info(), &path, &new_entry) {
            Ok((ttl, attr)) => {
                // NOTE: this results in the new link having a different inode from the original.
                // This is needed because our inode table is a 1:1 map between paths and inodes.
                if let Some((new_ino, generation)) = self
                    .table
                    .try_write()
                    .unwrap()
                    .add_leaf(newparent.0, newname)
                {
                    reply.entry(
                        &ttl,
                        &fuse_fileattr(attr, INodeNo(new_ino)),
                        Generation(generation),
                    );
                } else {
                    reply.error(Errno::EINVAL);
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn open(&self, req: &fuser::Request, ino: INodeNo, flags: OpenFlags, reply: fuser::ReplyOpen) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("open: {:?}", path);
        match self.target.open(req.info(), &path, flags.0 as u32) {
            // TODO: change flags to i32
            Ok((fh, flags)) => reply.opened(FileHandle(fh), FopenFlags::from_bits_retain(flags)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn read(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,              // TODO
        _lock_owner: Option<LockOwner>, // TODO
        reply: fuser::ReplyData,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        self.target
            .read(req.info(), &path, fh.0, offset, size, |result| {
                match result {
                    Ok(data) => reply.data(data),
                    Err(e) => reply.error(e.into()),
                }
                CallbackResult {
                    _private: std::marker::PhantomData {},
                }
            });
    }

    fn write(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags, // TODO
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>, // TODO
        reply: fuser::ReplyWrite,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        // The target API owns the write buffer, while fuser gives us borrowed request data.
        let data_buf = Vec::from(data);
        match self
            .target
            .write(req.info(), &path, fh.0, offset, data_buf, flags.0 as u32)
        {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(e.into()),
        }
    }

    fn flush(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("flush: {:?}", path);
        match self.target.flush(req.info(), &path, fh.0, lock_owner.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

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
        let path = get_resolved_path!(self, ino, reply);
        debug!("release: {:?}", path);
        match self.target.release(
            req.info(),
            &path,
            fh.0,
            flags.0 as u32,
            lock_owner.map(|owner| owner.0).unwrap_or(0), /* TODO */
            flush,
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn fsync(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("fsync: {:?}", path);
        match self.target.fsync(req.info(), &path, fh.0, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn opendir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        flags: OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("opendir: {:?}", path);
        match self.target.opendir(req.info(), &path, flags.0 as u32) {
            Ok((fh, flags)) => {
                let dcache_key = self.directory_cache.try_write().unwrap().new_entry(fh);
                reply.opened(FileHandle(dcache_key), FopenFlags::from_bits_retain(flags));
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn readdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("readdir: {:?} @ {}", path, offset);

        let parent_inode = if ino == INodeNo::ROOT {
            ino
        } else {
            match self.table.try_read().unwrap().get_parent_inode(ino.0) {
                Some(inode) => INodeNo(inode),
                None => {
                    error!("readdir: unable to get parent inode for {:?}", &path);
                    reply.error(Errno::EIO);
                    return;
                }
            }
        };

        let cached_entries = {
            self.directory_cache
                .try_write()
                .unwrap()
                .get_mut(fh.0)
                .entries
                .clone()
        };

        let entries = match cached_entries {
            Some(entries) => entries,
            None => {
                let real_fh = self.directory_cache.try_read().unwrap().real_fh(fh.0);
                debug!("entries not yet fetched; requesting with fh {}", real_fh);
                match self.target.readdir(req.info(), &path, real_fh) {
                    Ok(entries) => {
                        self.directory_cache
                            .try_write()
                            .unwrap()
                            .get_mut(fh.0)
                            .entries = Some(entries.clone());
                        entries
                    }
                    Err(e) => {
                        reply.error(e.into());
                        return;
                    }
                }
            }
        };

        debug!("directory has {} entries", entries.len());

        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let entry_inode = if entry.name == Path::new(".") {
                ino
            } else if entry.name == Path::new("..") {
                parent_inode
            } else {
                // Don't bother looking in the inode table for the entry; FUSE doesn't pre-
                // populate its inode cache with this value, so subsequent access to these
                // files is going to involve it issuing a LOOKUP operation anyway.
                INodeNo(!1u64)
            };

            debug!(
                "readdir: adding entry #{}, {:?}",
                offset + index as u64,
                entry.name
            );

            let buffer_full: bool = reply.add(
                entry_inode,
                offset + index as u64 + 1,
                entry.kind,
                entry.name.as_os_str(),
            );

            if buffer_full {
                debug!("readdir: reply buffer is full");
                break;
            }
        }

        reply.ok();
    }

    fn releasedir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("releasedir: {:?}", path);
        let real_fh = self.directory_cache.try_read().unwrap().real_fh(fh.0);
        match self
            .target
            .releasedir(req.info(), &path, real_fh, flags.0 as u32)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
        self.directory_cache.try_write().unwrap().delete(fh.0);
    }

    fn fsyncdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);
        let real_fh = self.directory_cache.try_read().unwrap().real_fh(fh.0);
        match self.target.fsyncdir(req.info(), &path, real_fh, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn statfs(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyStatfs) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("statfs: {:?}", path);
        match self.target.statfs(req.info(), &path) {
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
            Err(e) => reply.error(e.into()),
        }
    }

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
        let path = get_resolved_path!(self, ino, reply);
        debug!(
            "setxattr: {:?} {:?} ({} bytes, flags={:#x}, pos={:#x}",
            path,
            name,
            value.len(),
            flags,
            position
        );
        match self
            .target
            .setxattr(req.info(), &path, name, value, flags as u32, position)
        {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn getxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("getxattr: {:?} {:?}", path, name);
        match self.target.getxattr(req.info(), &path, name, size) {
            Ok(Xattr::Size(size)) => {
                debug!("getxattr: sending size {}", size);
                reply.size(size)
            }
            Ok(Xattr::Data(vec)) => {
                debug!("getxattr: sending {} bytes", vec.len());
                reply.data(&vec)
            }
            Err(e) => {
                debug!("getxattr: error {}", e);
                reply.error(e.into())
            }
        }
    }

    fn listxattr(&self, req: &fuser::Request, ino: INodeNo, size: u32, reply: fuser::ReplyXattr) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("listxattr: {:?}", path);
        match self.target.listxattr(req.info(), &path, size) {
            Ok(Xattr::Size(size)) => {
                debug!("listxattr: sending size {}", size);
                reply.size(size)
            }
            Ok(Xattr::Data(vec)) => {
                debug!("listxattr: sending {} bytes", vec.len());
                reply.data(&vec)
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn removexattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("removexattr: {:?}, {:?}", path, name);
        match self.target.removexattr(req.info(), &path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn access(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        mask: AccessFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("access: {:?}, mask={:#o}", path, mask.bits());
        match self.target.access(req.info(), &path, mask.bits() as u32) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn create(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let entry = resolve_from_parent!(self, parent, name, reply);
        debug!("create: {:?} (mode={:#o}, flags={:#x})", entry, mode, flags);
        match self.target.create(req.info(), &entry, mode, flags as u32) {
            Ok(create) => {
                if let Some((ino, generation)) = self.add_leaf(parent, name) {
                    let attr = fuse_fileattr(create.attr, INodeNo(ino));
                    reply.created(
                        &create.ttl,
                        &attr,
                        Generation(generation),
                        FileHandle(create.fh),
                        FopenFlags::from_bits_retain(create.flags),
                    );
                } else {
                    reply.error(Errno::EINVAL);
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    // getlk

    // setlk

    // bmap

    #[cfg(target_os = "macos")]
    fn setvolname(&self, req: &fuser::Request, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!("setvolname: {:?}", name);
        match self.target.setvolname(req.info(), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    // exchange (macOS only, undocumented)

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, req: &fuser::Request, ino: INodeNo, reply: fuser::ReplyXTimes) {
        let path = get_resolved_path!(self, ino, reply);
        debug!("getxtimes: {:?}", path);
        match self.target.getxtimes(req.info(), &path) {
            Ok(xtimes) => {
                reply.xtimes(xtimes.bkuptime, xtimes.crtime);
            }
            Err(e) => reply.error(e.into()),
        }
    }
}
