#![cfg(target_os = "linux")]

use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> io::Result<Self> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        for attempt in 0..100 {
            let path = std::env::temp_dir().join(format!(
                "fuser_ng_{label}_{}_{}_{}",
                std::process::id(),
                now,
                attempt
            ));

            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err),
            }
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "unable to allocate temporary directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct PassthroughProcess {
    child: Child,
    mountpoint: PathBuf,
}

impl Drop for PassthroughProcess {
    fn drop(&mut self) {
        let _ = Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mountpoint)
            .status();
        let _ = Command::new("fusermount")
            .arg("-u")
            .arg(&self.mountpoint)
            .status();
        let _ = Command::new("umount").arg(&self.mountpoint).status();

        for _ in 0..20 {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_passthrough(backing: &Path, mountpoint: &Path) -> io::Result<PassthroughProcess> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let child = Command::new(cargo)
        .args(["run", "--quiet", "-p", "passthrufs", "--"])
        .arg(backing)
        .arg(mountpoint)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let mut process = PassthroughProcess {
        child,
        mountpoint: mountpoint.to_path_buf(),
    };

    let probe = mountpoint.join("existing/source.txt");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if probe.is_file() {
            return Ok(process);
        }

        if let Some(status) = process.child.try_wait()? {
            return Err(io::Error::other(format!(
                "passthrufs exited before the mount was ready: {status}"
            )));
        }

        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for passthrufs mount",
            ));
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn path_cstring(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes().to_vec()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path contains an interior NUL byte: {path:?}"),
        )
    })
}

fn cvt_unit(result: libc::c_int) -> io::Result<()> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn assert_error<T>(result: io::Result<T>, context: &str) {
    assert!(result.is_err(), "{context} unexpectedly succeeded");
}

fn assert_error_kind<T>(result: io::Result<T>, kind: io::ErrorKind, context: &str) {
    match result {
        Ok(_) => panic!("{context} unexpectedly succeeded"),
        Err(err) => assert_eq!(
            kind,
            err.kind(),
            "{context} failed with an unexpected error: {err:?}"
        ),
    }
}

fn assert_access(path: &Path, mode: libc::c_int) -> io::Result<()> {
    let path = path_cstring(path)?;
    cvt_unit(unsafe { libc::access(path.as_ptr(), mode) })
}

fn assert_file_content(path: &Path, expected: &[u8]) -> io::Result<()> {
    let mut data = Vec::new();
    File::open(path)?.read_to_end(&mut data)?;
    assert_eq!(expected, data.as_slice());
    Ok(())
}

fn read_dir_names(path: &Path) -> io::Result<Vec<OsString>> {
    fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect()
}

fn chown_current_group(path: &Path) -> io::Result<()> {
    let path = path_cstring(path)?;
    let result = cvt_unit(unsafe {
        libc::chown(
            path.as_ptr(),
            u32::MAX as libc::uid_t,
            libc::getgid() as libc::gid_t,
        )
    });

    // Some systems still reject no-owner-change chown without extra privileges.
    match result {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == Some(libc::EPERM) => Ok(()),
        Err(err) => Err(err),
    }
}

fn set_times(path: &Path) -> io::Result<()> {
    let path = path_cstring(path)?;
    let times = [
        libc::timespec {
            tv_sec: 1_700_000_000,
            tv_nsec: 123_000_000,
        },
        libc::timespec {
            tv_sec: 1_700_000_100,
            tv_nsec: 456_000_000,
        },
    ];

    cvt_unit(unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            path.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    })
}

fn statfs(path: &Path) -> io::Result<libc::statfs> {
    let path = path_cstring(path)?;
    let mut stat = unsafe { std::mem::zeroed() };
    cvt_unit(unsafe { libc::statfs(path.as_ptr(), &mut stat) })?;
    Ok(stat)
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    let path = path_cstring(path)?;
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }

    let sync_result = cvt_unit(unsafe { libc::fsync(fd) });
    let close_result = cvt_unit(unsafe { libc::close(fd) });
    sync_result.and(close_result)
}

fn mkfifo(path: &Path) -> io::Result<()> {
    let path = path_cstring(path)?;
    cvt_unit(unsafe { libc::mkfifo(path.as_ptr(), 0o644 as libc::mode_t) })
}

fn is_xattr_unsupported(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(errno)
            if errno == libc::ENOTSUP
                || errno == libc::EOPNOTSUPP
                || errno == libc::ENOSYS
    )
}

fn set_xattr(path: &Path, name: &[u8], value: &[u8]) -> io::Result<()> {
    let path = path_cstring(path)?;
    let name = CString::new(name).unwrap();
    cvt_unit(unsafe {
        libc::setxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_ptr().cast(),
            value.len(),
            0,
        )
    })
}

fn get_xattr(path: &Path, name: &[u8]) -> io::Result<Vec<u8>> {
    let path = path_cstring(path)?;
    let name = CString::new(name).unwrap();
    let size = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if size == -1 {
        return Err(io::Error::last_os_error());
    }

    let mut data = Vec::<u8>::with_capacity(size as usize);
    let read = unsafe {
        libc::getxattr(
            path.as_ptr(),
            name.as_ptr(),
            data.as_mut_ptr().cast(),
            data.capacity(),
        )
    };
    if read == -1 {
        return Err(io::Error::last_os_error());
    }

    unsafe { data.set_len(read as usize) };
    Ok(data)
}

fn list_xattr(path: &Path) -> io::Result<Vec<u8>> {
    let path = path_cstring(path)?;
    let size = unsafe { libc::listxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
    if size == -1 {
        return Err(io::Error::last_os_error());
    }

    let mut data = Vec::<u8>::with_capacity(size as usize);
    let read = unsafe { libc::listxattr(path.as_ptr(), data.as_mut_ptr().cast(), data.capacity()) };
    if read == -1 {
        return Err(io::Error::last_os_error());
    }

    unsafe { data.set_len(read as usize) };
    Ok(data)
}

fn remove_xattr(path: &Path, name: &[u8]) -> io::Result<()> {
    let path = path_cstring(path)?;
    let name = CString::new(name).unwrap();
    cvt_unit(unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) })
}

fn check_xattrs(path: &Path) -> io::Result<()> {
    let name = b"user.fuser_ng_test";
    match set_xattr(path, name, b"value") {
        Ok(()) => {}
        Err(err) if is_xattr_unsupported(&err) => return Ok(()),
        Err(err) => return Err(err),
    }

    assert_eq!(b"value", get_xattr(path, name)?.as_slice());

    let names = list_xattr(path)?;
    assert!(
        names
            .split(|byte| *byte == 0)
            .any(|listed| listed == &name[..])
    );

    remove_xattr(path, name)?;
    let missing = get_xattr(path, name).unwrap_err();
    assert_eq!(Some(libc::ENODATA), missing.raw_os_error());
    Ok(())
}

#[test]
//#[ignore = "requires a working FUSE setup and permission to mount"]
fn passthrough_exercises_fuse_methods() -> io::Result<()> {
    let backing = TempDir::new("backing")?;
    let mount = TempDir::new("mount")?;

    fs::create_dir(backing.path().join("existing"))?;
    fs::write(backing.path().join("existing/source.txt"), b"initial")?;

    let _process = start_passthrough(backing.path(), mount.path())?;
    let root = mount.path();

    assert!(fs::metadata(root)?.is_dir());
    assert!(fs::metadata(root.join("existing/source.txt"))?.is_file());
    assert_access(&root.join("existing/source.txt"), libc::R_OK)?;
    assert_error(
        File::open(root.join("missing.txt")),
        "opening a missing file",
    );

    let entries = read_dir_names(root)?;
    assert!(entries.iter().any(|name| name == OsStr::new("existing")));

    let work = root.join("work");
    fs::create_dir(&work)?;
    assert_error(fs::create_dir(&work), "creating an existing directory");

    let created_path = work.join("created.txt");
    let mut created = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&created_path)?;
    created.write_all(b"hello world")?;
    created.flush()?;
    created.sync_data()?;
    created.sync_all()?;
    created.seek(SeekFrom::Start(6))?;

    let mut tail = String::new();
    created.read_to_string(&mut tail)?;
    assert_eq!("world", tail);
    created.set_len(5)?;
    drop(created);

    assert_file_content(&created_path, b"hello")?;
    assert_file_content(&backing.path().join("work/created.txt"), b"hello")?;
    assert_error(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&created_path),
        "creating an existing file",
    );

    fs::set_permissions(&created_path, Permissions::from_mode(0o600))?;
    assert_eq!(
        0o600,
        fs::metadata(&created_path)?.permissions().mode() & 0o777
    );
    chown_current_group(&created_path)?;
    set_times(&created_path)?;

    let rename_source = work.join("rename-source.txt");
    let rename_destination = work.join("rename-destination.txt");
    fs::write(&rename_source, b"source")?;
    fs::write(&rename_destination, b"destination")?;
    fs::rename(&rename_source, &rename_destination)?;
    assert_file_content(&rename_destination, b"source")?;
    assert_error_kind(
        fs::metadata(&rename_source),
        io::ErrorKind::NotFound,
        "stat source after rename over existing file",
    );

    let move_destination_dir = work.join("move-destination");
    fs::create_dir(&move_destination_dir)?;
    let moved_file = work.join("moved-between-parents.txt");
    let moved_file_destination = move_destination_dir.join("moved-between-parents.txt");
    fs::write(&moved_file, b"moved")?;
    fs::rename(&moved_file, &moved_file_destination)?;
    assert_file_content(&moved_file_destination, b"moved")?;
    assert_error_kind(
        fs::metadata(&moved_file),
        io::ErrorKind::NotFound,
        "stat source after moving file between parents",
    );

    let unlink_open_path = work.join("unlink-open.txt");
    fs::write(&unlink_open_path, b"open after unlink")?;
    let mut unlink_open_file = File::open(&unlink_open_path)?;
    fs::remove_file(&unlink_open_path)?;
    assert_error_kind(
        fs::metadata(&unlink_open_path),
        io::ErrorKind::NotFound,
        "stat unlinked open file",
    );
    let mut unlinked_data = Vec::new();
    unlink_open_file.read_to_end(&mut unlinked_data)?;
    assert_eq!(b"open after unlink", unlinked_data.as_slice());

    let hard_link = work.join("hard-link.txt");
    fs::hard_link(&created_path, &hard_link)?;
    assert_file_content(&hard_link, b"hello")?;

    let symlink_path = work.join("symlink.txt");
    std::os::unix::fs::symlink("created.txt", &symlink_path)?;
    assert_eq!(PathBuf::from("created.txt"), fs::read_link(&symlink_path)?);

    let fifo_path = work.join("fifo");
    mkfifo(&fifo_path)?;
    assert!(fs::symlink_metadata(&fifo_path)?.file_type().is_fifo());

    let stat = statfs(root)?;
    assert!(stat.f_bsize > 0);
    fsync_dir(&work)?;
    check_xattrs(&created_path)?;

    let non_empty = work.join("non-empty");
    fs::create_dir(&non_empty)?;
    fs::write(non_empty.join("file.txt"), b"x")?;
    assert_error(fs::remove_dir(&non_empty), "removing a non-empty directory");
    fs::remove_file(non_empty.join("file.txt"))?;
    fs::remove_dir(&non_empty)?;

    let open_tree = root.join("open-tree");
    fs::create_dir(&open_tree)?;
    fs::create_dir(open_tree.join("child"))?;
    let open_descendant_path = open_tree.join("child/file.txt");
    fs::write(&open_descendant_path, b"before")?;

    let mut open_descendant = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&open_descendant_path)?;
    let renamed_open_tree = root.join("renamed-open-tree");
    fs::rename(&open_tree, &renamed_open_tree)?;

    assert_error_kind(
        fs::metadata(&open_descendant_path),
        io::ErrorKind::NotFound,
        "stat old descendant path after parent rename",
    );

    let renamed_descendant_path = renamed_open_tree.join("child/file.txt");
    assert!(fs::metadata(&renamed_descendant_path)?.is_file());
    assert_file_content(&renamed_descendant_path, b"before")?;

    open_descendant.seek(SeekFrom::End(0))?;
    open_descendant.write_all(b"+after")?;
    open_descendant.flush()?;
    open_descendant.sync_data()?;
    open_descendant.seek(SeekFrom::Start(0))?;

    let mut descendant_data = Vec::new();
    open_descendant.read_to_end(&mut descendant_data)?;
    assert_eq!(b"before+after", descendant_data.as_slice());
    drop(open_descendant);

    assert_file_content(
        &backing.path().join("renamed-open-tree/child/file.txt"),
        b"before+after",
    )?;
    fs::remove_file(&renamed_descendant_path)?;
    fs::remove_dir(renamed_open_tree.join("child"))?;
    fs::remove_dir(&renamed_open_tree)?;

    let tree = root.join("tree");
    fs::create_dir(&tree)?;
    fs::create_dir(tree.join("child"))?;
    fs::write(tree.join("child/file.txt"), b"subtree")?;

    let renamed_tree = root.join("renamed-tree");
    fs::rename(&tree, &renamed_tree)?;
    assert_file_content(&renamed_tree.join("child/file.txt"), b"subtree")?;
    fs::write(
        renamed_tree.join("child/created-after-rename.txt"),
        b"after",
    )?;
    assert_file_content(
        &backing
            .path()
            .join("renamed-tree/child/created-after-rename.txt"),
        b"after",
    )?;

    fs::remove_file(renamed_tree.join("child/created-after-rename.txt"))?;
    fs::remove_file(renamed_tree.join("child/file.txt"))?;
    fs::remove_dir(renamed_tree.join("child"))?;
    fs::remove_dir(&renamed_tree)?;

    fs::remove_file(&created_path)?;
    fs::remove_file(&rename_destination)?;
    fs::remove_file(&moved_file_destination)?;
    fs::remove_dir(&move_destination_dir)?;
    assert_error(fs::remove_file(&created_path), "removing a missing file");
    fs::remove_file(&hard_link)?;
    fs::remove_file(&symlink_path)?;
    fs::remove_file(&fifo_path)?;
    fs::remove_dir(&work)?;

    Ok(())
}
