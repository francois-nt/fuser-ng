# FUSER-NG

This code is a wrapper on top of the Rust FUSE crate with the following additions:
* Translate inodes into paths, to simplify filesystem implementation.

fuser-ng started as a fork of fuse-mt, updated for fuser 0.17, and now focuses on providing a higher-level path-oriented API on top of `fuser`.

The `fuser` crate provides a minimal, low-level access to the FUSE kernel API, whereas this crate is more high-level, like the FUSE C API.

It includes a sample filesystem that uses the crate to pass all system calls through to another filesystem at any arbitrary path.

This is a work-in-progress. Bug reports, pull requests, and other feedback are welcome!

Some note on the implementation:
* The trait that filesystems will implement is called `Filesystem`, and instead of the FUSE crate's convention of having methods return void and including a "reply" parameter, the methods return their values. This feels more idiomatic to me. They also take `&ResolvedPath` or `&EntryName` arguments instead of only inode numbers.
