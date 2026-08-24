This directory contains synchronous and asynchronous sample programs using
`fuser_ng`.

Both implement a filesystem that forwards all requests to another filesystem
at any arbitrary location.

Run the synchronous example from the workspace root:

    cargo run -p passthrufs --bin passthrufs -- <path to filesystem> <mount point>

Add `--features readdirplus` to force the use of FUSE_READDIRPLUS.

Run the asynchronous example, which forwards blocking filesystem calls through
Tokio's blocking thread pool:

    cargo run -p passthrufs --features async --bin async-passthrough -- <path to filesystem> <mount point>

Use `--features async,readdirplus` to force FUSE_READDIRPLUS in the asynchronous
example.

Unmount it with `fusermount -u <mount point>` or just CTRL-C the running program.
