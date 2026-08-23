// Async entry point for the passthrough example.

use std::env;
use std::ffi::OsString;

#[macro_use]
extern crate log;

mod libc_extras;
mod libc_wrappers;
mod passthrough;

struct ConsoleLogger;

impl log::Log for ConsoleLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        println!("{}: {}: {}", record.target(), record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: ConsoleLogger = ConsoleLogger;

fn main() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Debug);

    let args: Vec<OsString> = env::args_os().collect();

    if args.len() != 3 {
        println!(
            "usage: {} <target> <mountpoint>",
            env::args().next().unwrap()
        );
        std::process::exit(-1);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let filesystem = passthrough::PassthroughFS {
        target: args[1].clone(),
    };
    let fuse_args = [fuser_ng::MountOption::FSName("async-passthrough".into())];

    fuser_ng::mount(
        fuser_ng::AsyncFuserNG::new(filesystem, runtime.handle().clone()),
        &args[2],
        &fuse_args,
        1.into(),
    )
    .unwrap();
}
