// Main Entry Point :: A fuser_ng test program.
//
// Copyright (c) 2016-2022 by William R. Fraser, 2026 by François NT
//

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
            &env::args().next().unwrap()
        );
        std::process::exit(-1);
    }

    let filesystem = passthrough::PassthroughFS {
        target: args[1].clone(),
    };

    let fuse_args = [fuser_ng::MountOption::FSName("passthrufs".into())];

    fuser_ng::mount(
        fuser_ng::FuserNG::new(filesystem),
        &args[2],
        &fuse_args[..],
        1.into(),
    )
    .unwrap();
}
