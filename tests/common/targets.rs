use std::ffi::OsString;

use crate::common::process::*;

//------------------------------------------

pub fn target_cmd<S, I>(cmd: S, args: I) -> Command
where
    S: Into<OsString>,
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    const RUST_PATH: &str = env!("CARGO_BIN_EXE_blk-archive");

    let mut all_args = vec![Into::<OsString>::into(cmd)];
    for a in args {
        all_args.push(Into::<OsString>::into(a));
    }

    Command::new(Into::<OsString>::into(RUST_PATH), all_args)
}

pub fn create_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    target_cmd("create", args)
}

pub fn pack_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    target_cmd("pack", args)
}

pub fn unpack_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    target_cmd("unpack", args)
}

pub fn verify_cmd<I>(args: I) -> Command
where
    I: IntoIterator,
    I::Item: Into<OsString>,
{
    target_cmd("verify", args)
}

//------------------------------------------
