use anyhow::Result;
use clap::ArgMatches;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thinp::report::*;

//-----------------------------------------

fn create_sub_dir(root: &Path, sub: &str) -> Result<()> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push(sub);
    fs::create_dir(p)?;
    Ok(())
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let dir = Path::new(matches.value_of("DIR").unwrap());
    let _block_size = matches
        .value_of("BLOCK_SIZE")
        .map(|s| s.parse::<usize>())
        .or(Some(Ok(4096)))
        .unwrap()?;

    let report = std::sync::Arc::new(mk_simple_report());

    fs::create_dir(dir)?;

    create_sub_dir(dir, "data")?;
    create_sub_dir(dir, "streams")?;
    create_sub_dir(dir, "indexes")?;

    Ok(())
}

//-----------------------------------------
