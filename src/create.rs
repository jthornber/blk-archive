use anyhow::Result;
use clap::ArgMatches;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use thinp::report::*;

use crate::config::*;

//-----------------------------------------

fn create_sub_dir(root: &Path, sub: &str) -> Result<()> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push(sub);
    fs::create_dir(p)?;
    Ok(())
}

fn write_config(root: &Path, block_size: usize) -> Result<()> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.toml");

    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(p)?;

    let config = Config {
        block_size,
        splitter_alg: "RollingHashV0".to_string(),
    };

    write!(output, "{}", &toml::to_string(&config).unwrap())?;
    Ok(())
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let dir = Path::new(matches.value_of("DIR").unwrap());
    let block_size = matches
        .value_of("BLOCK_SIZE")
        .map(|s| s.parse::<usize>())
        .or(Some(Ok(4096)))
        .unwrap()?;

    let report = std::sync::Arc::new(mk_simple_report());

    fs::create_dir(dir)?;
    write_config(dir, block_size)?;
    create_sub_dir(dir, "data")?;
    create_sub_dir(dir, "streams")?;
    create_sub_dir(dir, "indexes")?;

    Ok(())
}

//-----------------------------------------
