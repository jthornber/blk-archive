use anyhow::{Context, Result};
use clap::ArgMatches;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use thinp::report::*;

use crate::config::*;
use crate::slab::*;

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

fn adjust_block_size(n: usize) -> usize {
    // We have a max block size of 1M currently
    let max_bs = 1024 * 1024;
    if n > max_bs {
        return max_bs;
    }

    let mut p = 1;
    while p < n {
        p *= 2;
    }

    p
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let dir = Path::new(matches.value_of("DIR").unwrap());
    let mut block_size = matches
        .value_of("BLOCK_SIZE")
        .map(|s| s.parse::<usize>())
        .or(Some(Ok(4096)))
        .unwrap().context("couldn't parse --block-size argument")?;

    let report = std::sync::Arc::new(mk_simple_report());

    let new_block_size = adjust_block_size(block_size);
    if new_block_size != block_size {
        report.info(&format!("adjusting block size to {}", new_block_size));
        block_size = new_block_size;
    }

    fs::create_dir(dir)?;
    write_config(dir, block_size)?;
    create_sub_dir(dir, "data")?;
    create_sub_dir(dir, "streams")?;
    create_sub_dir(dir, "indexes")?;

    std::env::set_current_dir(&dir)?;

    // Create empty data and hash slab files
    let data_path: PathBuf = ["data", "data"].iter().collect();
    let mut data_file = SlabFile::create(&data_path, 1, true)?;
    data_file.close()?;

    let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
    let mut hashes_file = SlabFile::create(&hashes_path, 1, false)?;
    hashes_file.close()?;

    Ok(())
}

//-----------------------------------------
