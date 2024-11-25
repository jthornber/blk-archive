use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use rkyv::{Archive, Deserialize, Serialize};

use serde_derive::Deserialize as SDeserialize;
use serde_derive::Serialize as SSerialize;

//-----------------------------------------

#[derive(SDeserialize, SSerialize, Deserialize, Archive, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct Config {
    pub block_size: u64,
    pub splitter_alg: String,
    pub hash_cache_size_meg: u64,
    pub data_cache_size_meg: u64,
}

pub fn read_config<P: AsRef<Path>>(root: P) -> Result<Config> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.toml");
    let input = fs::read_to_string(p).context("couldn't read config file")?;
    let config: Config = toml::from_str(&input).context("couldn't parse config file")?;
    Ok(config)
}

//-----------------------------------------
