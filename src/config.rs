use anyhow::{Context, Result};
use bincode::{Decode, Encode};
use serde_derive::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

//-----------------------------------------

#[derive(Deserialize, Serialize, Encode, Decode, PartialEq)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
    pub hash_cache_size_meg: usize,
    pub data_cache_size_meg: usize,
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
