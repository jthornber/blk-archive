use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
}

pub fn read_config<P: AsRef<Path>>(root: P) -> Result<Config> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.toml");
    let input = std::fs::read_to_string(p).context("couldn't read config file")?;
    let config: Config = toml::from_str(&input).context("couldn't parse config file")?;
    Ok(config)
}

//-----------------------------------------
