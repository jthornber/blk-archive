use anyhow::{Context, Result};
use chrono::prelude::*;
use serde_derive::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

//-----------------------------------------

#[derive(Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamConfig {
    pub name: Option<String>,
    pub source_path: String,
    pub pack_time: toml::value::Datetime,
    pub size: u64,
    pub mapped_size: u64,
    pub packed_size: u64,
    pub thin_id: Option<u32>,
}

fn stream_cfg_path(stream_id: &str) -> PathBuf {
    ["streams", stream_id, "config.toml"].iter().collect()
}

pub fn read_stream_config(stream_id: &str) -> Result<StreamConfig> {
    let p = stream_cfg_path(stream_id);
    let input =
        fs::read_to_string(&p).with_context(|| format!("couldn't read stream config '{:?}", &p))?;
    let config: StreamConfig =
        toml::from_str(&input).context("couldn't parse stream config file")?;
    Ok(config)
}

pub fn write_stream_config(stream_id: &str, cfg: &StreamConfig) -> Result<()> {
    let p = stream_cfg_path(stream_id);
    let mut output = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(p)?;
    let toml = toml::to_string(cfg).unwrap();
    output.write_all(toml.as_bytes())?;
    Ok(())
}

pub fn now() -> toml::value::Datetime {
    let dt = Utc::now();
    let str = dt.to_rfc3339();
    str.parse::<toml::value::Datetime>().unwrap()
}

pub fn to_date_time(t: &toml::value::Datetime) -> chrono::DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(&t.to_string()).unwrap()
}

//-----------------------------------------
