use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::paths::*;

//-----------------------------------------

#[derive(Deserialize, Serialize)]
pub struct Config {
    pub block_size: usize,
    pub splitter_alg: String,
    pub hash_cache_size_meg: usize,
    pub data_cache_size_meg: usize,
}

fn numeric_override<T: std::str::FromStr>(matches: &ArgMatches, name: &str) -> Result<Option<T>> {
    match matches.try_get_one::<String>(name) {
        Ok(Some(s)) => s
            .parse::<T>()
            .map_err(|_| anyhow!("could not parse {} argument", name))
            .map(|n| Some(n)),
        Ok(None) => Ok(None),
        Err(_) => Err(anyhow!("Error retrieving {} argument", name)),
    }
}

pub fn read_config<P: AsRef<Path>>(root: P, overrides: &ArgMatches) -> Result<Config> {
    let mut p = PathBuf::new();
    p.push(root);
    p.push("dm-archive.yaml");
    let input = fs::read_to_string(p).context("couldn't read config file")?;
    let mut config: Config = serde_yaml::from_str(&input).context("couldn't parse config file")?;

    if let Some(data_cache_meg) = numeric_override::<usize>(overrides, "DATA_CACHE_SIZE_MEG")? {
        config.data_cache_size_meg = data_cache_meg;
    }

    Ok(config)
}

//-----------------------------------------

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct StreamConfig {
    pub name: Option<String>,
    pub source_path: String,
    pub pack_time: String,
    pub size: u64,
    pub mapped_size: u64,
    pub packed_size: u64,
    pub thin_id: Option<u32>,
}

pub fn read_stream_config(stream_id: &str) -> Result<StreamConfig> {
    let p = stream_config(stream_id);
    let input =
        fs::read_to_string(&p).with_context(|| format!("couldn't read stream config '{:?}", &p))?;
    let config: StreamConfig =
        serde_yaml::from_str(&input).context("couldn't parse stream config file")?;
    Ok(config)
}

pub fn write_stream_config(stream_id: &str, cfg: &StreamConfig) -> Result<()> {
    let p = stream_config(stream_id);
    let mut output = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(p)?;
    let yaml = serde_yaml::to_string(cfg).unwrap();
    output.write_all(yaml.as_bytes())?;
    Ok(())
}

pub fn now() -> String {
    let dt = Utc::now();
    dt.to_rfc3339()
}

pub fn to_date_time(t: &str) -> chrono::DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(t).unwrap()
}

//-----------------------------------------

#[cfg(test)]
mod config_tests {

    use super::*;

    #[test]
    fn test_simple() {
        let config = StreamConfig {
            name: Some(String::from("test_file")),
            source_path: String::from("/home/some_user/test_file"),
            pack_time: String::from("2023-11-14T22:06:02.101221624+00:00"),
            size: u64::MAX,
            mapped_size: u64::MAX,
            packed_size: u64::MAX,
            thin_id: None,
        };

        let ser = serde_yaml::to_string(&config).unwrap();
        let des_config: StreamConfig = serde_yaml::from_str(&ser).unwrap();
        assert!(config == des_config);
    }
}
