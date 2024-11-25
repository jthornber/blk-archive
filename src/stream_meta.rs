use anyhow::{Context, Result};
use chrono::prelude::*;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use rkyv::{Archive, Deserialize, Serialize};
use serde_derive::Deserialize as SDeserialize;
use serde_derive::Serialize as SSerialize;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::{Builder, TempDir};

use crate::paths;
use crate::slab::*;
use crate::wire;

// Assumes we've chdir'd to the archive
fn new_stream_path_(rng: &mut ChaCha20Rng) -> Result<Option<(String, PathBuf)>> {
    // choose a random number
    let n: u64 = rng.gen();

    // turn this into a path
    let name = format!("{:>016x}", n);
    let path: PathBuf = ["streams", &name].iter().collect();

    if path.exists() {
        Ok(None)
    } else {
        Ok(Some((name, path)))
    }
}

fn new_stream_path() -> Result<(String, PathBuf)> {
    let mut rng = ChaCha20Rng::from_entropy();
    loop {
        if let Some(r) = new_stream_path_(&mut rng)? {
            return Ok(r);
        }
    }

    // Can't get here
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct StreamMetaInfo {
    pub stream_id: String,
    pub name: Option<String>,
    pub source_path: String,
    pub pack_time: String,
    pub stats: StreamStats,
    pub thin_id: Option<u32>,
}

pub struct StreamMeta {
    pub stream_id: String,
    _stream_tmp_dir: TempDir,
    stream_dir: PathBuf,
    names: StreamNames,
    pub thin_id: Option<u32>,
    pub stream_file: SlabFile,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct StreamStats {
    pub size: u64,
    pub mapped_size: u64,
    pub written: u64,
    pub fill_size: u64,
    pub hashes_written: u64,
    pub stream_written: u64,
}

impl StreamStats {
    pub fn zero() -> Self {
        StreamStats {
            size: 0,
            mapped_size: 0,
            written: 0,
            fill_size: 0,
            hashes_written: 0,
            stream_written: 0,
        }
    }
}

pub struct StreamNames {
    pub name: String,
    pub input_file: PathBuf,
}

#[derive(SDeserialize, SSerialize, Archive, Deserialize, Serialize, Debug, PartialEq)]
#[rkyv(
    // This will generate a PartialEq impl between our unarchived
    // and archived types
    compare(PartialEq),
    // Derives can be passed through to the generated type:
    derive(Debug),
)]
pub struct StreamConfig {
    pub name: Option<String>,
    pub source_path: String,
    pub pack_time: String,
    pub size: u64,        // This is raw size
    pub mapped_size: u64, // Size of data that is actually allocated, will match size for thick
    pub packed_size: u64, // size of stream + amount written to data slab, this also used to include hashes written, but that isn't simple when you have multiple clients writing to the same slab at the same time
    pub thin_id: Option<u32>,
}

impl StreamMeta {
    pub fn new(names: StreamNames, thin_id: Option<u32>, sending: bool) -> Result<Self> {
        let tmp_dir = create_temp_stream_dir(sending)?;
        let (stream_id, _) = new_stream_path()?;

        let stream_dir = tmp_dir.path().join(stream_id.clone());
        std::fs::create_dir(stream_dir.clone())?;
        let tmp_stream_file = stream_dir.join("stream");

        let stream_file = SlabFileBuilder::create(tmp_stream_file.clone())
            .queue_depth(16)
            .compressed(true)
            .build()
            .context("couldn't open stream slab file")?;

        Ok(StreamMeta {
            names,
            stream_id,
            _stream_tmp_dir: tmp_dir,
            stream_dir,
            thin_id,
            stream_file,
        })
    }

    pub fn complete(&self, stats: &mut StreamStats) -> Result<()> {
        let stream_size = thinp::file_utils::file_size(self.stream_dir.join("stream"))?;

        stats.stream_written = stream_size;

        let cfg = StreamConfig {
            name: Some(self.names.name.clone()),
            source_path: self.names.input_file.to_string_lossy().into_owned(),
            pack_time: now_string(),
            size: stats.size,
            mapped_size: stats.mapped_size,
            packed_size: stats.written + stream_size + stats.hashes_written, //data_written + stream_written + hashes written,
            thin_id: self.thin_id,
        };
        write_stream_config(&self.stream_dir, &cfg)?;

        let dest = std::env::current_dir()?
            .join("streams")
            .join(self.stream_id.clone());
        std::fs::rename(self.stream_dir.clone(), dest)?;
        Ok(())
    }

    pub fn package(&self, stats: &mut StreamStats) -> Result<wire::Rpc> {
        let stream_bytes = fs::read(self.stream_dir.join("stream"))?;
        let stream_offset_bytes = fs::read(self.stream_dir.join("stream.offsets"))?;

        stats.stream_written = stream_bytes.len() as u64;

        let stream_files = wire::stream_files(stream_bytes, stream_offset_bytes);
        let stream_file_bytes = wire::stream_files_bytes(&stream_files);

        let sm = StreamMetaInfo {
            stream_id: self.stream_id.clone(),
            name: Some(self.names.name.clone()),
            source_path: self.names.input_file.to_string_lossy().into_owned(),
            pack_time: now_string(),
            stats: stats.clone(),
            thin_id: self.thin_id,
        };

        Ok(wire::Rpc::StreamSend(0, Box::new(sm), stream_file_bytes))
    }
}

pub fn write_file(file_name: &Path, contents: Vec<u8>) -> Result<()> {
    let mut stream = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_name)?;

    stream.write_all(&contents)?;
    stream.flush()?;
    Ok(())
}

pub fn package_unwrap(
    sm: &StreamMetaInfo,
    stream_file: Vec<u8>,
    stream_offsets: Vec<u8>,
) -> Result<()> {
    let stream_dir = std::env::current_dir()?
        .join("streams")
        .join(sm.stream_id.clone());

    std::fs::create_dir(stream_dir.clone())?;

    let stream_config = StreamConfig {
        name: sm.name.clone(),
        source_path: sm.source_path.clone(),
        pack_time: sm.pack_time.clone(),
        size: sm.stats.size,
        mapped_size: sm.stats.mapped_size,
        packed_size: sm.stats.written + stream_file.len() as u64,
        thin_id: sm.thin_id,
    };

    write_stream_config(&stream_dir, &stream_config)?;

    let stream_file_name = stream_dir.join("stream");
    write_file(&stream_file_name, stream_file)?;
    let stream_file_name_offsets = stream_dir.join("stream.offsets");
    write_file(&stream_file_name_offsets, stream_offsets)?;

    // TODO: Flush containing directory and archive/stream directory too

    Ok(())
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

pub fn stream_id_to_stream_files(stream_id: &str) -> Result<Option<wire::StreamFiles>> {
    let stream_data = fs::read(paths::stream_path(stream_id));

    if let Err(e) = stream_data {
        if e.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(e.into());
    }

    let stream_data = stream_data.unwrap();
    let stream_offset_data = fs::read(paths::stream_path_offsets(stream_id))
        .context(format!("stream index not found for {}", stream_id))?;

    Ok(Some(wire::StreamFiles {
        stream: stream_data,
        offsets: stream_offset_data,
    }))
}

pub fn write_stream_config(dir_location: &Path, cfg: &StreamConfig) -> Result<()> {
    let p = dir_location.join("config.toml");
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

pub fn now_string() -> String {
    let dt = Utc::now();
    dt.to_rfc3339()
}

pub fn now() -> toml::value::Datetime {
    let dt = Utc::now();
    let str = dt.to_rfc3339();
    str.parse::<toml::value::Datetime>().unwrap()
}

pub fn to_date_time(t: &str) -> chrono::DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(t).unwrap()
}

pub fn create_temp_stream_dir(non_local: bool) -> anyhow::Result<TempDir> {
    // If we are working non-locally, we'll use the standard default tmp directory, else we
    // will use a tmp directory within the archive itself
    if non_local {
        Ok(Builder::new()
            .prefix("blk_archive_stream_")
            .rand_bytes(10)
            .tempdir()?)
    } else {
        let cwd = std::env::current_dir()?.join("tmp");
        Ok(Builder::new()
            .prefix("blk_archive_stream_")
            .rand_bytes(10)
            .tempdir_in(cwd)?)
    }
}

//-----------------------------------------
#[cfg(test)]
mod stream_meta_tests {
    use super::{StreamMeta, StreamNames};
    use std::path::PathBuf;

    #[test]
    fn simple_create() {
        let sn = StreamNames {
            name: "what".to_string(),
            input_file: PathBuf::from("/var/what"),
        };
        let _sm = StreamMeta::new(sn, None, true).unwrap();
    }
}
