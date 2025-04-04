use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::args;
use crate::common::process::*;
use crate::common::targets::*;

//-----------------------------------------

pub struct BlkArchive {
    archive: PathBuf,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct PackStats {
    pub data_written: u64,
    pub mapped_size: u64,
    pub fill_size: u64,
}
#[derive(Deserialize, Serialize, Debug)]
pub struct PackResponse {
    pub stream_id: String,
    pub stats: PackStats,
}

impl BlkArchive {
    pub fn new(archive: &Path) -> Result<Self> {
        Self::new_with(archive, 4096, true)
    }

    pub fn new_with(archive: &Path, block_size: usize, data_compression: bool) -> Result<Self> {
        let bs_str = block_size.to_string();
        let compression = if data_compression { "y" } else { "n" };

        run_ok(create_cmd(args![
            "-a",
            archive,
            "--block-size",
            &bs_str,
            "--data-compression",
            &compression
        ]))?;
        Ok(Self {
            archive: archive.to_path_buf(),
        })
    }

    pub fn from_path(archive: &Path) -> Result<Self> {
        Ok(Self {
            archive: archive.to_path_buf(),
        })
    }

    pub fn data_size(&self) -> std::io::Result<u64> {
        fn file_size(path: &PathBuf) -> std::io::Result<u64> {
            fs::metadata(path).map(|meta| meta.len())
        }

        let base_path = self.archive.clone();
        let data_size = file_size(&base_path.join("data/data"))?;
        Ok(data_size)
    }

    pub fn pack_cmd(&self, input: &Path) -> Command {
        pack_cmd(args!["-a", &self.archive, &input, "-j"])
    }

    pub fn pack(&self, input: &Path) -> Result<PackResponse> {
        let stdout = run_ok(self.pack_cmd(input))?;
        let response: PackResponse = serde_json::from_str(&stdout)?;
        Ok(response)
    }

    pub fn unpack_cmd(&self, stream: &str, output: &Path, create: bool) -> Command {
        let mut args = args!["-a", &self.archive, "-s", stream, &output].to_vec();
        if create {
            args.push(std::ffi::OsStr::new("--create"));
        }
        unpack_cmd(args)
    }

    pub fn unpack(&self, stream: &str, output: &Path, create: bool) -> Result<()> {
        run_ok(self.unpack_cmd(stream, output, create))?;
        Ok(())
    }

    pub fn verify_cmd(&self, input: &Path, stream: &str) -> Command {
        verify_cmd(args!["-a", &self.archive, "-s", stream, input])
    }

    pub fn verify(&self, input: &Path, stream: &str) -> Result<()> {
        run_ok(self.verify_cmd(input, stream))?;
        Ok(())
    }
}

//-----------------------------------------
