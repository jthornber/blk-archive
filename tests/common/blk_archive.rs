use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

use crate::args;
use crate::common::process::*;
use crate::common::targets::*;

//-----------------------------------------

pub struct BlkArchive {
    archive: PathBuf,
}

impl BlkArchive {
    pub fn new(archive: &Path) -> Result<Self> {
        Self::new_with(archive, 4096)
    }

    pub fn new_with(archive: &Path, block_size: usize) -> Result<Self> {
        let bs_str = block_size.to_string();
        run_ok(create_cmd(args!["-a", archive, "--block-size", &bs_str]))?;
        Ok(Self {
            archive: archive.to_path_buf(),
        })
    }

    pub fn from_path(archive: &Path) -> Result<Self> {
        Ok(Self {
            archive: archive.to_path_buf(),
        })
    }

    pub fn pack_cmd(&self, input: &Path) -> Command {
        pack_cmd(args!["-a", &self.archive, &input, "-j"])
    }

    pub fn pack(&self, input: &Path) -> Result<String> {
        let stdout = run_ok(self.pack_cmd(input))?;
        let v: serde_json::Value = serde_json::from_str(&stdout)?;
        let sid = v["stream_id"]
            .as_str()
            .ok_or(anyhow!("stream_id not found"))?;
        Ok(sid.to_string())
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
