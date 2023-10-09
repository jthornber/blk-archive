use anyhow::{anyhow, Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::os::unix::prelude::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::ArgMatches;

use crate::output::Output;
use crate::paths;
use crate::slab::*;

fn remove_incomplete_stream(cp: &CheckPoint) -> Result<()> {
    // Data and hashes has been validated, remove the stream file directory
    let stream_dir_to_del = PathBuf::from(&cp.stream_path).canonicalize()?;
    fs::remove_dir_all(stream_dir_to_del.clone())?;

    // Everything should be good now, remove the check point
    CheckPoint::end()?;

    Ok(())
}

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let repair = matches.is_present("REPAIR");

    env::set_current_dir(archive_dir.clone())?;

    let data_path = paths::data_path();
    let hashes_path = paths::hashes_path();

    output.report.progress(0);
    output.report.set_title("Verifying archive");

    // Load up a check point if we have one.
    let cp = CheckPoint::read(archive_dir.clone())?;

    let num_data_slabs = SlabFile::verify(data_path.clone(), repair);
    output.report.progress(25);
    let num_hash_slabs = SlabFile::verify(hashes_path.clone(), repair);
    output.report.progress(50);
    if (num_data_slabs.is_err() || num_hash_slabs.is_err())
        || (num_data_slabs.as_ref().unwrap() != num_hash_slabs.as_ref().unwrap())
    {
        if !repair || cp.is_none() {
            return Err(anyhow!(
                "The number of slabs in the data file {} != {} number in hashes file!",
                num_data_slabs?,
                num_hash_slabs?
            ));
        }

        output.report.set_title("Repairing archive ...");

        // We tried to do a non-loss data fix which didn't work, we now have to revert the data
        // slab to a known good state. It doesn't matter if one of the slabs verifies ok, we need
        // to be a matched set.  So we put both is known good state, but before we do we will make
        // sure our slabs are bigger than our starting sizes.  If they aren't there is nothing we
        // can do to fix this and we won't do anything.
        let cp = cp.unwrap();
        let data_meta = fs::metadata(&data_path)?;
        let hashes_meta = fs::metadata(&hashes_path)?;

        if data_meta.len() >= cp.data_start_size && hashes_meta.len() >= cp.hash_start_size {
            output
                .report
                .info("Rolling back archive to previous state...");

            // Make sure the truncated size verifies by verifying what part we are keeping first
            SlabFile::truncate(data_path.clone(), cp.data_start_size, false)?;
            SlabFile::truncate(hashes_path.clone(), cp.hash_start_size, false)?;

            // Do the actual truncate which also fixes up the offsets file to match
            SlabFile::truncate(data_path, cp.data_start_size, true)?;
            output.report.progress(75);
            SlabFile::truncate(hashes_path, cp.hash_start_size, true)?;

            remove_incomplete_stream(&cp)?;
            output.report.progress(100);
            output.report.info("Archive restored to previous state.");
        } else {
            return Err(anyhow!(
                "We're unable to repair this archive, check point sizes > \
                current file sizes, missing data!"
            ));
        }
    } else if repair && cp.is_some() {
        remove_incomplete_stream(&cp.unwrap())?;
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckPoint {
    pub source_path: String,
    pub stream_path: String,
    pub data_start_size: u64,
    pub hash_start_size: u64,
    pub data_curr_size: u64,
    pub hash_curr_size: u64,
    pub input_offset: u64,
    pub checksum: u64,
}

pub fn checkpoint_path(root: &str) -> PathBuf {
    [root, "checkpoint.toml"].iter().collect()
}

impl CheckPoint {
    pub fn start(
        source_path: &str,
        stream_path: &str,
        data_start_size: u64,
        hash_start_size: u64,
    ) -> Self {
        CheckPoint {
            source_path: String::from(source_path),
            stream_path: String::from(stream_path),
            data_start_size,
            hash_start_size,
            data_curr_size: data_start_size,
            hash_curr_size: hash_start_size,
            input_offset: 0,
            checksum: 0,
        }
    }

    pub fn write(&mut self, root: &Path) -> Result<()> {
        let file_name = checkpoint_path(
            root.to_str()
                .ok_or_else(|| anyhow!("Invalid root path {}", root.display()))?,
        );

        {
            //TODO: make the checksum mean somthing and check it in the read.  It's important
            // that the values are correct before we destroy data during a repair.  Maybe this
            // file shouldn't be in a human readable format?
            let mut output = fs::OpenOptions::new()
                .read(false)
                .write(true)
                .custom_flags(libc::O_SYNC)
                .create_new(true)
                .open(file_name)
                .context("Previous operation interrupted, please run verify-all")?;

            let toml = toml::to_string(self).unwrap();
            output.write_all(toml.as_bytes())?;
        }

        // Sync containing dentry to ensure checkpoint file exists
        fs::File::open(root)?.sync_all()?;

        Ok(())
    }

    pub fn end() -> Result<()> {
        let root = env::current_dir()?;
        let root_str = root.clone().into_os_string().into_string().unwrap();
        let cp_file = checkpoint_path(root_str.as_str());

        fs::remove_file(cp_file).context("error removing checkpoint file!")?;

        fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    fn read<P: AsRef<Path>>(root: P) -> Result<Option<Self>> {
        let file_name = checkpoint_path(root.as_ref().to_str().unwrap());
        let content = fs::read_to_string(file_name);
        if content.is_err() {
            // No checkpoint
            Ok(None)
        } else {
            let cp: CheckPoint =
                toml::from_str(&content.unwrap()).context("couldn't parse checkpoint file")?;
            Ok(Some(cp))
        }
    }
}
