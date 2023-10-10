use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use nom::AsBytes;
use serde_derive::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{Read, Seek, Write};
use std::os::unix::prelude::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bincode::*;
use clap::ArgMatches;

use crate::hash::*;
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
    let cp = CheckPoint::read(&archive_dir)?;

    let num_data_slabs = SlabFile::verify(&data_path, repair);
    output.report.progress(25);
    let num_hash_slabs = SlabFile::verify(&hashes_path, repair);
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
            SlabFile::truncate(&data_path, cp.data_start_size, false)?;
            SlabFile::truncate(&hashes_path, cp.hash_start_size, false)?;

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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CheckPoint {
    pub magic: u64,
    pub version: u32,
    pub data_start_size: u64,
    pub hash_start_size: u64,
    pub data_curr_size: u64,
    pub hash_curr_size: u64,
    pub input_offset: u64,
    pub source_path: String,
    pub stream_path: String,
}

pub fn checkpoint_path(root: &str) -> PathBuf {
    [root, "checkpoint.bin"].iter().collect()
}

impl CheckPoint {
    pub const VERSION: u32 = 0;
    pub const MAGIC: u64 = 0xD00DDEAD10CCD00D;
    pub const MIN_SIZE: u64 = 76;

    pub fn start(
        source_path: &str,
        stream_path: &str,
        data_start_size: u64,
        hash_start_size: u64,
    ) -> Self {
        CheckPoint {
            magic: Self::MAGIC,
            version: Self::VERSION,
            data_start_size,
            hash_start_size,
            data_curr_size: data_start_size,
            hash_curr_size: hash_start_size,
            input_offset: 0,
            source_path: String::from(source_path),
            stream_path: String::from(stream_path),
        }
    }

    pub fn write(&mut self, root: &Path) -> Result<()> {
        let file_name = checkpoint_path(
            root.to_str()
                .ok_or_else(|| anyhow!("Invalid root path {}", root.display()))?,
        );

        {
            let ser = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian();
            let objbytes = ser.serialize(self)?;
            let checksum = hash_64(&objbytes);

            let mut cpf = fs::OpenOptions::new()
                .read(false)
                .write(true)
                .custom_flags(libc::O_SYNC)
                .create_new(true)
                .open(file_name)
                .context("Previous pack operation interrupted, please run verify-all")?;

            cpf.write_all(&objbytes)?;
            cpf.write_all(checksum.as_bytes())?;
        }

        // Sync containing dentry to ensure checkpoint file exists
        fs::File::open(root)?.sync_all()?;

        Ok(())
    }

    fn read<P: AsRef<Path>>(root: P) -> Result<Option<Self>> {
        let file_name = checkpoint_path(root.as_ref().to_str().unwrap());

        let cpf = fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create_new(false)
            .open(file_name);

        if cpf.is_err() {
            // No checkpoint
            Ok(None)
        } else {
            let mut cpf = cpf?;
            let len = cpf.metadata()?.len();

            if len < Self::MIN_SIZE {
                return Err(anyhow!(
                    "Checkpoint file is too small to be valid, require {} {len} bytes",
                    Self::MIN_SIZE
                ));
            }

            let mut payload = vec![0; (len - 8) as usize];
            cpf.read_exact(&mut payload)?;
            let expected_cs = cpf.read_u64::<LittleEndian>()?;

            assert!(cpf.stream_position()? == len);

            let actual_cs = u64::from_le_bytes(hash_64(&payload).into());
            if actual_cs != expected_cs {
                return Err(anyhow!("Checkpoint file checksum is invalid!"));
            }

            let des = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_little_endian();
            let cp: CheckPoint = des.deserialize(&payload)?;
            if cp.version != Self::VERSION {
                return Err(anyhow!(
                    "Incorrect version {} expected {}",
                    cp.version,
                    Self::VERSION
                ));
            }

            if cp.magic != Self::MAGIC {
                return Err(anyhow!(
                    "Magic incorrect {} expected {}",
                    cp.magic,
                    Self::MAGIC
                ));
            }
            Ok(Some(cp))
        }
    }

    pub fn end() -> Result<()> {
        let root = env::current_dir()?;
        let root_str = root.clone().into_os_string().into_string().unwrap();
        let cp_file = checkpoint_path(root_str.as_str());

        fs::remove_file(cp_file).context("error removing checkpoint file!")?;

        fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    pub fn interrupted() -> Result<()> {
        let root = env::current_dir()?;
        match Self::read(root).context("error while checking for checkpoint file!")? {
            Some(cp) => Err(anyhow!(
                "pack operation of {} was interrupted, run verify-all -r to correct",
                cp.source_path
            )),
            None => Ok(()),
        }
    }
}

#[test]
fn check_ranges() {
    let test_archive = PathBuf::from("/tmp/check_ranges");

    std::fs::create_dir_all(&test_archive).unwrap();

    let mut t = CheckPoint::start("/tmp/testing", "/tmp/testing/stream", u64::MAX, u64::MAX);

    let write_result = t.write(&test_archive);
    assert!(
        write_result.is_ok(),
        "CheckPoint.write failed {:?}",
        write_result.unwrap()
    );

    let read_back = CheckPoint::read(&test_archive).unwrap();

    assert!(std::fs::remove_dir_all(&test_archive).is_ok());

    assert!(
        read_back.is_some(),
        "CheckPoint::read error {:?}",
        read_back.unwrap()
    );

    assert!(read_back.unwrap() == t);
}

#[test]
fn check_small() {
    let test_archive = PathBuf::from("/tmp/check_small");

    std::fs::create_dir_all(&test_archive).unwrap();

    let mut t = CheckPoint::start("", "", u64::MAX, u64::MAX);

    let write_result = t.write(&test_archive);
    assert!(
        write_result.is_ok(),
        "CheckPoint.write failed {:?}",
        write_result.unwrap()
    );

    let read_back = CheckPoint::read(&test_archive).unwrap();

    assert!(std::fs::remove_dir_all(&test_archive).is_ok());

    assert!(
        read_back.is_some(),
        "CheckPoint::read error {:?}",
        read_back.unwrap()
    );

    assert!(read_back.unwrap() == t);
}

#[test]
fn check_fields() {
    let test_archive = PathBuf::from("/tmp/check_fields");
    std::fs::create_dir_all(&test_archive).unwrap();

    let mut t = CheckPoint::start("source/path", "stream/path/yes", 1024, 384);

    t.data_curr_size = t.data_start_size * 2;
    t.hash_curr_size = t.hash_start_size * 2;
    t.input_offset = 1024 * 1024 * 1024;

    let write_result = t.write(&test_archive);
    assert!(
        write_result.is_ok(),
        "CheckPoint.write failed {:?}",
        write_result.unwrap()
    );

    let read_back = CheckPoint::read(&test_archive).unwrap();

    assert!(std::fs::remove_dir_all(&test_archive).is_ok());

    assert!(
        read_back.is_some(),
        "CheckPoint::read error {:?}",
        read_back.unwrap()
    );

    assert!(read_back.unwrap() == t);
}
