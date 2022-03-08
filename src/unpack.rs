use anyhow::{anyhow, Result};
use clap::ArgMatches;
use io::Write;
use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::hash::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;

//-----------------------------------------

#[allow(dead_code)]
struct SlabInfo {
    offsets: Vec<(Hash256, u32, u32)>,
    data: Vec<u8>,
}

#[allow(dead_code)]
struct Unpacker {
    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    slabs: BTreeMap<u64, Arc<SlabInfo>>,
}

impl Unpacker {
    // Assumes current directory is the root of the archive.
    fn new(stream: &str) -> Result<Self> {
        let data_path: PathBuf = ["data", "data"].iter().collect();
        let data_file = SlabFile::open_for_read(&data_path)?;

        let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
        let hashes_file = SlabFile::open_for_read(&hashes_path)?;

        let stream_path: PathBuf = ["streams", stream, "stream"].iter().collect();
        let stream_file = SlabFile::open_for_read(stream_path)?;

        Ok(Self {
            data_file,
            hashes_file,
            stream_file,
            slabs: BTreeMap::new(),
        })
    }

    // Returns the len of the data entry
    fn parse_hash_entry(input: &[u8]) -> IResult<&[u8], (Hash256, u32)> {
        let (input, hash) = take(std::mem::size_of::<Hash256>())(input)?;
        let hash = Hash256::clone_from_slice(hash);
        let (input, len) = le_u32(input)?;
        Ok((input, (hash, len)))
    }

    fn parse_slab_info(input: &[u8]) -> IResult<&[u8], Vec<(Hash256, u32, u32)>> {
        let (input, lens) = many0(Self::parse_hash_entry)(input)?;

        let mut r = Vec::with_capacity(lens.len());
        let mut total = 0;
        for (h, l) in lens {
            r.push((h, total, l));
            total += l;
        }

        Ok((input, r))
    }

    fn read_info(&mut self, slab: u64) -> Result<Arc<SlabInfo>> {
        // Read the hashes slab
        let hashes = self.hashes_file.read(slab)?;

        // Find location and length of data
        let (_, offsets) =
            Self::parse_slab_info(&hashes).map_err(|_| anyhow!("unable to parse slab hashes"))?;

        // Read data slab
        let data = self.data_file.read(slab)?;

        Ok(Arc::new(SlabInfo { offsets, data }))
    }

    fn get_info(&mut self, slab: u64) -> Result<Arc<SlabInfo>> {
        if let Some(info) = self.slabs.get(&slab) {
            Ok(info.clone())
        } else {
            let info = self.read_info(slab)?;
            self.slabs.insert(slab, info.clone());
            Ok(info)
        }
    }

    fn unpack_entry<W: Write>(&mut self, e: &MapEntry, w: &mut W) -> Result<()> {
        match e {
            MapEntry::Zero { len } => {
                // FIXME: don't keep initialising this buffer,
                // keep a suitable one around instead
                let zeroes: Vec<u8> = vec![0; *len as usize];
                w.write_all(&zeroes)?;
            }
            MapEntry::Data { slab, offset } => {
                let info = self.get_info(*slab)?;
                let (expected_hash, offset, len) = info.offsets[*offset as usize];
                let data_begin = offset as usize;
                let data_end = data_begin + len as usize;
                assert!(data_end <= info.data.len());

                // FIXME: make this paranioa check optional
                // Verify hash
                let actual_hash = hash_256(&info.data[data_begin..data_end]);
                assert_eq!(actual_hash, expected_hash);

                // Copy data
                w.write_all(&info.data[data_begin..data_end])?;
            }
        }

        Ok(())
    }

    pub fn unpack<W: Write>(&mut self, report: &Arc<Report>, w: &mut W) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut unpacker = stream::MappingUnpacker::default();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u64)?;
            let entries = unpacker.unpack(&stream_data[..])?;
            let nr_entries = entries.len();

            for (i, e) in entries.iter().enumerate() {
                self.unpack_entry(&e, w)?;

                if i % 10240 == 0 {
                    // update progress bar
                    let entry_fraction = i as f64 / nr_entries as f64;
                    let slab_fraction = s as f64 / nr_slabs as f64;
                    let percent =
                        ((slab_fraction + (entry_fraction / nr_slabs as f64)) * 100.0) as u8;
                    report.progress(percent as u8);
                }
            }
        }

        Ok(())
    }
}

//-----------------------------------------

pub fn run(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let stream = matches.value_of("STREAM").unwrap();
    let report = std::sync::Arc::new(mk_progress_bar_report());

    let mut output = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(output_file)?;

    env::set_current_dir(&archive_dir)?;

    report.set_title(&format!("Unpacking {} ...", output_file.display()));
    let mut u = Unpacker::new(&stream)?;
    u.unpack(&report, &mut output)
}

//-----------------------------------------
