use anyhow::{anyhow, Result};
use clap::ArgMatches;
use io::{Seek, Write};
use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::config;
use crate::hash::*;
use crate::pack::SLAB_SIZE_TARGET;
use crate::paths::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;

//-----------------------------------------

#[allow(dead_code)]
struct SlabInfo {
    offsets: Vec<(Hash256, u32, u32)>,
}

#[allow(dead_code)]
struct Unpacker {
    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    slabs: BTreeMap<u32, Arc<SlabInfo>>,
    partial: Option<(u32, u32)>,
}

impl Unpacker {
    // Assumes current directory is the root of the archive.
    fn new(stream: &str, cache_nr_entries: usize) -> Result<Self> {
        let data_file = SlabFileBuilder::open(data_path())
            .cache_nr_entries(cache_nr_entries)
            .build()?;
        let hashes_file = SlabFileBuilder::open(hashes_path()).build()?;
        let stream_file = SlabFileBuilder::open(stream_path(stream)).build()?;

        Ok(Self {
            data_file,
            hashes_file,
            stream_file,
            slabs: BTreeMap::new(),
            partial: None,
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

    fn read_info(&mut self, slab: u32) -> Result<Arc<SlabInfo>> {
        // Read the hashes slab
        let hashes = self.hashes_file.read(slab)?;

        // Find location and length of data
        let (_, offsets) =
            Self::parse_slab_info(&hashes).map_err(|_| anyhow!("unable to parse slab hashes"))?;

        Ok(Arc::new(SlabInfo { offsets }))
    }

    fn get_info(&mut self, slab: u32) -> Result<Arc<SlabInfo>> {
        if let Some(info) = self.slabs.get(&slab) {
            Ok(info.clone())
        } else {
            let info = self.read_info(slab)?;
            self.slabs.insert(slab, info.clone());
            Ok(info)
        }
    }

    fn unpack_entry<W: Seek + Write>(&mut self, e: &MapEntry, w: &mut W) -> Result<()> {
        use MapEntry::*;

        match e {
            Fill { byte, len } => {
                assert!(self.partial.is_none());
                // len may be very big, so we have to be prepared to write in chunks.
                // FIXME: if we're writing to a file would this be zeroes anyway?  fallocate?
                const MAX_BUFFER: u64 = 16 * 1024 * 1024;
                let mut written = 0;
                while written < *len {
                    let write_len = std::cmp::min(*len - written, MAX_BUFFER);

                    // FIXME: don't keep initialising this buffer,
                    // keep a suitable one around instead
                    let bytes: Vec<u8> = vec![*byte; write_len as usize];
                    w.write_all(&bytes)?;
                    written += write_len;
                }
            }
            Unmapped { len } => {
                assert!(self.partial.is_none());
                w.seek(std::io::SeekFrom::Current(*len as i64))?;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                    let info = self.get_info(*slab)?;
                    let data = self.data_file.read(*slab)?;
                    let (_expected_hash, offset, _len) = info.offsets[*offset as usize];
                    let data_begin = offset as usize;
                    let (_expected_hash, offset, len) =
                        info.offsets[(offset as usize) + *nr_entries as usize];
                    let data_end = offset as usize + len as usize;
                    assert!(data_end <= data.len());

                    // Copy data
                    if let Some((begin, end)) = self.partial {
                        let data_end = data_begin + end as usize;
                        let data_begin = data_begin + begin as usize;
                        w.write_all(&data[data_begin..data_end])?;
                        self.partial = None;
                    } else {
                        w.write_all(&data[data_begin..data_end])?;
                    }
            }
            Partial { begin, end } => {
                assert!(self.partial.is_none());
                self.partial = Some((*begin, *end));
            }
            Ref { .. } => {
                // Can't get here.
                return Err(anyhow!("unexpected MapEntry::Ref (shouldn't be possible)"));
            }
        }

        Ok(())
    }

    pub fn unpack<W: Seek + Write>(&mut self, report: &Arc<Report>, w: &mut W) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut unpacker = stream::MappingUnpacker::default();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let (entries, _positions) = unpacker.unpack(&stream_data[..])?;
            let nr_entries = entries.len();

            for (i, e) in entries.iter().enumerate() {
                self.unpack_entry(e, w)?;

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
        report.progress(100);

        Ok(())
    }
}

//-----------------------------------------

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let stream = matches.value_of("STREAM").unwrap();

    let mut output = fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .open(output_file)?;

    env::set_current_dir(&archive_dir)?;

    let config = config::read_config(".")?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    report.set_title(&format!("Unpacking {} ...", output_file.display()));
    let mut u = Unpacker::new(stream, cache_nr_entries)?;
    u.unpack(&report, &mut output)
}

//-----------------------------------------
