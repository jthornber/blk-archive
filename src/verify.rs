use anyhow::Result;
use clap::ArgMatches;
use io::Read;
// use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::config;
use crate::hash::*;
use crate::hash_index::*;
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
struct Verifier {
    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    slabs: BTreeMap<u32, Arc<ByIndex>>,
    total_verified: u64,
}

impl Verifier {
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
            total_verified: 0,
        })
    }

    fn read_info(&mut self, slab: u32) -> Result<ByIndex> {
        let hashes = self.hashes_file.read(slab)?;
        ByIndex::new(hashes.to_vec()) // FIXME: redundant copy?
    }

    fn get_info(&mut self, slab: u32) -> Result<Arc<ByIndex>> {
        if let Some(info) = self.slabs.get(&slab) {
            Ok(info.clone())
        } else {
            let info = Arc::new(self.read_info(slab)?);
            self.slabs.insert(slab, info.clone());
            Ok(info)
        }
    }

    fn verify_entry<R: Read>(&mut self, e: &MapEntry, r: &mut R) -> Result<u64> {
        use MapEntry::*;

        let len = match e {
            Fill { byte, len } => {
                // FIXME: don't keep initialising this buffer,
                // keep a suitable one around instead
                // FIXME: put in a loop if len is too long
                let expected: Vec<u8> = vec![*byte; *len as usize];
                let mut actual = vec![*byte; *len as usize];
                r.read_exact(&mut actual)?;
                assert_eq!(&actual, &expected);
                self.total_verified += *len as u64;
                *len as u64
            }
            Unmapped { len } => {
                todo!();
                *len as u64
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let mut total_len = 0;
                for entry in 0..*nr_entries {
                    let data = self.data_file.read(*slab)?;
                    let info = self.get_info(*slab)?;
                    let (data_begin, data_end, expected_hash) = info.get(*offset as usize + entry as usize).unwrap();
                    assert!(*data_end <= data.len() as u32);

                    // FIXME: make this paranioa check optional
                    // Verify hash
                    let actual_hash = hash_256(&data[*data_begin as usize..*data_end as usize]);
                    assert_eq!(actual_hash, *expected_hash);

                    // Verify data
                    let mut actual = vec![0; (data_end - data_begin) as usize];
                    r.read_exact(&mut actual)?;
                    if actual != &data[*data_begin as usize..*data_end as usize] {
                        eprintln!("mismatched data at offset {}", self.total_verified);
                        assert!(false);
                    }

                    self.total_verified += actual.len() as u64;
                    total_len += (data_end - data_begin) as u64;
                }
                total_len
            }
        };

        Ok(len)
    }

    pub fn verify<R: Read>(&mut self, report: &Arc<Report>, r: &mut R) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut current_pos = 0;

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let (entries, positions) = stream::unpack(&stream_data[..])?;
            let nr_entries = entries.len();
            let mut pos_iter = positions.iter();
            let mut next_pos = pos_iter.next();

            for (i, e) in entries.iter().enumerate() {
                if let Some(pos) = next_pos {
                    if pos.1 == i {
                        if pos.0 != current_pos {
                            eprintln!(
                                "pos didn't match: expected {} != actual {}",
                                pos.0, current_pos
                            );
                            assert!(false);
                        }
                        next_pos = pos_iter.next();
                    }
                }

                let entry_len = self.verify_entry(&e, r)?;

                // FIXME: shouldn't inc before checking pos
                current_pos += entry_len;

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
        report.info("Verify successful");

        Ok(())
    }
}

//-----------------------------------------

/*
fn thick_verifier(
    report: Arc<Report>,
    input_file: &PathBuf,
    input_name: String,
    config: &config::Config,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file.clone())
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(&input_file)?;

    let mapped_size = input_size;
    let input_iter = Box::new(FileChunker::new(input, 16 * 1024 * 1024)?);
    let thin_id = None;

    Ok(Packer::new(
        report,
        input_file.clone(),
        input_name,
        input_iter,
        input_size,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
    ))
}
*/

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let stream = matches.value_of("STREAM").unwrap();

    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(input_file)?;

    env::set_current_dir(&archive_dir)?;

    let config = config::read_config(".")?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream
    ));
    let mut v = Verifier::new(&stream, cache_nr_entries)?;
    v.verify(&report, &mut input)
}

//-----------------------------------------
