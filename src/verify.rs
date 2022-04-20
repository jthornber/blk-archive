use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::chunkers::*;
use crate::config;
use crate::hash::*;
use crate::hash_index::*;
use crate::pack::SLAB_SIZE_TARGET;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;
use crate::thin_metadata::*;

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

    input_it: Box<dyn Iterator<Item = Result<Chunk>>>,
    input_size: u64,
    chunk: Option<Chunk>,
    chunk_offset: u64,

    slabs: BTreeMap<u32, Arc<ByIndex>>,
    total_verified: u64,
}

impl Verifier {
    // Assumes current directory is the root of the archive.
    fn new(
        stream: &str,
        cache_nr_entries: usize,
        input_it: Box<dyn Iterator<Item = Result<Chunk>>>,
        input_size: u64,
    ) -> Result<Self> {
        let data_file = SlabFileBuilder::open(data_path())
            .cache_nr_entries(cache_nr_entries)
            .build()?;
        let hashes_file = SlabFileBuilder::open(hashes_path()).build()?;
        let stream_file = SlabFileBuilder::open(stream_path(stream)).build()?;

        Ok(Self {
            data_file,
            hashes_file,
            stream_file,
            input_it,
            input_size,
            chunk: None,
            chunk_offset: 0,
            slabs: BTreeMap::new(),
            total_verified: 0,
        })
    }

    fn fail(&self, msg: &str) -> anyhow::Error {
        anyhow!(format!(
            "verify failed at offset ~{}: {}",
            self.total_verified, msg
        ))
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

    fn ensure_chunk(&mut self) -> Result<()> {
        if self.chunk.is_none() {
            match self.input_it.next() {
                Some(rc) => {
                    self.chunk = Some(rc?);
                    self.chunk_offset = 0;
                    Ok(())
                }
                None => Err(self.fail("archived stream is longer than expected")),
            }
        } else {
            Ok(())
        }
    }

    fn peek_data<'a>(&'a mut self, max_len: u64) -> Result<&'a [u8]> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let len = std::cmp::min(bytes.len() - self.chunk_offset as usize, max_len as usize);
                Ok(&bytes[self.chunk_offset as usize..(self.chunk_offset + len as u64) as usize])
            }
            Some(Chunk::Unmapped(_)) => {
                return Err(self.fail("expected data, got unmapped"));
            }
            None => {
                return Err(self.fail("ensure_chunk() failed"));
            }
        }
    }

    fn consume_data(&mut self, len: u64) -> Result<()> {
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let c_len = bytes.len() as u64 - self.chunk_offset;
                if c_len < len {
                    return Err(self.fail("bad consume, chunk too short"));
                } else if c_len > len {
                    self.chunk_offset += len;
                } else {
                    self.chunk = None;
                }
            }
            Some(Chunk::Unmapped(_)) => {
                return Err(self.fail("bad consume, unexpected unmapped chunk"));
            }
            None => {
                return Err(self.fail("bad consume, no chunk"));
            }
        }
        Ok(())
    }

    fn get_unmapped(&mut self, max_len: u64) -> Result<u64> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(_)) => {
                return Err(self.fail("expected unmapped, got data"));
            }
            Some(Chunk::Unmapped(len)) => {
                let len = *len;
                if len <= max_len {
                    self.chunk = None;
                    Ok(len)
                } else {
                    self.chunk = Some(Chunk::Unmapped(len - max_len));
                    Ok(max_len)
                }
            }
            None => {
                return Err(self.fail("stream shorter than input"));
            }
        }
    }

    fn verify_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            for b in actual {
                // FIXME: use intrinsics
                if *b != byte {
                    return Err(self.fail(&format!("fill with value {}, len {}", byte, len)));
                }
            }
            let actual_len = actual.len() as u64;
            drop(actual);
            self.consume_data(actual_len)?;
            remaining -= actual_len;
        }

        Ok(())
    }

    fn verify_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let len = self.get_unmapped(remaining)?;
            remaining -= len;
        }
        Ok(())
    }

    fn verify_data_(&mut self, expected: &[u8]) -> Result<()> {
        let mut remaining = expected.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            let actual_len = actual.len() as u64;
            if actual != &expected[offset as usize..(offset + actual_len) as usize] {
                return Err(self.fail("data mismatch"));
            }
            drop(actual);
            self.consume_data(actual_len)?;
            remaining -= actual_len;
            offset += actual_len;
        }
        Ok(())
    }

    fn verify_data(&mut self, slab: u32, offset: u32, nr_entries: u32) -> Result<u64> {
        let mut total_len = 0;
        let data = self.data_file.read(slab)?;
        let info = self.get_info(slab)?;
        for entry in 0..nr_entries {
            let (data_begin, data_end, expected_hash) =
                info.get(offset as usize + entry as usize).unwrap();
            assert!(*data_end <= data.len() as u32);

            // FIXME: make this paranioa check optional
            // Verify hash
            let actual_hash = hash_256(&data[*data_begin as usize..*data_end as usize]);
            assert_eq!(actual_hash, *expected_hash);

            // Verify data
            let expected = &data[*data_begin as usize..*data_end as usize];
            self.verify_data_(expected)?;
            total_len += (data_end - data_begin) as u64;
        }
        Ok(total_len)
    }

    fn verify_entry(&mut self, e: &MapEntry) -> Result<u64> {
        use MapEntry::*;

        match e {
            Fill { byte, len } => {
                let len = *len as u64;
                self.verify_fill(*byte, len)?;
                self.total_verified += len;
                Ok(len)
            }
            Unmapped { len } => {
                let len = *len;
                self.verify_unmapped(len)?;
                Ok(len)
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let len = self.verify_data(*slab, *offset, *nr_entries)?;
                self.total_verified += len;
                Ok(len)
            }
        }
    }

    pub fn verify(&mut self, report: &Arc<Report>) -> Result<()> {
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

                let entry_len = self.verify_entry(&e)?;

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

fn thick_verifier(input_file: &Path, stream: &str, config: &config::Config) -> Result<Verifier> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file.clone())
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(&input_file)?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let input_it = Box::new(ThickChunker::new(input, 16 * 1024 * 1024)?);

    let v = Verifier::new(&stream, cache_nr_entries, input_it, input_size)?;
    Ok(v)
}

fn thin_verifier(input_file: &Path, stream: &str, config: &config::Config) -> Result<Verifier> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file.clone())
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(&input_file)?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let mappings = read_thin_mappings(&input_file)?;
    /*
    let _mapped_size =
        mappings.provisioned_blocks.len() as u64 * mappings.data_block_size as u64 * 512;
        */
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_it = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));

    let v = Verifier::new(&stream, cache_nr_entries, input_it, input_size)?;
    Ok(v)
}

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;
    let stream = matches.value_of("STREAM").unwrap();

    env::set_current_dir(&archive_dir)?;

    let config = config::read_config(".")?;

    report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream
    ));

    let mut v = if is_thin_device(&input_file)? {
        thin_verifier(&input_file, stream, &config)?
    } else {
        thick_verifier(&input_file, stream, &config)?
    };

    v.verify(&report)
}

//-----------------------------------------
