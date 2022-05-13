use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;
use io::{Seek, Write};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::Arc;
use thinp::report::*;

use crate::chunkers::*;
use crate::config;
use crate::hash_index::*;
use crate::pack::SLAB_SIZE_TARGET;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;
use crate::thin_metadata::*;

//-----------------------------------------

// Unpack and verify do different things with the data.
trait UnpackDest {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()>;
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()>;
    fn handle_unmapped(&mut self, len: u64) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

struct Unpacker<D: UnpackDest> {
    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    // FIXME: make this an lru cache
    slabs: BTreeMap<u32, Arc<ByIndex>>,
    partial: Option<(u32, u32)>,

    dest: D,
}

impl<D: UnpackDest> Unpacker<D> {
    // Assumes current directory is the root of the archive.
    fn new(stream: &str, cache_nr_entries: usize, dest: D) -> Result<Self> {
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
            dest,
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

    fn unpack_entry(&mut self, e: &MapEntry) -> Result<()> {
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
                    let bytes: Vec<u8> = vec![*byte; write_len as usize];
                    self.dest.handle_mapped(&bytes[..])?;
                    written += write_len;
                }
            }
            Unmapped { len } => {
                assert!(self.partial.is_none());
                self.dest.handle_unmapped(*len)?;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let info = self.get_info(*slab)?;

                let (data_begin, data_end) = if *nr_entries == 1 {
                    let (data_begin, data_end, _expected_hash) =
                        info.get(*offset as usize).unwrap();
                    (*data_begin as usize, *data_end as usize)
                } else {
                    let (data_begin, _data_end, _expected_hash) =
                        info.get(*offset as usize).unwrap();
                    let (_data_begin, data_end, _expected_hash) = info
                        .get((*offset as usize) + (*nr_entries as usize) - 1)
                        .unwrap();
                    (*data_begin as usize, *data_end as usize)
                };

                let data = self.data_file.read(*slab)?;
                if let Some((begin, end)) = self.partial {
                    let data_end = data_begin + end as usize;
                    let data_begin = data_begin + begin as usize;
                    self.dest.handle_mapped(&data[data_begin..data_end])?;
                    self.partial = None;
                } else {
                    self.dest.handle_mapped(&data[data_begin..data_end])?;
                }
            }
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => {
                assert!(self.partial.is_none());
                self.partial = Some((*begin, *end));
                self.unpack_entry(&MapEntry::Data {
                    slab: *slab,
                    offset: *offset,
                    nr_entries: *nr_entries,
                })?;
            }
            Ref { .. } => {
                // Can't get here.
                return Err(anyhow!("unexpected MapEntry::Ref (shouldn't be possible)"));
            }
        }

        Ok(())
    }

    pub fn unpack(&mut self, report: &Arc<Report>) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut unpacker = stream::MappingUnpacker::default();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let (entries, _positions) = unpacker.unpack(&stream_data[..])?;
            let nr_entries = entries.len();

            for (i, e) in entries.iter().enumerate() {
                self.unpack_entry(e)?;

                if i % 1024 == 0 {
                    // update progress bar
                    let entry_fraction = i as f64 / nr_entries as f64;
                    let slab_fraction = s as f64 / nr_slabs as f64;
                    let percent =
                        ((slab_fraction + (entry_fraction / nr_slabs as f64)) * 100.0) as u8;
                    report.progress(percent as u8);
                }
            }
        }
        self.dest.complete()?;
        report.progress(100);

        Ok(())
    }
}

//-----------------------------------------

struct ThickDest<W: Write> {
    output: W,
}

fn write_bytes<W: Write>(w: &mut W, byte: u8, len: u64) -> Result<()> {
    let buf_size = std::cmp::min(len, 64 * 1024 * 1024);
    let buf = vec![byte; buf_size as usize];

    let mut remaining = len;
    while remaining > 0 {
        let w_len = std::cmp::min(buf_size, remaining);
        w.write_all(&buf[0..(w_len as usize)])?;
        remaining -= w_len;
    }

    Ok(())
}

impl<W: Write> UnpackDest for ThickDest<W> {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        write_bytes(&mut self.output, byte, len)
    }

    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        write_bytes(&mut self.output, 0, len)
    }

    fn complete(&mut self) -> Result<()> {
        Ok(())
    }
}

//-----------------------------------------

struct ThinDest<W: Seek + Write> {
    output: W,
}

impl<W: Seek + Write> UnpackDest for ThinDest<W> {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        write_bytes(&mut self.output, byte, len)
    }

    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        self.output.seek(std::io::SeekFrom::Current(len as i64))?;
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        Ok(())
    }
}

//-----------------------------------------

pub fn run_unpack(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let stream = matches.value_of("STREAM").unwrap();
    let create = matches.is_present("CREATE");
    env::set_current_dir(&archive_dir)?;

    report.set_title(&format!("Unpacking {} ...", output_file.display()));
    if create {
        let output = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(output_file)?;

        let config = config::read_config(".")?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        let dest = ThickDest { output };
        let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
        u.unpack(&report)
    } else {
        // Check the size matches the stream size.
        let stream_cfg = config::read_stream_config(&stream)?;
        let stream_size = stream_cfg.size;
        let output_size = thinp::file_utils::file_size(output_file)?;
        if output_size != stream_size {
            return Err(anyhow!("Destination size doesn't not match stream size"));
        }

        let output = fs::OpenOptions::new()
            .read(false)
            .write(true)
            .open(output_file)?;

        let config = config::read_config(".")?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        report.set_title(&format!("Unpacking {} ...", output_file.display()));
        if is_thin_device(output_file)? {
            let mappings = read_thin_mappings(output_file)?;
            let run_iter = RunIter::new(
                mappings.provisioned_blocks,
                (output_size / (mappings.data_block_size as u64 * 512)) as u32,
            );

            let dest = ThinDest { output };
            let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
            u.unpack(&report)
        } else {
            let dest = ThickDest { output };
            let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
            u.unpack(&report)
        }
    }
}

//-----------------------------------------

struct VerifyDest {
    input_it: Box<dyn Iterator<Item = Result<Chunk>>>,
    chunk: Option<Chunk>,
    chunk_offset: u64,
    total_verified: u64,
}

impl VerifyDest {
    fn new(input_it: Box<dyn Iterator<Item = Result<Chunk>>>) -> Self {
        Self {
            input_it,
            chunk: None,
            chunk_offset: 0,
            total_verified: 0,
        }
    }
}

impl VerifyDest {
    fn fail(&self, msg: &str) -> anyhow::Error {
        anyhow!(format!(
            "verify failed at offset ~{}: {}",
            self.total_verified, msg
        ))
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

    fn peek_data(&mut self, max_len: u64) -> Result<&[u8]> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let len = std::cmp::min(bytes.len() - self.chunk_offset as usize, max_len as usize);
                Ok(&bytes[self.chunk_offset as usize..(self.chunk_offset + len as u64) as usize])
            }
            Some(Chunk::Unmapped(_)) => Err(self.fail("expected data, got unmapped")),
            Some(Chunk::Ref(_)) => Err(self.fail("expected data, got ref")),
            None => Err(self.fail("ensure_chunk() failed")),
        }
    }

    fn consume_data(&mut self, len: u64) -> Result<()> {
        use std::cmp::Ordering::*;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let c_len = bytes.len() as u64 - self.chunk_offset;
                match c_len.cmp(&len) {
                    Less => {
                        return Err(self.fail("bad consume, chunk too short"));
                    }
                    Greater => {
                        self.chunk_offset += len;
                    }
                    Equal => {
                        self.chunk = None;
                    }
                }
            }
            Some(Chunk::Unmapped(_)) => {
                return Err(self.fail("bad consume, unexpected unmapped chunk"));
            }
            Some(Chunk::Ref(_)) => {
                return Err(self.fail("bad consume, unexpected ref chunk"));
            }
            None => {
                return Err(self.fail("archived stream longer than input"));
            }
        }
        Ok(())
    }

    fn get_unmapped(&mut self, max_len: u64) -> Result<u64> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(_)) => Err(self.fail("expected unmapped, got data")),
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
            Some(Chunk::Ref(_)) => Err(self.fail("unexpected Ref")),
            None => Err(self.fail("archived stream longer than input")),
        }
    }

    fn more_data(&self) -> bool {
        self.chunk.is_some()
    }
}

impl UnpackDest for VerifyDest {
    fn handle_fill(&mut self, byte: u8, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            let actual_len = actual.len() as u64;

            for b in actual {
                if *b != byte {
                    return Err(self.fail("fill mismatch"));
                }
            }
            self.consume_data(actual_len)?;
            remaining -= actual_len;
        }
        self.total_verified += len;
        Ok(())
    }

    fn handle_mapped(&mut self, expected: &[u8]) -> Result<()> {
        let mut remaining = expected.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let actual = self.peek_data(remaining)?;
            let actual_len = actual.len() as u64;
            if actual != &expected[offset as usize..(offset + actual_len) as usize] {
                return Err(self.fail("data mismatch"));
            }
            self.consume_data(actual_len)?;
            remaining -= actual_len;
            offset += actual_len;
        }
        self.total_verified += expected.len() as u64;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let len = self.get_unmapped(remaining)?;
            remaining -= len;
        }
        self.total_verified += len;
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        if self.more_data() {
            return Err(anyhow!("archived stream is too short"));
        }
        Ok(())
    }
}

fn thick_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input_it = Box::new(ThickChunker::new(input_file, 16 * 1024 * 1024)?);
    Ok(VerifyDest::new(input_it))
}

fn thin_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;
    let mappings = read_thin_mappings(&input_file)?;

    // FIXME: what if input_size is not a multiple of the block size?
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_it = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));

    Ok(VerifyDest::new(input_it))
}

pub fn run_verify(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;
    let stream = matches.value_of("STREAM").unwrap();

    env::set_current_dir(&archive_dir)?;

    let config = config::read_config(".")?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream
    ));

    let dest = if is_thin_device(&input_file)? {
        thin_verifier(&input_file)?
    } else {
        thick_verifier(&input_file)?
    };

    let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
    u.unpack(&report)
}

//-----------------------------------------
