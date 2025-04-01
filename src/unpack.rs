use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
use io::{Read, Seek, Write};
use size_display::Size;
use std::env;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thinp::report::*;

use crate::archive;
use crate::archive::SLAB_SIZE_TARGET;
use crate::chunkers::*;
use crate::config;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::builder::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;
use crate::thin_metadata::*;

//-----------------------------------------

// Unpack and verify do different things with the data.
trait UnpackDest {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()>;
    fn handle_unmapped(&mut self, len: u64) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

struct Unpacker<D: UnpackDest> {
    stream_file: SlabFile,
    archive: archive::Data,
    dest: D,
}

impl<D: UnpackDest> Unpacker<D> {
    // Assumes current directory is the root of the archive.
    fn new(stream: &str, cache_nr_entries: usize, dest: D) -> Result<Self> {
        let data_file = SlabFileBuilder::open(data_path())
            .cache_nr_entries(cache_nr_entries)
            .build()?;
        let hashes_file = Arc::new(Mutex::new(SlabFileBuilder::open(hashes_path()).build()?));
        let stream_file = SlabFileBuilder::open(stream_path(stream)).build()?;

        Ok(Self {
            stream_file,
            archive: archive::Data::new(data_file, hashes_file, cache_nr_entries)?,
            dest,
        })
    }

    fn unpack_entry(&mut self, e: &MapEntry) -> Result<()> {
        use MapEntry::*;
        match e {
            Fill { byte, len } => {
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
                self.dest.handle_unmapped(*len)?;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let (data, start, end) =
                    self.archive.data_get(*slab, *offset, *nr_entries, None)?;
                self.dest.handle_mapped(&data[start..end])?;
            }
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => {
                let partial = Some((*begin, *end));
                let (data, start, end) =
                    self.archive
                        .data_get(*slab, *offset, *nr_entries, partial)?;
                self.dest.handle_mapped(&data[start..end])?;
            }
            Ref { .. } => {
                // Can't get here.
                return Err(anyhow!("unexpected MapEntry::Ref (shouldn't be possible)"));
            }
        }

        Ok(())
    }

    fn unpack(&mut self, report: &Arc<Report>, total: u64) -> Result<()> {
        report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut unpacker = stream::MappingUnpacker::default();

        let start_time: DateTime<Utc> = Utc::now();

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
                    report.progress(percent);
                }
            }
        }

        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

        report.info(&format!(
            "speed            : {:.2}/s",
            Size((total as f64 / elapsed) as u64)
        ));

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

// defined in include/uapi/linux/fs.h
const BLK_IOC_CODE: u8 = 0x12;
const BLKDISCARD_SEQ: u8 = 119;
nix::ioctl_write_ptr_bad!(
    ioctl_blkdiscard,
    nix::request_code_none!(BLK_IOC_CODE, BLKDISCARD_SEQ),
    [u64; 2]
);

struct ThinDest {
    block_size: u64,
    output: File,
    pos: u64,
    provisioned: RunIter,

    // (provisioned, len bytes)
    run: Option<(bool, u64)>,
    writes_avoided: u64,
}

impl ThinDest {
    fn issue_discard(&mut self, len: u64) -> Result<()> {
        let begin = self.pos;
        let end = begin + len;

        // Discards should always be block aligned
        assert_eq!(begin % self.block_size, 0);
        assert_eq!(end % self.block_size, 0);

        unsafe {
            ioctl_blkdiscard(self.output.as_raw_fd(), &[begin, len])?;
        }

        Ok(())
    }

    //------------------

    // These low level io functions update the position.
    fn forward(&mut self, len: u64) -> Result<()> {
        self.output.seek(std::io::SeekFrom::Current(len as i64))?;
        self.pos += len;
        Ok(())
    }

    fn rewind(&mut self, len: u64) -> Result<()> {
        self.output
            .seek(std::io::SeekFrom::Current(-(len as i64)))?;
        self.pos -= len;
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        self.pos += data.len() as u64;
        Ok(())
    }

    fn discard(&mut self, len: u64) -> Result<()> {
        self.issue_discard(len)?;
        self.forward(len)?;
        Ok(())
    }

    fn read(&mut self, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        self.output.read_exact(&mut buf[..])?;
        self.pos += len;
        Ok(buf)
    }

    //------------------
    fn handle_mapped_unprovisioned(&mut self, data: &[u8]) -> Result<()> {
        self.write(data)
    }

    fn handle_mapped_provisioned(&mut self, data: &[u8]) -> Result<()> {
        let actual = self.read(data.len() as u64)?;
        if actual == data {
            self.writes_avoided += data.len() as u64;
        } else {
            self.rewind(data.len() as u64)?;
            self.write(data)?;
        }

        Ok(())
    }

    fn handle_unmapped_unprovisioned(&mut self, len: u64) -> Result<()> {
        self.forward(len)
    }

    fn handle_unmapped_provisioned(&mut self, len: u64) -> Result<()> {
        self.discard(len)
    }

    fn ensure_run(&mut self) -> Result<()> {
        if self.run.is_none() {
            match self.provisioned.next() {
                Some((provisioned, run)) => {
                    self.run = Some((provisioned, (run.end - run.start) as u64 * self.block_size));
                }
                None => {
                    return Err(anyhow!("internal error: out of runs"));
                }
            }
        }

        Ok(())
    }

    fn next_run(&mut self, max_len: u64) -> Result<(bool, u64)> {
        self.ensure_run()?;
        let (provisioned, run_len) = self.run.take().unwrap();
        if run_len <= max_len {
            Ok((provisioned, run_len))
        } else {
            self.run = Some((provisioned, run_len - max_len));
            Ok((provisioned, max_len))
        }
    }
}

impl UnpackDest for ThinDest {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        let mut remaining = data.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            if provisioned {
                self.handle_mapped_provisioned(&data[offset as usize..(offset + c_len) as usize])?;
            } else {
                self.handle_mapped_unprovisioned(
                    &data[offset as usize..(offset + c_len) as usize],
                )?;
            }

            remaining -= c_len;
            offset += c_len;
        }

        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            if provisioned {
                self.handle_unmapped_provisioned(c_len)?;
            } else {
                self.handle_unmapped_unprovisioned(c_len)?;
            }

            remaining -= c_len;
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        assert!(self.run.is_none());
        assert!(self.provisioned.next().is_none());
        Ok(())
    }
}

//-----------------------------------------

pub fn run_unpack(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap())
        .canonicalize()
        .context("Bad archive dir")?;
    let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());
    let stream = matches.get_one::<String>("STREAM").unwrap();
    let create = matches.get_flag("CREATE");

    let output = if create {
        fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(output_file)
            .context("Couldn't open output")?
    } else {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(output_file)
            .context("Couldn't open output")?
    };
    env::set_current_dir(archive_dir)?;
    let stream_cfg = config::read_stream_config(stream)?;

    report.set_title(&format!("Unpacking {} ...", output_file.display()));
    if create {
        let config = config::read_config(".", matches)?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        let dest = ThickDest { output };
        let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
        u.unpack(&report, stream_cfg.size)
    } else {
        // Check the size matches the stream size.
        let stream_size = stream_cfg.size;
        let output_size = thinp::file_utils::file_size(output_file)?;
        if output_size != stream_size {
            return Err(anyhow!("Destination size doesn't not match stream size"));
        }

        let config = config::read_config(".", matches)?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        if is_thin_device(output_file)? {
            let mappings = read_thin_mappings(output_file)?;
            let block_size = mappings.data_block_size as u64 * 512;
            let provisioned = RunIter::new(
                mappings.provisioned_blocks,
                (output_size / block_size) as u32,
            );

            let dest = ThinDest {
                block_size,
                output,
                pos: 0,
                provisioned,
                run: None,
                writes_avoided: 0,
            };
            let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
            u.unpack(&report, stream_size)
        } else {
            let dest = ThickDest { output };
            let mut u = Unpacker::new(stream, cache_nr_entries, dest)?;
            u.unpack(&report, stream_size)
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
}

impl UnpackDest for VerifyDest {
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
        if self.chunk.is_some() || self.input_it.next().is_some() {
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
    let mappings = read_thin_mappings(input_file)?;

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
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap()).canonicalize()?;
    let stream = matches.get_one::<String>("STREAM").unwrap();

    env::set_current_dir(archive_dir)?;

    let config = config::read_config(".", matches)?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let stream_cfg = config::read_stream_config(stream)?;

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
    u.unpack(&report, stream_cfg.size)
}

//-----------------------------------------
