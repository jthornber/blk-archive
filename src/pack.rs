use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::prelude::*;
use clap::ArgMatches;
use io::prelude::*;
use io::Write;
use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use size_display::Size;
use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Cursor;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::config;
use crate::content_sensitive_splitter::*;
use crate::cuckoo_filter::*;
use crate::hash::*;
use crate::iovec::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;
use crate::thin_metadata::*;
use crate::paths;

//-----------------------------------------

fn all_same(iov: &IoVec) -> (u8, u64, bool) {
    let mut len = 0;
    let mut same = true;
    let first_b = iov[0][0];

    for b in iov[0] {
        if *b != first_b {
            same = false;
            break;
        }
    }
    len += iov[0].len();

    for v in iov.iter().skip(1) {
        if same {
            for b in *v {
                if *b != first_b {
                    same = false;
                    break;
                }
            }
        }
        len += v.len();
    }

    (first_b, len as u64, same)
}

//-----------------------------------------

const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

struct DedupHandler {
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    // seen: BTreeSet<Hash64>,
    seen: CuckooFilter,
    hashes: BTreeMap<Hash256, MapEntry>,

    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    current_slab: u32,
    current_entries: u32,

    data_buf: Vec<u8>,
    hashes_buf: Vec<u8>,
    stream_buf: Vec<u8>,

    mapping_builder: MappingBuilder,

    data_written: u64,
    mapped_size: u64,
    fill_size: u64,
}

impl DedupHandler {
    // Returns the len of the data entry
    fn parse_hash_entry(input: &[u8]) -> IResult<&[u8], Hash256> {
        let (input, hash) = take(std::mem::size_of::<Hash256>())(input)?;
        let hash = Hash256::clone_from_slice(hash);
        let (input, _len) = le_u32(input)?;
        Ok((input, hash))
    }

    fn parse_hashes(input: &[u8]) -> IResult<&[u8], Vec<Hash256>> {
        many0(Self::parse_hash_entry)(input)
    }

    fn read_hashes(
        hashes_file: &mut SlabFile,
    ) -> Result<(CuckooFilter, BTreeMap<Hash256, MapEntry>)> {
        let seen = CuckooFilter::read(paths::index_path())?; // FIXME: handle resizing

        let mut r = BTreeMap::new();
        let nr_slabs = hashes_file.get_nr_slabs();
        for s in 0..nr_slabs {
            let buf = hashes_file.read(s as u32)?;
            let (_, hashes) =
                Self::parse_hashes(&buf).map_err(|_| anyhow!("couldn't parse hashes"))?;

            let mut i = 0;
            for h in hashes {
                /*
                let mini_hash = hash_64(&h[..]);
                let mut c = Cursor::new(&mini_hash);
                let mini_hash = c.read_u64::<LittleEndian>()?;
                */
                r.insert(
                    h,
                    MapEntry::Data {
                        slab: s as u32,
                        offset: i,
                        nr_entries: 1,
                    },
                );
                i += 1;
            }
        }

        Ok((seen, r))
    }

    fn new(data_file: SlabFile, mut hashes_file: SlabFile, stream_file: SlabFile) -> Result<Self> {
        let (seen, hashes) = Self::read_hashes(&mut hashes_file)?;
        let nr_slabs = data_file.get_nr_slabs() as u32;
        assert_eq!(data_file.get_nr_slabs(), hashes_file.get_nr_slabs());

        Ok(Self {
            nr_chunks: 0,

            seen,
            hashes,

            data_file,
            hashes_file,
            stream_file,

            current_slab: nr_slabs,
            current_entries: 0,

            data_buf: Vec::new(),
            hashes_buf: Vec::new(),
            stream_buf: Vec::new(),

            mapping_builder: MappingBuilder::default(),

            // Stats
            data_written: 0,
            mapped_size: 0,
            fill_size: 0,
        })
    }

    fn rebuild_index(&mut self, _capacity: usize) -> Result<()> {
        todo!();
    }

    fn ensure_extra_capacity(&mut self, blocks: usize) -> Result<()> {
        if self.seen.capacity() < self.seen.len() + blocks {
            self.rebuild_index(self.seen.len() + blocks)?;
        }

        Ok(())
    }

    fn complete_slab_(slab: &mut SlabFile, buf: &mut Vec<u8>) -> Result<()> {
        slab.write_slab(&buf)?;
        buf.clear();
        Ok(())
    }

    fn complete_slab(slab: &mut SlabFile, buf: &mut Vec<u8>, threshold: usize) -> Result<bool> {
        if buf.len() > threshold {
            Self::complete_slab_(slab, buf)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn maybe_complete_data(&mut self) -> Result<()> {
        if Self::complete_slab(&mut self.data_file, &mut self.data_buf, SLAB_SIZE_TARGET)? {
            Self::complete_slab_(&mut self.hashes_file, &mut self.hashes_buf)?;
            self.current_slab += 1;
            self.current_entries = 0;
        }
        Ok(())
    }

    fn maybe_complete_stream(&mut self) -> Result<()> {
        Self::complete_slab(
            &mut self.stream_file,
            &mut self.stream_buf,
            SLAB_SIZE_TARGET,
        )?;
        Ok(())
    }

    // Returns the (slab, entry) for the newly added entry
    fn add_data_entry(&mut self, iov: &IoVec) -> Result<(u32, u32)> {
        let r = (self.current_slab, self.current_entries);
        for v in iov {
            self.data_buf.extend(v.iter()); // FIXME: this looks slow
            self.data_written += v.len() as u64;
        }
        self.current_entries += 1;
        Ok(r)
    }

    fn add_hash_entry(&mut self, h: Hash256, len: u32) -> Result<()> {
        self.hashes_buf.write_all(&h)?;
        self.hashes_buf.write_u32::<LittleEndian>(len)?;
        Ok(())
    }

    fn add_stream_entry(&mut self, e: &MapEntry, len: u64) -> Result<()> {
        self.mapping_builder.next(e, len, &mut self.stream_buf)
    }

    fn do_add(&mut self, h: Hash256, iov: &IoVec, len: u64) -> Result<MapEntry> {
        self.add_hash_entry(h, len as u32)?;
        let (slab, offset) = self.add_data_entry(iov)?;
        let me = MapEntry::Data { slab, offset, nr_entries: 1 };
        self.hashes.insert(h, me);
        self.maybe_complete_data()?;
        Ok(me)
    }
}

impl IoVecHandler for DedupHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;
        let (first_byte, len, same) = all_same(iov);
        self.mapped_size += len;

        if same {
            self.fill_size += len;
            self.add_stream_entry(&MapEntry::Fill { byte: first_byte, len }, len)?;
            self.maybe_complete_stream()?;
        } else {
            let h = hash_256_iov(iov);
            let mini_hash = hash_64(&h);
            let mut c = Cursor::new(&mini_hash);
            let mini_hash = c.read_u64::<LittleEndian>()?;

            let me: MapEntry;
            if self.seen.test_and_set(mini_hash, self.current_slab)? {
                me = self.do_add(h, iov, len)?;
            } else {
                if let Some(e) = self.hashes.get(&h) {
                    me = *e;
                } else {
                    me = self.do_add(h, iov, len)?;
                }
            }

            self.add_stream_entry(&me, len)?;
            self.maybe_complete_stream()?;
        }

        Ok(())
    }

    fn handle_gap(&mut self, len: u64) -> Result<()> {
        self.add_stream_entry(&MapEntry::Unmapped { len }, len)?;
        self.maybe_complete_stream()?;

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        let mut mapping_builder = MappingBuilder::default();
        std::mem::swap(&mut mapping_builder, &mut self.mapping_builder);
        mapping_builder.complete(&mut self.stream_buf)?;

        Self::complete_slab(&mut self.hashes_file, &mut self.hashes_buf, 0)?;
        Self::complete_slab(&mut self.data_file, &mut self.data_buf, 0)?;
        Self::complete_slab(&mut self.stream_file, &mut self.stream_buf, 0)?;

        self.hashes_file.close()?;
        self.data_file.close()?;
        self.stream_file.close()?;

        self.seen.write(paths::index_path())?;

        Ok(())
    }
}

//-----------------------------------------

#[derive(Debug)]
pub enum Chunk {
    Mapped(Vec<u8>),
    Unmapped(u64),
}

struct FileChunker {
    input: File,
    input_size: u64,
    total_read: u64,
    block_size: usize,
}

impl FileChunker {
    fn new(input: File, block_size: usize) -> Result<Self> {
        let input_size = input.metadata()?.len();

        Ok(Self {
            input,
            input_size,
            total_read: 0,
            block_size,
        })
    }

    // FIXME: stop reallocating and zeroing these buffers
    fn do_read(&mut self, mut buffer: Vec<u8>) -> Result<Option<Chunk>> {
        self.input.read_exact(&mut buffer)?;
        self.total_read += buffer.len() as u64;
        return Ok(Some(Chunk::Mapped(buffer)));
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let remaining = self.input_size - self.total_read;

        if remaining == 0 {
            Ok(None)
        } else if remaining >= self.block_size as u64 {
            let buf = vec![0; self.block_size];
            self.do_read(buf)
        } else {
            let buf = vec![0; remaining as usize];
            self.do_read(buf)
        }
    }
}

impl Iterator for FileChunker {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Err(e) => Some(Err(e)),
            Ok(None) => None,
            Ok(Some(c)) => Some(Ok(c)),
        }
    }
}

//-----------------------------------------

struct ThinChunker<'a> {
    input: File,
    run_iter: RunIter<'a>,
    data_block_size: u64,

    max_read_size: usize,
    current_run: Option<(bool, Range<u64>)>,
}

impl<'a> ThinChunker<'a> {
    fn new(input: File, run_iter: RunIter<'a>, data_block_size: u64) -> Self {
        Self {
            input,
            run_iter,
            data_block_size,

            max_read_size: 16 * 1024 * 1024,
            current_run: None,
        }
    }

    fn next_run_bytes(&mut self) -> Option<(bool, Range<u64>)> {
        self.run_iter.next().map(|(b, Range { start, end })| {
            (
                b,
                Range {
                    start: start as u64 * self.data_block_size,
                    end: end as u64 * self.data_block_size,
                },
            )
        })
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut run = None;
        std::mem::swap(&mut run, &mut self.current_run);

        match run.or_else(|| self.next_run_bytes()) {
            Some((false, run)) => Ok(Some(Chunk::Unmapped(run.end - run.start))),
            Some((true, run)) => {
                let run_len = run.end - run.start;
                if run_len <= self.max_read_size as u64 {
                    let mut buf = vec![0; run_len as usize];
                    self.input.read_exact_at(&mut buf, run.start)?;
                    Ok(Some(Chunk::Mapped(buf)))
                } else {
                    let mut buf = vec![0; self.max_read_size];
                    self.input.read_exact_at(&mut buf, run.start)?;
                    self.current_run = Some((true, (run.start + buf.len() as u64)..run.end));
                    Ok(Some(Chunk::Mapped(buf)))
                }
            }
            None => Ok(None),
        }
    }
}

impl<'a> Iterator for ThinChunker<'a> {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        let mc = self.next_chunk();
        match mc {
            Err(e) => Some(Err(e)),
            Ok(Some(c)) => Some(Ok(c)),
            Ok(None) => None,
        }
    }
}

//-----------------------------------------

// Assumes we've chdir'd to the archive
fn new_stream_path_(rng: &mut ChaCha20Rng) -> Result<Option<(String, PathBuf)>> {
    // choose a random number
    let n: u64 = rng.gen();

    // turn this into a path
    let name = format!("{:>016x}", n);
    let path: PathBuf = ["streams", &name].iter().collect();

    if path.exists() {
        Ok(None)
    } else {
        Ok(Some((name, path)))
    }
}

fn new_stream_path() -> Result<(String, PathBuf)> {
    let mut rng = ChaCha20Rng::from_entropy();
    loop {
        if let Some(r) = new_stream_path_(&mut rng)? {
            return Ok(r);
        }
    }

    // Can't get here
}

pub fn pack_<'a, I>(
    report: &Arc<Report>,
    input_path: &str,
    stream_name: &str,
    it: I,
    input_size: u64,
    mapped_size: u64,
    block_size: usize,
    thin_id: Option<u32>,
) -> Result<()>
where
    I: Iterator<Item = Result<Chunk>>,
{
    let mut splitter = ContentSensitiveSplitter::new(block_size as u32);

    let data_path: PathBuf = ["data", "data"].iter().collect();
    let data_file =
        SlabFile::open_for_write(&data_path, 128).context("couldn't open data slab file")?;
    let data_size = data_file.get_file_size();

    let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
    let hashes_file =
        SlabFile::open_for_write(&hashes_path, 16).context("couldn't open hashes slab file")?;
    let hashes_size = hashes_file.get_file_size();
    let (stream_id, mut stream_path) = new_stream_path()?;

    std::fs::create_dir(stream_path.clone())?;
    stream_path.push("stream");

    let stream_file =
        SlabFile::create(stream_path, 16, true).context("couldn't open stream slab file")?;

    let mut handler = DedupHandler::new(data_file, hashes_file, stream_file)?;
    handler.ensure_extra_capacity(mapped_size as usize / block_size);

    report.progress(0);
    let start_time: DateTime<Utc> = Utc::now();

    let mut total_read = 0;
    for chunk in it {
        match chunk? {
            Chunk::Mapped(buffer) => {
                let len = buffer.len();
                splitter.next_data(buffer, &mut handler)?;
                total_read += len as u64;
                report.progress(((100 * total_read) / mapped_size) as u8);
            }
            Chunk::Unmapped(len) => {
                splitter.next_break(len, &mut handler)?;
            }
        }
    }

    splitter.complete(&mut handler)?;
    report.progress(100);
    let end_time: DateTime<Utc> = Utc::now();
    let elapsed = end_time - start_time;
    let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

    report.info(&format!("stream id        : {}", stream_id));
    report.info(&format!("file size        : {:.2}", Size(input_size)));
    report.info(&format!("mapped size      : {:.2}", Size(mapped_size)));
    report.info(&format!(
        "fills size       : {:.2}",
        Size(handler.fill_size)
    ));
    report.info(&format!(
        "duplicate data   : {:.2}",
        Size(total_read - handler.data_written - handler.fill_size)
    ));

    let data_written = handler.data_file.get_file_size() - data_size;
    let hashes_written = handler.hashes_file.get_file_size() - hashes_size;
    let stream_written = handler.stream_file.get_file_size();

    report.info(&format!("data written     : {:.2}", Size(data_written)));
    report.info(&format!("hashes written   : {:.2}", Size(hashes_written)));
    report.info(&format!("stream written   : {:.2}", Size(stream_written)));

    let compression = ((data_written + hashes_written + stream_written) * 100) as f64 / total_read as f64;
    report.info(&format!("compression      : {:.2}%", compression));
    report.info(&format!(
        "speed            : {:.2}/s",
        Size((total_read as f64 / elapsed) as u64)
    ));

    // write the stream config
    let cfg = config::StreamConfig {
        name: Some(stream_name.to_string()),
        source_path: input_path.to_string(),
        pack_time: config::now(),
        size: input_size,
        packed_size: data_written + hashes_written + stream_written,
        thin_id,
    };
    config::write_stream_config(&stream_id, &cfg)?;

    Ok(())
}

pub fn pack(report: &Arc<Report>, input_file: &Path, input_name: &str, block_size: usize) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = input.metadata()?.len();
    let input_iter = FileChunker::new(input, 16 * 1024 * 1024)?;
    report.set_title(&format!("Packing {} ...", input_file.display()));
    pack_(
        report,
        &input_file.display().to_string(),
        &input_name,
        input_iter,
        input_size,
        input_size,
        block_size,
        None,
    )
}

//-----------------------------------------

fn mk_report() -> Arc<Report> {
    if atty::is(atty::Stream::Stdout) {
        Arc::new(mk_progress_bar_report())
    } else {
        Arc::new(mk_simple_report())
    }
}

pub fn run(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let input_name = input_file.file_name().unwrap();
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;
    let report = mk_report();

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;
    pack(&report, &input_file, &input_name.to_str().unwrap(), config.block_size)
}

pub fn run_thin(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let input_name = input_file.file_name().unwrap();
    let input_file = input_file.canonicalize()?;
    let report = mk_report();

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;

    let mappings = read_thin_mappings(input_file.clone())?;

    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file.clone())
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(&input_file)?;
    let mapped_size =
        mappings.provisioned_blocks.len() as u64 * mappings.data_block_size as u64 * 512;
    let run_iter = RunIter::new(
        &mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_iter = ThinChunker::new(input, run_iter, mappings.data_block_size as u64 * 512);
    report.set_title(&format!("Packing {} ...", input_file.display()));
    pack_(
        &report,
        &input_file.display().to_string(),
        &input_name.to_str().unwrap(),
        input_iter,
        input_size,
        mapped_size,
        config.block_size,
        Some(mappings.thin_id),
    )
}

pub fn run_thin_delta(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let input_name = input_file.file_name().unwrap();
    let input_file = input_file.canonicalize()?;
    let report = mk_report();

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;

    let mappings = read_thin_mappings(input_file.clone())?;

    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file.clone())
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(&input_file)?;
    let mapped_size =
        mappings.provisioned_blocks.len() as u64 * mappings.data_block_size as u64 * 512;
    let run_iter = RunIter::new(
        &mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_iter = ThinChunker::new(input, run_iter, mappings.data_block_size as u64 * 512);
    report.set_title(&format!("Packing {} ...", input_file.display()));
    pack_(
        &report,
        &input_file.display().to_string(),
        &input_name.to_str().unwrap(),
        input_iter,
        input_size,
        mapped_size,
        config.block_size,
        Some(mappings.thin_id),
    )
}

//-----------------------------------------
