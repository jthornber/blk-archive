use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::prelude::*;
use clap::ArgMatches;
use io::Write;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use size_display::Size;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::chunkers::*;
use crate::config;
use crate::content_sensitive_splitter::*;
use crate::cuckoo_filter::*;
use crate::hash::*;
use crate::hash_index::*;
use crate::iovec::*;
use crate::paths;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;
use crate::thin_metadata::*;

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

pub const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

struct DedupHandler {
    nr_chunks: usize,

    seen: CuckooFilter,
    hashes: lru::LruCache<u32, ByHash>,

    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    current_slab: u32,
    current_entries: usize,
    current_index: IndexBuilder,

    data_buf: Vec<u8>,
    hashes_buf: Vec<u8>,
    stream_buf: Vec<u8>,

    mapping_builder: MappingBuilder,

    data_written: u64,
    mapped_size: u64,
    fill_size: u64,
}

impl DedupHandler {
    fn get_hash_index(&mut self, slab: u32) -> Result<&ByHash> {
        // the current slab is not inserted into the self.hashes
        assert!(slab != self.current_slab);

        // FIXME: double lookup
        if self.hashes.get(&slab).is_none() {
            let buf = self.hashes_file.read(slab)?;
            self.hashes.put(slab, ByHash::new(buf.to_vec())?);  // FIXME: is the to_vec() causing a copy?
        }

        Ok(self.hashes.get(&slab).unwrap())
    }

    fn new(
        data_file: SlabFile,
        hashes_file: SlabFile,
        stream_file: SlabFile,
        slab_capacity: usize,
    ) -> Result<Self> {
        let seen = CuckooFilter::read(paths::index_path())?;
        let hashes = lru::LruCache::new(slab_capacity);
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
            current_index: IndexBuilder::with_capacity(1024), // FIXME: estimate

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

    fn rebuild_index(&mut self, new_capacity: usize) -> Result<()> {
        let mut seen = CuckooFilter::with_capacity(new_capacity);

        // Scan the hashes file.
        let nr_slabs = self.hashes_file.get_nr_slabs();
        for s in 0..nr_slabs {
            let buf = self.hashes_file.read(s as u32)?;
            let hi = ByHash::new(buf.to_vec())?;
            for i in 0..hi.len() {
                let h = hi.get(i);
                let mini_hash = hash_64(&h[..]);
                let mut c = Cursor::new(&mini_hash);
                let mini_hash = c.read_u64::<LittleEndian>()?;
                seen.test_and_set(mini_hash, s as u32)?;
            }
        }

        std::mem::swap(&mut seen, &mut self.seen);

        Ok(())
    }

    fn ensure_extra_capacity(&mut self, blocks: usize) -> Result<()> {
        if self.seen.capacity() < self.seen.len() + blocks {
            self.rebuild_index(self.seen.len() + blocks)?;
            eprintln!("resized index to {}", self.seen.capacity());
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

    fn maybe_complete_data(&mut self, target: usize) -> Result<()> {
        if Self::complete_slab(&mut self.data_file, &mut self.data_buf, target)? {
            let mut builder = IndexBuilder::with_capacity(1024);   // FIXME: estimate properly
            std::mem::swap(&mut builder, &mut self.current_index);
            let buffer = builder.build()?;
            self.hashes_buf.write_all(&buffer[..])?;
            let index = ByHash::new(buffer)?;
            self.hashes.put(self.current_slab, index);

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
        let r = (self.current_slab as u32, self.current_entries as u32);
        for v in iov {
            self.data_buf.extend(v.iter()); // FIXME: this looks slow
            self.data_written += v.len() as u64;
        }
        self.current_entries += 1;
        Ok(r)
    }

/*
    fn add_hash_entry(&mut self, h: Hash256, len: u32) -> Result<()> {
        self.hashes_buf.write_all(&h)?;
        self.hashes_buf.write_u32::<LittleEndian>(len)?;
        Ok(())
    }
*/

    fn add_stream_entry(&mut self, e: &MapEntry, len: u64) -> Result<()> {
        self.mapping_builder.next(e, len, &mut self.stream_buf)
    }

    fn do_add(&mut self, h: Hash256, iov: &IoVec, len: u64) -> Result<MapEntry> {
        let (slab, offset) = self.add_data_entry(iov)?;
        let me = MapEntry::Data {
            slab,
            offset,
            nr_entries: 1,
        };
        self.current_index.insert(h, len as usize);
        self.maybe_complete_data(SLAB_SIZE_TARGET)?;
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
            self.add_stream_entry(
                &MapEntry::Fill {
                    byte: first_byte,
                    len,
                },
                len,
            )?;
            self.maybe_complete_stream()?;
        } else {
            let h = hash_256_iov(iov);
            let mini_hash = hash_64(&h);
            let mut c = Cursor::new(&mini_hash);
            let mini_hash = c.read_u64::<LittleEndian>()?;

            let me: MapEntry;
            match self.seen.test_and_set(mini_hash, self.current_slab)? {
                InsertResult::Inserted => {
                    me = self.do_add(h, iov, len)?;
                }
                InsertResult::AlreadyPresent(s) => {
                    if s == self.current_slab {
                        // FIXME: remove this clause
                        me = self.do_add(h, iov, len)?;
                    } else {
                        let hi = self.get_hash_index(s)?;
                        if let Some(offset) = hi.lookup(&h) {
                            me = MapEntry::Data {slab: s, offset: offset as u32, nr_entries: 1};
                        } else {
                            me = self.do_add(h, iov, len)?;
                        }
                    }
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

        self.maybe_complete_data(0)?;
        Self::complete_slab(&mut self.stream_file, &mut self.stream_buf, 0)?;

        self.hashes_file.close()?;
        self.data_file.close()?;
        self.stream_file.close()?;

        self.seen.write(paths::index_path())?;

        Ok(())
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

struct Packer {
    report: Arc<Report>,
    input_path: PathBuf,
    stream_name: String,
    it: Box<dyn Iterator<Item = Result<Chunk>>>,
    input_size: u64,
    mapped_size: u64,
    block_size: usize,
    thin_id: Option<u32>,
    hash_cache_size_meg: usize,
}

impl Packer {
    fn new(
        report: Arc<Report>,
        input_path: PathBuf,
        stream_name: String,
        it: Box<dyn Iterator<Item = Result<Chunk>>>,
        input_size: u64,
        mapped_size: u64,
        block_size: usize,
        thin_id: Option<u32>,
        hash_cache_size_meg: usize,
    ) -> Self {
        Self {
            report,
            input_path,
            stream_name,
            it,
            input_size,
            mapped_size,
            block_size,
            thin_id,
            hash_cache_size_meg,
        }
    }

    fn pack<'a>(&mut self) -> Result<()> {
        let mut splitter = ContentSensitiveSplitter::new(self.block_size as u32);

        let data_file = SlabFileBuilder::open(data_path())
            .write(true)
            .queue_depth(128)
            .build()
            .context("couldn't open data slab file")?;
        let data_size = data_file.get_file_size();

        let hashes_file = SlabFileBuilder::open(hashes_path())
            .write(true)
            .queue_depth(16)
            .build()
            .context("couldn't open hashes slab file")?;
        let hashes_size = hashes_file.get_file_size();
        let (stream_id, mut stream_path) = new_stream_path()?;

        std::fs::create_dir(stream_path.clone())?;
        stream_path.push("stream");

        let stream_file = SlabFileBuilder::create(stream_path)
            .queue_depth(16)
            .compressed(true)
            .build()
            .context("couldn't open stream slab file")?;

        let hashes_per_slab = std::cmp::max(SLAB_SIZE_TARGET / self.block_size, 1);
        let slab_capacity = ((self.hash_cache_size_meg * 1024 * 1024)
            / std::mem::size_of::<Hash256>())
            / hashes_per_slab;

        let mut handler = DedupHandler::new(data_file, hashes_file, stream_file, slab_capacity)?;
        handler.ensure_extra_capacity(self.mapped_size as usize / self.block_size)?;

        self.report.progress(0);
        let start_time: DateTime<Utc> = Utc::now();

        let mut total_read = 0;
        for chunk in &mut self.it {
            match chunk? {
                Chunk::Mapped(buffer) => {
                    let len = buffer.len();
                    splitter.next_data(buffer, &mut handler)?;
                    total_read += len as u64;
                    self.report
                        .progress(((100 * total_read) / self.mapped_size) as u8);
                }
                Chunk::Unmapped(len) => {
                    splitter.next_break(len, &mut handler)?;
                }
            }
        }

        splitter.complete(&mut handler)?;
        self.report.progress(100);
        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

        self.report
            .info(&format!("stream id        : {}", stream_id));
        self.report
            .info(&format!("file size        : {:.2}", Size(self.input_size)));
        self.report
            .info(&format!("mapped size      : {:.2}", Size(self.mapped_size)));
        self.report.info(&format!(
            "fills size       : {:.2}",
            Size(handler.fill_size)
        ));
        self.report.info(&format!(
            "duplicate data   : {:.2}",
            Size(total_read - handler.data_written - handler.fill_size)
        ));

        let data_written = handler.data_file.get_file_size() - data_size;
        let hashes_written = handler.hashes_file.get_file_size() - hashes_size;
        let stream_written = handler.stream_file.get_file_size();

        self.report
            .info(&format!("data written     : {:.2}", Size(data_written)));
        self.report
            .info(&format!("hashes written   : {:.2}", Size(hashes_written)));
        self.report
            .info(&format!("stream written   : {:.2}", Size(stream_written)));

        let ratio = (total_read as f64) / ((data_written + hashes_written + stream_written) as f64);
        self.report
            .info(&format!("ratio            : {:.2}", ratio));
        self.report.info(&format!(
            "speed            : {:.2}/s",
            Size((total_read as f64 / elapsed) as u64)
        ));

        // write the stream config
        let cfg = config::StreamConfig {
            name: Some(self.stream_name.to_string()),
            source_path: self.input_path.display().to_string(),
            pack_time: config::now(),
            size: self.input_size,
            packed_size: data_written + hashes_written + stream_written,
            thin_id: self.thin_id,
        };
        config::write_stream_config(&stream_id, &cfg)?;

        Ok(())
    }
}

//-----------------------------------------

fn thick_packer(
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

fn thin_packer(
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

    let mappings = read_thin_mappings(input_file.clone())?;
    let mapped_size =
        mappings.provisioned_blocks.len() as u64 * mappings.data_block_size as u64 * 512;
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_iter = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));
    let thin_id = Some(mappings.thin_id);

    report.set_title(&format!("Packing {} ...", input_file.display()));
    Ok(Packer::new(
        report.clone(),
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

pub fn run(matches: &ArgMatches, report: Arc<Report>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let input_name = input_file.file_name().unwrap();
    let input_name = input_name.to_str().unwrap().to_string();
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;

    report.set_title(&format!("Packing {} ...", input_file.clone().display()));

    let mut packer = if is_thin_device(&input_file)? {
        thin_packer(report, &input_file, input_name, &config)?
    } else {
        thick_packer(report, &input_file, input_name, &config)?
    };
    packer.pack()
}

/*
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
        config.hash_cache_size_meg,
    )
}
*/

//-----------------------------------------
