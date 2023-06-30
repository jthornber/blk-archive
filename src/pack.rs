use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use chrono::prelude::*;
use clap::ArgMatches;
use io::Write;
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use serde_json::json;
use serde_json::to_string_pretty;
use size_display::Size;
use std::boxed::Box;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::chunkers::*;
use crate::config;
use crate::content_sensitive_splitter::*;
use crate::cuckoo_filter::*;
use crate::hash::*;
use crate::hash_index::*;
use crate::iovec::*;
use crate::output::Output;
use crate::paths;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;
use crate::stream_builders::*;
use crate::thin_metadata::*;

//-----------------------------------------

fn iov_len_(iov: &IoVec) -> u64 {
    let mut len = 0;
    for v in iov {
        len += v.len() as u64;
    }

    len
}

fn first_b_(iov: &IoVec) -> Option<u8> {
    if let Some(v) = iov.iter().find(|v| !v.is_empty()) {
        return Some(v[0]);
    }

    None
}

fn all_same(iov: &IoVec) -> Option<u8> {
    if let Some(first_b) = first_b_(iov) {
        for v in iov.iter() {
            for b in *v {
                if *b != first_b {
                    return None;
                }
            }
        }
        Some(first_b)
    } else {
        None
    }
}

//-----------------------------------------

pub const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

struct DedupHandler {
    nr_chunks: usize,

    seen: CuckooFilter,
    hashes: lru::LruCache<u32, ByHash>,

    data_file: SlabFile,
    hashes_file: Arc<Mutex<SlabFile>>,
    stream_file: SlabFile,

    current_slab: u32,
    current_entries: usize,
    current_index: IndexBuilder,

    data_buf: Vec<u8>,
    hashes_buf: Vec<u8>,
    stream_buf: Vec<u8>,

    mapping_builder: Arc<Mutex<dyn Builder>>,

    data_written: u64,
    mapped_size: u64,
    fill_size: u64,
}

impl DedupHandler {
    fn get_hash_index(&mut self, slab: u32) -> Result<&ByHash> {
        // the current slab is not inserted into the self.hashes
        assert!(slab != self.current_slab);

        self.hashes.try_get_or_insert(slab, || {
            let mut hashes_file = self.hashes_file.lock().unwrap();
            let buf = hashes_file.read(slab)?;
            ByHash::new(buf)
        })
    }

    fn new(
        data_file: SlabFile,
        hashes_file: Arc<Mutex<SlabFile>>,
        stream_file: SlabFile,
        slab_capacity: usize,
        mapping_builder: Arc<Mutex<dyn Builder>>,
    ) -> Result<Self> {
        let seen = CuckooFilter::read(paths::index_path())?;
        let hashes = lru::LruCache::new(NonZeroUsize::new(slab_capacity).unwrap());
        let nr_slabs = data_file.get_nr_slabs() as u32;

        {
            let hashes_file = hashes_file.lock().unwrap();
            assert_eq!(data_file.get_nr_slabs(), hashes_file.get_nr_slabs());
        }

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

            mapping_builder,

            // Stats
            data_written: 0,
            mapped_size: 0,
            fill_size: 0,
        })
    }

    fn rebuild_index(&mut self, new_capacity: usize) -> Result<()> {
        let mut seen = CuckooFilter::with_capacity(new_capacity);

        // Scan the hashes file.
        let mut hashes_file = self.hashes_file.lock().unwrap();
        let nr_slabs = hashes_file.get_nr_slabs();
        for s in 0..nr_slabs {
            let buf = hashes_file.read(s as u32)?;
            let hi = ByHash::new(buf)?;
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
        slab.write_slab(buf)?;
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
            let mut builder = IndexBuilder::with_capacity(1024); // FIXME: estimate properly
            std::mem::swap(&mut builder, &mut self.current_index);
            let buffer = builder.build()?;
            self.hashes_buf.write_all(&buffer[..])?;
            let index = ByHash::new(buffer)?;
            self.hashes.put(self.current_slab, index);

            let mut hashes_file = self.hashes_file.lock().unwrap();
            Self::complete_slab_(&mut hashes_file, &mut self.hashes_buf)?;
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
        let r = (self.current_slab, self.current_entries as u32);
        for v in iov {
            self.data_buf.extend_from_slice(v);
            self.data_written += v.len() as u64;
        }
        self.current_entries += 1;
        Ok(r)
    }

    fn add_stream_entry(&mut self, e: &MapEntry, len: u64) -> Result<()> {
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.next(e, len, &mut self.stream_buf)
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

    fn handle_gap(&mut self, len: u64) -> Result<()> {
        self.add_stream_entry(&MapEntry::Unmapped { len }, len)?;
        self.maybe_complete_stream()?;

        Ok(())
    }

    fn handle_ref(&mut self, len: u64) -> Result<()> {
        self.add_stream_entry(&MapEntry::Ref { len }, len)?;
        self.maybe_complete_stream()?;

        Ok(())
    }
}

impl IoVecHandler for DedupHandler {
    fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;
        let len = iov_len_(iov);
        self.mapped_size += len;

        if let Some(first_byte) = all_same(iov) {
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
                        if let Some(offset) = self.current_index.lookup(&h) {
                            me = MapEntry::Data {
                                slab: s,
                                offset,
                                nr_entries: 1,
                            };
                        } else {
                            me = self.do_add(h, iov, len)?;
                        }
                    } else {
                        let hi = self.get_hash_index(s)?;
                        if let Some(offset) = hi.lookup(&h) {
                            me = MapEntry::Data {
                                slab: s,
                                offset: offset as u32,
                                nr_entries: 1,
                            };
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

    fn complete(&mut self) -> Result<()> {
        let mut builder = self.mapping_builder.lock().unwrap();
        builder.complete(&mut self.stream_buf)?;
        drop(builder);

        self.maybe_complete_data(0)?;
        Self::complete_slab(&mut self.stream_file, &mut self.stream_buf, 0)?;

        let mut hashes_file = self.hashes_file.lock().unwrap();
        hashes_file.close()?;
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
    output: Arc<Output>,
    input_path: PathBuf,
    stream_name: String,
    it: Box<dyn Iterator<Item = Result<Chunk>>>,
    input_size: u64,
    mapping_builder: Arc<Mutex<dyn Builder>>,
    mapped_size: u64,
    block_size: usize,
    thin_id: Option<u32>,
    hash_cache_size_meg: usize,
}

impl Packer {
    #[allow(clippy::too_many_arguments)]
    fn new(
        output: Arc<Output>,
        input_path: PathBuf,
        stream_name: String,
        it: Box<dyn Iterator<Item = Result<Chunk>>>,
        input_size: u64,
        mapping_builder: Arc<Mutex<dyn Builder>>,
        mapped_size: u64,
        block_size: usize,
        thin_id: Option<u32>,
        hash_cache_size_meg: usize,
    ) -> Self {
        Self {
            output,
            input_path,
            stream_name,
            it,
            input_size,
            mapping_builder,
            mapped_size,
            block_size,
            thin_id,
            hash_cache_size_meg,
        }
    }

    fn pack(mut self, hashes_file: Arc<Mutex<SlabFile>>) -> Result<()> {
        let mut splitter = ContentSensitiveSplitter::new(self.block_size as u32);

        let data_file = SlabFileBuilder::open(data_path())
            .write(true)
            .queue_depth(128)
            .build()
            .context("couldn't open data slab file")?;
        let data_size = data_file.get_file_size();

        let hashes_size = {
            let hashes_file = hashes_file.lock().unwrap();
            hashes_file.get_file_size()
        };

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

        let mut handler = DedupHandler::new(
            data_file,
            hashes_file,
            stream_file,
            slab_capacity,
            self.mapping_builder.clone(),
        )?;
        handler.ensure_extra_capacity(self.mapped_size as usize / self.block_size)?;

        self.output.report.progress(0);
        let start_time: DateTime<Utc> = Utc::now();

        let mut total_read = 0u64;
        for chunk in &mut self.it {
            match chunk? {
                Chunk::Mapped(buffer) => {
                    let len = buffer.len();
                    splitter.next_data(buffer, &mut handler)?;
                    total_read += len as u64;
                    self.output
                        .report
                        .progress(((100 * total_read) / self.mapped_size) as u8);
                }
                Chunk::Unmapped(len) => {
                    assert!(len > 0);
                    splitter.next_break(&mut handler)?;
                    handler.handle_gap(len)?;
                }
                Chunk::Ref(len) => {
                    splitter.next_break(&mut handler)?;
                    handler.handle_ref(len)?;
                }
            }
        }

        splitter.complete(&mut handler)?;
        self.output.report.progress(100);
        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;
        let data_written = handler.data_file.get_file_size() - data_size;

        let hashes_written = {
            let hashes_file = handler.hashes_file.lock().unwrap();
            hashes_file.get_file_size() - hashes_size
        };
        let stream_written = handler.stream_file.get_file_size();
        let ratio =
            (self.mapped_size as f64) / ((data_written + hashes_written + stream_written) as f64);

        if self.output.json {
            // Should all the values simply be added to the json too?  We can always add entries, but
            // we can never take any away to maintains backwards compatibility with JSON consumers.
            let result = json!({ "stream_id": stream_id });
            println!("{}", to_string_pretty(&result).unwrap());
        } else {
            self.output
                .report
                .info(&format!("elapsed          : {}", elapsed));
            self.output
                .report
                .info(&format!("stream id        : {}", stream_id));
            self.output
                .report
                .info(&format!("file size        : {:.2}", Size(self.input_size)));
            self.output
                .report
                .info(&format!("mapped size      : {:.2}", Size(self.mapped_size)));
            self.output
                .report
                .info(&format!("total read       : {:.2}", Size(total_read)));
            self.output.report.info(&format!(
                "fills size       : {:.2}",
                Size(handler.fill_size)
            ));
            self.output.report.info(&format!(
                "duplicate data   : {:.2}",
                Size(total_read - handler.data_written - handler.fill_size)
            ));

            self.output
                .report
                .info(&format!("data written     : {:.2}", Size(data_written)));
            self.output
                .report
                .info(&format!("hashes written   : {:.2}", Size(hashes_written)));
            self.output
                .report
                .info(&format!("stream written   : {:.2}", Size(stream_written)));
            self.output
                .report
                .info(&format!("ratio            : {:.2}", ratio));
            self.output.report.info(&format!(
                "speed            : {:.2}/s",
                Size((total_read as f64 / elapsed) as u64)
            ));
        }

        // write the stream config
        let cfg = config::StreamConfig {
            name: Some(self.stream_name.to_string()),
            source_path: self.input_path.display().to_string(),
            pack_time: config::now(),
            size: self.input_size,
            mapped_size: self.mapped_size,
            packed_size: data_written + hashes_written + stream_written,
            thin_id: self.thin_id,
        };
        config::write_stream_config(&stream_id, &cfg)?;

        Ok(())
    }
}

//-----------------------------------------

fn thick_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
) -> Result<Packer> {
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mapped_size = input_size;
    let input_iter = Box::new(ThickChunker::new(input_file, 16 * 1024 * 1024)?);
    let thin_id = None;
    let builder = Arc::new(Mutex::new(MappingBuilder::default()));

    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
    ))
}

fn thin_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mappings = read_thin_mappings(input_file)?;
    let mapped_size = mappings.provisioned_blocks.len() * mappings.data_block_size as u64 * 512;
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
    let builder = Arc::new(Mutex::new(MappingBuilder::default()));

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
    ))
}

// FIXME: slow
fn open_thin_stream(stream_id: &str) -> Result<SlabFile> {
    SlabFileBuilder::open(stream_path(stream_id))
        .build()
        .context("couldn't open old stream file")
}

fn thin_delta_packer(
    output: Arc<Output>,
    input_file: &Path,
    input_name: String,
    config: &config::Config,
    delta_device: &Path,
    delta_id: &str,
    hashes_file: Arc<Mutex<SlabFile>>,
) -> Result<Packer> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;

    let mappings = read_thin_delta(delta_device, input_file)?;
    let old_config = config::read_stream_config(delta_id)?;
    let mapped_size = old_config.mapped_size;

    let run_iter = DualIter::new(
        mappings.additions,
        mappings.removals,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );

    let input_iter = Box::new(DeltaChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));
    let thin_id = Some(mappings.thin_id);

    let old_stream = open_thin_stream(delta_id)?;
    let old_entries = StreamIter::new(old_stream)?;
    let builder = Arc::new(Mutex::new(DeltaBuilder::new(old_entries, hashes_file)));

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    Ok(Packer::new(
        output,
        input_file.to_path_buf(),
        input_name,
        input_iter,
        input_size,
        builder,
        mapped_size,
        config.block_size,
        thin_id,
        config.hash_cache_size_meg,
    ))
}

// Looks up both --delta-stream and --delta-device
fn get_delta_args(matches: &ArgMatches) -> Result<Option<(String, PathBuf)>> {
    match (
        matches.value_of("DELTA_STREAM"),
        matches.value_of("DELTA_DEVICE"),
    ) {
        (None, None) => Ok(None),
        (Some(stream), Some(device)) => {
            let mut buf = PathBuf::new();
            buf.push(device);
            Ok(Some((stream.to_string(), buf)))
        }
        _ => Err(anyhow!(
            "--delta-stream and --delta-device must both be given"
        )),
    }
}

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let input_name = input_file
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;

    env::set_current_dir(archive_dir)?;
    let config = config::read_config(".")?;

    output
        .report
        .set_title(&format!("Building packer {} ...", input_file.display()));

    let hashes_file = Arc::new(Mutex::new(
        SlabFileBuilder::open(hashes_path())
            .write(true)
            .queue_depth(16)
            .build()
            .context("couldn't open hashes slab file")?,
    ));

    let packer = if let Some((delta_stream, delta_device)) = get_delta_args(matches)? {
        thin_delta_packer(
            output.clone(),
            &input_file,
            input_name,
            &config,
            &delta_device,
            &delta_stream,
            hashes_file.clone(),
        )?
    } else if is_thin_device(&input_file)? {
        thin_packer(output.clone(), &input_file, input_name, &config)?
    } else {
        thick_packer(output.clone(), &input_file, input_name, &config)?
    };

    output
        .report
        .set_title(&format!("Packing {} ...", input_file.display()));
    packer.pack(hashes_file)
}

//-----------------------------------------
