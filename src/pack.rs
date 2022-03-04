use anyhow::{anyhow, Result, Context};
use byteorder::{LittleEndian, WriteBytesExt};
use clap::ArgMatches;
use io::prelude::*;
use io::Write;
use nom::{bytes::complete::*, multi::*, number::complete::*, IResult};
use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use size_display::Size;
use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thinp::report::*;

use crate::config;
use crate::content_sensitive_splitter::*;
use crate::hash::*;
use crate::iovec::*;
use crate::slab::*;
use crate::splitter::*;
use crate::stream::*;

//-----------------------------------------

fn all_zeroes(iov: &IoVec) -> (u64, bool) {
    let mut len = 0;
    let mut zeroes = true;
    for v in iov {
        if zeroes {
            for b in *v {
                if *b != 0 {
                    zeroes = false;
                    break;
                }
            }
        }
        len += v.len();
    }

    (len as u64, zeroes)
}

//-----------------------------------------

const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

struct DedupHandler {
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    hashes: BTreeMap<Hash256, MapEntry>,

    data_file: SlabFile,
    hashes_file: SlabFile,
    stream_file: SlabFile,

    current_slab: u64,
    current_entries: u32,

    data_buf: Vec<u8>,
    hashes_buf: Vec<u8>,
    stream_buf: Vec<u8>,

    mapping_builder: MappingBuilder,

    data_written: u64,
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

    fn read_hashes(hashes_file: &mut SlabFile) -> Result<BTreeMap<Hash256, MapEntry>> {
        let mut r = BTreeMap::new();
        let nr_slabs = hashes_file.get_nr_slabs();
        for s in 0..nr_slabs {
            let buf = hashes_file.read(s as u64)?;
            let (_, hashes) =
                Self::parse_hashes(&buf).map_err(|_| anyhow!("couldn't parse hashes"))?;

            let mut i = 0;
            for h in hashes {
                r.insert(
                    h,
                    MapEntry::Data {
                        slab: s as u64,
                        offset: i,
                    },
                );
                i += 1;
            }
        }

        Ok(r)
    }

    fn new(data_file: SlabFile, mut hashes_file: SlabFile, stream_file: SlabFile) -> Result<Self> {
        let hashes = Self::read_hashes(&mut hashes_file)?;
        Ok(Self {
            nr_chunks: 0,
            hashes,

            data_file,
            hashes_file,
            stream_file,

            current_slab: 0,
            current_entries: 0,

            data_buf: Vec::new(),
            hashes_buf: Vec::new(),
            stream_buf: Vec::new(),

            mapping_builder: MappingBuilder::default(),

            // Stats
            data_written: 0,
        })
    }

    fn complete_slab_(slab: &mut SlabFile, buf: &mut Vec<u8>) -> Result<()> {
        let mut packer = mk_packer(slab);
        packer.write(buf)?;
        packer.complete()?;
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
    fn add_data_entry(&mut self, iov: &IoVec) -> Result<(u64, u32)> {
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

    fn add_stream_entry(&mut self, e: &MapEntry) -> Result<()> {
        self.mapping_builder.next(e, &mut self.stream_buf)
    }
}

impl IoVecHandler for DedupHandler {
    fn handle(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;
        let (len, _zeroes) = all_zeroes(iov);

        /*
        // FIXME: not sure zeroes isn't working
            if zeroes {
                self.add_stream_entry(&MapEntry::Zero { len })?;
                self.maybe_complete_stream()?;
            } else {
                */
        let h = hash_256(iov);

        /*
        // FIXME: can we just use the bottom 32 bits of h?
        let mh = hash_32(&vec![&h[..]]);
        */
        let mh = h;

        let me: MapEntry;
        if let Some(e) = self.hashes.get(&mh) {
            // FIXME: We need to double check the proper hash.
            me = *e;
        } else {
            self.add_hash_entry(h, len as u32)?;
            let (slab, offset) = self.add_data_entry(iov)?;
            me = MapEntry::Data { slab, offset };
            self.hashes.insert(mh, me);
            self.maybe_complete_data()?;
        }

        self.add_stream_entry(&me)?;
        self.maybe_complete_stream()?;
        //}

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

        Ok(())
    }
}

//-----------------------------------------

// Assumes we've chdir'd to the archive
fn new_stream_path() -> Result<(String, PathBuf)> {
    loop {
        // choose a random number
        let mut rng = ChaCha20Rng::from_entropy();
        let n: u64 = rng.gen();

        // turn this into a path
        let name = format!("{:>016x}", n);
        let path: PathBuf = ["streams", &name].iter().collect();

        if !path.exists() {
            return Ok((name, path));
        }
    }

    // Can't get here
}

pub fn pack(report: &Arc<Report>, input_file: &Path, block_size: usize) -> Result<()> {
    let mut splitter = ContentSensitiveSplitter::new(block_size as u32);

    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = input.metadata()?.len();

    let data_path: PathBuf = ["data", "data"].iter().collect();
    let data_file = SlabFile::open_for_write(&data_path, 128)
        .context("couldn't open data slab file")?;
    let data_size = data_file.get_file_size()?;


    let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
    let hashes_file = SlabFile::open_for_write(&hashes_path, 16)
        .context("couldn't open hashes slab file")?;
    let hashes_size = hashes_file.get_file_size()?;
    let (stream_id, mut stream_path) = new_stream_path()?;

    std::fs::create_dir(stream_path.clone())?;
    stream_path.push("stream");

    let stream_file = SlabFile::create(stream_path, 16, true)
        .context("couldn't open stream slab file")?;

    let mut handler = DedupHandler::new(data_file, hashes_file, stream_file)?;

    report.set_title(&format!("Packing {} ...", input_file.display()));
    report.progress(0);

    const BUFFER_SIZE: usize = 16 * 1024 * 1024;
    let complete_blocks = input_size / BUFFER_SIZE as u64;
    let remainder = input_size - (complete_blocks * BUFFER_SIZE as u64);

    let mut total_read: u64 = 0;
    for _ in 0..complete_blocks {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        input.read_exact(&mut buffer[..])?;
        splitter.next(buffer, &mut handler)?;
        total_read += BUFFER_SIZE as u64;
        report.progress(((100 * total_read) / input_size) as u8);
    }

    if remainder > 0 {
        let mut buffer = vec![0u8; remainder as usize];
        input.read_exact(&mut buffer[..])?;
        splitter.next(buffer, &mut handler)?;
        total_read += remainder as u64;
        report.progress(((100 * total_read) / input_size) as u8);
    }

    splitter.complete(&mut handler)?;
    report.progress(100);
    report.info(&format!("stream id        : {}", stream_id));
    report.info(&format!("file size        : {:.2}", Size(total_read)));
    report.info(&format!("duplicate data   : {:.2}",
        Size(total_read - handler.data_written)
    ));

    let data_written = handler.data_file.get_file_size()? - data_size;
    let hashes_written = handler.hashes_file.get_file_size()? - hashes_size;
    let stream_written = handler.stream_file.get_file_size()?;

    report.info(&format!("data written     : {:.2}", Size(data_written)));
    report.info(&format!("hashes written   : {:.2}", Size(hashes_written)));
    report.info(&format!("stream written   : {:.2}", Size(stream_written)));

    let compression = ((data_written + hashes_written + stream_written) * 100) / total_read;
    report.info(&format!("compression      : {:.2}%", 100 - compression));

    // write the stream config
    let cfg = config::StreamConfig {
        name: None,
        source_path: input_file.display().to_string(),
        pack_time: config::now(),
        size: input_size,
        packed_size: data_written + hashes_written + stream_written,
    };
    config::write_stream_config(&stream_id, &cfg)?;

    Ok(())
}

//-----------------------------------------

pub fn run(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;
    let report = std::sync::Arc::new(mk_progress_bar_report());

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;
    pack(&report, &input_file, config.block_size)
}

//-----------------------------------------
