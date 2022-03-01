use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use clap::ArgMatches;
use flate2::{write::ZlibEncoder, Compression};
use io::prelude::*;
use io::Write;
use std::collections::BTreeMap;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use thinp::commands::utils::*;
use thinp::report::*;
use size_display::Size;

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

struct Packer {
    index: SlabIndex,
    offset: u32,
    packer: ZlibEncoder<Vec<u8>>,
    tx: SyncSender<SlabData>,
}

impl Packer {
    fn new(index: SlabIndex, tx: SyncSender<SlabData>) -> Self {
        Self {
            index,
            offset: 0,
            packer: ZlibEncoder::new(Vec::new(), Compression::default()),
            tx,
        }
    }

    fn write(&mut self, v: &[u8]) -> Result<()> {
        self.offset += v.len() as u32;
        self.packer.write_all(v)?;
        Ok(())
    }

    fn complete(mut self) -> Result<()> {
        let data = self.packer.reset(Vec::new())?;
        self.tx.send(SlabData {
            index: self.index,
            data,
        })?;
        Ok(())
    }
}

//-----------------------------------------

const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

struct DedupHandler {
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    // hashes: BTreeMap<Hash32, MapEntry>,
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
    hashes_written: u64,
    stream_written: u64,
}

fn mk_packer(file: &mut SlabFile) -> Packer {
    let (index, tx) = file.reserve_slab();
    Packer::new(index, tx)
}

impl DedupHandler {
    fn new(data_file: SlabFile, hashes_file: SlabFile, stream_file: SlabFile) -> Self {
        Self {
            nr_chunks: 0,
            hashes: BTreeMap::new(),

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
            hashes_written: 0,
            stream_written: 0,
        }
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
            self.data_buf.extend(v.iter());  // FIXME: this looks slow
            self.data_written += v.len() as u64;
        }
        self.current_entries += 1;
        Ok(r)
    }

    fn add_hash_entry(&mut self, h: Hash256, len: u32) -> Result<()> {
        use std::mem::size_of;

        self.hashes_buf.write_all(&h)?;
        self.hashes_buf.write_u32::<LittleEndian>(len)?;
        self.hashes_written += size_of::<Hash256>() as u64 + size_of::<u32>() as u64;
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
        Ok(())
    }
}

//-----------------------------------------

pub fn pack(report: &Arc<Report>, input_file: &Path, block_size: usize) -> Result<()> {
    let mut splitter = ContentSensitiveSplitter::new(block_size as u32);

    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)?;
    let input_size = input.metadata()?.len();

    let data_path: PathBuf = ["data", "data"].iter().collect();
    let data_file = SlabFile::create(&data_path, 128)?;
    let data_size = data_file.get_file_size()?;

    let hashes_path: PathBuf = ["data", "hashes"].iter().collect();
    let hashes_file = SlabFile::create(&hashes_path, 16)?;
    let hashes_size = hashes_file.get_file_size()?;

    let stream_path: PathBuf = ["streams", "00000000"].iter().collect();
    let stream_file = SlabFile::create(stream_path, 16)?;
    let stream_size = stream_file.get_file_size()?;

    let mut handler = DedupHandler::new(data_file, hashes_file, stream_file);

    report.set_title(&format!("Packing {} ...", input_file.display()));
    report.progress(0);
    const BUFFER_SIZE: usize = 16 * 1024 * 1024;
    let mut total_read: u64 = 0;
    loop {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let n = input.read(&mut buffer[..])?;

        if n == 0 {
            break;
        } else if n == BUFFER_SIZE {
            splitter.next(buffer, &mut handler)?;
        } else {
            buffer.truncate(n);
            splitter.next(buffer, &mut handler)?;
        }

        total_read += n as u64;
        report.progress(((100 * total_read) / input_size) as u8);
    }

    splitter.complete(&mut handler)?;
    report.progress(100);
    report.info(&format!("file size: {:.2}", Size(total_read)));
    report.info(&format!("duplicates found: {:.2}", Size(total_read - handler.data_written)));

    let data_written = handler.data_file.get_file_size()? - data_size;
    let hashes_written = handler.hashes_file.get_file_size()? - hashes_size;
    let stream_written = handler.stream_file.get_file_size()? - stream_size;

    report.info(&format!("data written: {:.2}", Size(data_written)));
    report.info(&format!("hashes written: {:.2}", Size(hashes_written)));
    report.info(&format!("stream written: {:.2}", Size(stream_written)));

    let compression = ((data_written + hashes_written + stream_written) * 100) / total_read;
    report.info(&format!("compression: {:.2}%", compression));

    Ok(())
}

//-----------------------------------------

pub fn run(matches: &ArgMatches) -> Result<()> {
    let archive_dir = Path::new(matches.value_of("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = Path::new(matches.value_of("INPUT").unwrap()).canonicalize()?;
    let report = std::sync::Arc::new(mk_progress_bar_report());
    check_input_file(&input_file, &report);

    env::set_current_dir(&archive_dir)?;
    let config = config::read_config(".")?;
    pack(&report, &input_file, config.block_size)
}

//-----------------------------------------
