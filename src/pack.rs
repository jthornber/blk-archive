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

use crate::archive::*;
use crate::config;
use crate::content_sensitive_splitter::*;
use crate::device_mapping::*;
use crate::hash::*;
use crate::iovec::*;
use crate::slab::*;
use crate::splitter::*;

//-----------------------------------------

fn all_zeroes(iov: &IoVec) -> (usize, bool) {
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

    (len, zeroes)
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

    fn write_iov(&mut self, iov: &IoVec) -> Result<()> {
        for v in iov {
            self.offset += v.len() as u32;
            self.packer.write(v)?;
        }

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

struct DedupHandler {
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    hashes: BTreeMap<Hash32, MapEntry>,

    data_file: SlabFile,
    index_file: SlabFile,
    stream_file: SlabFile,

    data_entries: u32,
    data_buf: Vec<u8>,
    index_buf: Vec<u8>,
    stream_buf: Vec<u8>,

    slabs: Vec<Vec<SlabEntry>>,
    mapping_builder: MappingBuilder,
}

fn mk_packer(file: &mut SlabFile) -> Packer {
    let (index, tx) = file.reserve_slab();
    Packer::new(index, tx)
}

impl DedupHandler {
    fn new(data_file: SlabFile, index_file: SlabFile, stream_file: SlabFile) -> Self {
        Self {
            nr_chunks: 0,
            hashes: BTreeMap::new(),

            data_file,
            index_file,
            stream_file,

            data_entries: 0,

            data_buf: Vec::new(),
            index_buf: Vec::new(),
            stream_buf: Vec::new(),

            slabs: Vec::new(),
            mapping_builder: MappingBuilder::default(),
        }
    }

    fn complete_slab(slab: &mut SlabFile, buf: &mut Vec<u8>) -> Result<()> {
        if buf.len() > 0 {
            let mut packer = mk_packer(slab);
            packer.write_iov(&vec![&buf[..]])?;
            packer.complete()?;
            buf.clear();
        }
        Ok(())
    }

    fn maybe_complete_slab(slab: &mut SlabFile, buf: &mut Vec<u8>) -> Result<bool> {
        if buf.len() > SLAB_SIZE_TARGET {
            Self::complete_slab(slab, buf)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn add_data_entry(&mut self, iov: &IoVec) -> Result<()> {
        for v in iov {
            self.data_buf.extend(v.iter());
        }
        self.data_entries += 1;

        if Self::maybe_complete_slab(&mut self.data_file, &mut self.data_buf)? {
            self.data_entries = 0;
        }

        Ok(())
    }

    fn add_index_entry(&mut self, h: Hash256, len: u32) -> Result<()> {
        self.index_buf.write_all(&h)?;
        self.index_buf.write_u32::<LittleEndian>(len)?;
        Self::maybe_complete_slab(&mut self.index_file, &mut self.index_buf)?;
        Ok(())
    }

    fn add_stream_entry(&mut self, e: &MapEntry) -> Result<()> {
        self.mapping_builder.next(e, &mut self.stream_buf)?;
        Self::maybe_complete_slab(&mut self.stream_file, &mut self.stream_buf)?;
        Ok(())
    }
}

const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

impl IoVecHandler for DedupHandler {
    fn handle(&mut self, iov: &IoVec) -> Result<()> {
        use std::collections::btree_map::Entry::*;

        self.nr_chunks += 1;
        let (len, zeroes) = all_zeroes(iov);

        if zeroes {
            self.mapping_builder
                .next(&MapEntry::Zero { len: len as u64 }, &mut self.stream_buf)?;
        } else {
            let h = hash_256(iov);
            // FIXME: can we just use the bottom 32 bits of h?
            let mh = hash_32(&vec![&h[..]]);

            let me: MapEntry;
            match self.hashes.entry(mh) {
                e @ Vacant(_) => {
                    me = e
                        .or_insert(MapEntry::Data {
                            slab: self.slabs.len() as u64,
                            offset: self.data_entries as u32,
                        })
                        .clone();

                    self.add_data_entry(iov)?;
                    self.add_index_entry(h, len as u32)?;
                }
                Occupied(e) => {
                    // FIXME: We need to double check the proper hash.
                    me = *e.get();
                }
            }
            self.add_stream_entry(&me)?;
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        let mut mapping_builder = MappingBuilder::default();
        std::mem::swap(&mut mapping_builder, &mut self.mapping_builder);
        mapping_builder.complete(&mut self.stream_buf)?;

        Self::complete_slab(&mut self.data_file, &mut self.data_buf)?;
        Self::complete_slab(&mut self.index_file, &mut self.index_buf)?;
        Self::complete_slab(&mut self.stream_file, &mut self.stream_buf)?;
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

    let output_path: PathBuf = ["data", "data"].iter().collect();
    let output_file = SlabFile::create(&output_path, 128)?;

    let index_path: PathBuf = ["data", "index"].iter().collect();
    let index_file = SlabFile::create(&index_path, 16)?;

    let stream_path: PathBuf = ["streams", "00000000"].iter().collect();
    let stream_file = SlabFile::create(stream_path, 16)?;

    let mut handler = DedupHandler::new(output_file, index_file, stream_file);

    report.set_title(&format!("Packing {} ...", input_file.display()));
    report.progress(0);
    const BUFFER_SIZE: usize = 4 * 1024 * 1024;
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
