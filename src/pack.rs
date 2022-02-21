use anyhow::Result;
use blake2::{Blake2b, Digest};
use clap::{ArgMatches};
use io::prelude::*;
use io::Write;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use thinp::commands::utils::*;
use thinp::report::*;

use crate::archive::*;
use crate::content_sensitive_splitter::*;
use crate::device_mapping::*;
use crate::splitter::*;

//-----------------------------------------

fn all_zeroes(iov: &IoVec) -> Option<usize> {
    let mut len = 0;
    for v in iov {
        for b in *v {
            if *b != 0 {
                return None;
            }
        }
        len += v.len();
    }
    Some(len)
}

//-----------------------------------------

// 32bit digest used to hash the hashes
type Blake2b32 = Blake2b<generic_array::typenum::U4>;
type Blake2b256 = Blake2b<generic_array::typenum::U32>;
pub type MiniHash = generic_array::GenericArray<u8, generic_array::typenum::U4>;

struct DedupHandler<'a, W: Write> {
    output: &'a mut W,
    index: &'a mut W,
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    hashes: BTreeMap<MiniHash, MapEntry>,
    slabs: Vec<Vec<SlabEntry>>,
    packer: Slab,
    mapping_builder: DevMapBuilder,
}

impl<'a, W: Write> DedupHandler<'a, W> {
    fn new(output: &'a mut W, index: &'a mut W) -> Self {
        Self {
            output,
            index,
            nr_chunks: 0,
            hashes: BTreeMap::new(),
            slabs: Vec::new(),
            packer: Slab::default(),
            mapping_builder: DevMapBuilder::default(),
        }
    }

    #[inline]
    fn step_seq(&mut self, e: &MapEntry) -> Result<()> {
        self.mapping_builder.next(e, self.index)
    }
}

impl<'a, W: Write> IoVecHandler for DedupHandler<'a, W> {
    fn handle(&mut self, iov: &IoVec) -> Result<()> {
        use std::collections::btree_map::Entry::*;

        self.nr_chunks += 1;

        if let Some(len) = all_zeroes(iov) {
            self.mapping_builder
                .next(&MapEntry::Zero { len: len as u64 }, self.index)?;
        } else {
            let mut hasher = Blake2b256::new();
            for v in iov {
                hasher.update(&v[..]);
            }
            let h = hasher.finalize();

            let mut mhasher = Blake2b32::new();
            mhasher.update(&h[..]);
            let mh = mhasher.finalize();

            let me: MapEntry;
            match self.hashes.entry(mh) {
                e @ Vacant(_) => {
                    me = e
                        .or_insert(MapEntry::Data {
                            slab: self.slabs.len() as u64,
                            offset: self.packer.nr_entries() as u32,
                        })
                        .clone();
                    self.packer.add_chunk(h, iov)?;
                }
                Occupied(e) => {
                    // FIXME: We need to double check the proper hash.
                    me = *e.get();
                }
            }
            self.step_seq(&me)?;

            // FIXME: define constant
            if self.packer.entries_len() > 4 * 1024 * 1024 {
                // Write the slab
                let mut packer = Slab::default();
                std::mem::swap(&mut packer, &mut self.packer);
                let slab_header = packer.complete(self.output)?;
                self.slabs.push(slab_header);
            }
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        let mut mapping_builder = DevMapBuilder::default();
        std::mem::swap(&mut mapping_builder, &mut self.mapping_builder);
        mapping_builder.complete(self.index)
    }
}

//-----------------------------------------

pub fn archive(input_file: &Path, output_file: &Path, block_size: usize) -> Result<()> {
    let mut splitter = ContentSensitiveSplitter::new(block_size as u32);

    let mut input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)?;

    let mut output = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)?;

    let mut index_file = PathBuf::new();
    index_file.push(output_file);
    index_file.set_extension("idx");

    let mut index = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(index_file)?;

    let mut handler = DedupHandler::new(&mut output, &mut index);

    const BUFFER_SIZE: usize = 4 * 1024 * 1024;
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
    }

    splitter.complete(&mut handler)?;

    Ok(())
}

//-----------------------------------------

pub fn run(matches: &ArgMatches) -> Result<()> {
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let block_size = matches
        .value_of("BLOCK_SIZE").unwrap().parse::<usize>().unwrap();

    let report = std::sync::Arc::new(mk_simple_report());
    check_input_file(input_file, &report);

    archive(input_file, output_file, block_size)
}

//-----------------------------------------
