use anyhow::Result;
use blake2::{Blake2s256, Digest};
use clap::{App, Arg};
use io::prelude::*;
use io::Write;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::process::exit;
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

#[derive(Clone, Copy, PartialEq, Eq)]
struct SeqEntry {
    slab: u32,
    index: u32,
}

struct DedupHandler<'a, W: Write> {
    output: &'a mut W,
    index: &'a mut W,
    nr_chunks: usize,

    // Maps hashes to the slab they're in
    hashes: BTreeMap<Hash, MapEntry>,
    current_slab: u64,
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
            current_slab: 0,
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
            let mut hasher = Blake2s256::new();
            for v in iov {
                hasher.update(&v[..]);
            }
            let h = hasher.finalize();

            let me: MapEntry;
            match self.hashes.entry(h) {
                e @ Vacant(_) => {
                    me = e
                        .or_insert(MapEntry::Data {
                            slab: self.current_slab as u64,
                            offset: self.packer.nr_entries() as u32,
                        })
                        .clone();
                    self.packer.add_chunk(h, iov)?;
                }
                Occupied(e) => {
                    me = *e.get();
                }
            }
            self.step_seq(&me)?;

            // FIXME: define constant
            if self.packer.entries_len() > 4 * 1024 * 1024 {
                // Write the slab
                let mut packer = Slab::default();
                std::mem::swap(&mut packer, &mut self.packer);
                packer.complete(self.output)?;
                self.current_slab += 1;
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

pub fn run(args: &[std::ffi::OsString]) {
    let parser = App::new("dm-archive add")
        .version(crate::version::tools_version())
        .about("archives a device or file")
        .arg(
            Arg::new("INPUT")
                .help("Specify a device or file to archive")
                .required(true)
                .short('i')
                .value_name("DEV")
                .takes_value(true),
        )
        .arg(
            Arg::new("OUTPUT")
                .help("Specify packed output file")
                .required(true)
                .short('o')
                .value_name("FILE")
                .takes_value(true),
        )
        .arg(
            Arg::new("BLOCK_SIZE")
                .help("Specify average block size")
                .required(false)
                .validator(|s| s.parse::<usize>())
                .default_value("4096")
                .short('b')
                .value_name("BLOCK_SIZE")
                .takes_value(true),
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());
    let block_size = matches
        .value_of("BLOCK_SIZE").unwrap().parse::<usize>().unwrap();

    let report = std::sync::Arc::new(mk_simple_report());
    check_input_file(input_file, &report);

    if let Err(reason) = archive(input_file, output_file, block_size) {
        report.fatal(&format!("Application error: {}\n", reason));
        exit(1);
    }
}

//-----------------------------------------
