use anyhow::Result;
use blake2::{Blake2s256, Digest};
use clap::{App, Arg};
use io::prelude::*;
use io::Write;
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::process::exit;
use thinp::commands::utils::*;
use thinp::report::*;

use crate::splitter::*;
use crate::content_sensitive_splitter::*;
use crate::archive::*;

//-----------------------------------------

struct DedupHandler<'a, W: Write> {
    output: &'a mut W,
    nr_chunks: usize,
    hashes: BTreeSet<Hash>,
    packer: Slab,
}

impl<'a, W: Write> DedupHandler<'a, W> {
    fn new(w: &'a mut W) -> Self {
        Self {
            output: w,
            nr_chunks: 0,
            hashes: BTreeSet::new(),
            packer: Slab::default(),
        }
    }
}

impl<'a, W: Write> IoVecHandler for DedupHandler<'a, W> {
    fn handle(&mut self, iov: &IoVec) -> Result<()> {
        self.nr_chunks += 1;

        let mut hasher = Blake2s256::new();
        for v in iov {
            hasher.update(&v[..]);
        }
        let h = hasher.finalize();

        if self.hashes.insert(h) {
            self.packer.add_chunk(h, iov)?;

            // FIXME: define constant
            if self.packer.entries_len() > 4 * 1024 * 1024 {
                // Write the slab
                let mut packer = Slab::default();
                std::mem::swap(&mut packer, &mut self.packer);
                packer.complete(self.output)?;
            }
        }

        Ok(())
    }
}

//-----------------------------------------

pub fn archive(input_file: &Path, output_file: &Path) -> Result<()> {
    // const BLOCK_SIZE: usize = 8192;
    const BLOCK_SIZE: usize = 4096;
    let mut splitter = ContentSensitiveSplitter::new(BLOCK_SIZE as u32);

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

    let mut handler = DedupHandler::new(&mut output);

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
    let parser = App::new("archive")
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
        );

    let matches = parser.get_matches_from(args);
    let input_file = Path::new(matches.value_of("INPUT").unwrap());
    let output_file = Path::new(matches.value_of("OUTPUT").unwrap());

    let report = std::sync::Arc::new(mk_simple_report());
    check_input_file(input_file, &report);

    if let Err(reason) = archive(input_file, output_file) {
        report.fatal(&format!("Application error: {}\n", reason));
        exit(1);
    }
}
