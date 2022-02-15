use anyhow::{anyhow, Context, Result};
use blake2::{Blake2s256, Digest};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use clap::{App, Arg};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use io::prelude::*;
use io::Write;
use rand::prelude::*;
use rand::prelude::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fs::OpenOptions;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::exit;
use std::sync::mpsc::{sync_channel, Receiver};
use thinp::commands::utils::*;
use thinp::report::*;

use crate::rolling_hash::*;

//-----------------------------------------

type IoVec<'a> = Vec<&'a [u8]>;

trait IoVecHandler {
    fn handle(&mut self, iov: &IoVec) -> Result<()>;
}

trait Splitter {
    fn next(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()>;
    fn complete(self, handler: &mut dyn IoVecHandler) -> Result<()>;
}

#[derive(PartialEq, Eq)]
struct Cursor {
    block: usize,
    offset: usize,
}

impl Default for Cursor {
    fn default() -> Self {
        Self {
            block: 0,
            offset: 0,
        }
    }
}

impl Cursor {
    fn lt(&self, rhs: &Cursor) -> bool {
        (self.block < rhs.block) || ((self.block == rhs.block) && (self.offset < rhs.offset))
    }
}

struct HashSplitter {
    rhash: RollingHash,

    offset: usize,
    len: usize, // length since last consume
    blocks: VecDeque<Vec<u8>>,

    leading_c: Cursor,
    trailing_c: Cursor,
    consume_c: Cursor,

    div: u32,
}

impl HashSplitter {
    fn new(window_size: u32) -> Self {
        Self {
            rhash: RollingHash::new(window_size),

            offset: 0,
            len: 0,
            blocks: VecDeque::new(),

            leading_c: Cursor::default(),
            trailing_c: Cursor::default(),
            consume_c: Cursor::default(),

            div: window_size - 1,
        }
    }

    fn eof(&self) -> bool {
        self.leading_c.block >= self.blocks.len()
    }

    fn get_byte(&self, c: &Cursor) -> u8 {
        self.blocks[c.block][c.offset]
    }

    fn get_leading(&self) -> u8 {
        self.get_byte(&self.leading_c)
    }

    // This returns 0 if the front has advanced less than window_size
    fn get_trailing(&self) -> u8 {
        if self.offset < self.rhash.window_size as usize {
            0
        } else {
            self.get_byte(&self.trailing_c)
        }
    }

    fn inc_cursor(blocks: &VecDeque<Vec<u8>>, c: &mut Cursor) {
        let b = &blocks[c.block];

        if c.offset < b.len() {
            c.offset += 1;
            if c.offset == b.len() {
                c.offset = 0;
                c.block += 1;
            }
        } else {
            c.offset = 0;
            c.block += 1;
        }
    }

    // FIXME: drop old blocks
    fn advance(&mut self) {
        self.len += 1;
        self.offset += 1;
        HashSplitter::inc_cursor(&self.blocks, &mut self.leading_c);

        if self.offset > self.rhash.window_size as usize {
            HashSplitter::inc_cursor(&self.blocks, &mut self.trailing_c);
        }
    }

    fn consume(&mut self) -> IoVec {
        let mut begin = &mut self.consume_c;
        let mut end = &mut self.leading_c;
        let blocks = &self.blocks;

        assert!(begin != end);

        let mut r = IoVec::new();
        while begin.lt(end) {
            let b = &blocks[begin.block];

            if begin.block == end.block {
                r.push(&b[begin.offset..end.offset]);
                begin.offset = end.offset;
            } else {
                r.push(&b[begin.offset..]);
                begin.offset = 0;
                begin.block += 1;
            }
        }
        self.len = 0;
        r
    }

    fn consume_all(&mut self) -> IoVec {
        let mut c = &mut self.consume_c;
        let mut r = IoVec::new();
        while c.block < self.blocks.len() {
            let b = &self.blocks[c.block];
            r.push(&b[c.offset..]);
            c.offset = 0;
            c.block += 1;
        }
        self.len = 0;
        r
    }

    fn hit_break(&self, mask: u32) -> bool {
        let h = self.rhash.hash >> 8;
        ((h & mask) == 0) && (self.len >= (self.rhash.window_size as usize) / 2)
    }
}

impl Splitter for HashSplitter {
    fn next(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()> {
        self.blocks.push_back(buffer);

        while !self.eof() {
            let _h = self.rhash.step(self.get_trailing(), self.get_leading());
            self.advance(); // So the current byte is included in the block
            if self.hit_break(self.div) {
                handler.handle(&self.consume())?;
            }
        }

        Ok(())
    }

    fn complete(mut self, handler: &mut dyn IoVecHandler) -> Result<()> {
        let iov = self.consume_all();
        if iov.len() > 0 {
            handler.handle(&iov)?;
        }
        Ok(())
    }
}

//-------------------

#[cfg(test)]
mod splitter_tests {
    use super::*;
    use std::io::{BufReader, BufWriter, Read, Write};
    use std::string::String;

    fn rand_buffer(count: usize) -> Vec<u8> {
        let mut buffer = Vec::new();
        for _ in 0..count {
            let b = rand::thread_rng().gen_range(0..256) as u8;
            buffer.push(b);
        }
        buffer
    }

    fn corrupt(buf: &mut [u8], len: usize) {
        let offset = rand::thread_rng().gen_range(0..(buf.len() - len));
        for i in offset..(offset + len) {
            buf[i] = rand::thread_rng().gen_range(0..256) as u8;
        }
    }

    fn prep_data() -> Vec<u8> {
        // Generate 64k of random data.
        let pat = rand_buffer(64 * 1024);

        // repeat this data several times with junk in between.
        let mut slices: Vec<Vec<u8>> = Vec::new();
        for _ in 0..8 {
            slices.push(pat.clone());

            let padding = rand_buffer(rand::thread_rng().gen_range(16..1024));
            slices.push(padding);
        }
        let mut data = slices.concat();

        // now we stamp short random sequences across th whole buffer.
        for _ in 0..32 {
            corrupt(&mut data[..], rand::thread_rng().gen_range(16..64));
        }

        data
    }

    //-----------

    struct CatHandler<'a, W: Write> {
        output: &'a mut W,
    }

    impl<'a, W: Write> CatHandler<'a, W> {
        fn new(output: &'a mut W) -> Self {
            Self { output }
        }
    }

    impl<'a, W: Write> IoVecHandler for CatHandler<'a, W> {
        fn handle(&mut self, iov: &IoVec) -> Result<()> {
            for v in iov {
                println!("{:?}", v);
                self.output.write(v);
            }

            Ok(())
        }
    }

    //-----------

    struct Entry {
        hits: usize,
        len: usize,
        zero: bool,
    }

    impl Default for Entry {
        fn default() -> Self {
            Self {
                hits: 0,
                len: 0,
                zero: false,
            }
        }
    }

    struct TestHandler {
        nr_chunks: usize,
        hashes: BTreeMap<Hash, Entry>,
    }

    impl Default for TestHandler {
        fn default() -> Self {
            Self {
                nr_chunks: 0,
                hashes: BTreeMap::new(),
            }
        }
    }

    impl TestHandler {
        fn histogram(&self) -> BTreeMap<usize, (u32, u32)> {
            let mut r = BTreeMap::new();
            for (_, Entry { hits, zero, .. }) in &self.hashes {
                let e = r.entry(*hits).or_insert((0, 0));
                e.0 += 1;
                if *zero {
                    e.1 += 1;
                }
            }
            r
        }

        fn lengths(&self) -> BTreeMap<usize, usize> {
            let mut r = BTreeMap::new();
            for (_, Entry { len, .. }) in &self.hashes {
                let e = r.entry(*len).or_insert(0);
                *e += 1;
            }
            r
        }

        fn unique_len(&self) -> usize {
            let mut total = 0;
            for (_, Entry { hits, len, .. }) in &self.hashes {
                if *hits == 1 {
                    total += len;
                }
            }
            total
        }

        fn estimate_dedup_len(&self) -> usize {
            let mut total = 0;
            for (_, Entry { len, .. }) in &self.hashes {
                total += len + 32; // 32 because we've got to store the hash too
            }
            total
        }
    }

    impl IoVecHandler for TestHandler {
        fn handle(&mut self, iov: &IoVec) -> Result<()> {
            self.nr_chunks += 1;

            let mut len = 0;
            let mut hasher = Blake2s256::new();
            for v in iov {
                len += v.len();
                hasher.update(&v[..]);
            }

            let e = self
                .hashes
                .entry(hasher.finalize())
                .or_insert(Entry::default());

            e.hits += 1;
            e.len = len;

            Ok(())
        }
    }

    //-----------

    fn split<R: Read, H: IoVecHandler>(input: &mut R, handler: &mut H) {
        // const BLOCK_SIZE: usize = 128;
        const BLOCK_SIZE: usize = 128;
        let mut splitter = HashSplitter::new(BLOCK_SIZE as u32);

        const BUFFER_SIZE: usize = 4 * 1024 * 1024;
        loop {
            let mut buffer = vec![0u8; BUFFER_SIZE];
            let n = input.read(&mut buffer[..]).expect("couldn't read input");

            if n == 0 {
                break;
            } else if n == BUFFER_SIZE {
                splitter.next(buffer, handler).expect("split next failed");
            } else {
                buffer.truncate(n);
                splitter.next(buffer, handler).expect("split next failed");
            }
        }

        splitter.complete(handler).expect("split complete failed");
    }

    #[test]
    fn splitter_emits_all_data() {
        let input_buf = prep_data();
        let input_buf = b"the quick brown fox jumps over the lazy dog.  The quick brown fox jumps over the lazy dog.".to_vec();
        let mut input = BufReader::new(&input_buf[..]);

        let mut output_buf = Vec::new();
        let mut output = BufWriter::new(&mut output_buf);
        let mut handler = CatHandler::new(&mut output);

        split(&mut input, &mut handler);

        drop(input);
        drop(output);
        assert_eq!(input_buf, output_buf);
    }

    #[test]
    fn splitter_finds_similar_blocks() {
        let input_buf = prep_data();
        let mut input = BufReader::new(&input_buf[..]);

        let mut handler = TestHandler::default();
        split(&mut input, &mut handler);

        eprintln!("{} chunks", handler.nr_chunks);
        let hist = handler.histogram();
        for (hits, (blocks, zs)) in hist {
            eprintln!("{} hits: {}, zeroes: {}", hits, blocks, zs);
        }

        let lengths = handler.lengths();
        let mut csv = std::fs::File::create("dedup-lengths.csv").unwrap();
        for (len, hits) in lengths {
            write!(csv, "{}, {}\n", len, hits);
        }

        assert!(handler.unique_len() < (input_buf.len() / 4));
    }
}

//-----------------------------------------

struct SlabEntry {
    h: Hash,
    offset: u32,
}

struct Slab {
    offset: u32,
    blocks: Vec<SlabEntry>,
    packer: ZlibEncoder<Vec<u8>>,
}

impl Default for Slab {
    fn default() -> Self {
        Self {
            offset: 0,
            blocks: Vec::new(),
            packer: ZlibEncoder::new(Vec::new(), Compression::default()),
        }
    }
}

impl Slab {
    pub fn add_chunk(&mut self, h: Hash, iov: &IoVec) -> Result<()> {
        self.blocks.push(SlabEntry {
            h,
            offset: self.offset,
        });

        for v in iov {
            self.offset += v.len() as u32;
            self.packer.write(v)?;
        }

        Ok(())
    }

    pub fn complete<W: Write>(mut self, w: &mut W) -> Result<()> {
        w.write_u64::<LittleEndian>(self.blocks.len() as u64)?;
        for b in &self.blocks {
            w.write(&b.h[..])?;
            w.write_u32::<LittleEndian>(b.offset as u32)?;
        }

        let compressed = self.packer.reset(Vec::new())?;
        w.write(&compressed[..])?;
        Ok(())
    }
}

//-----------------------------------------

struct Entry {
    hits: usize,
    len: usize,
}

impl Default for Entry {
    fn default() -> Self {
        Self { hits: 0, len: 0 }
    }
}

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

        let mut len = 0;
        let mut hasher = Blake2s256::new();
        for v in iov {
            len += v.len();
            hasher.update(&v[..]);
        }
        let h = hasher.finalize();

        if self.hashes.insert(h) {
            self.packer.add_chunk(h, iov)?;

            // FIXME: define constant
            if self.packer.offset > 4 * 1024 * 1024 {
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
    let mut splitter = HashSplitter::new(BLOCK_SIZE as u32);

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
            Arg::with_name("INPUT")
                .help("Specify a device or file to archive")
                .required(true)
                .short('i')
                .value_name("DEV")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("OUTPUT")
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
