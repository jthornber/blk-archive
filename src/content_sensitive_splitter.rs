use anyhow::Result;
use std::collections::VecDeque;

use crate::iovec::*;
use crate::splitter::*;
use crate::utils::round_pow2;

//-----------------------------------------

#[derive(PartialEq, Eq, Debug, Default)]
struct Cursor {
    block: usize,
    offset: usize,
}

pub struct ContentSensitiveSplitter {
    window_size: u32,
    hasher: gearhash::Hasher<'static>,
    mask_s: u64,
    mask_l: u64,

    unconsumed_len: u64,
    blocks: VecDeque<Vec<u8>>,

    leading_c: Cursor,
    consume_c: Cursor,
}

impl ContentSensitiveSplitter {
    pub fn new(window_size: u32) -> Self {
        let rounded_window_size = round_pow2(window_size);
        let shift = 36;

        Self {
            window_size: rounded_window_size as u32,

            hasher: gearhash::Hasher::default(),
            mask_s: (((rounded_window_size) << 1) - 1) << shift,
            mask_l: (((rounded_window_size) >> 1) - 1) << shift,

            unconsumed_len: 0,
            blocks: VecDeque::new(),

            leading_c: Cursor::default(),
            consume_c: Cursor::default(),
        }
    }

    fn eof(&self) -> bool {
        self.leading_c.block >= self.blocks.len()
    }

    fn contiguous_size(&self, c: &Cursor) -> usize {
        let b = &self.blocks[c.block];
        b.len() - c.offset
    }

    fn inc_cursor(blocks: &VecDeque<Vec<u8>>, c: &mut Cursor, mut amt: usize) {
        while amt > 0 {
            let b = &blocks[c.block];

            if c.offset < b.len() {
                let len = std::cmp::min(b.len() - c.offset, amt);
                c.offset += len;
                amt -= len;
            }

            if c.offset == b.len() {
                c.offset = 0;
                c.block += 1;
            }
        }
    }

    fn ref_chunk<'a>(blocks: &'a VecDeque<Vec<u8>>, c: &Cursor, len: usize) -> &'a [u8] {
        &blocks[c.block][c.offset..(c.offset + len)]
    }

    fn drop_old_blocks(&mut self) {
        let first_used = self.consume_c.block;
        for _ in 0..first_used {
            self.blocks.pop_front();
        }

        self.consume_c.block -= first_used;
        self.leading_c.block -= first_used;
    }

    fn consume(&mut self, len: usize) -> IoVec {
        let c = &mut self.consume_c;
        let blocks = &self.blocks;

        assert!(len != 0);

        let mut remaining = len;
        let mut r = IoVec::new();
        while remaining > 0 {
            let b = &blocks[c.block];
            let blen = b.len() - c.offset;

            if blen == 0 {
                c.offset = 0;
                c.block += 1;
            } else if blen >= remaining {
                r.push(&b[c.offset..(c.offset + remaining)]);
                c.offset += remaining;
                remaining = 0;
            } else {
                r.push(&b[c.offset..]);
                remaining -= blen;
                c.offset = 0;
                c.block += 1;
            }
        }
        self.unconsumed_len -= len as u64;
        r
    }

    fn consume_all(&mut self) -> IoVec {
        let c = &mut self.consume_c;
        let mut r = IoVec::new();
        while c.block < self.blocks.len() {
            let b = &self.blocks[c.block];
            r.push(&b[c.offset..]);
            c.offset = 0;
            c.block += 1;
        }
        self.unconsumed_len = 0;
        r
    }

    // Returns a vec of consume lengths
    fn next_data_(&mut self, data: &[u8]) -> Vec<usize> {
        let mut consumes = Vec::with_capacity(1024); // FIXME: estimate good capacity

        let mut offset = 0;
        let mut remainder = self.unconsumed_len as usize;
        let min_size = self.window_size as usize / 4;
        let ws = self.window_size as usize;

        while offset < data.len() {
            let end = data.len();
            if let Some(boundary) = self.hasher.next_match(&data[offset..end], self.mask_s) {
                consumes.push(remainder + boundary);
                offset += boundary;

                let skip_size = std::cmp::min(data.len() - offset, min_size);
                offset += skip_size;
                remainder = skip_size;
                continue;
            } else {
                offset += ws;
                remainder += ws;
            }

            if offset >= data.len() {
                break;
            }

            if let Some(boundary) = self.hasher.next_match(&data[offset..], self.mask_l) {
                consumes.push(remainder + boundary);
                offset += boundary;

                let skip_size = std::cmp::min(data.len() - offset, min_size);
                offset += skip_size;
                remainder = skip_size;
            } else {
                break;
            }
        }

        consumes
    }
}

impl Splitter for ContentSensitiveSplitter {
    fn next_data(&mut self, buffer: Vec<u8>, handler: &mut dyn IoVecHandler) -> Result<()> {
        self.blocks.push_back(buffer);

        while !self.eof() {
            let len = self.contiguous_size(&self.leading_c);

            let mut blocks = VecDeque::new();
            std::mem::swap(&mut self.blocks, &mut blocks);
            let consumes = self.next_data_(Self::ref_chunk(&blocks, &self.leading_c, len));
            std::mem::swap(&mut self.blocks, &mut blocks);

            self.unconsumed_len += len as u64;

            for consume_len in consumes {
                handler.handle_data(&self.consume(consume_len))?;
            }
            Self::inc_cursor(&self.blocks, &mut self.leading_c, len);
        }

        self.drop_old_blocks();
        Ok(())
    }

    fn next_break(&mut self, handler: &mut dyn IoVecHandler) -> Result<()> {
        let iov = self.consume_all();
        if !iov.is_empty() {
            handler.handle_data(&iov)?;
            self.drop_old_blocks();
        }

        self.leading_c = Cursor::default();
        self.consume_c = Cursor::default();

        Ok(())
    }

    fn complete(mut self, handler: &mut dyn IoVecHandler) -> Result<()> {
        self.next_break(handler)?;
        handler.complete()?;
        Ok(())
    }
}

//-------------------

#[cfg(test)]
mod splitter_tests {
    use super::*;
    use blake2::{Blake2s256, Digest};
    use rand::*;
    use std::collections::BTreeMap;
    use std::io::{BufReader, BufWriter, Read, Write};

    use crate::hash::*;

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
        fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
            for v in iov {
                println!("{:?}", v);
                self.output.write(v)?;
            }

            Ok(())
        }

        fn complete(&mut self) -> Result<()> {
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
        hashes: BTreeMap<Hash256, Entry>,
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
    }

    impl IoVecHandler for TestHandler {
        fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
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

        fn complete(&mut self) -> Result<()> {
            Ok(())
        }
    }

    //-----------

    fn split<R: Read, H: IoVecHandler>(input: &mut R, handler: &mut H) {
        const BLOCK_SIZE: usize = 128;
        let mut splitter = ContentSensitiveSplitter::new(BLOCK_SIZE as u32);

        const BUFFER_SIZE: usize = 4 * 1024 * 1024;
        loop {
            let mut buffer = vec![0u8; BUFFER_SIZE];
            let n = input.read(&mut buffer[..]).expect("couldn't read input");

            if n == 0 {
                break;
            } else if n == BUFFER_SIZE {
                splitter
                    .next_data(buffer, handler)
                    .expect("split next failed");
            } else {
                buffer.truncate(n);
                splitter
                    .next_data(buffer, handler)
                    .expect("split next failed");
            }
        }

        splitter.complete(handler).expect("split complete failed");
    }

    #[test]
    fn splitter_emits_all_data() {
        let input_buf = prep_data();
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
            writeln!(csv, "{}, {}", len, hits).expect("write failed");
        }

        eprintln!(
            "input len = {}, unique len = {}",
            input_buf.len(),
            handler.unique_len()
        );
        assert!(handler.unique_len() < (input_buf.len() / 4));
    }
}

//-----------------------------------------
