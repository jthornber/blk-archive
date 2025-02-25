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

            consume_c: Cursor::default(),
        }
    }

    fn drop_old_blocks(&mut self) {
        let first_used = self.consume_c.block;
        for _ in 0..first_used {
            self.blocks.pop_front();
        }

        self.consume_c.block -= first_used;
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
            } else if blen > remaining {
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

        if self.unconsumed_len == 0 {
            assert!(c.block == self.blocks.len());
        }

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
        let max_size = self.window_size as usize * 8;
        let ws = self.window_size as usize;

        if remainder < min_size {
            offset += min_size - remainder;
            remainder = min_size;
        }
        while offset < data.len() {
            let len_s = ws - remainder;
            if len_s > 0 {
                let end = std::cmp::min(data.len(), offset + len_s);
                if let Some(boundary) = self.hasher.next_match(&data[offset..end], self.mask_s) {
                    consumes.push(remainder + boundary);
                    offset += boundary + min_size;
                    remainder = min_size;
                    continue;
                } else {
                    offset += len_s;
                    remainder += len_s;
                }

                if offset >= data.len() {
                    break;
                }
            }
            let len_l = max_size - remainder;
            let end = std::cmp::min(data.len(), offset + len_l);
            if let Some(boundary) = self.hasher.next_match(&data[offset..end], self.mask_l) {
                consumes.push(remainder + boundary);
                offset += boundary + min_size;
                remainder = min_size;
            } else {
                consumes.push(end - offset + remainder);
                offset = end + min_size;
                remainder = min_size;
            }
        }

        consumes
    }
}

impl Splitter for ContentSensitiveSplitter {
    fn next_data(&mut self, buffer: Vec<u8>, handler: &mut impl IoVecHandler) -> Result<()> {
        let consumes = self.next_data_(&buffer);

        let len = buffer.len();
        self.blocks.push_back(buffer);
        self.unconsumed_len += len as u64;

        for consume_len in consumes {
            handler.handle_data(&self.consume(consume_len))?;
        }

        self.drop_old_blocks();
        Ok(())
    }

    fn next_break(&mut self, handler: &mut impl IoVecHandler) -> Result<()> {
        let iov = self.consume_all();
        if !iov.is_empty() {
            handler.handle_data(&iov)?;
            self.drop_old_blocks();
        }

        self.consume_c = Cursor::default();

        Ok(())
    }

    fn complete(mut self, handler: &mut impl IoVecHandler) -> Result<()> {
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
        buf.iter_mut()
            .skip(offset)
            .take(len)
            .for_each(|v| *v = rand::thread_rng().gen_range(0..256) as u8);
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

    impl<W: Write> IoVecHandler for CatHandler<'_, W> {
        fn handle_data(&mut self, iov: &IoVec) -> Result<()> {
            for v in iov {
                println!("{:?}", v);
                self.output.write_all(v)?;
            }

            Ok(())
        }

        fn complete(&mut self) -> Result<()> {
            Ok(())
        }
    }

    //-----------

    #[derive(Default)]
    struct Entry {
        hits: usize,
        len: usize,
        zero: bool,
    }

    #[derive(Default)]
    struct TestHandler {
        nr_chunks: usize,
        hashes: BTreeMap<Hash256, Entry>,
    }

    impl TestHandler {
        fn histogram(&self) -> BTreeMap<usize, (u32, u32)> {
            let mut r = BTreeMap::new();
            for Entry { hits, zero, .. } in self.hashes.values() {
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
            for Entry { len, .. } in self.hashes.values() {
                let e = r.entry(*len).or_insert(0);
                *e += 1;
            }
            r
        }

        fn unique_len(&self) -> usize {
            let mut total = 0;
            for Entry { hits, len, .. } in self.hashes.values() {
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

            let e = self.hashes.entry(hasher.finalize()).or_default();

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
