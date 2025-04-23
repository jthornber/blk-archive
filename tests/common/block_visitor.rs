use anyhow::{anyhow, Result};
use std::os::unix::fs::FileExt;
use thinp::io_engine::buffer::Buffer;
use thinp::io_engine::PAGE_SIZE;
use thinp::math::div_up;

use crate::common::random::{Generator, Pattern};

//------------------------------------------

pub trait BlockVisitor {
    fn visit(&mut self, blocknr: u64) -> Result<()>;
}

pub fn visit_blocks(nr_blocks: u64, v: &mut dyn BlockVisitor) -> Result<()> {
    for b in 0..nr_blocks {
        v.visit(b)?;
    }
    Ok(())
}

//------------------------------------------

pub struct Stamper<T> {
    file: T,
    block_size: usize, // bytes
    seed: u64,
    buf: Buffer,
    offset: u64,     // bytes
    len: u64,        // bytes
    size_limit: u64, // bytes
    generator: Generator,
}

impl<T: FileExt> Stamper<T> {
    pub fn new(file: T, seed: u64, block_size: usize, pattern: Pattern) -> Self {
        let buf = Buffer::new(block_size, PAGE_SIZE);
        let generator = Generator::new(pattern);
        Self {
            file,
            block_size,
            seed,
            buf,
            offset: 0,
            len: u64::MAX,
            size_limit: u64::MAX,
            generator,
        }
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self.size_limit = offset + self.len;
        self
    }

    pub fn len(mut self, len: u64) -> Self {
        self.len = len;
        self.size_limit = self.offset + len;
        self
    }

    pub fn stamp_file(&mut self) -> Result<()> {
        let nr_blocks = div_up(self.len, self.block_size as u64);
        visit_blocks(nr_blocks, self)
    }
}

impl<T: FileExt> BlockVisitor for Stamper<T> {
    fn visit(&mut self, blocknr: u64) -> Result<()> {
        let offset = blocknr * self.block_size as u64 + self.offset;
        if offset < self.size_limit {
            self.generator
                .fill_buffer(self.seed ^ blocknr, self.buf.get_data())?;
            let len = std::cmp::min(self.block_size as u64, self.size_limit - offset);
            self.file
                .write_all_at(&self.buf.get_data()[..len as usize], offset)?;
        }
        Ok(())
    }
}

//------------------------------------------

pub struct Verifier<T> {
    file: T,
    block_size: usize, // bytes
    seed: u64,
    buf: Buffer,
    offset: u64,
    len: u64,
    size_limit: u64,
    generator: Generator,
}

impl<T: FileExt> Verifier<T> {
    pub fn new(file: T, seed: u64, block_size: usize, pattern: Pattern) -> Self {
        let buf = Buffer::new(block_size, PAGE_SIZE);
        let generator = Generator::new(pattern);
        Self {
            file,
            block_size,
            seed,
            buf,
            offset: 0,
            len: u64::MAX,
            size_limit: u64::MAX,
            generator,
        }
    }

    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self.size_limit = offset + self.len;
        self
    }

    pub fn len(mut self, len: u64) -> Self {
        self.len = len;
        self.size_limit = self.offset + len;
        self
    }

    pub fn verify(&mut self) -> Result<()> {
        let nr_blocks = div_up(self.len, self.block_size as u64);
        visit_blocks(nr_blocks, self)
    }
}

impl<T: FileExt> BlockVisitor for Verifier<T> {
    fn visit(&mut self, blocknr: u64) -> Result<()> {
        let offset = blocknr * self.block_size as u64 + self.offset;
        if offset < self.size_limit {
            let len = std::cmp::min(self.block_size as u64, self.size_limit - offset);
            let buf = &mut self.buf.get_data()[..len as usize];
            self.file.read_exact_at(buf, offset)?;

            if !self.generator.verify_buffer(self.seed ^ blocknr, buf)? {
                return Err(anyhow!("data verify failed for data block {}", blocknr));
            }
        }
        Ok(())
    }
}

//------------------------------------------
