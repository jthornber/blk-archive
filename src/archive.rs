use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use io::Write;
use std::io;

use crate::iovec::*;
use crate::hash::*;

//-----------------------------------------

struct DataPacker {
    offset: u32,
    packer: zstd::Encoder<'static, Vec<u8>>,
}

impl DataPacker {
    fn new() -> Result<Self> {
        Ok(Self {
            offset: 0,
            packer: zstd::Encoder::new(Vec::new(), 0)?,
        })
    }

    fn write_iov(&mut self, iov: &IoVec) -> Result<()> {
        for v in iov {
            self.offset += v.len() as u32;
            self.packer.write_all(v)?;
        }

        Ok(())
    }

    fn complete(self) -> Result<Vec<u8>> {
        let r = self.packer.finish()?;
        Ok(r)
    }
}

//-----------------------------------------

pub struct SlabEntry {
    h: Hash256,
    offset: u32,
}

pub struct Slab {
    blocks: Vec<SlabEntry>,
    packer: DataPacker,
}

impl Slab {
    pub fn new() -> Result<Self> {
        Ok(Self {
            blocks: Vec::new(),
            packer: DataPacker::new()?,
        })
    }

    pub fn add_chunk(&mut self, h: Hash256, iov: &IoVec) -> Result<()> {
        self.blocks.push(SlabEntry {
            h,
            offset: self.packer.offset,
        });

        self.packer.write_iov(iov)?;
        Ok(())
    }

    pub fn complete<W: Write>(mut self, w: &mut W) -> Result<Vec<SlabEntry>> {
        w.write_u64::<LittleEndian>(self.blocks.len() as u64)?;
        for b in &self.blocks {
            w.write_all(&b.h[..])?;
            w.write_u32::<LittleEndian>(b.offset as u32)?;
        }

        let compressed = self.packer.complete()?;
        w.write_all(&compressed[..])?;
        let mut blocks = Vec::new();
        std::mem::swap(&mut blocks, &mut self.blocks);
        Ok(blocks)
    }

    pub fn nr_entries(&self) -> usize {
        self.blocks.len()
    }

    pub fn entries_len(&self) -> usize {
        self.packer.offset as usize
    }
}

//-----------------------------------------

