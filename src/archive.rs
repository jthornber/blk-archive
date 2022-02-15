use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use flate2::{write::ZlibEncoder, Compression};
use io::Write;
use std::io;

use crate::splitter::*;
use crate::content_sensitive_splitter::*;

//-----------------------------------------

pub struct SlabEntry {
    h: Hash,
    offset: u32,
}

pub struct Slab {
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

    pub fn entries_len(&self) -> usize {
        self.offset as usize
    }
}

//-----------------------------------------

