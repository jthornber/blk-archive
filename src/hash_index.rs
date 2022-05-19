use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, WriteBytesExt};
use nom::multi::*;
use nom::number::complete::*;
use nom::IResult;
use std::collections::BTreeMap;
use std::io::Write;

use crate::hash::*;

//--------------------------------

struct BuilderEntry {
    h: Hash256,
    index: usize,
    begin: usize,
    len: usize,
}

pub struct IndexBuilder {
    index: BTreeMap<Hash256, u32>,
    entries: Vec<BuilderEntry>,
    offset: usize,
}

impl IndexBuilder {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            index: BTreeMap::new(),
            entries: Vec::with_capacity(n),
            offset: 0,
        }
    }

    pub fn insert(&mut self, h: Hash256, len: usize) {
        self.index.insert(h, self.entries.len() as u32);
        self.entries.push(BuilderEntry {
            h,
            index: self.entries.len(),
            begin: self.offset,
            len,
        });
        self.offset += len;
    }

    pub fn build(mut self) -> Result<Vec<u8>> {
        let mut w = Vec::with_capacity(self.entries.len() * (32 + 10) + 32);
        self.entries.sort_by(|l, r| l.h.partial_cmp(&r.h).unwrap());

        assert!(self.entries.len() <= u16::MAX as usize);
        w.write_u16::<LittleEndian>(self.entries.len() as u16)?;
        for e in &self.entries {
            assert!(e.index <= u16::MAX as usize);
            w.write_u16::<LittleEndian>(e.index as u16)?;

            // FIXME: we don't need to write this since we can infer
            // from index and the lens.  It will make paging in the
            // index take longer though.
            assert!(e.begin <= u32::MAX as usize);
            w.write_u32::<LittleEndian>(e.begin as u32)?;

            assert!(e.len <= u32::MAX as usize);
            w.write_u32::<LittleEndian>(e.len as u32)?;
        }

        for e in &self.entries {
            w.write_all(&e.h)?;
        }

        Ok(w)
    }

    pub fn lookup(&self, h: &Hash256) -> Option<u32> {
        self.index.get(h).cloned()
    }
}

//--------------------------------

fn parse_entry(input: &[u8]) -> IResult<&[u8], (u16, u32, u32)> {
    let (input, index) = le_u16(input)?;
    let (input, begin) = le_u32(input)?;
    let (input, len) = le_u32(input)?;
    Ok((input, (index, begin, len)))
}

fn parse_entries(input: &[u8]) -> IResult<&[u8], Vec<(u16, u32, u32)>> {
    let (input, nr_entries) = le_u16(input)?;
    count(parse_entry, nr_entries as usize)(input)
}

fn pos(index: usize) -> usize {
    index * 32
}

fn get_hash(data: &[u8], i: usize) -> &Hash256 {
    Hash256::from_slice(&data[pos(i as usize)..pos((i + 1) as usize)])
}

fn bsearch(h: &Hash256, data: &[u8], max: usize) -> Option<usize> {
    use std::cmp::Ordering;

    let mut lo: i32 = -1;
    let mut hi: i32 = max as i32;

    while hi - lo > 1 {
        let mid: i32 = lo + ((hi - lo) / 2);
        let mid_hash = get_hash(data, mid as usize);

        match h.cmp(mid_hash) {
            Ordering::Less => {
                hi = mid;
            }
            Ordering::Equal => {
                return Some(mid as usize);
            }
            Ordering::Greater => {
                lo = mid;
            }
        }
    }
    None
}

//--------------------------------

// Maps Hash256 -> DataIndex
pub struct ByHash {
    data: Vec<u8>,
    indexes: Vec<u16>,

    // offset into data where the sorted hashes start
    hashes_offset: usize,
}

impl ByHash {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let (_input, entries) =
            parse_entries(&data).map_err(|_| anyhow!("couldn't parse hash entries"))?;
        let indexes = entries.iter().map(|(index, _, _)| index).cloned().collect();

        let hashes_offset = (entries.len() * 10) + 2;

        Ok(Self {
            data,
            indexes,
            hashes_offset,
        })
    }

    pub fn lookup(&self, h: &Hash256) -> Option<usize> {
        bsearch(h, &self.data[self.hashes_offset..], self.indexes.len())
            .map(|i| self.indexes[i as usize] as usize)
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn get(&self, i: usize) -> &Hash256 {
        get_hash(&self.data[self.hashes_offset..], i)
    }
}

//--------------------------------

// Maps data_index -> (data_begin, data_end, hash256)
pub struct ByIndex {
    entries: BTreeMap<u16, (u32, u32, Hash256)>,
}

impl ByIndex {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let (_input, entries_) =
            parse_entries(&data).map_err(|_| anyhow!("couldn't parse hash entries"))?;
        let offset = (entries_.len() * 10) + 2;
        let mut entries = BTreeMap::new();
        for (i, e) in entries_.iter().enumerate() {
            entries.insert(e.0, (e.1, e.1 + e.2, *get_hash(&data[offset..], i)));
        }

        Ok(Self { entries })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, i: usize) -> Option<&(u32, u32, Hash256)> {
        self.entries.get(&(i as u16))
    }
}

//--------------------------------
