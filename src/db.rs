use anyhow::{Context, Result};

use crate::cuckoo_filter::*;
use crate::hash::*;
use crate::hash_index::*;
use crate::iovec::*;
use crate::paths;
use crate::paths::*;
use crate::slab::*;
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

pub const SLAB_SIZE_TARGET: usize = 4 * 1024 * 1024;

pub struct Db {
    seen: CuckooFilter,
    hashes: lru::LruCache<u32, ByHash>,

    data_file: SlabFile,
    hashes_file: Arc<Mutex<SlabFile>>,

    current_slab: u32,
    current_entries: usize,
    current_index: IndexBuilder,

    data_buf: Vec<u8>,
    hashes_buf: Vec<u8>,
}

fn complete_slab_(slab: &mut SlabFile, buf: &mut Vec<u8>) -> Result<()> {
    slab.write_slab(buf)?;
    buf.clear();
    Ok(())
}

pub fn complete_slab(slab: &mut SlabFile, buf: &mut Vec<u8>, threshold: usize) -> Result<bool> {
    if buf.len() > threshold {
        complete_slab_(slab, buf)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

impl Db {
    pub fn new(data_file: SlabFile, slab_capacity: usize) -> Result<Self> {
        let seen = CuckooFilter::read(paths::index_path())?;
        let hashes = lru::LruCache::new(NonZeroUsize::new(slab_capacity).unwrap());
        let nr_slabs = data_file.get_nr_slabs() as u32;

        let hashes_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(hashes_path())
                .write(true)
                .queue_depth(16)
                .build()
                .context("couldn't open hashes slab file")?,
        ));

        {
            let hashes_file = hashes_file.lock().unwrap();
            assert_eq!(data_file.get_nr_slabs(), hashes_file.get_nr_slabs());
        }

        Ok(Self {
            seen,
            hashes,
            data_file,
            hashes_file,
            current_slab: nr_slabs,
            current_index: IndexBuilder::with_capacity(1024), // FIXME: estimate
            current_entries: 0,
            data_buf: Vec::new(),
            hashes_buf: Vec::new(),
        })
    }

    pub fn ensure_extra_capacity(&mut self, blocks: usize) -> Result<()> {
        eprintln!("db.ensure_extra_capacity {}", blocks);

        if self.seen.capacity() < self.seen.len() + blocks {
            self.rebuild_index(self.seen.len() + blocks)?;
            eprintln!("resized index to {}", self.seen.capacity());
        }

        Ok(())
    }

    fn get_hash_index(&mut self, slab: u32) -> Result<&ByHash> {
        // the current slab is not inserted into the self.hashes
        assert!(slab != self.current_slab);

        self.hashes.try_get_or_insert(slab, || {
            let mut hashes_file = self.hashes_file.lock().unwrap();
            let buf = hashes_file.read(slab)?;
            ByHash::new(buf)
        })
    }

    fn rebuild_index(&mut self, new_capacity: usize) -> Result<()> {
        let mut seen = CuckooFilter::with_capacity(new_capacity);

        // Scan the hashes file.
        let mut hashes_file = self.hashes_file.lock().unwrap();
        let nr_slabs = hashes_file.get_nr_slabs();
        for s in 0..nr_slabs {
            let buf = hashes_file.read(s as u32)?;
            let hi = ByHash::new(buf)?;
            for i in 0..hi.len() {
                let h = hi.get(i);
                let mini_hash = hash_le_u64(&h);
                seen.test_and_set(mini_hash, s as u32)?;
            }
        }

        std::mem::swap(&mut seen, &mut self.seen);

        Ok(())
    }

    fn maybe_complete_data(&mut self, target: usize) -> Result<()> {
        if complete_slab(&mut self.data_file, &mut self.data_buf, target)? {
            let mut builder = IndexBuilder::with_capacity(1024); // FIXME: estimate properly
            std::mem::swap(&mut builder, &mut self.current_index);
            let buffer = builder.build()?;
            self.hashes_buf.write_all(&buffer[..])?;
            let index = ByHash::new(buffer)?;
            self.hashes.put(self.current_slab, index);

            let mut hashes_file = self.hashes_file.lock().unwrap();
            complete_slab_(&mut hashes_file, &mut self.hashes_buf)?;
            self.current_slab += 1;
            self.current_entries = 0;
        }
        Ok(())
    }

    // Returns the (slab, entry) for the newly added entry
    pub fn add_data_entry(&mut self, h: Hash256, iov: &IoVec, len: u64) -> Result<(u32, u32)> {
        // There is an inherent race condition between checking if we have it and adding it,
        // check before we add when this functionality ends up on a server side.
        if let Some(location) = self.is_known(&h)? {
            return Ok(location);
        }

        // Add entry to cuckoo filter, not checking return value as we could get an "AlreadyPresent"
        // when its not really present.  Cuckoo filters have the following behavior which is
        // "possibly in set" or "definitely not in set"
        //
        self.seen.test_and_set(hash_le_u64(&h), self.current_slab)?;

        let r = (self.current_slab, self.current_entries as u32);
        for v in iov {
            self.data_buf.extend_from_slice(v);
        }
        self.current_entries += 1;
        self.current_index.insert(h, len as usize);
        self.maybe_complete_data(SLAB_SIZE_TARGET)?;
        Ok(r)
    }

    // Have we seen this hash before, if we have we will return the slab and offset
    // Note: This function does not modify any state
    pub fn is_known(&mut self, h: &Hash256) -> Result<Option<(u32, u32)>> {
        let mini_hash = hash_le_u64(&h);
        let rc = match self.seen.test(mini_hash)? {
            // This is a possibly in set
            InsertResult::AlreadyPresent(s) => {
                if self.current_slab == s {
                    if let Some(offset) = self.current_index.lookup(&h) {
                        Some((self.current_slab, offset))
                    } else {
                        None
                    }
                } else {
                    let hi = self.get_hash_index(s)?;
                    if let Some(offset) = hi.lookup(&h) {
                        Some((s, offset as u32))
                    } else {
                        None
                    }
                }
            }
            _ => None,
        };
        Ok(rc)
    }

    // NOTE: This won't work for multiple clients and one server!
    pub fn file_sizes(&mut self) -> (u64, u64) {
        let hashes_written = {
            let hashes_file = self.hashes_file.lock().unwrap();
            hashes_file.get_file_size()
        };

        (self.data_file.get_file_size(), hashes_written)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        eprintln!("closing db!");
        self.maybe_complete_data(0)
            .expect("db.drop: maybe_complete_data error!");
        let mut hashes_file = self.hashes_file.lock().unwrap();
        hashes_file
            .close()
            .expect("db.drop: hashes_file.close() error!");
        self.data_file
            .close()
            .expect("db.drop: data_file.close() error!");
        self.seen
            .write(paths::index_path())
            .expect("db.drop: seen.write() error!");
    }
}
