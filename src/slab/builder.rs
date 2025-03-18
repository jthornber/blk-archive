use anyhow::Result;
use std::path::Path;

use crate::slab::*;

//-----------------------------------------

pub struct SlabFileBuilder<P: AsRef<Path>> {
    path: P,
    create: bool,
    queue_depth: usize,
    read: bool,
    write: bool,
    compressed: bool,
    cache_nr_entries: usize,
}

impl<P: AsRef<Path>> SlabFileBuilder<P> {
    pub fn create(path: P) -> Self {
        Self {
            path,
            create: true,
            queue_depth: 1,
            read: true,
            write: true,
            compressed: false,
            cache_nr_entries: 1,
        }
    }

    pub fn open(path: P) -> Self {
        Self {
            path,
            create: false,
            queue_depth: 1,
            read: true,
            write: false,
            compressed: false,
            cache_nr_entries: 1,
        }
    }

    pub fn queue_depth(mut self, qd: usize) -> Self {
        self.queue_depth = qd;
        self
    }

    pub fn write(mut self, flag: bool) -> Self {
        self.write = flag;
        if self.create {
            assert!(self.write);
        }
        self
    }

    pub fn read(mut self, flag: bool) -> Self {
        self.read = flag;
        self
    }

    pub fn compressed(mut self, flag: bool) -> Self {
        assert!(self.create); // compressed can only be determined at create time
        self.compressed = flag;
        self
    }

    pub fn cache_nr_entries(mut self, count: usize) -> Self {
        self.cache_nr_entries = count;
        self
    }

    pub fn build(self) -> Result<SlabFile> {
        if self.create {
            SlabFile::create(
                self.path,
                self.queue_depth,
                self.compressed,
                self.cache_nr_entries,
            )
        } else if self.write {
            SlabFile::open_for_write(self.path, self.queue_depth, self.cache_nr_entries)
        } else {
            SlabFile::open_for_read(self.path, self.cache_nr_entries)
        }
    }
}

//-----------------------------------------
