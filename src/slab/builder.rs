use anyhow::{anyhow, Result};
use std::path::Path;

use crate::slab::file::*;

//-----------------------------------------

/// Builder for creating and configuring `SlabFile` instances.
///
/// This builder provides an interface for configuring how a slab file
/// should be created or opened. It handles various configuration options like
/// read/write access, compression, queue depth for async operations, and caching.
///
/// # Examples
///
/// Creating a new compressed slab file:
/// ```
/// # use anyhow::Result;
/// # use blk_archive::slab::*;
/// # fn example() -> Result<()> {
/// let slab_file = SlabFileBuilder::create("data.slab")
///     .compressed(true)
///     .queue_depth(8)
///     .cache_nr_entries(100)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// Opening an existing slab file for reading:
/// ```
/// # use anyhow::Result;
/// # use blk_archive::slab::*;
/// # fn example() -> Result<()> {
/// let slab_file = SlabFileBuilder::open("data.slab")
///     .cache_nr_entries(100)
///     .build()?;
/// # Ok(())
/// # }
pub struct SlabFileBuilder<P: AsRef<Path>> {
    path: P,
    create: bool,
    queue_depth: usize,
    read: bool,
    write: bool,

    // compressed should only be set if we're creating a new
    // slab file.
    compressed: Option<bool>,
    cache_nr_entries: usize,
}

impl<P: AsRef<Path>> SlabFileBuilder<P> {
    /// Create a new builder that creates a new slab file
    pub fn create(path: P) -> Self {
        Self {
            path,
            create: true,
            queue_depth: 1,
            read: true,
            write: true,
            compressed: Some(false),
            cache_nr_entries: 1,
        }
    }

    /// Create a new builder that opens an existing slab file
    pub fn open(path: P) -> Self {
        Self {
            path,
            create: false,
            queue_depth: 1,
            read: true,
            write: false,
            compressed: None,
            cache_nr_entries: 1,
        }
    }

    /// Set the queue depth for asynchronous operations
    pub fn queue_depth(mut self, qd: usize) -> Self {
        self.queue_depth = qd;
        self
    }

    /// Set whether the file should be opened for writing
    pub fn write(mut self, flag: bool) -> Self {
        self.write = flag;
        self
    }

    /// Set whether the file should be opened for reading
    pub fn read(mut self, flag: bool) -> Self {
        self.read = flag;
        self
    }

    /// Set whether the file should use compression
    pub fn compressed(mut self, flag: bool) -> Self {
        self.compressed = Some(flag);
        self
    }

    /// Set the number of entries to cache
    pub fn cache_nr_entries(mut self, count: usize) -> Self {
        self.cache_nr_entries = count;
        self
    }

    /// Build the SlabFile according to the configuration
    pub fn build(self) -> Result<SlabFile> {
        // Validate configuration
        if self.create && !self.write {
            return Err(anyhow!("Cannot create a file without write access"));
        }

        if self.compressed.is_some() && !self.create {
            return Err(anyhow!(
                "Cannot specify compression unless creating a new slab file"
            ));
        }

        if self.create {
            SlabFile::create(
                self.path,
                self.queue_depth,
                self.compressed.unwrap(),
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
