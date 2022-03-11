use anyhow::{anyhow, Result};
use roaring::bitmap::RoaringBitmap;
use std::collections::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use thinp::io_engine::*;
use thinp::pdata::btree;
use thinp::pdata::btree::*;
use thinp::pdata::btree_walker::*;
use thinp::thin::block_time::*;
use thinp::thin::device_detail::*;
use thinp::thin::superblock::*;

//---------------------------------

#[allow(dead_code)]
pub struct ThinPool {
    pool_dev: PathBuf,
    metadata_dev: PathBuf,
}

impl ThinPool {
    pub fn new<P: AsRef<Path>>(_pool_dev: P) -> Result<ThinPool> {
        todo!();
    }

    fn take_metadata_snap(&mut self) -> Result<MetadataLock> {
        todo!();
    }
}

//---------------------------------

struct MetadataLock {}

impl Drop for MetadataLock {
    fn drop(&mut self) {
        todo!();
    }
}

//---------------------------------

#[derive(Default)]
struct MappingCollector {
    provisioned: Mutex<RoaringBitmap>,
}

impl MappingCollector {
    fn provisioned(self) -> RoaringBitmap {
        self.provisioned.into_inner().unwrap()
    }
}

impl NodeVisitor<BlockTime> for MappingCollector {
    fn visit(
        &self,
        _path: &[u64],
        _kr: &KeyRange,
        _header: &NodeHeader,
        keys: &[u64],
        _values: &[BlockTime],
    ) -> btree::Result<()> {
        let mut bits = self.provisioned.lock().unwrap();
        for k in keys {
            assert!(*k <= u32::MAX as u64);
            bits.insert(*k as u32);
        }
        Ok(())
    }

    fn visit_again(&self, _path: &[u64], _b: u64) -> btree::Result<()> {
        Ok(())
    }

    fn end_walk(&self) -> btree::Result<()> {
        Ok(())
    }
}

#[allow(dead_code)]
pub struct ThinInfo {
    data_block_size: u32,
    details: DeviceDetail,
    provisioned_blocks: RoaringBitmap,
}

fn read_info(pool: &ThinPool, thin_id: u32) -> Result<ThinInfo> {
    let engine = Arc::new(SyncIoEngine::new(pool.metadata_dev.as_path(), 8, false)?);

    // Read metadata superblock
    let sb = read_superblock_snap(&*engine)?;

    // look up dev details, we don't actually have a lookup method, but the details
    // tree is small, so we slurp the whole lot.
    let details: DeviceDetail = {
        let mut path = vec![];
        let details: BTreeMap<u64, DeviceDetail> =
            btree_to_map(&mut path, engine.clone(), true, sb.details_root)?;
        let thin_id = thin_id as u64;
        if let Some(d) = details.get(&thin_id) {
            d.clone()
        } else {
            return Err(anyhow!("couldn't find thin device with that id"));
        }
    };

    // walk mapping tree
    let ignore_non_fatal = true;
    let walker = BTreeWalker::new(engine, ignore_non_fatal);
    let collector = MappingCollector::default();
    let mut path = vec![];
    walker.walk(&mut path, &collector, sb.mapping_root)?;
    let provisioned_blocks = collector.provisioned();

    Ok(ThinInfo {
        data_block_size: sb.data_block_size,
        details,
        provisioned_blocks,
    })
}

//---------------------------------

pub fn read_thin_mappings(pool: &mut ThinPool, thin_id: u32) -> Result<ThinInfo> {
    let _lock = pool.take_metadata_snap()?;
    read_info(pool, thin_id)
}

//---------------------------------
