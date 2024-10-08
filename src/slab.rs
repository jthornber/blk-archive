use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use threadpool::ThreadPool;

use crate::hash::*;
use crate::lru::*;

//------------------------------------------------

struct DataCache {
    lru: LRU,
    tree: BTreeMap<u32, Arc<Vec<u8>>>,
    hits: u64,
    misses: u64,
}

impl DataCache {
    fn new(nr_entries: usize) -> Self {
        let lru = LRU::with_capacity(nr_entries);
        let tree = BTreeMap::new();

        Self {
            lru,
            tree,
            hits: 0,
            misses: 0,
        }
    }

    fn find(&mut self, slab: u32) -> Option<Arc<Vec<u8>>> {
        let r = self.tree.get(&slab).cloned();
        if r.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        r
    }

    fn insert(&mut self, slab: u32, data: Arc<Vec<u8>>) {
        use PushResult::*;

        match self.lru.push(slab) {
            AlreadyPresent => {
                // Nothing to do
            }
            Added => {
                self.tree.insert(slab, data);
            }
            AddAndEvict(old_slab) => {
                self.tree.remove(&old_slab);
                self.tree.insert(slab, data);
            }
        }
    }
}

//------------------------------------------------

#[derive(Default)]
struct SlabOffsets {
    offsets: Vec<u64>,
}

impl SlabOffsets {
    fn read_offset_file<P: AsRef<Path>>(p: P) -> Result<Self> {
        let r = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(p)
            .context("opening offset file")?;

        let len = r.metadata().context("offset metadata")?.len();
        let mut r = std::io::BufReader::new(r);
        let nr_entries = len / std::mem::size_of::<u64>() as u64;
        let mut offsets = Vec::with_capacity(nr_entries as usize);
        for _ in 0..nr_entries {
            offsets.push(
                r.read_u64::<LittleEndian>()
                    .context("offsets read failed")?,
            );
        }

        Ok(Self { offsets })
    }

    fn write_offset_file<P: AsRef<Path>>(&self, p: P) -> Result<()> {
        let mut w = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(true)
            .open(p)?;

        for o in &self.offsets {
            w.write_u64::<LittleEndian>(*o)?;
        }

        Ok(())
    }
}

//------------------------------------------------
// Slab files are used to store 'slabs' of data.  You
// may only append to a slab file, or read an existing
// slab.
//
// Along side the slab an 'index' file will be written
// that contains the offsets of all the slabs.  This is
// derived data, and can be rebuilt with the repair fn.
//
// file := <header> <slab>*
// header := <magic nr> <slab format version> <flags>
// slab := <magic nr> <len> <checksum> <compressed data>

const FILE_MAGIC: u64 = 0xb927f96a6b611180;
const SLAB_MAGIC: u64 = 0x20565137a3100a7c;

const FORMAT_VERSION: u32 = 0;

pub type SlabIndex = u64;

pub struct SlabData {
    pub index: SlabIndex,
    pub data: Vec<u8>,
}

struct SlabShared {
    data: File,
    offsets: SlabOffsets,
    file_size: u64,
}

// FIXME: add index file
pub struct SlabFile {
    compressed: bool,
    compressor: Option<CompressionService>,
    offsets_path: PathBuf,
    pending_index: u64,

    shared: Arc<Mutex<SlabShared>>,

    tx: Option<SyncSender<SlabData>>,
    tid: Option<thread::JoinHandle<()>>,

    data_cache: DataCache,
}

impl Drop for SlabFile {
    fn drop(&mut self) {
        let mut tx = None;
        std::mem::swap(&mut tx, &mut self.tx);
        drop(tx);

        let mut compressor = None;
        std::mem::swap(&mut compressor, &mut self.compressor);
        if let Some(c) = compressor {
            c.pool.join();
        }
    }
}

fn write_slab(shared: &Arc<Mutex<SlabShared>>, data: &[u8]) -> Result<()> {
    assert!(!data.is_empty());

    let mut shared = shared.lock().unwrap();

    let offset = shared.file_size;
    shared.offsets.offsets.push(offset);
    shared.file_size += 8 + 8 + 8 + data.len() as u64;

    shared.data.seek(SeekFrom::End(0))?;
    shared.data.write_u64::<LittleEndian>(SLAB_MAGIC)?;
    shared.data.write_u64::<LittleEndian>(data.len() as u64)?;
    let csum = hash_64(data);
    shared.data.write_all(&csum)?;
    shared.data.write_all(data)?;

    Ok(())
}

fn writer_(shared: Arc<Mutex<SlabShared>>, rx: Receiver<SlabData>) -> Result<()> {
    let mut write_index = 0;
    let mut queued: BTreeMap<SlabIndex, SlabData> = BTreeMap::new();

    loop {
        let buf = rx.recv();
        if buf.is_err() {
            // all send ends have been closed, so we're done.
            break;
        }

        let buf = buf.unwrap();
        if buf.index == write_index {
            write_slab(&shared, &buf.data)?;
            write_index += 1;

            while let Some(buf) = queued.remove(&write_index) {
                write_slab(&shared, &buf.data)?;
                write_index += 1;
            }
        } else {
            queued.insert(buf.index, buf);
        }
    }

    assert!(queued.is_empty());
    Ok(())
}

fn writer(shared: Arc<Mutex<SlabShared>>, rx: Receiver<SlabData>) {
    // FIXME: pass on error
    writer_(shared, rx).expect("write of slab failed");
}

fn offsets_path<P: AsRef<Path>>(p: P) -> PathBuf {
    let mut offsets_path = PathBuf::new();
    offsets_path.push(p);
    offsets_path.set_extension("offsets");
    offsets_path
}

fn read_slab_header(data: &mut std::fs::File) -> Result<u32> {
    let magic = data
        .read_u64::<LittleEndian>()
        .context("couldn't read magic")?;
    let version = data
        .read_u32::<LittleEndian>()
        .context("couldn't read version")?;
    let flags = data
        .read_u32::<LittleEndian>()
        .context("couldn't read flags")?;

    if magic != FILE_MAGIC {
        return Err(anyhow!(
            "slab file magic is invalid or corrupt, actual {} != {} expected",
            magic,
            FILE_MAGIC
        ));
    }

    if version != FORMAT_VERSION {
        return Err(anyhow!(
            "slab file version actual {} != {} expected",
            version,
            FORMAT_VERSION
        ));
    }

    if !(flags == 0 || flags == 1) {
        return Err(anyhow!(
            "slab file flag value unexpected {} != 0 or 1",
            flags
        ));
    }
    Ok(flags)
}

impl SlabFile {
    fn create<P: AsRef<Path>>(
        data_path: P,
        queue_depth: usize,
        compressed: bool,
        cache_nr_entries: usize,
    ) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)?;

        let (tx, rx) = sync_channel(queue_depth);
        let flags = if compressed { 1 } else { 0 };
        data.write_u64::<LittleEndian>(FILE_MAGIC)?;
        data.write_u32::<LittleEndian>(FORMAT_VERSION)?;
        data.write_u32::<LittleEndian>(flags)?;

        let offsets = SlabOffsets::default();
        let file_size = data.metadata()?.len();
        let shared = Arc::new(Mutex::new(SlabShared {
            data,
            offsets,
            file_size,
        }));

        let (compressor, tx) = if flags == 1 {
            let (c, tx) = CompressionService::new(1, tx);
            (Some(c), tx)
        } else {
            (None, tx)
        };

        let tid = {
            let shared = shared.clone();
            thread::Builder::new()
                .name("SlabFile::create (writer)".to_string())
                .spawn(move || writer(shared, rx))?
        };

        Ok(Self {
            compressed,
            compressor,
            offsets_path,
            pending_index: 0,
            shared,
            tx: Some(tx),
            tid: Some(tid),
            data_cache: DataCache::new(cache_nr_entries),
        })
    }

    fn open_for_write<P: AsRef<Path>>(
        data_path: P,
        queue_depth: usize,
        cache_nr_entries: usize,
    ) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(data_path)
            .context("open offsets")?;

        let flags = read_slab_header(&mut data)?;

        let compressed = flags == 1;
        let (tx, rx) = sync_channel(queue_depth);
        let (compressor, tx) = if flags == 1 {
            let (c, tx) = CompressionService::new(4, tx);
            (Some(c), tx)
        } else {
            (None, tx)
        };

        let offsets = SlabOffsets::read_offset_file(&offsets_path)?;
        let file_size = data.metadata()?.len();
        let shared = Arc::new(Mutex::new(SlabShared {
            data,
            offsets,
            file_size,
        }));

        let tid = {
            let shared = shared.clone();
            thread::Builder::new()
                .name("SlabFile::open_for_write (writer)".to_string())
                .spawn(move || writer(shared, rx))?
        };

        Ok(Self {
            compressed,
            compressor,
            offsets_path,
            pending_index: 0,
            shared,
            tx: Some(tx),
            tid: Some(tid),
            data_cache: DataCache::new(cache_nr_entries),
        })
    }

    fn open_for_read<P: AsRef<Path>>(data_path: P, cache_nr_entries: usize) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(data_path)?;

        let flags = read_slab_header(&mut data)?;
        let compressed = flags == 1;
        let compressor = None;

        let offsets = SlabOffsets::read_offset_file(&offsets_path)?;
        let file_size = data.metadata()?.len();
        let shared = Arc::new(Mutex::new(SlabShared {
            data,
            offsets,
            file_size,
        }));

        Ok(Self {
            compressed,
            compressor,
            offsets_path,
            pending_index: 0,
            shared,
            tx: None,
            tid: None,
            data_cache: DataCache::new(cache_nr_entries),
        })
    }

    pub fn close(&mut self) -> Result<()> {
        self.tx = None;
        let mut tid = None;
        std::mem::swap(&mut tid, &mut self.tid);
        if let Some(tid) = tid {
            tid.join().expect("join failed");
        }

        let shared = self.shared.lock().unwrap();
        shared.offsets.write_offset_file(&self.offsets_path)?;
        Ok(())
    }

    pub fn read_(&mut self, slab: u32) -> Result<Vec<u8>> {
        let mut shared = self.shared.lock().unwrap();

        let offset = shared.offsets.offsets[slab as usize];
        shared.data.seek(SeekFrom::Start(offset))?;

        let magic = shared.data.read_u64::<LittleEndian>()?;
        let len = shared.data.read_u64::<LittleEndian>()?;
        assert_eq!(magic, SLAB_MAGIC);

        let mut expected_csum: Hash64 = Hash64::default();
        shared.data.read_exact(&mut expected_csum)?;

        let mut buf = vec![0; len as usize];
        shared.data.read_exact(&mut buf)?;

        let actual_csum = hash_64(&buf);
        assert_eq!(actual_csum, expected_csum);

        if self.compressed {
            let decompress_buff_size_mb: usize = env::var("BLK_ARCHIVE_DECOMPRESS_BUFF_SIZE_MB")
                .unwrap_or(String::from("4"))
                .parse::<usize>()
                .unwrap_or(4);
            let mut z = zstd::Decoder::new(&buf[..])?;
            let mut buffer = Vec::with_capacity(decompress_buff_size_mb * 1024 * 1024);
            z.read_to_end(&mut buffer)?;
            Ok(buffer)
        } else {
            Ok(buf)
        }
    }

    pub fn read(&mut self, slab: u32) -> Result<Arc<Vec<u8>>> {
        if let Some(data) = self.data_cache.find(slab) {
            Ok(data)
        } else {
            let data = Arc::new(self.read_(slab)?);
            self.data_cache.insert(slab, data.clone());
            Ok(data)
        }
    }

    fn reserve_slab(&mut self) -> (SlabIndex, SyncSender<SlabData>) {
        let index = self.pending_index;
        self.pending_index += 1;
        let tx = self.tx.as_ref().unwrap().clone();
        (index, tx)
    }

    pub fn write_slab(&mut self, data: &[u8]) -> Result<()> {
        let (index, tx) = self.reserve_slab();
        tx.send(SlabData {
            index,
            data: data.to_vec(),
        })?;
        Ok(())
    }

    pub fn index(&self) -> SlabIndex {
        self.pending_index
    }

    pub fn get_nr_slabs(&self) -> usize {
        let shared = self.shared.lock().unwrap();
        shared.offsets.offsets.len()
    }

    pub fn get_file_size(&self) -> u64 {
        let shared = self.shared.lock().unwrap();
        shared.file_size
    }

    pub fn hits(&self) -> u64 {
        self.data_cache.hits
    }

    pub fn misses(&self) -> u64 {
        self.data_cache.misses
    }
}

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

struct CompressionService {
    pool: ThreadPool,
}

fn compression_worker_(rx: Arc<Mutex<Receiver<SlabData>>>, tx: SyncSender<SlabData>) -> Result<()> {
    loop {
        let data = {
            let rx = rx.lock().unwrap();
            rx.recv()
        };

        if data.is_err() {
            break;
        }

        let data = data.unwrap();

        let mut packer = zstd::Encoder::new(Vec::new(), 0)?;
        packer.write_all(&data.data)?;
        tx.send(SlabData {
            index: data.index,
            data: packer.finish()?,
        })?;
    }

    Ok(())
}

fn compression_worker(rx: Arc<Mutex<Receiver<SlabData>>>, tx: SyncSender<SlabData>) {
    // FIXME: handle error
    compression_worker_(rx, tx).unwrap();
}

impl CompressionService {
    fn new(nr_threads: usize, tx: SyncSender<SlabData>) -> (Self, SyncSender<SlabData>) {
        let pool = ThreadPool::new(nr_threads);
        let (self_tx, rx) = sync_channel(nr_threads * 64);

        // we can only have a single receiver
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..nr_threads {
            let tx = tx.clone();
            let rx = rx.clone();
            pool.execute(move || compression_worker(rx, tx));
        }

        (Self { pool }, self_tx)
    }
}

//-----------------------------------------

pub fn repair<P: AsRef<Path>>(_p: P) -> Result<()> {
    todo!();
}

//------------------------------------------------
