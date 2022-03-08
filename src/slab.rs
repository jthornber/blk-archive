use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::{read, write::ZlibEncoder, Compression};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use threadpool::ThreadPool;

use crate::hash::*;

//------------------------------------------------

#[derive(Default)]
struct SlabOffsets {
    offsets: Vec<u64>,
}

impl SlabOffsets {
    /*
    #[allow(dead_code)]
    fn scan_slab_file(r: &mut File, mut offset: u64) -> Result<Self> {
        let file_len = r.metadata()?.len();
        let mut offsets = Vec::new();
        while offset < file_len {
            let magic = r.read_u64::<LittleEndian>()?;
            let len = r.read_u64::<LittleEndian>()?;
            assert_eq!(magic, SLAB_MAGIC);
            let mut csum: Hash64 = Hash64::default();
            r.read_exact(&mut csum[..])?;

            r.seek(SeekFrom::Current(len as i64))?;

            offsets.push(offset);
            offset += 8 + 8 + 8 + 4 + len;
        }

        Ok(Self { offsets })
    }
    */

    fn read_offset_file<P: AsRef<Path>>(p: P) -> Result<Self> {
        let mut r = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(p)
            .context("opening offset file")?;

        let len = r.metadata().context("offset metadata")?.len();
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
// header := <magic nr> <slab format version>
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
}

impl Drop for SlabFile {
    fn drop(&mut self) {
        let mut compressor = None;
        std::mem::swap(&mut compressor, &mut self.compressor);
        if let Some(c) = compressor {
            c.pool.join();
        }
    }
}

fn write_slab(shared: &Arc<Mutex<SlabShared>>, data: &[u8]) -> Result<()> {
    assert!(data.len() > 0);

    let mut shared = shared.lock().unwrap();

    let offset = shared.file_size;
    shared.offsets.offsets.push(offset);
    shared.file_size += 8 + 8 + 8 + data.len() as u64;

    shared.data.seek(SeekFrom::End(0))?;
    shared.data.write_u64::<LittleEndian>(SLAB_MAGIC)?;
    shared
        .data
        .write_u64::<LittleEndian>(data.len() as u64)?;
    let csum = hash_64(&data);
    shared.data.write_all(&csum)?;
    shared.data.write_all(&data)?;

    Ok(())
}

fn writer_(shared: Arc<Mutex<SlabShared>>, rx: Receiver<SlabData>) -> Result<()> {
    let mut write_index = 0;
    let mut queued: BTreeMap<SlabIndex, SlabData> = BTreeMap::new();

    loop {
        let buf = rx.recv();
        if buf.is_err() {
            // all send ends have been closed, so we're done.
            assert!(queued.len() == 0);
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

impl SlabFile {
    pub fn create<P: AsRef<Path>>(
        data_path: P,
        queue_depth: usize,
        compressed: bool,
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
            let (c, tx) = CompressionService::new(8, tx.clone());
            (Some(c), tx)
        } else {
            (None, tx)
        };

        let tid = {
            let shared = shared.clone();
            thread::spawn(move || writer(shared, rx))
        };

        Ok(Self {
            compressed,
            compressor,
            offsets_path,
            pending_index: 0,
            shared,
            tx: Some(tx),
            tid: Some(tid),
        })
    }

    pub fn open_for_write<P: AsRef<Path>>(data_path: P, queue_depth: usize) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(data_path)
            .context("open offsets")?;

        let magic = data
            .read_u64::<LittleEndian>()
            .context("couldn't read magic")?;
        let version = data
            .read_u32::<LittleEndian>()
            .context("couldn't read version")?;
        let flags = data
            .read_u32::<LittleEndian>()
            .context("couldn't read flags")?;

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        assert!(flags == 0 || flags == 1);

        let compressed = flags == 1;
        let (tx, rx) = sync_channel(queue_depth);
        let (compressor, tx) = if flags == 1 {
            let (c, tx) = CompressionService::new(4, tx.clone());
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
            thread::spawn(move || writer(shared, rx))
        };

        Ok(Self {
            compressed,
            compressor,
            offsets_path,
            pending_index: 0,
            shared,
            tx: Some(tx),
            tid: Some(tid),
        })
    }

    pub fn open_for_read<P: AsRef<Path>>(data_path: P) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(data_path)?;

        let magic = data.read_u64::<LittleEndian>()?;
        let version = data.read_u32::<LittleEndian>()?;
        let flags = data
            .read_u32::<LittleEndian>()
            .context("couldn't read flags")?;

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        assert!(flags == 0 || flags == 1);

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

    pub fn read(&mut self, slab: u64) -> Result<Vec<u8>> {
        let mut shared = self.shared.lock().unwrap();

        {
            let offset = shared.offsets.offsets[slab as usize];

            // FIXME: use read_exact_at
            shared.data.seek(SeekFrom::Start(offset))?;
        }

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
            let mut z = read::ZlibDecoder::new(&buf[..]);
            let mut buffer = Vec::new();
            z.read_to_end(&mut buffer)?;
            Ok(buffer)
        } else {
            Ok(buf)
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

        let mut packer = ZlibEncoder::new(Vec::new(), Compression::default());
        packer.write_all(&data.data)?;
        tx.send(SlabData {
            index: data.index,
            data: packer.reset(Vec::new())?,
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
