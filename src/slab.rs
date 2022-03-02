use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::{read::ZlibDecoder};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::hash::*;

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

// FIXME: add index file
pub struct SlabFile {
    data: Arc<Mutex<File>>,
    pending_index: u64,
    offsets: Vec<u64>,

    tx: Option<SyncSender<SlabData>>,
    tid: Option<thread::JoinHandle<()>>,
}

// FIXME: the writer doesn't update the slab_offsets
fn write_slab(data: &Arc<Mutex<File>>, buf: SlabData) -> Result<()> {
    let mut data = data.lock().unwrap();
    data.write_u64::<LittleEndian>(SLAB_MAGIC)?;
    data.write_u64::<LittleEndian>(buf.data.len() as u64)?;
    let csum = hash_64(&vec![&buf.data[..]]);
    data.write_all(&csum)?;
    data.write_all(&buf.data[..])?;
    Ok(())
}

fn writer_(data: Arc<Mutex<File>>, rx: Receiver<SlabData>) -> Result<()> {
    let mut write_index = 0;
    let mut queued = BTreeMap::new();

    loop {
        let buf = rx.recv();
        if buf.is_err() {
            // all send ends have been closed, so we're done.
            break;
        }

        let buf = buf.unwrap();
        if buf.index == write_index {
            write_slab(&data, buf)?;
            write_index += 1;

            while let Some(buf) = queued.remove(&write_index) {
                write_slab(&data, buf)?;
                write_index += 1;
            }
        } else {
            queued.insert(buf.index, buf);
        }
    }

    assert!(queued.is_empty());
    Ok(())
}

fn writer(data: Arc<Mutex<File>>, rx: Receiver<SlabData>) {
    // FIXME: pass on error
    writer_(data, rx).expect("write failed");
}

fn read_slab_offsets(r: &mut File, mut offset: u64) -> Result<Vec<u64>> {
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
        offset += 8 + 8 + 8 + len;
    }

    Ok(offsets)
}

impl SlabFile {
    pub fn create<P: AsRef<Path>>(p: P, queue_depth: usize) -> Result<Self> {
        let mut data = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(true)
            .open(p)?;
        let (tx, rx) = sync_channel(queue_depth);

        data.write_u64::<LittleEndian>(FILE_MAGIC)?;
        data.write_u32::<LittleEndian>(FORMAT_VERSION)?;

        let data = Arc::new(Mutex::new(data));

        let tid = {
            let data = data.clone();
            thread::spawn(move || writer(data, rx))
        };

        Ok(Self {
            data,
            pending_index: 0,
            offsets: Vec::new(),
            tx: Some(tx),
            tid: Some(tid),
        })
    }

    pub fn open_for_write<P: AsRef<Path>>(p: P, queue_depth: usize) -> Result<Self> {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(p)?;

        let magic = data.read_u64::<LittleEndian>()?;
        let version = data.read_u32::<LittleEndian>()?;

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        let (tx, rx) = sync_channel(queue_depth);
        let offsets = read_slab_offsets(&mut data, 8 + 4)?;
        let data = Arc::new(Mutex::new(data));

        let tid = {
            let data = data.clone();
            thread::spawn(move || writer(data, rx))
        };

        Ok(Self {
            data,
            pending_index: 0,
            offsets,
            tx: Some(tx),
            tid: Some(tid),
        })
    }

    pub fn open_for_read<P: AsRef<Path>>(p: P) -> Result<Self> {
        let mut data = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(p)?;

        let magic = data.read_u64::<LittleEndian>()?;
        let version = data.read_u32::<LittleEndian>()?;

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        let offsets = read_slab_offsets(&mut data, 8 + 4)?;

        let data = Arc::new(Mutex::new(data));

        Ok(Self {
            data,
            pending_index: 0,
            offsets,
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
        Ok(())
    }

    pub fn read(&mut self, slab: u64) -> Result<Vec<u8>> {
        let mut data = self.data.lock().unwrap();

        let offset = self.offsets[slab as usize];
        data.seek(SeekFrom::Start(offset))?;

        let magic = data.read_u64::<LittleEndian>()?;
        let len = data.read_u64::<LittleEndian>()?;
        assert_eq!(magic, SLAB_MAGIC);

        let mut expected_csum: Hash64 = Hash64::default();
        data.read_exact(&mut expected_csum)?;

        let mut buf = vec![0; len as usize];
        data.read_exact(&mut buf)?;

        let actual_csum = hash_64(&vec![&buf]);
        assert_eq!(actual_csum, expected_csum);

        let mut z = ZlibDecoder::new(&buf[..]);
        let mut buffer = Vec::new();
        z.read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    pub fn reserve_slab(&mut self) -> (SlabIndex, SyncSender<SlabData>) {
        let index = self.pending_index;
        self.pending_index += 1;
        let tx = self.tx.as_ref().unwrap().clone();
        (index, tx)
    }

    pub fn index(&self) -> SlabIndex {
        self.pending_index
    }

    pub fn get_nr_slabs(&self) -> usize {
        self.offsets.len()
    }

    pub fn get_file_size(&self) -> Result<u64> {
        let mut data = self.data.lock().unwrap();
        data.flush()?;
        Ok(data.metadata()?.len())
    }
}

pub fn repair<P: AsRef<Path>>(_p: P) -> Result<()> {
    todo!();
}

//------------------------------------------------
