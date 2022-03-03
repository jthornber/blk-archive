use anyhow::{Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use flate2::read::ZlibDecoder;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{PathBuf, Path};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::hash::*;

//------------------------------------------------

#[derive(Default)]
struct SlabOffsets {
    offsets: Vec<u64>,
}

impl SlabOffsets {
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
            offset += 8 + 8 + 8 + len;
        }

        Ok(Self { offsets })
    }

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
            offsets.push(r.read_u64::<LittleEndian>().context("offsets read failed")?);
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

// FIXME: add index file
pub struct SlabFile {
    data: Arc<Mutex<File>>,
    offsets_path: PathBuf,
    pending_index: u64,
    offsets: Arc<Mutex<SlabOffsets>>,

    tx: Option<SyncSender<SlabData>>,
    tid: Option<thread::JoinHandle<()>>,
}

// FIXME: the writer doesn't update the slab_offsets
fn write_slab(data: &Arc<Mutex<File>>, offsets: &Arc<Mutex<SlabOffsets>>, buf: SlabData) -> Result<()> {
    let mut data = data.lock().unwrap();
    {
        let mut o = offsets.lock().unwrap();
        o.offsets.push(data.metadata()?.len());
    }

    data.write_u64::<LittleEndian>(SLAB_MAGIC)?;
    data.write_u64::<LittleEndian>(buf.data.len() as u64)?;
    let csum = hash_64(&vec![&buf.data[..]]);
    data.write_all(&csum)?;
    data.write_all(&buf.data[..])?;

    Ok(())
}

fn writer_(data: Arc<Mutex<File>>, offsets: Arc<Mutex<SlabOffsets>>, rx: Receiver<SlabData>) -> Result<()> {
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
            write_slab(&data, &offsets, buf)?;
            write_index += 1;

            while let Some(buf) = queued.remove(&write_index) {
                write_slab(&data, &offsets, buf)?;
                write_index += 1;
            }
        } else {
            queued.insert(buf.index, buf);
        }
    }

    assert!(queued.is_empty());
    Ok(())
}

fn writer(data: Arc<Mutex<File>>, offsets: Arc<Mutex<SlabOffsets>>, rx: Receiver<SlabData>) {
    // FIXME: pass on error
    writer_(data, offsets, rx).expect("write failed");
}

fn offsets_path<P: AsRef<Path>>(p: P) -> PathBuf {
    let mut offsets_path = PathBuf::new();
    offsets_path.push(p);
    offsets_path.set_extension("offsets");
    offsets_path
}

impl SlabFile {
    pub fn create<P: AsRef<Path>>(data_path: P, queue_depth: usize) -> Result<Self> {
        let offsets_path = offsets_path(&data_path);

        let mut data = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_path)?;

        let (tx, rx) = sync_channel(queue_depth);
        data.write_u64::<LittleEndian>(FILE_MAGIC)?;
        data.write_u32::<LittleEndian>(FORMAT_VERSION)?;

        let data = Arc::new(Mutex::new(data));
        let offsets = Arc::new(Mutex::new(SlabOffsets::default()));

        let tid = {
            let data = data.clone();
            let offsets = offsets.clone();
            thread::spawn(move || writer(data, offsets, rx))
        };

        Ok(Self {
            data,
            offsets_path,
            pending_index: 0,
            offsets: offsets,
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

        let magic = data.read_u64::<LittleEndian>().context("couldn't read magic")?;
        let version = data.read_u32::<LittleEndian>().context("couldn't read version")?;

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        let (tx, rx) = sync_channel(queue_depth);
        let offsets = Arc::new(Mutex::new(SlabOffsets::read_offset_file(&offsets_path)?));
        let data = Arc::new(Mutex::new(data));

        let tid = {
            let data = data.clone();
            let offsets = offsets.clone();
            thread::spawn(move || writer(data, offsets, rx))
        };

        Ok(Self {
            data,
            offsets_path,
            pending_index: 0,
            offsets,
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

        assert_eq!(magic, FILE_MAGIC); // FIXME: better error
        assert_eq!(version, FORMAT_VERSION);

        let offsets = Arc::new(Mutex::new(SlabOffsets::read_offset_file(&offsets_path)?));
        let data = Arc::new(Mutex::new(data));

        Ok(Self {
            data,
            offsets_path,
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

        let offsets = self.offsets.lock().unwrap();
        offsets.write_offset_file(&self.offsets_path)?;
        Ok(())
    }

    pub fn read(&mut self, slab: u64) -> Result<Vec<u8>> {
        let mut data = self.data.lock().unwrap();

	{
    	    let offsets = self.offsets.lock().unwrap();
            let offset = offsets.offsets[slab as usize];
            data.seek(SeekFrom::Start(offset))?;
	}

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
        let offsets = self.offsets.lock().unwrap();
        offsets.offsets.len()
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
