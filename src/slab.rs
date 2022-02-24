use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
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
    pending_index: u64,

    tx: Option<SyncSender<SlabData>>,
    tid: thread::JoinHandle<()>,
}

fn write_slab(data: &mut File, buf: SlabData) -> Result<()> {
    data.write_u64::<LittleEndian>(SLAB_MAGIC)?;
    data.write_u64::<LittleEndian>(buf.data.len() as u64)?;
    let csum = hash_64(&vec![&buf.data[..]]);
    data.write_all(&csum)?;
    data.write_all(&buf.data[..])?;
    Ok(())
}

fn writer_(mut data: File, rx: Receiver<SlabData>) -> Result<()> {
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
            write_slab(&mut data, buf)?;
            write_index += 1;

            while let Some(buf) = queued.remove(&write_index) {
                write_slab(&mut data, buf)?;
                write_index += 1;
            }
        } else {
            queued.insert(buf.index, buf);
        }
    }

    assert!(queued.len() == 0);
    Ok(())
}

fn writer(data: File, rx: Receiver<SlabData>) {
    // FIXME: pass on error
    writer_(data, rx).expect("write failed");
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

        let tid = thread::spawn(move || writer(data, rx));

        Ok(Self {
            pending_index: 0,
            tx: Some(tx),
            tid,
        })
    }

    pub fn open<P: AsRef<Path>>(_p: P, _writeable: bool) -> Self {
        todo!();
    }

    pub fn close(mut self) -> Result<()> {
        self.tx = None;
        self.tid.join().expect("join failed");
        Ok(())
    }

    pub fn read(&self, _slab: u64) -> Result<Vec<u8>> {
        todo!();
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
}

pub fn repair<P: AsRef<Path>>(_p: P) -> Result<()> {
    todo!();
}

//------------------------------------------------
