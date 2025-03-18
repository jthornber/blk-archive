use anyhow::Result;
use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::slab::SlabData;

//-----------------------------------------

pub struct CompressionService {
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
    pub fn new(nr_threads: usize, tx: SyncSender<SlabData>) -> (Self, SyncSender<SlabData>) {
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

    pub fn join(self) {
        self.pool.join();
    }
}

//-----------------------------------------
