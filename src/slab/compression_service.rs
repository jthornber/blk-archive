use anyhow::Result;
use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

use crate::slab::SlabData;

//-----------------------------------------

/// Represents different ways the compression service can be shut down
#[derive(Clone, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Process all queued items before shutting down
    Graceful,
    /// Stop as soon as possible, abandoning queued work
    Immediate,
}

type ShutdownRx = Receiver<ShutdownMode>;

/// A service that compresses SlabData using multiple worker threads
///
/// The service maintains a thread pool where each thread:
/// 1. Receives SlabData from an input channel
/// 2. Compresses the data using zstd
/// 3. Sends the compressed data to an output channel
pub struct CompressionService {
    pool: ThreadPool,

    // Option so we can 'take' it and prevent two calls to shutdown.
    // We have one channel per worker thread so they don't have to do
    // any locking.
    shutdown_txs: Option<Vec<SyncSender<ShutdownMode>>>,
}

fn compression_worker_(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
) -> Result<()> {
    let mut shutdown_mode = None;

    loop {
        // Check for shutdown signal (non-blocking)
        match shutdown_rx.try_recv() {
            Ok(mode) => {
                shutdown_mode = Some(mode);
                if matches!(shutdown_mode, Some(ShutdownMode::Immediate)) {
                    break;
                }
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {}
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }

        // Try to receive data with timeout
        let data = {
            let rx = rx.lock().unwrap();
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(data) => Some(data),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // If we're in graceful shutdown and no data is available, we can exit
                    if matches!(shutdown_mode, Some(ShutdownMode::Graceful)) {
                        break;
                    }
                    continue;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        };

        if let Some(data) = data {
            let mut packer = zstd::Encoder::new(Vec::new(), 0)?;
            packer.write_all(&data.data)?;
            tx.send(SlabData {
                index: data.index,
                data: packer.finish()?,
            })?;
        }
    }

    Ok(())
}

fn compression_worker(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
) {
    // FIXME: handle error
    compression_worker_(rx, tx, shutdown_rx).unwrap();
}

impl CompressionService {
    /// Creates a new compression service with the specified number of worker threads
    ///
    /// # Arguments
    ///
    /// * `nr_threads` - Number of compression worker threads to spawn
    /// * `tx` - Channel to send compressed data to
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * The compression service
    /// * A sender that can be used to submit data for compression
    pub fn new(nr_threads: usize, tx: SyncSender<SlabData>) -> (Self, SyncSender<SlabData>) {
        let pool = ThreadPool::new(nr_threads);
        let (self_tx, rx) = sync_channel(nr_threads * 64);
        let mut shutdown_txs = Vec::with_capacity(nr_threads);

        // we can only have a single receiver
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..nr_threads {
            let tx = tx.clone();
            let rx = rx.clone();
            let (shutdown_tx, shutdown_rx) = sync_channel(1);
            shutdown_txs.push(shutdown_tx);
            pool.execute(move || compression_worker(rx, tx, shutdown_rx));
        }

        (
            Self {
                pool,
                shutdown_txs: Some(shutdown_txs),
            },
            self_tx,
        )
    }

    /// Initiates shutdown of the compression service
    ///
    /// # Arguments
    ///
    /// * `mode` - Controls whether to process remaining items or abandon them
    pub fn shutdown(&mut self, mode: ShutdownMode) {
        if let Some(shutdown_txs) = self.shutdown_txs.take() {
            // Send shutdown signal to all workers
            for tx in shutdown_txs {
                let _ = tx.send(mode.clone());
            }
        }
    }

    /// Shuts down the service (gracefully by default) and waits for all workers to complete
    ///
    /// This method consumes the service, ensuring it cannot be used after joining.
    pub fn join(mut self) {
        // Default to graceful shutdown if not already shutting down
        self.shutdown(ShutdownMode::Graceful);

        self.pool.join();
    }
}

//-----------------------------------------
