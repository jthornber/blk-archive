use anyhow::Result;
use std::io::Write;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::slab::SlabData;

//-----------------------------------------

// First, define a trait for compression
pub trait Compressor: Sync + Send + 'static {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
}

// Implement the trait for zstd
pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level }
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = zstd::Encoder::new(Vec::new(), self.level)?;
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }
}

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
    threads: Option<Vec<thread::JoinHandle<()>>>,

    // Option so we can 'take' it and prevent two calls to shutdown.
    // We have one channel per worker thread so they don't have to do
    // any locking.
    shutdown_txs: Option<Vec<SyncSender<ShutdownMode>>>,

    // Channel for workers to report errors back to the main thread
    error_rx: Option<Receiver<anyhow::Error>>,

    // Store collected errors
    errors: Arc<Mutex<Vec<anyhow::Error>>>,
}

fn compression_worker_<C: Compressor>(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
    error_tx: SyncSender<anyhow::Error>,
    compressor: Arc<C>,
) -> Result<()> {
    let mut shutdown_mode = None;

    loop {
        // Check for shutdown signal (non-blocking)
        if shutdown_mode.is_none() {
            match shutdown_rx.try_recv() {
                Ok(mode) => {
                    shutdown_mode = Some(mode);
                    if matches!(shutdown_mode, Some(ShutdownMode::Immediate)) {
                        break;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    break;
                }
            }
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
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        };

        if let Some(data) = data {
            let compressed_data = match compressor.compress(&data.data) {
                Ok(data) => data,
                Err(e) => {
                    let _ = error_tx.send(e.into());
                    continue;
                }
            };

            if let Err(e) = tx.send(SlabData {
                index: data.index,
                data: compressed_data,
            }) {
                let _ = error_tx.send(e.into());
            }
        }
    }

    Ok(())
}

fn compression_worker<C: Compressor>(
    rx: Arc<Mutex<Receiver<SlabData>>>,
    tx: SyncSender<SlabData>,
    shutdown_rx: ShutdownRx,
    error_tx: SyncSender<anyhow::Error>,
    compressor: Arc<C>,
) {
    if let Err(e) = compression_worker_(rx, tx, shutdown_rx, error_tx.clone(), compressor) {
        let _ = error_tx.send(e);
    }
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
    pub fn new<C: Compressor>(
        nr_threads: usize,
        tx: SyncSender<SlabData>,
        compressor: C,
    ) -> (Self, SyncSender<SlabData>) {
        let mut threads = Vec::with_capacity(nr_threads);
        let (self_tx, rx) = sync_channel(nr_threads * 64);
        let mut shutdown_txs = Vec::with_capacity(nr_threads);
        let (error_tx, error_rx) = sync_channel(nr_threads * 2);

        // we can only have a single receiver
        let rx = Arc::new(Mutex::new(rx));
        let compressor = Arc::new(compressor);

        for _ in 0..nr_threads {
            let tx = tx.clone();
            let rx = rx.clone();
            let (shutdown_tx, shutdown_rx) = sync_channel(1);
            shutdown_txs.push(shutdown_tx);

            let worker_error_tx = error_tx.clone();
            let worker_compressor = compressor.clone();

            let tid = thread::spawn(move || {
                compression_worker(rx, tx, shutdown_rx, worker_error_tx, worker_compressor)
            });
            threads.push(tid);
        }

        (
            Self {
                threads: Some(threads),
                shutdown_txs: Some(shutdown_txs),
                error_rx: Some(error_rx),
                errors: Arc::new(Mutex::new(Vec::new())),
            },
            self_tx,
        )
    }

    fn collect_new_errors(&self) {
        if let Some(rx) = &self.error_rx {
            let mut errors = self.errors.lock().unwrap();
            while let Ok(err) = rx.try_recv() {
                errors.push(err);
            }
        }
    }

    /// Checks if any worker threads have reported errors
    ///
    /// Returns true if there are any pending errors
    pub fn has_errors(&self) -> bool {
        // First, collect any new errors from the channel
        self.collect_new_errors();

        // Then check if we have any stored errors
        let errors = self.errors.lock().unwrap();
        !errors.is_empty()
    }

    /// Collects all pending errors from worker threads
    pub fn collect_errors(&self) -> Vec<anyhow::Error> {
        // First collect any new errors
        self.collect_new_errors();

        // Then take all stored errors
        let mut errors = self.errors.lock().unwrap();
        std::mem::take(&mut *errors)
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
    pub fn join(&mut self) {
        // Default to graceful shutdown if not already shutting down
        self.shutdown(ShutdownMode::Graceful);

        // Join all worker threads
        if let Some(threads) = self.threads.take() {
            for tid in threads {
                let _ = tid.join();
            }
        }
    }
}

//-----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    struct MockCompressor;

    impl Compressor for MockCompressor {
        fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
            // Simple mock that just returns the original data
            // or you could return a predictable result
            Ok(data.to_vec())
        }
    }

    #[test]
    fn test_basic_compression_functionality() {
        // Create channels for input and output
        let (output_tx, output_rx) = sync_channel(100);

        // Create the compression service with a mock compressor
        let (mut service, input_tx) = CompressionService::new(
            2, // Use 2 worker threads
            output_tx,
            MockCompressor,
        );

        // Create test data
        let test_data = vec![
            SlabData {
                index: 1,
                data: vec![1, 2, 3, 4, 5],
            },
            SlabData {
                index: 2,
                data: vec![6, 7, 8, 9, 10],
            },
            SlabData {
                index: 3,
                data: vec![11, 12, 13, 14, 15],
            },
        ];

        // Send data to the compression service
        for data in &test_data {
            input_tx.send(data.clone()).expect("Failed to send data");
        }

        // Collect results
        let mut results = Vec::new();
        for _ in 0..test_data.len() {
            // Use a timeout to avoid hanging if the test fails
            match output_rx.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(data) => results.push(data),
                Err(e) => panic!("Failed to receive compressed data: {}", e),
            }
        }

        // Verify results (with MockCompressor, data should be unchanged)
        assert_eq!(results.len(), test_data.len());

        // Sort results by index to ensure consistent comparison
        results.sort_by_key(|d| d.index);

        for (original, compressed) in test_data.iter().zip(results.iter()) {
            assert_eq!(original.index, compressed.index);
            assert_eq!(original.data, compressed.data); // MockCompressor returns the same data
        }

        // Check for errors
        assert!(!service.has_errors());

        // Clean up
        service.join();
    }

    #[test]
    fn test_multiple_workers() {
        // Create channels with limited capacity to ensure work distribution
        let (output_tx, output_rx) = sync_channel(10);

        // Use more worker threads
        const NUM_WORKERS: usize = 4;
        const NUM_ITEMS: usize = 100;

        // Create a compressor that adds a small delay to simulate work
        struct SlowMockCompressor;
        impl Compressor for SlowMockCompressor {
            fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
                // Add a small delay to simulate compression work
                thread::sleep(std::time::Duration::from_millis(10));
                Ok(data.to_vec())
            }
        }

        // Create the service with multiple workers
        let (mut service, input_tx) =
            CompressionService::new(NUM_WORKERS, output_tx, SlowMockCompressor);

        // Create test data - more items than workers to ensure distribution
        let test_data: Vec<SlabData> = (0..NUM_ITEMS)
            .map(|i| SlabData {
                index: i as u64,
                data: vec![i as u8; 1000], // 1KB of data filled with the index value
            })
            .collect();

        // Record start time to measure parallel execution
        let start_time = std::time::Instant::now();

        // Send all data
        for data in &test_data {
            input_tx.send(data.clone()).expect("Failed to send data");
        }

        // Collect and verify results
        let mut received_indices = std::collections::HashSet::new();
        for _ in 0..NUM_ITEMS {
            match output_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                Ok(data) => {
                    // Verify data integrity
                    assert_eq!(data.data.len(), 1000);
                    assert!(data.data.iter().all(|&b| b == data.index as u8));

                    // Track which indices we've received
                    received_indices.insert(data.index);
                }
                Err(e) => panic!("Failed to receive data: {}", e),
            }
        }

        // Verify we received all expected indices
        assert_eq!(received_indices.len(), NUM_ITEMS);
        for i in 0..NUM_ITEMS {
            assert!(received_indices.contains(&(i as u64)));
        }

        // Measure elapsed time
        let elapsed = start_time.elapsed();

        // Calculate theoretical time for single-threaded execution
        // Each item takes ~10ms, so single-threaded would be ~NUM_ITEMS * 10ms
        let theoretical_single_thread = std::time::Duration::from_millis(NUM_ITEMS as u64 * 10);

        // With NUM_WORKERS, we expect to be significantly faster than single-threaded
        // Allow some overhead, but should be at least 2x faster with 4 workers
        assert!(
            elapsed < theoretical_single_thread / 2,
            "Expected parallel speedup not achieved: {:?} vs theoretical single-thread {:?}",
            elapsed,
            theoretical_single_thread
        );

        // Check for errors
        assert!(!service.has_errors());

        // Clean up
        service.join();
    }

    #[test]
    fn test_error_handling() {
        // Create channels
        let (output_tx, output_rx) = sync_channel(10);

        // Create a compressor that will fail on specific conditions
        struct ErrorCompressor {
            // Fail when compressing data with this index
            fail_on_indices: std::collections::HashSet<u64>,
        }

        impl Compressor for ErrorCompressor {
            fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
                // Extract the index from the first byte of data (for test purposes)
                if !data.is_empty() && self.fail_on_indices.contains(&(data[0] as u64)) {
                    return Err(anyhow::anyhow!(
                        "Simulated compression error on index {}",
                        data[0]
                    ));
                }
                Ok(data.to_vec())
            }
        }

        // Create the service with the error-generating compressor
        let fail_indices: std::collections::HashSet<u64> = [2, 5, 8].iter().cloned().collect();
        let (mut service, input_tx) = CompressionService::new(
            2,
            output_tx,
            ErrorCompressor {
                fail_on_indices: fail_indices.clone(),
            },
        );

        // Create test data - some will succeed, some will fail
        let test_data: Vec<SlabData> = (0..10)
            .map(|i| SlabData {
                index: i,
                data: vec![i as u8, 1, 2, 3, 4], // First byte is the index
            })
            .collect();

        // Send all data
        for data in &test_data {
            input_tx.send(data.clone()).expect("Failed to send data");
        }

        // Wait a bit for processing to complete
        thread::sleep(std::time::Duration::from_millis(500));

        // Collect successful results
        let mut successful_results = Vec::new();
        while let Ok(data) = output_rx.try_recv() {
            successful_results.push(data);
        }

        // Verify successful results
        let expected_success_count = test_data.len() - fail_indices.len();
        assert_eq!(successful_results.len(), expected_success_count);

        // Verify each successful result has an index not in the fail set
        for result in &successful_results {
            assert!(!fail_indices.contains(&result.index));
        }

        // Test has_errors() - should return true
        assert!(service.has_errors());

        // Test collect_errors() - should return all errors
        let errors = service.collect_errors();
        assert_eq!(errors.len(), fail_indices.len());

        // Verify each error message contains the expected text
        for error in &errors {
            let error_string = error.to_string();
            assert!(error_string.contains("Simulated compression error on index"));
        }

        // After collecting all errors, has_errors should return false
        assert!(!service.has_errors());

        // Send more data after collecting errors to ensure service still works
        let additional_data = SlabData {
            index: 20,
            data: vec![20, 1, 2, 3, 4],
        };
        input_tx
            .send(additional_data.clone())
            .expect("Failed to send additional data");

        // Verify we can still receive successful results
        match output_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(data) => assert_eq!(data.index, 20),
            Err(e) => panic!("Failed to receive additional data: {}", e),
        }

        // Clean up
        service.join();
    }

    #[test]
    fn test_graceful_shutdown() {
        // Create channels with a limited capacity to control the test flow
        let (output_tx, output_rx) = sync_channel(10);

        // Create a compressor that adds a delay to simulate work
        struct DelayCompressor;
        impl Compressor for DelayCompressor {
            fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
                // Add a delay to simulate compression work
                thread::sleep(std::time::Duration::from_millis(50));
                Ok(data.to_vec())
            }
        }

        // Create the service with 2 workers
        let (mut service, input_tx) = CompressionService::new(2, output_tx, DelayCompressor);

        // Number of items to test with
        const NUM_ITEMS: usize = 20;

        // Create and send test data
        let test_data: Vec<SlabData> = (0..NUM_ITEMS)
            .map(|i| SlabData {
                index: i as u64,
                data: vec![i as u8; 100],
            })
            .collect();

        for data in &test_data {
            input_tx.send(data.clone()).expect("Failed to send data");
        }

        // Initiate graceful shutdown before all items are processed
        // The delay in the compressor ensures some items are still in the queue
        thread::sleep(std::time::Duration::from_millis(100));
        service.shutdown(ShutdownMode::Graceful);

        // Drop the sender to close the channel
        drop(input_tx);

        // Collect all results
        let mut results = Vec::new();
        while results.len() < NUM_ITEMS {
            match output_rx.recv_timeout(std::time::Duration::from_secs(20)) {
                Ok(data) => results.push(data),
                Err(e) => {
                    panic!(
                        "Failed to receive all items during graceful shutdown. Got {}/{} items: {}",
                        results.len(),
                        NUM_ITEMS,
                        e
                    );
                }
            }
        }

        // Verify we received all items
        assert_eq!(results.len(), NUM_ITEMS);

        // Sort results by index for consistent comparison
        results.sort_by_key(|d| d.index);

        // Verify all data was processed correctly
        for i in 0..NUM_ITEMS {
            assert_eq!(results[i].index, i as u64);
            assert_eq!(results[i].data.len(), 100);
            assert_eq!(results[i].data[0], i as u8);
        }

        // Join the service - this should complete quickly since we already shut it down
        let join_start = std::time::Instant::now();
        service.join();
        let join_duration = join_start.elapsed();

        // Join should be quick since we already initiated shutdown
        assert!(
            join_duration < std::time::Duration::from_millis(500),
            "Join took too long after graceful shutdown: {:?}",
            join_duration
        );

        // Verify no errors occurred
        assert!(
            !service.has_errors(),
            "Unexpected errors after graceful shutdown"
        );
    }

    #[test]
    fn test_immediate_shutdown() {
        // Create channels with a limited capacity
        let (output_tx, output_rx) = sync_channel(100);

        // Create a compressor that adds a significant delay to simulate work
        struct SlowCompressor;
        impl Compressor for SlowCompressor {
            fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
                // Add a substantial delay to simulate slow compression
                thread::sleep(std::time::Duration::from_millis(200));
                Ok(data.to_vec())
            }
        }

        // Create the service with 2 workers
        let (mut service, input_tx) = CompressionService::new(2, output_tx, SlowCompressor);

        // Number of items to test with - more than can be processed quickly
        const NUM_ITEMS: usize = 20;

        // Create and send test data
        let test_data: Vec<SlabData> = (0..NUM_ITEMS)
            .map(|i| SlabData {
                index: i as u64,
                data: vec![i as u8; 100],
            })
            .collect();

        for data in &test_data {
            input_tx.send(data.clone()).expect("Failed to send data");
        }

        // Allow a few items to be processed
        thread::sleep(std::time::Duration::from_millis(500));

        // Count how many items were processed before shutdown
        let mut processed_before_shutdown = Vec::new();
        while let Ok(data) = output_rx.try_recv() {
            processed_before_shutdown.push(data);
        }

        println!(
            "Processed {} items before immediate shutdown",
            processed_before_shutdown.len()
        );

        // Initiate immediate shutdown
        let shutdown_time = std::time::Instant::now();
        service.shutdown(ShutdownMode::Immediate);

        // Join the service
        service.join();
        let shutdown_duration = shutdown_time.elapsed();

        // Immediate shutdown should be quick
        assert!(
            shutdown_duration < std::time::Duration::from_millis(500),
            "Immediate shutdown took too long: {:?}",
            shutdown_duration
        );

        // Try to receive any additional items that might have been processed
        let mut processed_after_shutdown = Vec::new();
        while let Ok(data) = output_rx.try_recv() {
            processed_after_shutdown.push(data);
        }

        println!(
            "Processed {} additional items after shutdown signal",
            processed_after_shutdown.len()
        );

        // Calculate total processed items
        let total_processed = processed_before_shutdown.len() + processed_after_shutdown.len();

        // With immediate shutdown, we expect that not all items were processed
        assert!(
            total_processed < NUM_ITEMS,
            "Expected fewer than {} items to be processed with immediate shutdown, but got {}",
            NUM_ITEMS,
            total_processed
        );

        // Verify no errors occurred during shutdown
        let errors = service.collect_errors();
        assert!(
            errors.is_empty(),
            "Unexpected errors during shutdown: {:?}",
            errors
        );

        // Verify the items that were processed have the correct data
        let all_processed: Vec<SlabData> = processed_before_shutdown
            .into_iter()
            .chain(processed_after_shutdown.into_iter())
            .collect();

        for processed in &all_processed {
            // Find the original data for this index
            let original = test_data
                .iter()
                .find(|d| d.index == processed.index)
                .expect("Processed item with unknown index");

            // Verify data matches (our mock compressor returns the same data)
            assert_eq!(processed.data, original.data);
        }
    }
}

//-----------------------------------------
