//! Request batcher - accumulates GetObject requests for batch processing

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::{debug, info};

use super::executor::BatchExecutor;
use super::types::PendingRequest;

/// Batches incoming requests and triggers batch execution
pub struct RequestBatcher {
    /// Channel to receive incoming requests
    request_tx: mpsc::Sender<PendingRequest>,
    /// Batch timeout in milliseconds
    batch_timeout_ms: u64,
    /// Maximum batch size
    max_batch_size: usize,
}

impl RequestBatcher {
    /// Create a new batcher and spawn its background task
    pub fn new(
        executor: Arc<BatchExecutor>,
        batch_timeout_ms: u64,
        max_batch_size: usize,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1024);

        // Spawn the background batching task
        tokio::spawn(Self::run_batcher(
            request_rx,
            executor,
            batch_timeout_ms,
            max_batch_size,
        ));

        Self {
            request_tx,
            batch_timeout_ms,
            max_batch_size,
        }
    }

    /// Submit a request to be batched
    pub async fn submit(&self, request: PendingRequest) -> Result<(), &'static str> {
        self.request_tx
            .send(request)
            .await
            .map_err(|_| "batcher channel closed")
    }

    /// Background task that collects and processes batches
    async fn run_batcher(
        mut request_rx: mpsc::Receiver<PendingRequest>,
        executor: Arc<BatchExecutor>,
        batch_timeout_ms: u64,
        max_batch_size: usize,
    ) {
        let timeout = Duration::from_millis(batch_timeout_ms);

        loop {
            let mut batch: Vec<PendingRequest> = Vec::with_capacity(max_batch_size);
            let batch_start = Instant::now();

            // Wait for the first request
            match request_rx.recv().await {
                Some(req) => batch.push(req),
                None => {
                    info!("Batcher channel closed, shutting down");
                    return;
                }
            }

            // Collect more requests until timeout or max size
            let remaining = timeout.saturating_sub(batch_start.elapsed());
            let deadline = tokio::time::Instant::now() + remaining;

            loop {
                if batch.len() >= max_batch_size {
                    debug!("Batch full ({} requests), executing", batch.len());
                    break;
                }

                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {
                        debug!("Batch timeout ({} requests), executing", batch.len());
                        break;
                    }
                    req = request_rx.recv() => {
                        match req {
                            Some(r) => batch.push(r),
                            None => {
                                info!("Batcher channel closed, processing final batch");
                                if !batch.is_empty() {
                                    executor.execute(batch).await;
                                }
                                return;
                            }
                        }
                    }
                }
            }

            // Execute the batch
            if !batch.is_empty() {
                let batch_size = batch.len();
                let exec = executor.clone();
                tokio::spawn(async move {
                    exec.execute(batch).await;
                    debug!("Batch of {} requests completed", batch_size);
                });
            }
        }
    }
}
