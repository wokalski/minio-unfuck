//! Request batching for optimized HDD throughput
//!
//! Accumulates GetObject requests over a configurable timeout window,
//! then sorts reads by physical offset to minimize disk seeks.

mod batcher;
mod executor;
pub mod types;

pub use batcher::RequestBatcher;
pub use executor::BatchExecutor;
pub use types::{BatchError, PendingRequest, ShardReadPlan, ShardReadResult};
