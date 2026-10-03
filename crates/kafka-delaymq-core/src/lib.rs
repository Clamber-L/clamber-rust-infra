//! 延迟消息服务的领域模型与核心抽象。
//!
//! 本 crate 定义"是什么"，不涉及"用什么存"或"用什么传" —— 它既不依赖
//! `rocksdb` 也不依赖 `rdkafka`。storage 与 kafka 两个 crate 反向依赖
//! 这里，由 delay-server 在组装时注入具体实现。
//!
//! 时间语义集中在 `docs/time-concepts.md`，动手改时间相关代码前先读它。

pub mod clock;
pub mod memory_store;
pub mod message;
pub mod store;
pub mod task;

pub use clock::{Clock, wall_clock_ms};
pub use memory_store::{MemoryStore, MemoryStoreError};
pub use message::{DelayMessage, DelayStatus, StoredDelayMessage};
pub use store::DelayStore;
pub use task::TimerTask;
