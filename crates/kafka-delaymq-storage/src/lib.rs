//! `DelayStore` 的 RocksDB 实现。
//!
//! 两个 column family：主数据按 `message_id` 存，时间索引按
//! `execute_at` 排序 —— 后者是 §4.2 预加载与 §17 recovery 的前提。
//! 两者的写入必须同批落盘，见 `RocksDbStore::put`。

pub mod codec;
pub mod error;
pub mod rocks_store;

pub use error::StorageError;
pub use rocks_store::{CF_BY_EXECUTE_AT, CF_MESSAGE, RocksDbStore};
