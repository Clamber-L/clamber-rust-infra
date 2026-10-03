//! Kafka 适配层。
//!
//! 只负责 Kafka 的收发与记录编解码，核心业务逻辑在 `kafka-delaymq-core`。
//! 见架构文档 §22。

pub mod consumer;
pub mod error;
pub mod producer;
pub mod record;

pub use consumer::{ConsumedMessage, ConsumerConfig, DelayConsumer, RecvError};
pub use error::KafkaError;
pub use producer::{KafkaSink, ProducerConfig};
pub use record::DelayRecord;
