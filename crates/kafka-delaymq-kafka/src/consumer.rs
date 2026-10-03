//! delay-topic 消费端。
//!
//! **第一版用静态 assign，不走 consumer group 的自动 rebalance。**
//! 每个节点持有本地 RocksDB，而 rebalance 后新节点读不到旧节点磁盘上
//! 的数据 —— offset 已 commit，Kafka 也不会重投，消息就永久丢了。
//! 详见架构文档 §20。

use std::time::Duration;

use kafka_delaymq_core::{DelayMessage, InvalidMessage};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::error::KafkaError;
use crate::record::DelayRecord;

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub brokers: String,
    pub group_id: String,
    pub delay_topic: String,

    /// 本节点负责的 partition。静态指定，不参与 rebalance。
    pub partitions: Vec<i32>,

    pub poll_timeout: Duration,
}

impl ConsumerConfig {
    pub fn new(
        brokers: impl Into<String>,
        group_id: impl Into<String>,
        delay_topic: impl Into<String>,
        partitions: Vec<i32>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            delay_topic: delay_topic.into(),
            partitions,
            poll_timeout: Duration::from_secs(1),
        }
    }
}

/// 从 delay-topic 读出的一条消息及其 offset。
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    pub message: DelayMessage,
    pub partition: i32,
    pub offset: i64,
}

/// 消费端。
pub struct DelayConsumer {
    consumer: StreamConsumer,
    poll_timeout: Duration,
    topic: String,
}

impl DelayConsumer {
    pub fn new(config: &ConsumerConfig) -> Result<Self, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            // offset 由我们按"已全部投递完成"的水位手动提交，
            // 自动提交会在消息入轮前就推进水位。见架构文档 §10.2。
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        // 静态 assign：不调 subscribe，因此不参与 rebalance。
        let mut tpl = TopicPartitionList::new();
        for &p in &config.partitions {
            tpl.add_partition_offset(&config.delay_topic, p, Offset::Stored)?;
        }
        consumer.assign(&tpl)?;

        Ok(Self {
            consumer,
            poll_timeout: config.poll_timeout,
            topic: config.delay_topic.clone(),
        })
    }

    /// 取下一条消息。超时返回 `Ok(None)`。
    ///
    /// 非法消息(ID 为空、非雪花格式等)返回 `Err(RecvError::Invalid)` ——
    /// 调用方应当记录并跳过，而不是停止消费：一条坏消息不该堵住整个
    /// partition。
    pub async fn recv(&self) -> Result<Option<ConsumedMessage>, RecvError> {
        let msg = match tokio::time::timeout(self.poll_timeout, self.consumer.recv()).await {
            Err(_) => return Ok(None), // poll 超时，无消息
            Ok(result) => result.map_err(KafkaError::Rdkafka)?,
        };

        let partition = msg.partition();
        let offset = msg.offset();

        let payload = msg.payload().ok_or(KafkaError::EmptyRecord)?;
        let record: DelayRecord =
            bincode::deserialize(payload).map_err(|e| RecvError::Malformed {
                partition,
                offset,
                source: KafkaError::Codec(e),
            })?;

        let message = record
            .into_message(partition, offset)
            .map_err(|source| RecvError::Invalid {
                partition,
                offset,
                source,
            })?;

        Ok(Some(ConsumedMessage {
            message,
            partition,
            offset,
        }))
    }

    /// 提交水位。
    ///
    /// **语义是"该 offset 之前的消息全部已投递完成"**，而不是"已消费"。
    /// 自动提交或消费即提交会让未投递的消息在崩溃后无法重放 ——
    /// 本地 RocksDB 丢了就真丢了。见架构文档 §20.2 方案 C。
    pub fn commit_watermark(&self, partition: i32, offset: i64) -> Result<(), KafkaError> {
        let mut tpl = TopicPartitionList::new();
        // Kafka 的 commit 语义是"下一条待读",所以要 +1
        tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset + 1))?;
        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Sync)?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    #[error(transparent)]
    Kafka(#[from] KafkaError),

    #[error("malformed record at {partition}/{offset}: {source}")]
    Malformed {
        partition: i32,
        offset: i64,
        source: KafkaError,
    },

    #[error("invalid message at {partition}/{offset}: {source}")]
    Invalid {
        partition: i32,
        offset: i64,
        source: InvalidMessage,
    },
}

impl RecvError {
    /// 该错误是否可以跳过继续消费。
    ///
    /// 格式错误与非法消息都是单条消息的问题，跳过即可；Kafka 本身的
    /// 错误(连接断开等)需要上层处理。
    pub fn is_skippable(&self) -> bool {
        matches!(self, Self::Malformed { .. } | Self::Invalid { .. })
    }
}
