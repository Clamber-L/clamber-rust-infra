use std::path::PathBuf;
use std::time::Duration;

use kafka_delaymq_core::SchedulerConfig;
use kafka_delaymq_kafka::{ConsumerConfig, ProducerConfig};

/// 服务配置。
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub brokers: String,
    pub group_id: String,
    pub delay_topic: String,

    /// 本节点负责的 partition。静态指定 —— 本地 RocksDB 与 consumer
    /// group 的自动 rebalance 不兼容，见架构文档 §20。
    pub partitions: Vec<i32>,

    /// 死信 topic。为空则投递耗尽重试后只标记 Failed，不转发。
    pub dead_letter_topic: Option<String>,

    pub data_dir: PathBuf,
    pub scheduler: SchedulerConfig,

    /// 时间轮推进周期。应当与 `scheduler.tick_ms` 同阶。
    pub tick_interval: Duration,

    pub delivery_timeout: Duration,

    /// 关停时等待 producer 刷盘的上限。
    pub shutdown_flush: Duration,
}

impl ServerConfig {
    pub fn new(
        brokers: impl Into<String>,
        delay_topic: impl Into<String>,
        partitions: Vec<i32>,
        data_dir: impl Into<PathBuf>,
    ) -> Self {
        let scheduler = SchedulerConfig::default();
        Self {
            brokers: brokers.into(),
            group_id: "delay-server".to_string(),
            delay_topic: delay_topic.into(),
            partitions,
            dead_letter_topic: None,
            data_dir: data_dir.into(),
            tick_interval: Duration::from_millis(scheduler.tick_ms),
            scheduler,
            delivery_timeout: Duration::from_secs(30),
            shutdown_flush: Duration::from_secs(10),
        }
    }

    pub fn consumer(&self) -> ConsumerConfig {
        ConsumerConfig::new(
            &self.brokers,
            &self.group_id,
            &self.delay_topic,
            self.partitions.clone(),
        )
    }

    pub fn producer(&self) -> ProducerConfig {
        let mut config = ProducerConfig::new(&self.brokers);
        config.delivery_timeout = self.delivery_timeout;
        config
    }
}
