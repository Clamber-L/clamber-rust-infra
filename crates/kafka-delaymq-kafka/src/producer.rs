//! Kafka 投递端：实现 core 的 [`MessageSink`]。

use std::time::Duration;

use kafka_delaymq_core::{DelayMessage, MessageSink};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;

use crate::error::KafkaError;

/// 投递端配置。
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub brokers: String,

    /// 等待 broker ack 的上限。
    pub delivery_timeout: Duration,

    /// 需要多少副本确认。默认 `all` —— 延迟消息不能因为单个 broker
    /// 故障就丢。
    pub acks: String,
}

impl ProducerConfig {
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            delivery_timeout: Duration::from_secs(30),
            acks: "all".to_string(),
        }
    }
}

/// 把到期消息投向业务 topic。
pub struct KafkaSink {
    producer: FutureProducer,
    delivery_timeout: Duration,
    /// 覆盖消息自带的 `target_topic`。死信队列用这个固定到 DLQ topic。
    override_topic: Option<String>,
}

impl KafkaSink {
    pub fn new(config: &ProducerConfig) -> Result<Self, KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("acks", &config.acks)
            // 幂等 producer：broker 侧去重，避免 retry 造成的重复
            .set("enable.idempotence", "true")
            .set(
                "message.timeout.ms",
                config.delivery_timeout.as_millis().to_string(),
            )
            .create()?;

        Ok(Self {
            producer,
            delivery_timeout: config.delivery_timeout,
            override_topic: None,
        })
    }

    /// 固定投向某个 topic，忽略消息自带的 `target_topic`。用于死信队列。
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.override_topic = Some(topic.into());
        self
    }

    /// 刷出所有待发消息。关停前调用，避免丢掉本地队列里的数据。
    pub fn flush(&self, timeout: Duration) -> Result<(), KafkaError> {
        self.producer.flush(Timeout::After(timeout))?;
        Ok(())
    }
}

impl MessageSink for KafkaSink {
    type Error = KafkaError;

    async fn send(&self, message: &DelayMessage) -> Result<(), Self::Error> {
        let topic = self
            .override_topic
            .as_deref()
            .unwrap_or(&message.target_topic);

        let mut headers = rdkafka::message::OwnedHeaders::new();
        for (k, v) in &message.headers {
            headers = headers.insert(rdkafka::message::Header {
                key: k,
                value: Some(v.as_bytes()),
            });
        }

        let record = FutureRecord::to(topic)
            // message_id 作 Kafka key：同一消息重投落到同一分区。
            // 注意这会按 hash(message_id) 打散，下游若依赖业务实体内
            // 有序，需要改用业务键，见架构文档 §15。
            .key(&message.message_id)
            .payload(&message.payload)
            .headers(headers);

        // **必须等到 broker ack**。仅放进本地队列就返回，会让上层误判
        // 投递成功并删除持久化记录，之后发送失败消息就永久丢了。
        // 见架构文档 §13。
        self.producer
            .send(record, Timeout::After(self.delivery_timeout))
            .await
            .map_err(|(err, _)| KafkaError::Rdkafka(err))?;

        Ok(())
    }
}
