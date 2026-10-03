//! delay-topic 上的线格式。
//!
//! 与 [`DelayMessage`] 分开定义：前者是跨进程契约，改动要考虑兼容性；
//! 后者是内部领域模型，可以自由重构。

use kafka_delaymq_core::{DelayMessage, InvalidMessage};
use serde::{Deserialize, Serialize};

/// 业务方投向 delay-topic 的消息格式。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelayRecord {
    /// 雪花 ID，由业务方生成。见架构文档 §15。
    pub message_id: String,

    /// 到期后投向哪个业务 topic。
    pub target_topic: String,

    /// 目标执行时刻，Unix 毫秒。
    pub execute_at: i64,

    /// 业务消息体。
    #[serde(with = "serde_bytes_compat")]
    pub payload: Vec<u8>,

    /// 透传给业务 topic 的 headers。
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

impl DelayRecord {
    /// 转为领域模型，并带上来源坐标。
    ///
    /// 坐标仅用于 checkpoint 与问题追踪，不用于回读 payload ——
    /// payload 自包含，见架构文档 §5。
    pub fn into_message(
        self,
        source_partition: i32,
        source_offset: i64,
    ) -> Result<DelayMessage, InvalidMessage> {
        let message = DelayMessage {
            message_id: self.message_id,
            target_topic: self.target_topic,
            source_partition,
            source_offset,
            execute_at: self.execute_at,
            payload: self.payload,
            headers: self.headers,
        };
        message.validate()?;
        Ok(message)
    }
}

/// bincode 不需要特殊处理，但保留这层让将来换 JSON 时 payload
/// 能用 base64 而不破坏格式。
mod serde_bytes_compat {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], ser: S) -> Result<S::Ok, S::Error> {
        bytes.serialize(ser)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
        Vec::<u8>::deserialize(de)
    }
}
