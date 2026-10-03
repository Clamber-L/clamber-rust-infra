use serde::{Deserialize, Serialize};

/// 一条延迟消息。
///
/// 时间语义见 `docs/time-concepts.md`：`execute_at` 是整个系统唯一的
/// 业务真相，其余所有时间量都是为了高效判断"它到了吗"而存在的索引。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelayMessage {
    /// 全局唯一 ID。同时用作 RocksDB key、幂等 key 与日志追踪 ID。
    pub message_id: String,

    /// 投递目标：业务 Topic。
    pub target_topic: String,

    /// 来源坐标：delay-topic 的 partition。
    /// 仅用于 checkpoint 与问题追踪，**不**用于回读 payload。
    pub source_partition: i32,

    /// 来源坐标：delay-topic 的 offset。同上，不用于回读。
    pub source_offset: i64,

    /// 目标执行时刻，Unix 毫秒(wall clock)。
    pub execute_at: i64,

    /// 消息内容。自包含 —— 到期时直接投递，不回读 delay-topic。
    pub payload: Vec<u8>,

    /// 透传给业务 Topic 的 Kafka headers。
    pub headers: Vec<(String, String)>,
}

impl DelayMessage {
    /// 校验从外部收到的消息是否可用。
    ///
    /// `message_id` 由业务方生成(雪花 ID)，所以基本合法性必须在入口处
    /// 检查 —— 它随后要充当 RocksDB 主键、幂等键与 CAS 目标，一个空串
    /// 或非数字 ID 会一路带到存储层才暴露。
    pub fn validate(&self) -> Result<(), InvalidMessage> {
        if self.message_id.is_empty() {
            return Err(InvalidMessage::EmptyMessageId);
        }

        // 雪花 ID 恒为正：1 位符号位不用 + 41 位时间戳 + 10 位机器号
        // + 12 位序列号。解析失败或非正数说明上游生成器有问题。
        match self.message_id.parse::<i64>() {
            Ok(n) if n > 0 => {}
            _ => {
                return Err(InvalidMessage::MalformedMessageId {
                    message_id: self.message_id.clone(),
                });
            }
        }

        if self.target_topic.is_empty() {
            return Err(InvalidMessage::EmptyTargetTopic {
                message_id: self.message_id.clone(),
            });
        }

        Ok(())
    }
}

/// 入口校验失败的原因。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidMessage {
    #[error("message_id is empty")]
    EmptyMessageId,

    #[error("message_id {message_id:?} is not a positive snowflake id")]
    MalformedMessageId { message_id: String },

    #[error("message {message_id} has an empty target_topic")]
    EmptyTargetTopic { message_id: String },
}

/// 投递状态机。
///
/// `Pending -> Delivering` 的原子转换是防重复投递的主要防线：并发的
/// 第二个 timer task 会在 CAS 处失败并退出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DelayStatus {
    /// 等待到期。
    Pending,

    /// 已抢占投递权，正在发送。
    ///
    /// 进程在此状态与删除之间崩溃时，消息会永久停在这里而不再被任何
    /// timer task 触发 —— recovery 必须把超时的 `Delivering` 重置回
    /// `Pending`，这是 at-least-once 下重复投递的来源之一。
    Delivering,

    /// 投递成功但尚未删除。默认流程不会产生该状态(成功即删除)，
    /// 只在需要保留投递历史时使用。
    Delivered,

    /// 重试耗尽，已进 DLQ。
    Failed,

    /// 已取消。时间轮不做删除，到期时发现该状态直接跳过。
    Cancelled,
}

impl DelayStatus {
    /// 是否已到终态，不会再被调度。
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::Failed | Self::Cancelled)
    }
}

/// 持久化形态：业务消息 + 调度元数据。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredDelayMessage {
    pub message: DelayMessage,
    pub status: DelayStatus,
    pub retry_count: u32,

    /// 入库时刻，Unix 毫秒。
    pub created_at: i64,

    /// 最后一次状态变更时刻，Unix 毫秒。
    /// `Delivering` 的超时判定依赖它。
    pub updated_at: i64,
}

impl StoredDelayMessage {
    /// 新入库的消息，状态为 `Pending`。
    pub fn new_pending(message: DelayMessage, now_ms: i64) -> Self {
        Self {
            message,
            status: DelayStatus::Pending,
            retry_count: 0,
            created_at: now_ms,
            updated_at: now_ms,
        }
    }

    /// 该消息是否卡在 `Delivering` 超过 `timeout_ms`。
    pub fn is_stale_delivering(&self, now_ms: i64, timeout_ms: i64) -> bool {
        self.status == DelayStatus::Delivering && now_ms - self.updated_at > timeout_ms
    }
}
