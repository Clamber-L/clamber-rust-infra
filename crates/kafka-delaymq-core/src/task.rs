use crate::message::DelayMessage;

/// 时间轮里的轻量任务索引。
///
/// 刻意只保留两个字段：`target_topic` / `source_partition` 等到期后都能
/// 从 RocksDB 读到，放进这里只会按存量成倍放大内存占用。
///
/// 估算(见 `docs/time-concepts.md` 与架构文档 §4.2)：
/// ```text
/// message_id  String  24 字节(本体) + 26 字节(ULID 堆数据)
/// execute_at  i64      8 字节
///                     ≈ 60 字节 + slot 容器开销
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerTask {
    /// 指回 RocksDB 的 key。
    pub message_id: String,

    /// 到期判断依据，Unix 毫秒(wall clock)。
    ///
    /// 时间轮内部把它换算成 `(slot, rounds)`，但到期时仍要用这个原值
    /// 做二次校验 —— 时间轮的 tick 由单调时钟推进，与 wall clock 会漂移。
    pub execute_at: i64,
}

impl TimerTask {
    pub fn new(message_id: String, execute_at: i64) -> Self {
        Self {
            message_id,
            execute_at,
        }
    }
}

impl From<&DelayMessage> for TimerTask {
    fn from(message: &DelayMessage) -> Self {
        Self {
            message_id: message.message_id.clone(),
            execute_at: message.execute_at,
        }
    }
}
