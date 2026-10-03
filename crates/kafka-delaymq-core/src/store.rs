use crate::message::{DelayMessage, DelayStatus, StoredDelayMessage};
use crate::task::TimerTask;

/// `DelayStore::put` 的结果。
///
/// 存在三种而非两种情形，因为 `message_id` 由业务方生成 —— 同一个 ID
/// 再次到达既可能是 Kafka 正常重投，也可能是生成方 worker_id 配重。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// 新消息，首次写入。
    Inserted,

    /// 同 ID 且 payload 相同 —— Kafka 重投或改期，已覆盖。
    /// 改期时索引已同步重建。
    Replaced,

    /// 同 ID 但 payload 不同 —— 判定为 ID 撞号，**未写入**。
    ///
    /// 调用方应当告警而非重试：这是上游 ID 生成器的配置问题，
    /// 重试不会让它变好，而静默覆盖会丢掉已存在的那条消息。
    Rejected,
}

impl PutOutcome {
    /// 消息是否已落库(撞号被拒时为 false)。
    pub fn is_stored(self) -> bool {
        matches!(self, Self::Inserted | Self::Replaced)
    }
}

/// 持久化抽象。
///
/// **定义在 core 而非 storage** —— 抽象属于使用方，不属于实现方(依赖
/// 倒置)。core 使用 `DelayStore`，storage 提供实现并反向依赖 core，
/// 由 delay-server 在组装时注入。若 trait 定义在 storage，则 core 必须
/// 依赖 storage，`rocksdb` 就会被拖进 core 的依赖树，抽象随之失效。
///
/// **签名是同步的。** RocksDB 的所有调用都是阻塞的，套一层 `async fn`
/// 只会骗人 —— 在 tokio worker 上直接调用会占住线程，高吞吐下足以让
/// consumer 与 tick 推进一起停摆。真正的隔离在调用侧(`spawn_blocking`
/// 或专用线程池)，不在 trait 签名上。
///
/// 同步签名顺带让 `dyn DelayStore` 可用 —— async trait(AFIT)产生的
/// trait 不能直接 `dyn`，而多实现(RocksDbStore / MemoryStore /
/// PostgresStore)通常需要它。
pub trait DelayStore: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// 写入消息。必须同时更新主数据与 `by_execute_at` 索引，
    /// 且两者在同一个原子批次内 —— 否则崩溃后会出现索引与数据不一致。
    ///
    /// `message_id` 由业务方生成(雪花 ID)，唯一性不由本服务保证，所以
    /// 实现必须区分"正常重投"与"ID 撞号"：payload 相同视为重投，直接
    /// 覆盖；payload 不同说明是两条不同的消息用了同一个 ID，必须拒绝，
    /// 否则静默覆盖等于丢消息。判定依据见 `PutOutcome`。
    fn put(&self, message: &DelayMessage, now_ms: i64) -> Result<PutOutcome, Self::Error>;

    fn get(&self, message_id: &str) -> Result<Option<StoredDelayMessage>, Self::Error>;

    /// 删除消息。同样要同时删主数据与索引，同一批次。
    fn delete(&self, message_id: &str) -> Result<(), Self::Error>;

    /// 按 `execute_at` 范围扫描待到期任务，走时间索引。
    ///
    /// 返回轻量 `TimerTask` 而非完整消息 —— 调用方(recovery 与
    /// preloader)只需要知道"谁该入轮"，不需要 payload。
    ///
    /// `limit` 是硬上限：全量加载在亿级存量下必然 OOM。
    fn scan_due(
        &self,
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error>;

    /// 原子状态转换。返回 `false` 表示当前状态不等于 `expect`。
    ///
    /// 这是防重复投递的主要防线，见 `DelayStatus::Delivering`。
    fn compare_and_swap_status(
        &self,
        message_id: &str,
        expect: DelayStatus,
        next: DelayStatus,
    ) -> Result<bool, Self::Error>;

    fn set_status(&self, message_id: &str, status: DelayStatus) -> Result<(), Self::Error>;

    fn mark_retry(&self, message_id: &str, retry_count: u32) -> Result<(), Self::Error>;

    /// 找出卡在 `Delivering` 超过 `timeout_ms` 的消息，供 recovery 重置。
    fn scan_stale_delivering(
        &self,
        now_ms: i64,
        timeout_ms: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error>;
}
