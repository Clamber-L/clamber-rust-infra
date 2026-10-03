use crate::message::{DelayMessage, DelayStatus, StoredDelayMessage};
use crate::task::TimerTask;

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
    fn put(&self, message: &DelayMessage, now_ms: i64) -> Result<(), Self::Error>;

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
