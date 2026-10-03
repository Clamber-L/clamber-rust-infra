//! 预加载器与时间轮驱动。
//!
//! 内存时间轮只装 horizon 内到期的任务，其余留在存储层 —— 这让内存
//! 占用与**到期速率**相关，而与**消息总存量**无关。见架构文档 §4.2。

use kafka_timing_wheel::{TimingWheel, WheelError};

use crate::clock::Clock;
use crate::store::DelayStore;
use crate::task::TimerTask;
use crate::worker::{StoreHandle, WorkerGone};

/// 调度参数。
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// 内存时间轮覆盖的时间跨度。只有 `execute_at <= now + horizon_ms`
    /// 的任务才入轮。
    pub horizon_ms: i64,

    /// 预加载周期。必须远小于 `horizon_ms`，否则窗口推进赶不上时间流逝，
    /// 任务会在尚未入轮时就已到期。
    pub preload_interval_ms: u64,

    /// 单次 scan 的上限。既防 OOM，也防 worker 队头阻塞。
    pub preload_batch: usize,

    /// 时间轮 tick 粒度。
    pub tick_ms: u64,

    /// 时间轮槽数，必须是 2 的幂。
    pub wheel_size: usize,

    /// `Delivering` 多久未完成视为卡住，由 recovery 重置回 `Pending`。
    pub delivering_timeout_ms: i64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            // 5 分钟 < 单圈 8.5 分钟，使 rounds 恒为 0
            horizon_ms: 5 * 60 * 1_000,
            // horizon 的 1/5，留出足够安全余量
            preload_interval_ms: 60 * 1_000,
            preload_batch: 10_000,
            tick_ms: kafka_timing_wheel::DEFAULT_TICK_MS,
            wheel_size: kafka_timing_wheel::DEFAULT_WHEEL_SIZE,
            delivering_timeout_ms: 30 * 1_000,
        }
    }
}

impl SchedulerConfig {
    /// 单圈跨度。
    pub fn revolution_ms(&self) -> i64 {
        self.tick_ms as i64 * self.wheel_size as i64
    }

    /// 校验参数自身是否自洽。返回的问题都会导致运行期的隐性故障，
    /// 所以在构造时就挡掉。
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.horizon_ms <= 0 {
            return Err(ConfigError::NonPositiveHorizon(self.horizon_ms));
        }
        if self.preload_interval_ms == 0 {
            return Err(ConfigError::ZeroPreloadInterval);
        }
        if self.preload_batch == 0 {
            return Err(ConfigError::ZeroPreloadBatch);
        }
        if !self.wheel_size.is_power_of_two() {
            return Err(ConfigError::WheelSizeNotPowerOfTwo(self.wheel_size));
        }
        if self.tick_ms == 0 {
            return Err(ConfigError::ZeroTick);
        }

        // 预加载周期必须远小于 horizon。相等或更大时，窗口推进赶不上
        // 时间流逝，任务会在入轮前就已到期。
        if (self.preload_interval_ms as i64) * 2 > self.horizon_ms {
            return Err(ConfigError::PreloadIntervalTooLong {
                preload_interval_ms: self.preload_interval_ms,
                horizon_ms: self.horizon_ms,
            });
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("horizon_ms must be positive, got {0}")]
    NonPositiveHorizon(i64),

    #[error("preload_interval_ms must be non-zero")]
    ZeroPreloadInterval,

    #[error("preload_batch must be non-zero")]
    ZeroPreloadBatch,

    #[error("wheel_size {0} is not a power of two")]
    WheelSizeNotPowerOfTwo(usize),

    #[error("tick_ms must be non-zero")]
    ZeroTick,

    #[error(
        "preload_interval_ms {preload_interval_ms} is too long for horizon_ms {horizon_ms}; \
         tasks may come due before being loaded"
    )]
    PreloadIntervalTooLong {
        preload_interval_ms: u64,
        horizon_ms: i64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError<E> {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error("store: {0}")]
    Store(E),

    #[error(transparent)]
    WorkerGone(#[from] WorkerGone),

    #[error("wheel: {0}")]
    Wheel(#[from] WheelError),
}

/// 一次预加载的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PreloadStats {
    /// 本次新入轮的任务数。
    pub loaded: usize,

    /// 因超出时间轮 horizon 上限而被拒的任务数。正常配置下应为 0。
    pub rejected: usize,
}

/// 预加载器 + 时间轮。
///
/// 不自带运行循环 —— `preload_once` 与 `advance_to` 都是显式调用，
/// 时间由参数注入。这让整个调度逻辑可以在测试里确定性地驱动，
/// 不需要 sleep。真正的循环由 delay-server 组装。
pub struct Scheduler<S: DelayStore> {
    store: StoreHandle<S::Error>,
    wheel: TimingWheel<TimerTask>,
    config: SchedulerConfig,
    /// 时间轮原点。
    start_ms: i64,
    /// 已预加载到哪个时刻 —— 下次 scan 的起点，避免重复扫描。
    preloaded_until: i64,
}

impl<S: DelayStore> Scheduler<S> {
    /// 以 `clock` 的基准时刻作为时间轮原点。
    pub fn new(
        store: StoreHandle<S::Error>,
        clock: &Clock,
        config: SchedulerConfig,
    ) -> Result<Self, ConfigError> {
        Self::with_origin(store, clock.start_ms(), config)
    }

    /// 显式指定时间轮原点。
    ///
    /// 服务侧通常用 [`Scheduler::new`]，但原点需要可控的场合(测试、
    /// 或从 checkpoint 恢复时对齐到某个历史时刻)用这个。
    ///
    /// 早于 `start_ms` 的任务无法入轮 —— 时间轮不接受原点之前的时刻。
    pub fn with_origin(
        store: StoreHandle<S::Error>,
        start_ms: i64,
        config: SchedulerConfig,
    ) -> Result<Self, ConfigError> {
        config.validate()?;

        let wheel = TimingWheel::new(config.tick_ms, config.wheel_size, start_ms);

        Ok(Self {
            store,
            wheel,
            config,
            start_ms,
            // 从原点开始扫 —— 更早的任务时间轮装不下，由 recovery
            // 单独处理(它们已经过期，应当立即投递)。
            preloaded_until: start_ms,
        })
    }

    /// 时间轮原点。
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// 当前在轮中的任务数。
    pub fn pending_in_wheel(&self) -> usize {
        self.wheel.len()
    }

    /// 已预加载到的时刻。
    pub fn preloaded_until(&self) -> i64 {
        self.preloaded_until
    }

    /// 扫一批到期窗口内的任务并入轮。
    ///
    /// 失败时**不推进** `preloaded_until`，下一轮会重扫同一区间 ——
    /// 宁可重复入轮(由投递前的 CAS 挡住)，也不能漏掉任务。
    pub async fn preload_once(&mut self, now_ms: i64) -> Result<PreloadStats, SchedulerError<S::Error>> {
        let upper = now_ms + self.config.horizon_ms;

        let tasks = self
            .store
            .scan_due(self.preloaded_until, upper, self.config.preload_batch)
            .await?
            .map_err(SchedulerError::Store)?;

        let mut stats = PreloadStats::default();

        for task in tasks {
            match self.wheel.schedule(task.execute_at, task.clone()) {
                Ok(()) => stats.loaded += 1,
                // 超出轮的 horizon 上限 —— 配置不自洽，但不该让整个
                // 预加载失败：记下来，下一轮窗口推进后它会重新入轮。
                Err(WheelError::DeadlineTooFar { .. }) => {
                    stats.rejected += 1;
                    tracing::warn!(
                        message_id = %task.message_id,
                        execute_at = task.execute_at,
                        "task rejected by wheel horizon"
                    );
                }
                // execute_at 早于轮原点：轮是重启后新建的，原点取自
                // 当时的 wall clock，更早的任务直接视为已到期。
                Err(WheelError::TimeBeforeStart { .. }) => {
                    stats.rejected += 1;
                }
            }
        }

        // 只在成功后推进，且只推进到本次扫描的上界。
        if stats.rejected == 0 {
            self.preloaded_until = upper;
        }

        Ok(stats)
    }

    /// 推进时间轮，返回本次到期的任务。
    ///
    /// 时间由参数注入，补偿逻辑在轮内部 —— 迟到多久都能一次追上。
    pub fn advance_to(&mut self, now_ms: i64, out: &mut Vec<TimerTask>) {
        self.wheel.advance_to(now_ms, out);
    }

    /// 启动时恢复：先把卡住的 `Delivering` 重置回 `Pending`，再预加载。
    ///
    /// 顺序不能颠倒 —— 先预加载的话，卡住的消息此时还是 `Delivering`，
    /// 会被 `scan_due` 过滤掉，重置之后却没人再去扫它。
    pub async fn recover(&mut self, now_ms: i64) -> Result<PreloadStats, SchedulerError<S::Error>> {
        let stale = self
            .store
            .scan_stale_delivering(now_ms, self.config.delivering_timeout_ms, self.config.preload_batch)
            .await?
            .map_err(SchedulerError::Store)?;

        for task in &stale {
            self.store
                .set_status(&task.message_id, crate::message::DelayStatus::Pending, now_ms)
                .await?
                .map_err(SchedulerError::Store)?;

            tracing::info!(
                message_id = %task.message_id,
                "reset stale Delivering back to Pending"
            );
        }

        self.preload_once(now_ms).await
    }
}
