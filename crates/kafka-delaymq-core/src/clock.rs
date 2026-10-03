use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Wall clock 时刻的来源，由单调时钟推导。
///
/// 时间轮用 `now_ms()` 推进。**不要直接用 `SystemTime::now()`** ——
/// NTP 校正会让系统时间跳变：
///
/// ```text
/// 前跳 1 小时  →  一次推进 3600 个 tick(安全但毫无必要)
/// 后跳 1 小时  →  推进变成 no-op 一小时(时间轮停摆)
/// ```
///
/// 用单调时钟推导出 wall clock，两者都免疫；重新校准成为显式动作，
/// 而不是随时可能发生的意外。详见 `docs/time-concepts.md`。
#[derive(Debug, Clone)]
pub struct Clock {
    start_instant: Instant,
    /// 与 `start_instant` 同时捕获的 wall clock 时刻。
    start_ms: i64,
}

impl Clock {
    /// 捕获当前时刻作为基准。
    pub fn new() -> Self {
        Self {
            start_instant: Instant::now(),
            start_ms: wall_clock_ms(),
        }
    }

    /// 由单调时钟推导出的当前 wall clock 毫秒。
    pub fn now_ms(&self) -> i64 {
        self.start_ms + self.start_instant.elapsed().as_millis() as i64
    }

    /// 基准被捕获时的 wall clock 时刻。可作为时间轮的 `start_ms`。
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// 用当前 wall clock 重新对齐基准，消除长期累积的漂移。
    ///
    /// 返回校准前后的差值(正数表示 wall clock 跑在前面)。调用方应据此
    /// 判断是否需要重建时间轮 —— 本方法只动 `Clock`，不会通知时间轮。
    pub fn recalibrate(&mut self) -> i64 {
        let derived = self.now_ms();
        let actual = wall_clock_ms();
        self.start_instant = Instant::now();
        self.start_ms = actual;
        actual - derived
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

/// 当前 Unix 毫秒。仅用于捕获基准与二次校验，不用于推进时间轮。
pub fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
