use crate::WheelError;

#[derive(Debug, Clone)]
struct Entry<T> {
    payload: T,
    rounds: u64,
}

pub struct TimingWheel<T> {
    tick_ms: u64,
    wheel_size: usize, // 2的幂
    mask: u64,         // wheel_size - 1
    slots: Vec<Vec<Entry<T>>>,
    start_ms: i64,     // tick 0 对应的wall clock时间
    current_tick: u64, // 下一个待处理的 tick
    len: usize,        // 当前总的 entry 数量
    max_horizon_ms: u64,
    /// 入轮时就已过期的任务，由下一次 `advance_to` 立即吐出。
    /// 不能放进 slot：slot 最早也要等到下一个 tick 边界才会被访问。
    expired: Vec<Entry<T>>,
    /// 复用的暂存区，避免每个 tick 为未到期 entry 重新分配。
    survivors: Vec<Entry<T>>,
}

/// `max_horizon_ms` 的默认值 = 单圈时长 × 该圈数。
///
/// 这个上限不是为了防算术溢出（i64 时间戳推出的 delay_ms 溢不出 u64），
/// 而是控制成本：`rounds > 0` 的任务每转一圈都要被遍历一次，圈数越大
/// 空转越多。配合预加载器（只有 horizon 内的任务才入轮），正常路径上
/// `rounds` 应当恒为 0。
const DEFAULT_MAX_REVOLUTIONS: u64 = 1024;

/// 本项目的 tick 粒度：1 秒。
///
/// 延迟消息的业务语义是"大约几分钟后"而非"精确到百毫秒"，而 tick 粒度
/// 直接决定唤醒频率。取 1 秒相比 100ms 省掉十分之九的唤醒与空转扫描，
/// 代价是触发最多晚 1 秒 —— 且因为 schedule 取 ceil，只会偏晚不会偏早。
pub const DEFAULT_TICK_MS: u64 = 1_000;

/// 默认槽数，2 的幂。单圈 = 512 × 1s ≈ 8.5 分钟，超过典型的预加载
/// horizon，使 `rounds` 在正常路径上恒为 0。
pub const DEFAULT_WHEEL_SIZE: usize = 512;

impl<T> TimingWheel<T> {
    /// 用本项目的默认粒度构造：`tick_ms = 1s`，`wheel_size = 512`。
    pub fn with_defaults(start_ms: i64) -> Self {
        Self::new(DEFAULT_TICK_MS, DEFAULT_WHEEL_SIZE, start_ms)
    }

    pub fn new(tick_ms: u64, wheel_size: usize, start_ms: i64) -> Self {
        assert!(tick_ms > 0, "tick_ms must be greater than zero");
        assert!(wheel_size.is_power_of_two());
        // vec![Vec::new(); n] 会要求 Entry<T>: Clone，而 T 没有该约束。
        let slots = (0..wheel_size).map(|_| Vec::new()).collect();
        TimingWheel {
            tick_ms,
            wheel_size,
            mask: wheel_size as u64 - 1,
            slots,
            start_ms,
            current_tick: 0,
            len: 0,
            max_horizon_ms: tick_ms
                .saturating_mul(wheel_size as u64)
                .saturating_mul(DEFAULT_MAX_REVOLUTIONS),
            expired: Vec::new(),
            survivors: Vec::new(),
        }
    }

    /// 覆盖默认的 horizon 上限。超过该跨度的 `schedule` 会返回
    /// [`WheelError::DeadlineTooFar`]。
    pub fn with_max_horizon_ms(mut self, max_horizon_ms: u64) -> Self {
        self.max_horizon_ms = max_horizon_ms;
        self
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 当前已推进到的位置，便于测试与 metrics 观测。
    pub fn current_tick(&self) -> u64 {
        self.current_tick
    }

    pub fn schedule(&mut self, execute_at: i64, payload: T) -> Result<(), WheelError> {
        if execute_at < self.start_ms {
            return Err(WheelError::TimeBeforeStart {
                execute_at,
                start_ms: self.start_ms,
            });
        }

        let delay_ms = (execute_at - self.start_ms) as u64;
        if delay_ms > self.max_horizon_ms {
            return Err(WheelError::DeadlineTooFar {
                execute_at,
                max_horizon_ms: self.max_horizon_ms,
            });
        }

        // schedule 取 ceil、advance 取 floor，保证任务不会早于 execute_at 触发。
        let target_tick = delay_ms.div_ceil(self.tick_ms);

        // 已过期的任务不能塞进 slot：轮已经推进过那一格，最早也要等到下一个
        // tick 边界才会再访问它。单独挂到 expired，由下一次 advance_to 吐出。
        if target_tick < self.current_tick {
            self.expired.push(Entry { payload, rounds: 0 });
            self.len += 1;
            return Ok(());
        }

        let delta = target_tick - self.current_tick;
        let slot_index = (target_tick & self.mask) as usize;
        let rounds = delta / self.wheel_size as u64;

        self.slots[slot_index].push(Entry { payload, rounds });
        self.len += 1;
        Ok(())
    }

    pub fn advance_to(&mut self, now_ms: i64, out: &mut Vec<T>) {
        if now_ms < self.start_ms {
            return;
        }
        // 先排空入轮时就已过期的任务，与 tick 边界无关。
        if !self.expired.is_empty() {
            self.len -= self.expired.len();
            out.extend(self.expired.drain(..).map(|entry| entry.payload));
        }

        let elapsed_ms = (now_ms - self.start_ms) as u64;
        let target_tick = elapsed_ms / self.tick_ms;

        // 时钟回退或重复调用时循环零次，天然是 no-op。
        while self.current_tick <= target_tick {
            let slot_index = (self.current_tick & self.mask) as usize;
            let mut slot = std::mem::take(&mut self.slots[slot_index]);

            for entry in slot.drain(..) {
                if entry.rounds == 0 {
                    out.push(entry.payload);
                    self.len -= 1;
                } else {
                    self.survivors.push(Entry {
                        rounds: entry.rounds - 1,
                        payload: entry.payload,
                    });
                }
            }

            // slot 此时已空(drain 过)，把未到期的搬回去并还原容量。
            slot.append(&mut self.survivors);
            self.slots[slot_index] = slot;

            self.current_tick += 1;
        }
    }
}
