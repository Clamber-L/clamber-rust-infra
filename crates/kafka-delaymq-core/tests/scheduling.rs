//! 时间轮与领域模型的集成：确认 `TimingWheel<TimerTask>` 真能跑通，
//! 并验证预加载 horizon 的取值使 `rounds` 恒为 0。

use kafka_delaymq_core::{Clock, DelayMessage, DelayStore, MemoryStore, TimerTask};
use kafka_timing_wheel::{DEFAULT_TICK_MS, DEFAULT_WHEEL_SIZE, TimingWheel};

const NOW: i64 = 1_700_000_000_000;
const HORIZON_MS: i64 = 5 * 60 * 1_000; // 5 分钟

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 0,
        execute_at,
        payload: Vec::new(),
        headers: Vec::new(),
    }
}

#[test]
fn timer_task_is_derived_from_message() {
    let msg = message("m1", NOW + 1_000);
    let task = TimerTask::from(&msg);

    assert_eq!(task.message_id, "m1");
    assert_eq!(task.execute_at, NOW + 1_000);
}

/// 预加载 → 入轮 → 到期，走一遍最小闭环。
#[test]
fn preload_scan_feeds_the_wheel() {
    let store = MemoryStore::new();
    store.put(&message("soon", NOW + 2_000), NOW).unwrap();
    store.put(&message("later", NOW + 60_000), NOW).unwrap();
    // 超出 horizon，不该入轮
    store.put(&message("far", NOW + HORIZON_MS * 10), NOW).unwrap();

    let mut wheel: TimingWheel<TimerTask> = TimingWheel::with_defaults(NOW);
    for task in store.scan_due(0, NOW + HORIZON_MS, 1_000).unwrap() {
        wheel.schedule(task.execute_at, task).unwrap();
    }

    assert_eq!(wheel.len(), 2, "only tasks inside the horizon should enter");

    let mut fired = Vec::new();
    wheel.advance_to(NOW + 2_000, &mut fired);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].message_id, "soon");

    fired.clear();
    wheel.advance_to(NOW + 60_000, &mut fired);
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].message_id, "later");
    assert!(wheel.is_empty());
}

/// horizon 必须小于单圈跨度，否则 rounds > 0，远期任务每圈被空转遍历。
#[test]
fn horizon_fits_within_one_revolution() {
    let revolution_ms = DEFAULT_TICK_MS as i64 * DEFAULT_WHEEL_SIZE as i64;

    assert!(
        HORIZON_MS < revolution_ms,
        "horizon {HORIZON_MS}ms exceeds one revolution {revolution_ms}ms — rounds > 0"
    );
}

#[test]
fn clock_advances_monotonically() {
    let clock = Clock::new();
    let first = clock.now_ms();
    let second = clock.now_ms();

    assert!(second >= first, "clock went backwards");
    // 基准应接近真实 wall clock
    let drift = (clock.start_ms() - kafka_delaymq_core::wall_clock_ms()).abs();
    assert!(drift < 1_000, "clock baseline drifted by {drift}ms");
}

#[test]
fn clock_start_ms_is_usable_as_wheel_origin() {
    let clock = Clock::new();
    let mut wheel: TimingWheel<TimerTask> = TimingWheel::with_defaults(clock.start_ms());

    let task = TimerTask::new("m1".to_string(), clock.start_ms() + 1_000);
    wheel.schedule(task.execute_at, task).unwrap();

    let mut fired = Vec::new();
    wheel.advance_to(clock.start_ms() + 1_000, &mut fired);
    assert_eq!(fired.len(), 1);
}
