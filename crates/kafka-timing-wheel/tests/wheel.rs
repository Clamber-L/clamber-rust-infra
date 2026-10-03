//! 全部确定性测试：时间由 `advance_to` 的参数注入，不依赖 sleep 或真实时钟。

use kafka_timing_wheel::{DEFAULT_TICK_MS, DEFAULT_WHEEL_SIZE, TimingWheel, WheelError};

const TICK_MS: u64 = 100;
const WHEEL_SIZE: usize = 8;
const START_MS: i64 = 0;

fn wheel() -> TimingWheel<&'static str> {
    TimingWheel::new(TICK_MS, WHEEL_SIZE, START_MS)
}

/// 推进到 `now_ms` 并返回本次到期的载荷。
fn advance(w: &mut TimingWheel<&'static str>, now_ms: i64) -> Vec<&'static str> {
    let mut out = Vec::new();
    w.advance_to(now_ms, &mut out);
    out
}

#[test]
fn does_not_fire_before_deadline() {
    let mut w = wheel();
    w.schedule(500, "a").unwrap();

    assert!(advance(&mut w, 499).is_empty());
    assert_eq!(w.len(), 1);
}

#[test]
fn fires_at_deadline() {
    let mut w = wheel();
    w.schedule(500, "a").unwrap();

    assert_eq!(advance(&mut w, 500), vec!["a"]);
    assert!(w.is_empty());
}

#[test]
fn does_not_fire_twice() {
    let mut w = wheel();
    w.schedule(500, "a").unwrap();

    assert_eq!(advance(&mut w, 500), vec!["a"]);
    assert!(advance(&mut w, 500).is_empty());
    assert!(advance(&mut w, 5_000).is_empty());
}

/// schedule 必须用 ceil：execute_at=150 落在 tick 1(100ms) 与 tick 2(200ms)
/// 之间，若用 floor 会在 100ms 提前触发。
#[test]
fn never_fires_early_for_non_tick_aligned_deadline() {
    let mut w = wheel();
    w.schedule(150, "a").unwrap();

    assert!(advance(&mut w, 100).is_empty(), "fired 50ms early");
    assert!(advance(&mut w, 199).is_empty());
    assert_eq!(advance(&mut w, 200), vec!["a"]);
}

#[test]
fn deadline_beyond_one_revolution_waits_for_its_round() {
    // 单圈 = 8 × 100ms = 800ms，execute_at=800 → target_tick 8, slot 0, rounds 1
    let mut w = wheel();
    w.schedule(800, "a").unwrap();

    // tick 0 经过 slot 0，但 rounds 还是 1，不该触发
    assert!(advance(&mut w, 0).is_empty());
    assert!(advance(&mut w, 700).is_empty());
    assert_eq!(advance(&mut w, 800), vec!["a"]);
}

#[test]
fn same_slot_different_rounds_fire_in_order() {
    // 三者都落在 slot 0：tick 0 / 8 / 16
    let mut w = wheel();
    w.schedule(0, "r0").unwrap();
    w.schedule(800, "r1").unwrap();
    w.schedule(1600, "r2").unwrap();

    assert_eq!(advance(&mut w, 0), vec!["r0"]);
    assert_eq!(advance(&mut w, 800), vec!["r1"]);
    assert_eq!(advance(&mut w, 1600), vec!["r2"]);
    assert!(w.is_empty());
}

/// tick 补偿：一次 advance 跨越大量 tick 时，中间所有到期任务都要收齐。
#[test]
fn single_advance_catches_up_across_many_ticks() {
    let mut w = wheel();
    for i in 1..=50u64 {
        w.schedule(i as i64 * 100, "x").unwrap();
    }

    let fired = advance(&mut w, 5_000);
    assert_eq!(fired.len(), 50, "catch-up dropped tasks");
    assert!(w.is_empty());
    assert_eq!(w.current_tick(), 51);
}

#[test]
fn clock_going_backwards_is_a_noop() {
    let mut w = wheel();
    w.schedule(1_000, "a").unwrap();

    advance(&mut w, 500);
    let tick_before = w.current_tick();

    assert!(advance(&mut w, 100).is_empty());
    assert!(advance(&mut w, -5_000).is_empty());
    assert_eq!(w.current_tick(), tick_before, "current_tick rewound");

    // 回退未破坏 rounds，正常时间仍能触发
    assert_eq!(advance(&mut w, 1_000), vec!["a"]);
}

#[test]
fn already_expired_deadline_fires_immediately() {
    let mut w = wheel();
    advance(&mut w, 1_000); // current_tick = 11

    w.schedule(500, "late").unwrap();
    assert_eq!(advance(&mut w, 1_000), vec!["late"]);
}

/// bug 回归：slot/rounds 必须按 target_tick 的绝对值算，而不是叠加 current_tick。
#[test]
fn slot_math_is_correct_after_wheel_has_turned() {
    let mut w = wheel();
    advance(&mut w, 400); // current_tick = 5

    // target_tick 8 → slot 0, delta 3 → rounds 0，应在 800ms 触发
    w.schedule(800, "a").unwrap();

    assert!(advance(&mut w, 700).is_empty());
    assert_eq!(advance(&mut w, 800), vec!["a"], "late by a full revolution");
}

#[test]
fn deadline_before_start_is_rejected() {
    let mut w = TimingWheel::<&str>::new(TICK_MS, WHEEL_SIZE, 1_000);

    assert_eq!(
        w.schedule(999, "a"),
        Err(WheelError::TimeBeforeStart {
            execute_at: 999,
            start_ms: 1_000,
        })
    );
    assert!(w.is_empty());
}

#[test]
fn deadline_too_far_is_rejected_without_overflow() {
    let mut w = wheel();

    assert!(matches!(
        w.schedule(i64::MAX, "a"),
        Err(WheelError::DeadlineTooFar { .. })
    ));
    assert!(w.is_empty());
}

#[test]
fn len_tracks_pending_entries() {
    let mut w = wheel();
    assert!(w.is_empty());

    w.schedule(100, "a").unwrap();
    w.schedule(200, "b").unwrap();
    w.schedule(800, "c").unwrap();
    assert_eq!(w.len(), 3);

    advance(&mut w, 200);
    assert_eq!(w.len(), 1, "only the cross-revolution entry should remain");

    advance(&mut w, 800);
    assert!(w.is_empty());
}

/// 同一毫秒的多个任务一次性全部触发，顺序不保证但数量必须对。
#[test]
fn multiple_tasks_on_same_deadline_all_fire() {
    let mut w = wheel();
    w.schedule(500, "a").unwrap();
    w.schedule(500, "b").unwrap();
    w.schedule(500, "c").unwrap();

    let mut fired = advance(&mut w, 500);
    fired.sort_unstable();
    assert_eq!(fired, vec!["a", "b", "c"]);
}

/// 确认文档声明的默认参数：tick = 1s，单圈 ≈ 8.5 分钟。
#[test]
fn default_granularity_is_one_second() {
    let mut w: TimingWheel<&str> = TimingWheel::with_defaults(0);

    // 1 秒粒度：999ms 和 1000ms 落在不同 tick
    w.schedule(1_000, "a").unwrap();
    assert!(advance(&mut w, 999).is_empty());
    assert_eq!(advance(&mut w, 1_000), vec!["a"]);

    // 单圈 = 512 × 1s，刚好一圈之后仍能正确触发
    let one_revolution = DEFAULT_TICK_MS as i64 * DEFAULT_WHEEL_SIZE as i64;
    assert_eq!(one_revolution, 512_000, "single revolution is 8.5 minutes");

    w.schedule(one_revolution, "b").unwrap();
    assert!(advance(&mut w, one_revolution - 1).is_empty());
    assert_eq!(advance(&mut w, one_revolution), vec!["b"]);
}

/// 1 秒粒度下的抖动方向：只会偏晚，最多晚一个 tick，绝不偏早。
#[test]
fn jitter_is_always_late_never_early() {
    let mut w: TimingWheel<i64> = TimingWheel::with_defaults(0);

    // 一批不对齐 tick 边界的 deadline
    let deadlines: Vec<i64> = vec![1, 500, 999, 1_001, 1_500, 2_499, 60_123];
    for &d in &deadlines {
        w.schedule(d, d).unwrap();
    }

    let mut out = Vec::new();
    for now in (0..62_000).step_by(250) {
        out.clear();
        w.advance_to(now, &mut out);
        for &deadline in &out {
            assert!(now >= deadline, "fired at {now}, deadline {deadline}");
            let lateness = now - deadline;
            assert!(
                lateness < DEFAULT_TICK_MS as i64 + 250,
                "deadline {deadline} fired {lateness}ms late"
            );
        }
    }
    assert!(w.is_empty());
}

/// 载荷是非 Copy 且带堆分配的类型，确认所有权正确转移。
/// 原实现用 mem::zeroed() 占位，对 String 是 UB。
#[test]
fn owned_payloads_survive_the_wheel() {
    let mut w: TimingWheel<String> = TimingWheel::new(TICK_MS, WHEEL_SIZE, START_MS);
    w.schedule(100, "short".to_string()).unwrap();
    w.schedule(900, "a".repeat(1024)).unwrap();

    let mut out = Vec::new();
    w.advance_to(100, &mut out);
    assert_eq!(out, vec!["short".to_string()]);

    out.clear();
    w.advance_to(900, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].len(), 1024);
    assert!(out[0].chars().all(|c| c == 'a'));
}

/// 不同时刻入轮、跨多圈的一批任务，每个都恰好触发一次且不早于其 deadline。
#[test]
fn every_task_fires_exactly_once_and_never_early() {
    let mut w: TimingWheel<i64> = TimingWheel::new(TICK_MS, WHEEL_SIZE, START_MS);

    let deadlines: Vec<i64> = (0..200).map(|i| (i * 37) % 3_000).collect();
    for &d in &deadlines {
        w.schedule(d, d).unwrap();
    }

    let mut seen = Vec::new();
    let mut out = Vec::new();
    // 不规则步长推进，模拟抖动的 tick
    for now in (0..3_200).step_by(7) {
        out.clear();
        w.advance_to(now, &mut out);
        for &deadline in &out {
            assert!(
                now >= deadline,
                "fired at {now} but deadline was {deadline}"
            );
            seen.push(deadline);
        }
    }

    assert!(w.is_empty());
    seen.sort_unstable();
    let mut expected = deadlines;
    expected.sort_unstable();
    assert_eq!(seen, expected, "task fired zero times or more than once");
}
