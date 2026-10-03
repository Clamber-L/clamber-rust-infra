//! 预加载器与时间轮驱动。时间全部由参数注入，不使用 sleep。

use kafka_delaymq_core::{
    Clock, ConfigError, DelayMessage, DelayStatus, MemoryStore, MemoryStoreError, Scheduler,
    SchedulerConfig, StoreHandle,
};

const NOW: i64 = 1_700_000_000_000;

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 0,
        execute_at,
        payload: b"body".to_vec(),
        headers: Vec::new(),
    }
}

/// 时间轮原点固定为 NOW，使所有断言可预测、不依赖真实时钟。
fn scheduler() -> (
    Scheduler<MemoryStore>,
    StoreHandle<MemoryStoreError>,
    SchedulerConfig,
) {
    scheduler_with(SchedulerConfig::default())
}

fn scheduler_with(
    config: SchedulerConfig,
) -> (
    Scheduler<MemoryStore>,
    StoreHandle<MemoryStoreError>,
    SchedulerConfig,
) {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sched = Scheduler::<MemoryStore>::with_origin(store.clone(), NOW, config).unwrap();
    (sched, store, config)
}

// --------------------------- 配置校验 ---------------------------

#[test]
fn default_config_is_valid() {
    let config = SchedulerConfig::default();
    assert!(config.validate().is_ok());
}

/// horizon 必须小于单圈，否则 rounds > 0，远期任务每圈被空转遍历。
#[test]
fn default_horizon_fits_in_one_revolution() {
    let config = SchedulerConfig::default();
    assert!(
        config.horizon_ms < config.revolution_ms(),
        "horizon {} >= revolution {}",
        config.horizon_ms,
        config.revolution_ms()
    );
}

#[test]
fn preload_interval_must_be_well_under_horizon() {
    let config = SchedulerConfig {
        horizon_ms: 10_000,
        preload_interval_ms: 9_000,
        ..SchedulerConfig::default()
    };
    assert!(matches!(
        config.validate(),
        Err(ConfigError::PreloadIntervalTooLong { .. })
    ));
}

#[test]
fn invalid_configs_are_rejected() {
    let base = SchedulerConfig::default();

    assert_eq!(
        SchedulerConfig { horizon_ms: 0, ..base }.validate(),
        Err(ConfigError::NonPositiveHorizon(0))
    );
    assert_eq!(
        SchedulerConfig { preload_interval_ms: 0, ..base }.validate(),
        Err(ConfigError::ZeroPreloadInterval)
    );
    assert_eq!(
        SchedulerConfig { preload_batch: 0, ..base }.validate(),
        Err(ConfigError::ZeroPreloadBatch)
    );
    assert_eq!(
        SchedulerConfig { wheel_size: 500, ..base }.validate(),
        Err(ConfigError::WheelSizeNotPowerOfTwo(500))
    );
    assert_eq!(
        SchedulerConfig { tick_ms: 0, ..base }.validate(),
        Err(ConfigError::ZeroTick)
    );
}

#[tokio::test]
async fn scheduler_rejects_invalid_config() {
    let store = StoreHandle::spawn(MemoryStore::new());
    let clock = Clock::new();
    let bad = SchedulerConfig { horizon_ms: -1, ..SchedulerConfig::default() };

    assert!(Scheduler::<MemoryStore>::new(store, &clock, bad).is_err());
}

// --------------------------- 预加载 ---------------------------

/// 只有 horizon 内到期的任务才入轮 —— 这是内存与总存量解耦的关键。
#[tokio::test]
async fn preload_loads_only_within_horizon() {
    let (mut sched, store, config) = scheduler();
    let now = sched.start_ms();

    store.put(message("soon", now + 1_000), now).await.unwrap().unwrap();
    store.put(message("edge", now + config.horizon_ms), now).await.unwrap().unwrap();
    store
        .put(message("far", now + config.horizon_ms * 10), now)
        .await
        .unwrap()
        .unwrap();

    let stats = sched.preload_once(now).await.unwrap();

    assert_eq!(stats.loaded, 2, "only soon + edge belong in the wheel");
    assert_eq!(stats.rejected, 0);
    assert_eq!(sched.pending_in_wheel(), 2);
}

/// 重复调用不该把同一任务再入一次轮。
#[tokio::test]
async fn preload_does_not_reload_the_same_window() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();
    store.put(message("m1", now + 1_000), now).await.unwrap().unwrap();

    let first = sched.preload_once(now).await.unwrap();
    let second = sched.preload_once(now).await.unwrap();

    assert_eq!(first.loaded, 1);
    assert_eq!(second.loaded, 0, "window was rescanned");
    assert_eq!(sched.pending_in_wheel(), 1);
}

/// 窗口随时间推进，之前超出 horizon 的任务后来会被捞进来。
#[tokio::test]
async fn preload_window_advances_over_time() {
    let (mut sched, store, config) = scheduler();
    let now = sched.start_ms();

    let later = now + config.horizon_ms * 2;
    store.put(message("later", later), now).await.unwrap().unwrap();

    assert_eq!(sched.preload_once(now).await.unwrap().loaded, 0);

    // 时间推进到它进入 horizon
    let advanced = later - config.horizon_ms + 1;
    assert_eq!(sched.preload_once(advanced).await.unwrap().loaded, 1);
}

/// batch 上限既防 OOM，也防 worker 队头阻塞。
#[tokio::test]
async fn preload_honours_batch_limit() {
    let (mut sched, store, _) = scheduler_with(SchedulerConfig {
        preload_batch: 5,
        ..SchedulerConfig::default()
    });
    let now = sched.start_ms();

    for i in 0..20i64 {
        store
            .put(message(&format!("m{i}"), now + i * 100), now)
            .await
            .unwrap()
            .unwrap();
    }

    let stats = sched.preload_once(now).await.unwrap();
    assert_eq!(stats.loaded, 5, "batch limit not honoured");
    assert_eq!(sched.pending_in_wheel(), 5);
}

/// 已被抢占投递权的消息不该被预加载器重复入轮。
#[tokio::test]
async fn preload_skips_non_pending() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    store.put(message("pending", now + 1_000), now).await.unwrap().unwrap();
    store.put(message("claimed", now + 1_000), now).await.unwrap().unwrap();
    store
        .compare_and_swap_status("claimed", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(sched.preload_once(now).await.unwrap().loaded, 1);
}

// --------------------------- 时间轮驱动 ---------------------------

#[tokio::test]
async fn preloaded_tasks_fire_at_their_deadline() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    store.put(message("m1", now + 2_000), now).await.unwrap().unwrap();
    sched.preload_once(now).await.unwrap();

    let mut out = Vec::new();
    sched.advance_to(now + 1_000, &mut out);
    assert!(out.is_empty(), "fired early");

    sched.advance_to(now + 2_000, &mut out);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].message_id, "m1");
    assert_eq!(sched.pending_in_wheel(), 0);
}

/// 一次 advance 跨越大量 tick 时，中间所有到期任务都要收齐。
#[tokio::test]
async fn advance_catches_up_after_a_stall() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    for i in 1..=60i64 {
        store
            .put(message(&format!("m{i}"), now + i * 1_000), now)
            .await
            .unwrap()
            .unwrap();
    }
    sched.preload_once(now).await.unwrap();

    let mut out = Vec::new();
    sched.advance_to(now + 60_000, &mut out);

    assert_eq!(out.len(), 60, "catch-up dropped tasks");
    assert_eq!(sched.pending_in_wheel(), 0);
}

/// 每个任务恰好触发一次，且不早于其 deadline。
#[tokio::test]
async fn every_task_fires_once_and_never_early() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    let deadlines: Vec<i64> = (0..100).map(|i| now + (i * 37) % 120_000).collect();
    for (i, &d) in deadlines.iter().enumerate() {
        store.put(message(&format!("m{i}"), d), now).await.unwrap().unwrap();
    }
    sched.preload_once(now).await.unwrap();

    let mut seen: Vec<i64> = Vec::new();
    let mut out = Vec::new();
    for step in 0..=125 {
        let t = now + step * 1_000;
        out.clear();
        sched.advance_to(t, &mut out);
        for task in &out {
            assert!(t >= task.execute_at, "fired at {t}, deadline {}", task.execute_at);
            seen.push(task.execute_at);
        }
    }

    assert_eq!(sched.pending_in_wheel(), 0);
    seen.sort_unstable();
    let mut expected = deadlines;
    expected.sort_unstable();
    assert_eq!(seen, expected, "task fired zero times or more than once");
}

// --------------------------- recovery ---------------------------

/// 卡住的 Delivering 必须先重置再预加载 —— 顺序颠倒会让它被永久跳过。
#[tokio::test]
async fn recover_resets_stale_delivering_then_preloads() {
    let (mut sched, store, config) = scheduler();
    let now = sched.start_ms();

    store.put(message("stuck", now + 1_000), now).await.unwrap().unwrap();
    store
        .compare_and_swap_status("stuck", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .unwrap()
        .unwrap();

    // 推进到超过 Delivering 超时
    let later = now + config.delivering_timeout_ms * 2;
    let stats = sched.recover(later).await.unwrap();

    let stored = store.get("stuck").await.unwrap().unwrap().unwrap();
    assert_eq!(stored.status, DelayStatus::Pending, "stale Delivering not reset");
    assert_eq!(stats.loaded, 1, "reset task was not preloaded");
}

/// 未超时的 Delivering 不该被碰 —— 它可能正在正常投递。
#[tokio::test]
async fn recover_leaves_fresh_delivering_alone() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    store.put(message("inflight", now + 1_000), now).await.unwrap().unwrap();
    store
        .compare_and_swap_status("inflight", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .unwrap()
        .unwrap();

    sched.recover(now).await.unwrap();

    let stored = store.get("inflight").await.unwrap().unwrap().unwrap();
    assert_eq!(stored.status, DelayStatus::Delivering, "in-flight delivery disturbed");
}

#[tokio::test]
async fn recover_on_empty_store_is_a_noop() {
    let (mut sched, _store, _) = scheduler();
    let stats = sched.recover(NOW).await.unwrap();
    assert_eq!(stats.loaded, 0);
    assert_eq!(stats.rejected, 0);
}

/// 已过期的任务(重启后加载)应当立即到期，而不是等下一个 tick 边界。
#[tokio::test]
async fn already_expired_tasks_fire_on_first_advance() {
    let (mut sched, store, _) = scheduler();
    let now = sched.start_ms();

    // 原点之后、但相对于 advance 时刻已经过去
    store.put(message("overdue", now + 1), now).await.unwrap().unwrap();
    sched.preload_once(now + 10_000).await.unwrap();

    let mut out = Vec::new();
    sched.advance_to(now + 10_000, &mut out);
    assert_eq!(out.len(), 1, "overdue task did not fire immediately");
}


