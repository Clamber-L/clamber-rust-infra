//! 端到端：入库 → 预加载 → 时间轮到期 → 投递 → 删除。
//!
//! 时间由参数注入，整条链路确定性地跑完，不用 sleep。唯一缺的是 Kafka
//! 两端（Step 6/7），这里用内存 store 与 fake sink 顶替。

use std::sync::{Arc, Mutex};

use kafka_delaymq_core::{
    DelayMessage, DelayStatus, Delivery, MemoryStore, MemoryStoreError, MessageSink, Processor,
    Scheduler, SchedulerConfig, StoreHandle, TimerTask,
};

const NOW: i64 = 1_700_000_000_000;

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 0,
        execute_at,
        payload: format!("payload-{id}").into_bytes(),
        headers: Vec::new(),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sink down")]
struct SinkError;

#[derive(Clone, Default)]
struct RecordingSink {
    sent: Arc<Mutex<Vec<(String, i64)>>>,
    fail: Arc<Mutex<bool>>,
}

impl RecordingSink {
    fn sent_ids(&self) -> Vec<String> {
        self.sent.lock().unwrap().iter().map(|(id, _)| id.clone()).collect()
    }
    fn count(&self) -> usize {
        self.sent.lock().unwrap().len()
    }
}

impl MessageSink for RecordingSink {
    type Error = SinkError;

    async fn send(&self, message: &DelayMessage) -> Result<(), Self::Error> {
        if *self.fail.lock().unwrap() {
            return Err(SinkError);
        }
        self.sent
            .lock()
            .unwrap()
            .push((message.message_id.clone(), message.execute_at));
        Ok(())
    }
}

type Store = StoreHandle<MemoryStoreError>;

struct Pipeline {
    store: Store,
    sched: Scheduler<MemoryStore>,
    processor: Processor<MemoryStore, RecordingSink>,
    sink: RecordingSink,
}

fn pipeline(config: SchedulerConfig) -> Pipeline {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sched = Scheduler::<MemoryStore>::with_origin(store.clone(), NOW, config).unwrap();
    let sink = RecordingSink::default();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());
    Pipeline { store, sched, processor, sink }
}

impl Pipeline {
    /// 驱动一轮：预加载 + 推进时间轮 + 处理到期任务。
    /// 返回本轮的处理结果。
    async fn tick(&mut self, now_ms: i64) -> Vec<Delivery> {
        self.sched.preload_once(now_ms).await.unwrap();

        let mut due = Vec::new();
        self.sched.advance_to(now_ms, &mut due);

        let mut outcomes = Vec::new();
        for task in due {
            outcomes.push(self.processor.process(&task, now_ms).await.unwrap());
        }
        outcomes
    }
}

/// 最小闭环：一条消息走完全程。
#[tokio::test]
async fn single_message_flows_end_to_end() {
    let mut p = pipeline(SchedulerConfig::default());
    p.store.put(message("m1", NOW + 3_000), NOW).await.unwrap().unwrap();

    // 未到期
    assert!(p.tick(NOW + 1_000).await.is_empty());
    assert_eq!(p.sink.count(), 0);

    // 到期
    assert_eq!(p.tick(NOW + 3_000).await, vec![Delivery::Delivered]);
    assert_eq!(p.sink.sent_ids(), vec!["m1"]);

    // 记录已删，不会重复投递
    assert!(p.store.get("m1").await.unwrap().unwrap().is_none());
    assert!(p.tick(NOW + 10_000).await.is_empty());
    assert_eq!(p.sink.count(), 1);
}

/// 一批消息按到期顺序投递，每条恰好一次。
#[tokio::test]
async fn batch_delivers_in_deadline_order_exactly_once() {
    let mut p = pipeline(SchedulerConfig::default());

    // 乱序入库
    for i in [5i64, 1, 4, 2, 3] {
        p.store
            .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
            .await
            .unwrap()
            .unwrap();
    }

    for step in 1..=5 {
        p.tick(NOW + step * 1_000).await;
    }

    assert_eq!(
        p.sink.sent_ids(),
        vec!["m1", "m2", "m3", "m4", "m5"],
        "not delivered in deadline order"
    );
}

/// 超出 horizon 的远期消息，等窗口推进后才入轮，最终仍准时投递。
#[tokio::test]
async fn far_future_message_is_loaded_when_window_reaches_it() {
    let config = SchedulerConfig::default();
    let mut p = pipeline(config);

    let deadline = NOW + config.horizon_ms * 3;
    p.store.put(message("far", deadline), NOW).await.unwrap().unwrap();

    // 第一轮：还在 horizon 之外
    p.tick(NOW).await;
    assert_eq!(p.sched.pending_in_wheel(), 0, "loaded too early");

    // 窗口推进到覆盖它
    p.tick(deadline - config.horizon_ms + 1).await;
    assert_eq!(p.sched.pending_in_wheel(), 1, "not loaded when in range");

    // 到期投递
    assert_eq!(p.tick(deadline).await, vec![Delivery::Delivered]);
    assert_eq!(p.sink.sent_ids(), vec!["far"]);
}

/// 卡顿后一次性追上：所有到期消息都要投递，不丢不重。
#[tokio::test]
async fn stall_then_catch_up_delivers_everything_once() {
    let mut p = pipeline(SchedulerConfig::default());

    for i in 1..=50i64 {
        p.store
            .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
            .await
            .unwrap()
            .unwrap();
    }

    // 模拟 50 秒停顿后一次推进
    let outcomes = p.tick(NOW + 50_000).await;

    assert_eq!(outcomes.len(), 50);
    assert!(outcomes.iter().all(|o| *o == Delivery::Delivered));
    assert_eq!(p.sink.count(), 50);
    assert_eq!(p.sched.pending_in_wheel(), 0);
}

/// 投递失败 → 重试 → 成功。记录在失败期间必须保留。
#[tokio::test]
async fn failed_delivery_recovers_on_a_later_tick() {
    let mut p = pipeline(SchedulerConfig::default());
    p.store.put(message("m1", NOW + 1_000), NOW).await.unwrap().unwrap();

    *p.sink.fail.lock().unwrap() = true;
    let outcomes = p.tick(NOW + 1_000).await;
    assert_eq!(outcomes, vec![Delivery::Retrying { retry_count: 1 }]);

    // 记录保留，状态回到 Pending
    let stored = p.store.get("m1").await.unwrap().unwrap().expect("kept");
    assert_eq!(stored.status, DelayStatus::Pending);
    assert_eq!(stored.retry_count, 1);

    // sink 恢复后，重新入轮的任务成功投递
    *p.sink.fail.lock().unwrap() = false;
    let task = TimerTask::new("m1".to_string(), NOW + 1_000);
    let outcome = p.processor.process(&task, NOW + 2_000).await.unwrap();

    assert_eq!(outcome, Delivery::Delivered);
    assert_eq!(p.sink.sent_ids(), vec!["m1"]);
}

/// 重启：时间轮是纯内存的，必须靠 recovery 从 store 重建。
#[tokio::test]
async fn restart_rebuilds_the_wheel_from_the_store() {
    let config = SchedulerConfig::default();
    let store = StoreHandle::spawn(MemoryStore::new());

    // 第一个 scheduler 入库若干消息后"崩溃"
    {
        let mut sched = Scheduler::<MemoryStore>::with_origin(store.clone(), NOW, config).unwrap();
        for i in 1..=3i64 {
            store
                .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
                .await
                .unwrap()
                .unwrap();
        }
        sched.preload_once(NOW).await.unwrap();
        assert_eq!(sched.pending_in_wheel(), 3);
    } // 时间轮随之消失

    // 新 scheduler 从 store 恢复
    let sink = RecordingSink::default();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());
    let mut sched = Scheduler::<MemoryStore>::with_origin(store.clone(), NOW, config).unwrap();

    let stats = sched.recover(NOW).await.unwrap();
    assert_eq!(stats.loaded, 3, "wheel not rebuilt from the store");

    // 恢复后的任务照常投递
    let mut due = Vec::new();
    sched.advance_to(NOW + 3_000, &mut due);
    for t in &due {
        processor.process(t, NOW + 3_000).await.unwrap();
    }

    let mut ids = sink.sent_ids();
    ids.sort();
    assert_eq!(ids, vec!["m1", "m2", "m3"]);
}

/// 崩在 Delivering 的消息：recovery 重置后必须重新投递，而非永久卡住。
#[tokio::test]
async fn message_stuck_in_delivering_is_recovered() {
    let config = SchedulerConfig::default();
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = RecordingSink::default();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());

    store.put(message("stuck", NOW), NOW).await.unwrap().unwrap();
    // 模拟抢到投递权后进程崩溃
    store
        .compare_and_swap_status("stuck", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .unwrap()
        .unwrap();

    let mut sched = Scheduler::<MemoryStore>::with_origin(store.clone(), NOW, config).unwrap();

    // 超时之后 recovery 才会动它
    let later = NOW + config.delivering_timeout_ms * 2;
    let stats = sched.recover(later).await.unwrap();
    assert_eq!(stats.loaded, 1, "stuck message was not recovered");

    let mut due = Vec::new();
    sched.advance_to(later, &mut due);
    assert_eq!(due.len(), 1);
    assert_eq!(
        processor.process(&due[0], later).await.unwrap(),
        Delivery::Delivered
    );
    assert_eq!(sink.sent_ids(), vec!["stuck"]);
}

/// 取消的消息走完整条链路也不该被投递。
#[tokio::test]
async fn cancelled_message_is_never_delivered() {
    let mut p = pipeline(SchedulerConfig::default());
    p.store.put(message("m1", NOW + 1_000), NOW).await.unwrap().unwrap();
    p.sched.preload_once(NOW).await.unwrap();

    // 入轮之后再取消 —— tombstone 式，不动时间轮
    p.store
        .set_status("m1", DelayStatus::Cancelled, NOW)
        .await
        .unwrap()
        .unwrap();

    let mut due = Vec::new();
    p.sched.advance_to(NOW + 1_000, &mut due);
    assert_eq!(due.len(), 1, "task should still fire from the wheel");

    let outcome = p.processor.process(&due[0], NOW + 1_000).await.unwrap();
    assert_eq!(outcome, Delivery::Cancelled);
    assert_eq!(p.sink.count(), 0, "cancelled message was delivered");
}

/// 混合负载：不同到期时刻、含取消与失败，每条消息的最终去向都要正确。
#[tokio::test]
async fn mixed_workload_settles_correctly() {
    let mut p = pipeline(SchedulerConfig::default());

    for i in 1..=20i64 {
        p.store
            .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
            .await
            .unwrap()
            .unwrap();
    }
    // 取消其中三条
    for i in [3i64, 7, 11] {
        p.store
            .set_status(&format!("m{i}"), DelayStatus::Cancelled, NOW)
            .await
            .unwrap()
            .unwrap();
    }

    for step in 1..=20 {
        p.tick(NOW + step * 1_000).await;
    }

    let delivered = p.sink.sent_ids();
    assert_eq!(delivered.len(), 17, "expected 20 minus 3 cancelled");
    for i in [3i64, 7, 11] {
        assert!(
            !delivered.contains(&format!("m{i}")),
            "cancelled m{i} was delivered"
        );
    }
}
