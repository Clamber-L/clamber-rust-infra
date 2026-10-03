//! 到期处理：CAS 抢占、二次校验、重试、死信。
//! 投递目标用可控的 fake，不涉及 Kafka。

use std::sync::{Arc, Mutex};

use kafka_delaymq_core::{
    DelayMessage, DelayStatus, Delivery, MemoryStore, MessageSink, Processor, RetryPolicy,
    StoreHandle, TimerTask,
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

#[derive(Debug, thiserror::Error)]
#[error("sink refused: {0}")]
struct SinkError(&'static str);

/// 可控的投递目标：记录收到的消息，可配置为失败。
#[derive(Clone, Default)]
struct FakeSink {
    sent: Arc<Mutex<Vec<String>>>,
    fail: Arc<Mutex<bool>>,
}

impl FakeSink {
    fn failing() -> Self {
        Self {
            sent: Arc::default(),
            fail: Arc::new(Mutex::new(true)),
        }
    }

    fn set_failing(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }

    fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }
}

impl MessageSink for FakeSink {
    type Error = SinkError;

    async fn send(&self, message: &DelayMessage) -> Result<(), Self::Error> {
        if *self.fail.lock().unwrap() {
            return Err(SinkError("configured to fail"));
        }
        self.sent.lock().unwrap().push(message.message_id.clone());
        Ok(())
    }
}

type Store = StoreHandle<kafka_delaymq_core::MemoryStoreError>;

fn setup() -> (Store, FakeSink, Processor<MemoryStore, FakeSink>) {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = FakeSink::default();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());
    (store, sink, processor)
}

fn task(id: &str, execute_at: i64) -> TimerTask {
    TimerTask::new(id.to_string(), execute_at)
}

// --------------------------- 正常投递 ---------------------------

/// 投递成功后记录必须删除 —— 不删会在重启后重复投递。
#[tokio::test]
async fn successful_delivery_removes_the_record() {
    let (store, sink, processor) = setup();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    let outcome = processor.process(&task("m1", NOW), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::Delivered);
    assert_eq!(sink.sent(), vec!["m1"]);
    assert!(store.get("m1").await.unwrap().unwrap().is_none());
}

#[tokio::test]
async fn missing_message_is_reported_vanished() {
    let (_store, sink, processor) = setup();

    let outcome = processor.process(&task("gone", NOW), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::Vanished);
    assert!(sink.sent().is_empty());
}

/// tombstone 式取消：时间轮没动，到期时发现状态直接跳过。
#[tokio::test]
async fn cancelled_message_is_skipped() {
    let (store, sink, processor) = setup();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();
    store
        .set_status("m1", DelayStatus::Cancelled, NOW)
        .await
        .unwrap()
        .unwrap();

    let outcome = processor.process(&task("m1", NOW), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::Cancelled);
    assert!(sink.sent().is_empty(), "cancelled message was delivered");
}

/// 二次校验：时间轮由单调时钟推进，与 execute_at 的 wall clock 会漂移。
#[tokio::test]
async fn premature_task_is_rescheduled_not_delivered() {
    let (store, sink, processor) = setup();
    store.put(message("m1", NOW + 5_000), NOW).await.unwrap().unwrap();

    let outcome = processor.process(&task("m1", NOW + 5_000), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::Rescheduled);
    assert!(sink.sent().is_empty(), "delivered before its deadline");
    // 状态不该被改动，仍可被后续正常抢占
    let stored = store.get("m1").await.unwrap().unwrap().unwrap();
    assert_eq!(stored.status, DelayStatus::Pending);
}

// --------------------------- 重复投递防线 ---------------------------

/// 核心：重复入轮的第二个 task 必须在 CAS 处退出，不能重复投递。
#[tokio::test]
async fn duplicate_task_loses_the_cas_and_does_not_deliver() {
    let (store, sink, processor) = setup();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    // 第一个 task 正常投递完(记录已删)
    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::Delivered
    );
    // 第二个 task 发现消息已不存在
    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::Vanished
    );

    assert_eq!(sink.sent(), vec!["m1"], "message was delivered twice");
}

/// 已被别人抢占 Delivering 的消息，本 task 必须退出。
#[tokio::test]
async fn already_claimed_message_is_not_delivered_again() {
    let (store, sink, processor) = setup();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    // 模拟另一个 task 已抢到投递权
    store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .unwrap()
        .unwrap();

    let outcome = processor.process(&task("m1", NOW), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::AlreadyClaimed);
    assert!(sink.sent().is_empty());
}

// --------------------------- 重试与死信 ---------------------------

/// 投递失败时记录必须保留，且状态回到 Pending 以便重新抢占。
#[tokio::test]
async fn failed_delivery_keeps_the_record_and_retries() {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = FakeSink::failing();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());

    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    let outcome = processor.process(&task("m1", NOW), NOW).await.unwrap();
    assert_eq!(outcome, Delivery::Retrying { retry_count: 1 });

    let stored = store.get("m1").await.unwrap().unwrap().expect("record kept");
    assert_eq!(stored.retry_count, 1);
    assert_eq!(
        stored.status,
        DelayStatus::Pending,
        "must be claimable again for the retry"
    );
}

/// 重试计数累加，超过上限后转死信。
#[tokio::test]
async fn retries_exhaust_into_dead_letter() {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = FakeSink::failing();
    let dlq = FakeSink::default();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone())
        .with_dead_letter(dlq.clone())
        .with_retry_policy(RetryPolicy {
            max_retries: 2,
            ..RetryPolicy::default()
        });

    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::Retrying { retry_count: 1 }
    );
    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::Retrying { retry_count: 2 }
    );
    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::DeadLettered
    );

    assert_eq!(dlq.sent(), vec!["m1"], "not forwarded to dead letter");
    let stored = store.get("m1").await.unwrap().unwrap().unwrap();
    assert_eq!(stored.status, DelayStatus::Failed);
}

/// 重试过程中恢复的 sink 应当成功投递。
#[tokio::test]
async fn recovered_sink_delivers_on_retry() {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = FakeSink::failing();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink.clone());

    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();
    processor.process(&task("m1", NOW), NOW).await.unwrap();

    sink.set_failing(false);
    let outcome = processor.process(&task("m1", NOW), NOW).await.unwrap();

    assert_eq!(outcome, Delivery::Delivered);
    assert_eq!(sink.sent(), vec!["m1"]);
    assert!(store.get("m1").await.unwrap().unwrap().is_none());
}

/// 没配死信时，耗尽重试仍要标记 Failed 而不是报错。
#[tokio::test]
async fn exhausted_without_dlq_marks_failed() {
    let store = StoreHandle::spawn(MemoryStore::new());
    let sink = FakeSink::failing();
    let processor = Processor::<MemoryStore, _>::new(store.clone(), sink)
        .with_retry_policy(RetryPolicy { max_retries: 0, ..RetryPolicy::default() });

    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    assert_eq!(
        processor.process(&task("m1", NOW), NOW).await.unwrap(),
        Delivery::DeadLettered
    );
    assert_eq!(
        store.get("m1").await.unwrap().unwrap().unwrap().status,
        DelayStatus::Failed
    );
}

#[test]
fn backoff_grows_exponentially_and_caps() {
    let policy = RetryPolicy {
        max_retries: 10,
        base_backoff_ms: 1_000,
        max_backoff_ms: 10_000,
    };

    assert_eq!(policy.backoff_ms(0), 1_000);
    assert_eq!(policy.backoff_ms(1), 2_000);
    assert_eq!(policy.backoff_ms(2), 4_000);
    assert_eq!(policy.backoff_ms(3), 8_000);
    assert_eq!(policy.backoff_ms(4), 10_000, "should cap");
    assert_eq!(policy.backoff_ms(100), 10_000, "must not overflow or panic");
}
