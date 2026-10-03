//! 单线程 store worker。串行化是 CAS 正确性的来源，见架构文档 §16.3。

use std::sync::Arc;

use kafka_delaymq_core::{DelayMessage, DelayStatus, MemoryStore, PutOutcome, StoreHandle};

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

fn handle() -> StoreHandle<kafka_delaymq_core::MemoryStoreError> {
    StoreHandle::spawn(MemoryStore::new())
}

#[tokio::test]
async fn put_and_get_through_the_worker() {
    let store = handle();

    let outcome = store.put(message("m1", NOW + 1_000), NOW).await.unwrap().unwrap();
    assert_eq!(outcome, PutOutcome::Inserted);

    let stored = store.get("m1").await.unwrap().unwrap().expect("exists");
    assert_eq!(stored.message.execute_at, NOW + 1_000);
}

#[tokio::test]
async fn all_commands_round_trip() {
    let store = handle();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    assert!(
        store
            .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
            .await
            .unwrap()
            .unwrap()
    );
    store.mark_retry("m1", 2, NOW).await.unwrap().unwrap();
    store
        .set_status("m1", DelayStatus::Failed, NOW)
        .await
        .unwrap()
        .unwrap();

    let stored = store.get("m1").await.unwrap().unwrap().unwrap();
    assert_eq!(stored.retry_count, 2);
    assert_eq!(stored.status, DelayStatus::Failed);

    assert!(store.scan_due(0, i64::MAX, 10).await.unwrap().unwrap().is_empty());
    assert!(
        store
            .scan_stale_delivering(NOW, 1_000, 10)
            .await
            .unwrap()
            .unwrap()
            .is_empty()
    );

    store.delete("m1").await.unwrap().unwrap();
    assert!(store.get("m1").await.unwrap().unwrap().is_none());
}

/// 核心价值：并发 CAS 只能有一个赢家。串行化让这一点无需额外同步。
#[tokio::test]
async fn concurrent_cas_has_exactly_one_winner() {
    let store = Arc::new(handle());
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    let mut tasks = Vec::new();
    for _ in 0..32 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            store
                .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
                .await
                .unwrap()
                .unwrap()
        }));
    }

    let mut winners = 0;
    for t in tasks {
        if t.await.unwrap() {
            winners += 1;
        }
    }

    assert_eq!(winners, 1, "exactly one claimant may win the delivery right");
}

/// 并发写不同消息时不应互相干扰。
#[tokio::test]
async fn concurrent_puts_all_land() {
    let store = Arc::new(handle());

    let mut tasks = Vec::new();
    for i in 0..64i64 {
        let store = Arc::clone(&store);
        tasks.push(tokio::spawn(async move {
            store
                .put(message(&format!("m{i}"), NOW + i), NOW)
                .await
                .unwrap()
                .unwrap()
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), PutOutcome::Inserted);
    }

    let due = store.scan_due(0, i64::MAX, 1_000).await.unwrap().unwrap();
    assert_eq!(due.len(), 64);
}

#[tokio::test]
async fn handle_is_cloneable_and_shares_one_store() {
    let store = handle();
    let clone = store.clone();

    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    // 克隆看到同一份数据
    assert!(clone.get("m1").await.unwrap().unwrap().is_some());
    assert!(!clone.is_finished());
}

/// 业务错误(撞号)要原样穿过 worker 回到调用方，不能被当成传输故障。
#[tokio::test]
async fn store_level_outcomes_propagate() {
    let store = handle();
    store.put(message("m1", NOW), NOW).await.unwrap().unwrap();

    let mut intruder = message("m1", NOW + 1_000);
    intruder.payload = b"different".to_vec();

    let outcome = store.put(intruder, NOW).await.unwrap().unwrap();
    assert_eq!(outcome, PutOutcome::Rejected);
}

/// 存储层的 Err 也要穿过来 —— MemoryStore 对不存在的消息报 NotFound。
#[tokio::test]
async fn store_errors_propagate() {
    let store = handle();

    let result = store
        .compare_and_swap_status("missing", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .await
        .expect("worker alive");

    assert!(result.is_err(), "NotFound should surface as a store error");
}
