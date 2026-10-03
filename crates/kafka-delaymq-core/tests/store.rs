//! `DelayStore` 的契约测试。全部针对 trait 而非具体实现，Step 4 实现
//! `RocksDbStore` 之后同一批断言应当照样通过。

use kafka_delaymq_core::{DelayMessage, DelayStatus, DelayStore, MemoryStore, PutOutcome};

const NOW: i64 = 1_700_000_000_000;

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 42,
        execute_at,
        payload: b"hello".to_vec(),
        headers: vec![("trace".to_string(), "abc".to_string())],
    }
}

#[test]
fn put_then_get_roundtrips() {
    let store = MemoryStore::new();
    let msg = message("m1", NOW + 5_000);
    store.put(&msg, NOW).unwrap();

    let stored = store.get("m1").unwrap().expect("message should exist");
    assert_eq!(stored.message, msg);
    assert_eq!(stored.status, DelayStatus::Pending);
    assert_eq!(stored.retry_count, 0);
    assert_eq!(stored.created_at, NOW);
}

#[test]
fn get_missing_returns_none() {
    let store = MemoryStore::new();
    assert!(store.get("nope").unwrap().is_none());
}

#[test]
fn delete_removes_message_and_index() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW + 1_000), NOW).unwrap();

    store.delete("m1").unwrap();

    assert!(store.get("m1").unwrap().is_none());
    // 索引也要清掉，否则 scan 会返回指向已删消息的孤儿任务
    let due = store.scan_due(0, NOW + 10_000, 100).unwrap();
    assert!(due.is_empty(), "stale index entry survived delete");
}

#[test]
fn delete_is_idempotent() {
    let store = MemoryStore::new();
    store.delete("never-existed").unwrap();
}

/// scan_due 必须按 execute_at 升序返回 —— 这是预加载器正确性的前提。
#[test]
fn scan_due_returns_tasks_in_deadline_order() {
    let store = MemoryStore::new();
    // 刻意乱序插入
    store.put(&message("c", NOW + 3_000), NOW).unwrap();
    store.put(&message("a", NOW + 1_000), NOW).unwrap();
    store.put(&message("b", NOW + 2_000), NOW).unwrap();

    let due = store.scan_due(0, NOW + 10_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

/// 插入顺序与 message_id 的字典序都不应影响扫描顺序，只有 execute_at 作数。
#[test]
fn scan_order_ignores_message_id_ordering() {
    let store = MemoryStore::new();
    // id 的字典序与到期顺序刻意相反
    store.put(&message("zzz", NOW + 1_000), NOW).unwrap();
    store.put(&message("aaa", NOW + 9_000), NOW).unwrap();

    let due = store.scan_due(0, NOW + 10_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["zzz", "aaa"], "scan followed id order, not deadline");
}

#[test]
fn scan_due_respects_upper_bound_inclusively() {
    let store = MemoryStore::new();
    store.put(&message("inside", NOW + 1_000), NOW).unwrap();
    store.put(&message("boundary", NOW + 5_000), NOW).unwrap();
    store.put(&message("outside", NOW + 5_001), NOW).unwrap();

    let due = store.scan_due(0, NOW + 5_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["inside", "boundary"]);
}

#[test]
fn scan_due_respects_lower_bound() {
    let store = MemoryStore::new();
    store.put(&message("early", NOW + 1_000), NOW).unwrap();
    store.put(&message("late", NOW + 8_000), NOW).unwrap();

    let due = store.scan_due(NOW + 2_000, NOW + 10_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["late"]);
}

#[test]
fn scan_due_honours_limit() {
    let store = MemoryStore::new();
    for i in 0..10 {
        store
            .put(&message(&format!("m{i}"), NOW + i * 1_000), NOW)
            .unwrap();
    }

    let due = store.scan_due(0, NOW + 100_000, 3).unwrap();
    assert_eq!(due.len(), 3);
    // limit 截断后仍应是最早的三个
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["m0", "m1", "m2"]);
}

/// 已抢占投递权的消息不该再被预加载器扫出来重复入轮。
#[test]
fn scan_due_skips_non_pending() {
    let store = MemoryStore::new();
    store.put(&message("pending", NOW + 1_000), NOW).unwrap();
    store.put(&message("delivering", NOW + 1_000), NOW).unwrap();
    store
        .compare_and_swap_status("delivering", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();

    let due = store.scan_due(0, NOW + 10_000, 100).unwrap();
    let ids: Vec<&str> = due.iter().map(|t| t.message_id.as_str()).collect();
    assert_eq!(ids, vec!["pending"]);
}

#[test]
fn cas_succeeds_on_expected_status() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW), NOW).unwrap();

    let won = store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();
    assert!(won);
    assert_eq!(store.get("m1").unwrap().unwrap().status, DelayStatus::Delivering);
}

/// 防重复投递的核心：第二个 timer task 必须抢不到投递权。
#[test]
fn cas_rejects_second_claimant() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW), NOW).unwrap();

    let first = store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();
    let second = store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();

    assert!(first, "first claimant should win");
    assert!(!second, "second claimant must lose — duplicate delivery");
}

#[test]
fn mark_retry_bumps_count_and_returns_to_pending() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW), NOW).unwrap();
    store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();

    store.mark_retry("m1", 1, NOW).unwrap();

    let stored = store.get("m1").unwrap().unwrap();
    assert_eq!(stored.retry_count, 1);
    assert_eq!(
        stored.status,
        DelayStatus::Pending,
        "a retryable message must become claimable again"
    );
}

#[test]
fn stale_delivering_is_detected_after_timeout() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW), NOW).unwrap();
    store
        .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering, NOW)
        .unwrap();

    let updated_at = store.get("m1").unwrap().unwrap().updated_at;

    // 未超时
    assert!(store
        .scan_stale_delivering(updated_at + 1_000, 30_000, 100)
        .unwrap()
        .is_empty());

    // 已超时
    let stale = store
        .scan_stale_delivering(updated_at + 60_000, 30_000, 100)
        .unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].message_id, "m1");
}

#[test]
fn pending_messages_are_never_reported_stale() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW), NOW).unwrap();

    let stale = store
        .scan_stale_delivering(NOW + 1_000_000, 1_000, 100)
        .unwrap();
    assert!(stale.is_empty(), "Pending must not be swept by recovery");
}

/// 覆盖写若改了 execute_at，旧索引项必须一并清除。
#[test]
fn rescheduling_replaces_the_time_index_entry() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW + 1_000), NOW).unwrap();
    store.put(&message("m1", NOW + 9_000), NOW).unwrap();

    let due = store.scan_due(0, NOW + 100_000, 100).unwrap();
    assert_eq!(due.len(), 1, "orphaned index entry from the old deadline");
    assert_eq!(due[0].execute_at, NOW + 9_000);
}

#[test]
fn terminal_statuses_are_classified() {
    assert!(DelayStatus::Delivered.is_terminal());
    assert!(DelayStatus::Failed.is_terminal());
    assert!(DelayStatus::Cancelled.is_terminal());
    assert!(!DelayStatus::Pending.is_terminal());
    assert!(!DelayStatus::Delivering.is_terminal());
}

// ---------------------------------------------------------------------
// message_id 由业务方生成(雪花 ID)，唯一性不由本服务保证。
// put 必须区分"Kafka 重投"与"ID 撞号"，见架构文档 §15.1。
// ---------------------------------------------------------------------

#[test]
fn first_put_reports_inserted() {
    let store = MemoryStore::new();
    let outcome = store.put(&message("m1", NOW + 1_000), NOW).unwrap();

    assert_eq!(outcome, PutOutcome::Inserted);
    assert!(outcome.is_stored());
}

/// Kafka 重投：同 ID 同 payload，应当覆盖而非报错。
#[test]
fn redelivery_with_same_payload_is_replaced() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW + 1_000), NOW).unwrap();

    let outcome = store.put(&message("m1", NOW + 1_000), NOW + 50).unwrap();

    assert_eq!(outcome, PutOutcome::Replaced);
    assert!(store.get("m1").unwrap().is_some());
}

/// 改期：payload 不变只改 execute_at，属于正常覆盖。
#[test]
fn reschedule_keeps_payload_and_is_replaced() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW + 1_000), NOW).unwrap();

    let outcome = store.put(&message("m1", NOW + 9_000), NOW).unwrap();
    assert_eq!(outcome, PutOutcome::Replaced);

    // 新 deadline 生效，且没有留下指向旧 deadline 的孤儿索引
    let due = store.scan_due(0, NOW + 100_000, 100).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].execute_at, NOW + 9_000);
}

/// ID 撞号：同 ID 不同 payload。必须拒绝 —— 静默覆盖等于丢掉已存在的消息。
#[test]
fn colliding_id_with_different_payload_is_rejected() {
    let store = MemoryStore::new();
    store.put(&message("m1", NOW + 1_000), NOW).unwrap();

    let mut intruder = message("m1", NOW + 2_000);
    intruder.payload = b"a different message entirely".to_vec();

    let outcome = store.put(&intruder, NOW).unwrap();
    assert_eq!(outcome, PutOutcome::Rejected);
    assert!(!outcome.is_stored());
}

/// 撞号被拒后，原消息必须完好无损 —— 这正是拒绝的目的。
#[test]
fn rejected_collision_leaves_original_intact() {
    let store = MemoryStore::new();
    let original = message("m1", NOW + 1_000);
    store.put(&original, NOW).unwrap();

    let mut intruder = message("m1", NOW + 2_000);
    intruder.payload = b"intruder".to_vec();
    store.put(&intruder, NOW).unwrap();

    let stored = store.get("m1").unwrap().unwrap();
    assert_eq!(stored.message, original, "original was overwritten");

    // 索引也不该被动过
    let due = store.scan_due(0, NOW + 100_000, 100).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].execute_at, NOW + 1_000);
}

/// 不同 ID 携带相同 payload 是完全正常的，不该被当成撞号。
#[test]
fn distinct_ids_with_identical_payload_both_stored() {
    let store = MemoryStore::new();

    assert_eq!(
        store.put(&message("m1", NOW + 1_000), NOW).unwrap(),
        PutOutcome::Inserted
    );
    assert_eq!(
        store.put(&message("m2", NOW + 1_000), NOW).unwrap(),
        PutOutcome::Inserted
    );

    assert_eq!(store.scan_due(0, NOW + 100_000, 100).unwrap().len(), 2);
}
