//! RocksDB 特有的行为：重启后的持久性、两个 CF 的一致性、
//! 以及索引排序在真实 key 比较下成立。

use kafka_delaymq_core::{DelayMessage, DelayStatus, DelayStore};
use kafka_delaymq_storage::{CF_BY_EXECUTE_AT, CF_MESSAGE, RocksDbStore};
use tempfile::TempDir;

const NOW: i64 = 1_700_000_000_000;

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 3,
        source_offset: 777,
        execute_at,
        payload: b"payload bytes".to_vec(),
        headers: vec![("k".to_string(), "v".to_string())],
    }
}

/// 核心价值：进程重启后数据还在。这是时间轮(纯内存)做不到的部分。
#[test]
fn data_survives_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let store = RocksDbStore::open(dir.path()).unwrap();
        store.put(&message("m1", NOW + 5_000), NOW).unwrap();
        store.put(&message("m2", NOW + 9_000), NOW).unwrap();
    } // store 析构，DB 关闭

    let store = RocksDbStore::open(dir.path()).unwrap();

    let stored = store.get("m1").unwrap().expect("m1 should survive");
    assert_eq!(stored.message.execute_at, NOW + 5_000);
    assert_eq!(stored.message.payload, b"payload bytes");
    assert_eq!(stored.status, DelayStatus::Pending);

    // 索引也要能用 —— recovery 依赖它
    let due = store.scan_due(0, NOW + 100_000, 100).unwrap();
    assert_eq!(due.len(), 2);
    assert_eq!(due[0].message_id, "m1");
    assert_eq!(due[1].message_id, "m2");
}

/// 状态变更也要落盘，否则 recovery 会把已投递的消息重新调度。
#[test]
fn status_changes_survive_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let store = RocksDbStore::open(dir.path()).unwrap();
        store.put(&message("m1", NOW), NOW).unwrap();
        store
            .compare_and_swap_status("m1", DelayStatus::Pending, DelayStatus::Delivering)
            .unwrap();
        store.mark_retry("m1", 3).unwrap();
    }

    let store = RocksDbStore::open(dir.path()).unwrap();
    let stored = store.get("m1").unwrap().unwrap();

    assert_eq!(stored.retry_count, 3);
    assert_eq!(stored.status, DelayStatus::Pending);
}

/// 两个 CF 必须同步 —— 删除后索引里不能残留孤儿项。
/// 直接查底层 CF，绕过 scan_due 的 Pending 过滤。
#[test]
fn delete_leaves_no_orphan_in_either_cf() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();

    store.put(&message("m1", NOW + 1_000), NOW).unwrap();
    store.delete("m1").unwrap();

    let db = rocksdb::DB::open_cf_for_read_only(
        &rocksdb::Options::default(),
        dir.path(),
        [CF_MESSAGE, CF_BY_EXECUTE_AT],
        false,
    )
    .unwrap();

    let msg_cf = db.cf_handle(CF_MESSAGE).unwrap();
    let idx_cf = db.cf_handle(CF_BY_EXECUTE_AT).unwrap();

    assert_eq!(
        db.iterator_cf(msg_cf, rocksdb::IteratorMode::Start).count(),
        0,
        "main CF still holds the message"
    );
    assert_eq!(
        db.iterator_cf(idx_cf, rocksdb::IteratorMode::Start).count(),
        0,
        "index CF still holds an orphan entry"
    );
}

/// 改期时旧索引项必须删掉，否则索引 CF 会膨胀且 scan 返回重复任务。
#[test]
fn reschedule_leaves_exactly_one_index_entry() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();

    store.put(&message("m1", NOW + 1_000), NOW).unwrap();
    store.put(&message("m1", NOW + 2_000), NOW).unwrap();
    store.put(&message("m1", NOW + 3_000), NOW).unwrap();

    let db = rocksdb::DB::open_cf_for_read_only(
        &rocksdb::Options::default(),
        dir.path(),
        [CF_MESSAGE, CF_BY_EXECUTE_AT],
        false,
    )
    .unwrap();
    let idx_cf = db.cf_handle(CF_BY_EXECUTE_AT).unwrap();

    assert_eq!(
        db.iterator_cf(idx_cf, rocksdb::IteratorMode::Start).count(),
        1,
        "stale index entries accumulated across reschedules"
    );
}

/// 真实 RocksDB 的字节序比较下，索引顺序必须等于到期顺序。
/// 这是 codec 单测在存储层的对应验证 —— 小端编码会在这里失败。
#[test]
fn index_ordering_holds_across_wide_time_range() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();

    // 刻意乱序插入，跨越负数与大正数
    let deadlines = [NOW + 500, -86_400_000, 0, NOW, NOW + 100, 1, -1];
    for (i, &d) in deadlines.iter().enumerate() {
        store.put(&message(&format!("m{i}"), d), NOW).unwrap();
    }

    let due = store.scan_due(i64::MIN, i64::MAX, 100).unwrap();
    let got: Vec<i64> = due.iter().map(|t| t.execute_at).collect();

    let mut expected = deadlines.to_vec();
    expected.sort_unstable();
    assert_eq!(got, expected, "index byte order != time order");
}

/// 大批量下 limit 截断仍返回最早的那些，且顺序正确。
#[test]
fn scan_due_returns_earliest_under_limit_at_scale() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();

    for i in 0..500i64 {
        // 倒序插入，确保结果顺序来自索引而非插入顺序
        let d = NOW + (500 - i) * 1_000;
        store.put(&message(&format!("m{i}"), d), NOW).unwrap();
    }

    let due = store.scan_due(0, i64::MAX, 10).unwrap();
    assert_eq!(due.len(), 10);

    let mut sorted = due.clone();
    sorted.sort_by_key(|t| t.execute_at);
    assert_eq!(due, sorted, "not returned in deadline order");
    assert_eq!(due[0].execute_at, NOW + 1_000, "did not start at the earliest");
}

/// scan_due 的边界与退化输入。
#[test]
fn scan_due_handles_degenerate_ranges() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();
    store.put(&message("m1", NOW), NOW).unwrap();

    assert!(store.scan_due(0, i64::MAX, 0).unwrap().is_empty(), "limit 0");
    assert!(
        store.scan_due(NOW + 1, NOW, 100).unwrap().is_empty(),
        "inverted range"
    );
    assert_eq!(
        store.scan_due(NOW, NOW, 100).unwrap().len(),
        1,
        "single-point range must be inclusive"
    );
}

#[test]
fn reopening_an_existing_db_does_not_lose_column_families() {
    let dir = TempDir::new().unwrap();

    for round in 0..3 {
        let store = RocksDbStore::open(dir.path()).unwrap();
        store
            .put(&message(&format!("m{round}"), NOW + round * 1_000), NOW)
            .unwrap();
        let count = store.scan_due(0, i64::MAX, 100).unwrap().len();
        assert_eq!(count as i64, round + 1, "round {round} lost data");
    }
}

/// 空 payload 与较大 payload 都要能正确往返。
#[test]
fn payload_edge_sizes_roundtrip() {
    let dir = TempDir::new().unwrap();
    let store = RocksDbStore::open(dir.path()).unwrap();

    let mut empty = message("m1", NOW);
    empty.payload = Vec::new();
    store.put(&empty, NOW).unwrap();

    let mut big = message("m2", NOW);
    big.payload = vec![0xab; 256 * 1024];
    store.put(&big, NOW).unwrap();

    assert!(store.get("m1").unwrap().unwrap().message.payload.is_empty());
    let got = store.get("m2").unwrap().unwrap();
    assert_eq!(got.message.payload.len(), 256 * 1024);
    assert!(got.message.payload.iter().all(|&b| b == 0xab));
}
