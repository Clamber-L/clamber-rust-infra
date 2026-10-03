//! 端到端，但跑在真实 RocksDB 上。
//!
//! core 的 pipeline 测试用内存 store，这里换成落盘实现并额外验证
//! **重启后从磁盘重建时间轮** —— 那是内存实现无法覆盖的部分。

use std::sync::{Arc, Mutex};

use kafka_delaymq_core::{
    DelayMessage, DelayStatus, Delivery, MessageSink, Processor, Scheduler, SchedulerConfig,
    StoreHandle,
};
use kafka_delaymq_storage::{RocksDbStore, StorageError};
use tempfile::TempDir;

const NOW: i64 = 1_700_000_000_000;

fn message(id: &str, execute_at: i64) -> DelayMessage {
    DelayMessage {
        message_id: id.to_string(),
        target_topic: "business-topic".to_string(),
        source_partition: 0,
        source_offset: 0,
        execute_at,
        payload: format!("payload-{id}").into_bytes(),
        headers: vec![("trace".into(), id.into())],
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sink down")]
struct SinkError;

#[derive(Clone, Default)]
struct RecordingSink {
    sent: Arc<Mutex<Vec<String>>>,
}

impl RecordingSink {
    fn ids(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }
}

impl MessageSink for RecordingSink {
    type Error = SinkError;

    async fn send(&self, message: &DelayMessage) -> Result<(), Self::Error> {
        self.sent.lock().unwrap().push(message.message_id.clone());
        Ok(())
    }
}

fn open(dir: &TempDir) -> StoreHandle<StorageError> {
    StoreHandle::spawn(RocksDbStore::open(dir.path()).expect("open rocksdb"))
}

/// 完整链路跑在落盘存储上。
#[tokio::test]
async fn full_flow_on_rocksdb() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    let sink = RecordingSink::default();
    let processor = Processor::<RocksDbStore, _>::new(store.clone(), sink.clone());
    let mut sched =
        Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, SchedulerConfig::default())
            .unwrap();

    for i in 1..=10i64 {
        store
            .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
            .await
            .unwrap()
            .unwrap();
    }

    for step in 1..=10 {
        let t = NOW + step * 1_000;
        sched.preload_once(t).await.unwrap();
        let mut due = Vec::new();
        sched.advance_to(t, &mut due);
        for task in &due {
            assert_eq!(
                processor.process(task, t).await.unwrap(),
                Delivery::Delivered
            );
        }
    }

    let ids = sink.ids();
    assert_eq!(ids.len(), 10);
    assert_eq!(ids[0], "m1", "not delivered in deadline order");
    assert_eq!(ids[9], "m10");
}

/// 这是 RocksDB 版本独有的价值：进程重启后数据仍在磁盘上，
/// recovery 能把时间轮完整重建出来。
#[tokio::test]
async fn wheel_is_rebuilt_from_disk_after_restart() {
    let dir = TempDir::new().unwrap();

    // 第一个进程：写入并入轮，然后"崩溃"
    {
        let store = open(&dir);
        for i in 1..=5i64 {
            store
                .put(message(&format!("m{i}"), NOW + i * 1_000), NOW)
                .await
                .unwrap()
                .unwrap();
        }
        let mut sched =
            Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, SchedulerConfig::default())
                .unwrap();
        sched.preload_once(NOW).await.unwrap();
        assert_eq!(sched.pending_in_wheel(), 5);
    } // store handle 与时间轮一起消失，DB 关闭

    // 第二个进程：重开同一目录
    let store = open(&dir);
    let sink = RecordingSink::default();
    let processor = Processor::<RocksDbStore, _>::new(store.clone(), sink.clone());
    let mut sched =
        Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, SchedulerConfig::default())
            .unwrap();

    let stats = sched.recover(NOW).await.unwrap();
    assert_eq!(stats.loaded, 5, "wheel not rebuilt from disk");

    let mut due = Vec::new();
    sched.advance_to(NOW + 5_000, &mut due);
    for task in &due {
        processor.process(task, NOW + 5_000).await.unwrap();
    }

    let mut ids = sink.ids();
    ids.sort();
    assert_eq!(ids, vec!["m1", "m2", "m3", "m4", "m5"]);
}

/// 已投递的消息在重启后不该复活 —— 投递成功时记录已删除。
#[tokio::test]
async fn delivered_messages_do_not_resurrect_after_restart() {
    let dir = TempDir::new().unwrap();

    {
        let store = open(&dir);
        let sink = RecordingSink::default();
        let processor = Processor::<RocksDbStore, _>::new(store.clone(), sink.clone());
        let mut sched = Scheduler::<RocksDbStore>::with_origin(
            store.clone(),
            NOW,
            SchedulerConfig::default(),
        )
        .unwrap();

        store.put(message("m1", NOW), NOW).await.unwrap().unwrap();
        store.put(message("m2", NOW + 60_000), NOW).await.unwrap().unwrap();

        sched.preload_once(NOW).await.unwrap();
        let mut due = Vec::new();
        sched.advance_to(NOW, &mut due);
        for t in &due {
            processor.process(t, NOW).await.unwrap();
        }
        assert_eq!(sink.ids(), vec!["m1"]);
    }

    // 重启后只剩未投递的那条
    let store = open(&dir);
    let mut sched =
        Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, SchedulerConfig::default())
            .unwrap();

    let stats = sched.recover(NOW).await.unwrap();
    assert_eq!(stats.loaded, 1, "delivered message came back");
    assert!(store.get("m1").await.unwrap().unwrap().is_none());
    assert!(store.get("m2").await.unwrap().unwrap().is_some());
}

/// 跨重启的 Delivering 恢复 —— 最接近真实崩溃场景的一条。
#[tokio::test]
async fn delivering_stuck_across_restart_is_recovered() {
    let dir = TempDir::new().unwrap();
    let config = SchedulerConfig::default();

    {
        let store = open(&dir);
        store.put(message("stuck", NOW), NOW).await.unwrap().unwrap();
        // 抢到投递权之后进程就没了
        store
            .compare_and_swap_status("stuck", DelayStatus::Pending, DelayStatus::Delivering, NOW)
            .await
            .unwrap()
            .unwrap();
    }

    let store = open(&dir);
    // 状态确实留在磁盘上
    assert_eq!(
        store.get("stuck").await.unwrap().unwrap().unwrap().status,
        DelayStatus::Delivering
    );

    let sink = RecordingSink::default();
    let processor = Processor::<RocksDbStore, _>::new(store.clone(), sink.clone());
    let mut sched = Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, config).unwrap();

    let later = NOW + config.delivering_timeout_ms * 2;
    assert_eq!(sched.recover(later).await.unwrap().loaded, 1);

    let mut due = Vec::new();
    sched.advance_to(later, &mut due);
    for t in &due {
        assert_eq!(processor.process(t, later).await.unwrap(), Delivery::Delivered);
    }
    assert_eq!(sink.ids(), vec!["stuck"]);
}

/// 较大批量，确认索引顺序与 limit 在真实存储上表现一致。
#[tokio::test]
async fn larger_batch_delivers_in_order() {
    let dir = TempDir::new().unwrap();
    let store = open(&dir);
    let sink = RecordingSink::default();
    let processor = Processor::<RocksDbStore, _>::new(store.clone(), sink.clone());
    let mut sched =
        Scheduler::<RocksDbStore>::with_origin(store.clone(), NOW, SchedulerConfig::default())
            .unwrap();

    // 倒序写入，确保投递顺序来自索引而非写入顺序
    for i in (1..=200i64).rev() {
        store
            .put(message(&format!("m{i:03}"), NOW + i * 1_000), NOW)
            .await
            .unwrap()
            .unwrap();
    }

    sched.preload_once(NOW).await.unwrap();
    let mut due = Vec::new();
    sched.advance_to(NOW + 200_000, &mut due);

    for task in &due {
        processor.process(task, NOW + 200_000).await.unwrap();
    }

    let ids = sink.ids();
    assert_eq!(ids.len(), 200);
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "delivery order did not follow deadlines");
}
