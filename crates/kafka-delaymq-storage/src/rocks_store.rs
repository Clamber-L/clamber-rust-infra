use std::path::Path;

use kafka_delaymq_core::{
    DelayMessage, DelayStatus, DelayStore, PutOutcome, StoredDelayMessage, TimerTask,
};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options, WriteBatch};

use crate::codec::{TS_LEN, encode_ts, index_key};
use crate::error::StorageError;

/// 主数据：`message_id -> StoredDelayMessage`
pub const CF_MESSAGE: &str = "delay_message";

/// 时间索引：`execute_at(8B BE) || message_id -> ()`
pub const CF_BY_EXECUTE_AT: &str = "by_execute_at";

/// RocksDB 实现。
///
/// **本类型不是并发安全的使用方式** —— 虽然 `DB` 自身允许多线程访问，
/// 但 `compare_and_swap_status` 是 read-modify-write，RocksDB 没有原生
/// CAS。正确用法是把本实例交给单个 store worker 线程串行驱动，
/// 见架构文档 §16.2 / §16.3。
pub struct RocksDbStore {
    db: DB,
}

impl RocksDbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_MESSAGE, Options::default()),
            ColumnFamilyDescriptor::new(CF_BY_EXECUTE_AT, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
    }

    fn cf_message(&self) -> Result<&rocksdb::ColumnFamily, StorageError> {
        self.db
            .cf_handle(CF_MESSAGE)
            .ok_or(StorageError::MissingColumnFamily(CF_MESSAGE))
    }

    fn cf_index(&self) -> Result<&rocksdb::ColumnFamily, StorageError> {
        self.db
            .cf_handle(CF_BY_EXECUTE_AT)
            .ok_or(StorageError::MissingColumnFamily(CF_BY_EXECUTE_AT))
    }

    fn load(&self, message_id: &str) -> Result<Option<StoredDelayMessage>, StorageError> {
        match self.db.get_cf(self.cf_message()?, message_id.as_bytes())? {
            None => Ok(None),
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
        }
    }

    /// 写回主数据。调用方负责保证索引仍然一致 —— 仅改状态/重试次数时
    /// `execute_at` 不变，索引无需触碰。
    fn store(&self, stored: &StoredDelayMessage) -> Result<(), StorageError> {
        let bytes = bincode::serialize(stored)?;
        self.db.put_cf(
            self.cf_message()?,
            stored.message.message_id.as_bytes(),
            bytes,
        )?;
        Ok(())
    }

    fn mutate<F>(&self, message_id: &str, f: F) -> Result<(), StorageError>
    where
        F: FnOnce(&mut StoredDelayMessage),
    {
        let mut stored = self
            .load(message_id)?
            .ok_or_else(|| StorageError::NotFound(message_id.to_string()))?;
        f(&mut stored);
        self.store(&stored)
    }
}

impl DelayStore for RocksDbStore {
    type Error = StorageError;

    fn put(&self, message: &DelayMessage, now_ms: i64) -> Result<PutOutcome, Self::Error> {
        let existing = self.load(&message.message_id)?;

        let outcome = match &existing {
            None => PutOutcome::Inserted,
            // payload 相同 → Kafka 重投或改期，允许覆盖
            Some(prev) if prev.message.payload == message.payload => PutOutcome::Replaced,
            // payload 不同 → ID 撞号，拒绝写入，见 §15.1
            Some(_) => return Ok(PutOutcome::Rejected),
        };

        let stored = StoredDelayMessage::new_pending(message.clone(), now_ms);

        // 主数据与索引必须同批落盘，否则崩溃后会出现"有索引无消息"
        // 或"有消息无索引"。
        let mut batch = WriteBatch::default();

        // 改期时 execute_at 变了，旧索引项必须删掉，否则 scan_due 会
        // 返回指向同一 message_id 的两个任务。
        if let Some(prev) = existing
            .as_ref()
            .filter(|prev| prev.message.execute_at != message.execute_at)
        {
            batch.delete_cf(
                self.cf_index()?,
                index_key(prev.message.execute_at, &message.message_id),
            );
        }

        batch.put_cf(
            self.cf_message()?,
            message.message_id.as_bytes(),
            bincode::serialize(&stored)?,
        );
        batch.put_cf(
            self.cf_index()?,
            index_key(message.execute_at, &message.message_id),
            [],
        );

        self.db.write(batch)?;
        Ok(outcome)
    }

    fn get(&self, message_id: &str) -> Result<Option<StoredDelayMessage>, Self::Error> {
        self.load(message_id)
    }

    fn delete(&self, message_id: &str) -> Result<(), Self::Error> {
        let Some(stored) = self.load(message_id)? else {
            return Ok(()); // 幂等
        };

        let mut batch = WriteBatch::default();
        batch.delete_cf(self.cf_message()?, message_id.as_bytes());
        batch.delete_cf(
            self.cf_index()?,
            index_key(stored.message.execute_at, message_id),
        );
        self.db.write(batch)?;
        Ok(())
    }

    fn scan_due(
        &self,
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error> {
        if limit == 0 || from_execute_at > to_execute_at {
            return Ok(Vec::new());
        }

        let upper = encode_ts(to_execute_at);
        let mut out = Vec::new();

        let iter = self.db.iterator_cf(
            self.cf_index()?,
            IteratorMode::From(&encode_ts(from_execute_at), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item?;

            // 上界闭区间：to_execute_at 自身正是"此刻到期"的那些。
            // 比较编码后的前缀即可，无需解码。
            if key.len() < TS_LEN || key[..TS_LEN] > upper[..] {
                break;
            }

            let Some((execute_at, message_id)) = crate::codec::decode_index_key(&key) else {
                continue;
            };

            // 只有 Pending 才该入轮 —— 已抢占投递权的消息不能被预加载器
            // 再次扫出来重复调度。
            match self.load(message_id)? {
                Some(stored) if stored.status == DelayStatus::Pending => {
                    out.push(TimerTask::new(message_id.to_string(), execute_at));
                    if out.len() >= limit {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(out)
    }

    fn compare_and_swap_status(
        &self,
        message_id: &str,
        expect: DelayStatus,
        next: DelayStatus,
        now_ms: i64,
    ) -> Result<bool, Self::Error> {
        // 这里的 read-modify-write 不是原子的。原子性来自调用约定：
        // 整个 store 由单个 worker 线程串行驱动，见类型文档。
        let mut stored = self
            .load(message_id)?
            .ok_or_else(|| StorageError::NotFound(message_id.to_string()))?;

        if stored.status != expect {
            return Ok(false);
        }

        stored.status = next;
        stored.updated_at = now_ms;
        self.store(&stored)?;
        Ok(true)
    }

    fn set_status(
        &self,
        message_id: &str,
        status: DelayStatus,
        now_ms: i64,
    ) -> Result<(), Self::Error> {
        self.mutate(message_id, |stored| {
            stored.status = status;
            stored.updated_at = now_ms;
        })
    }

    fn mark_retry(
        &self,
        message_id: &str,
        retry_count: u32,
        now_ms: i64,
    ) -> Result<(), Self::Error> {
        self.mutate(message_id, |stored| {
            stored.retry_count = retry_count;
            stored.status = DelayStatus::Pending;
            stored.updated_at = now_ms;
        })
    }

    fn scan_stale_delivering(
        &self,
        now_ms: i64,
        timeout_ms: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        // Delivering 卡住是罕见情况，没有专门索引，只在 recovery 时全扫。
        let mut out = Vec::new();
        for item in self.db.iterator_cf(self.cf_message()?, IteratorMode::Start) {
            let (_, bytes) = item?;
            let stored: StoredDelayMessage = bincode::deserialize(&bytes)?;

            if stored.is_stale_delivering(now_ms, timeout_ms) {
                out.push(TimerTask::from(&stored.message));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}
