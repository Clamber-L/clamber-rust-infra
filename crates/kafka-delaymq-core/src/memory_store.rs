use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::message::{DelayMessage, DelayStatus, StoredDelayMessage};
use crate::store::DelayStore;
use crate::task::TimerTask;

/// 内存实现，用于测试与本地验证。
///
/// 刻意复刻 RocksDB 的两份结构，这样针对 `DelayStore` 写的测试对两种
/// 实现都有意义：
///
/// ```text
/// messages  HashMap  message_id -> StoredDelayMessage   (CF: delay_message)
/// by_due    BTreeMap (execute_at, message_id) -> ()     (CF: by_execute_at)
/// ```
///
/// `BTreeMap` 的有序性对应 RocksDB 按字节序排序的时间索引 —— 两者都要
/// 求"字节序 = 时间序"，这正是真实实现里 `execute_at` 必须用大端编码
/// 的原因。
#[derive(Debug, Default)]
pub struct MemoryStore {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    messages: BTreeMap<String, StoredDelayMessage>,
    by_due: BTreeMap<(i64, String), ()>,
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryStoreError {
    #[error("message {0} not found")]
    NotFound(String),

    #[error("store lock poisoned")]
    LockPoisoned,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.read().map(|i| i.messages.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl DelayStore for MemoryStore {
    type Error = MemoryStoreError;

    fn put(&self, message: &DelayMessage, now_ms: i64) -> Result<(), Self::Error> {
        let mut inner = self.inner.write().map_err(|_| MemoryStoreError::LockPoisoned)?;

        // 覆盖写时先清掉旧索引，否则 execute_at 变更会留下孤儿索引项。
        if let Some(old) = inner.messages.get(&message.message_id) {
            let old_key = (old.message.execute_at, message.message_id.clone());
            inner.by_due.remove(&old_key);
        }

        let stored = StoredDelayMessage::new_pending(message.clone(), now_ms);
        inner
            .by_due
            .insert((message.execute_at, message.message_id.clone()), ());
        inner.messages.insert(message.message_id.clone(), stored);
        Ok(())
    }

    fn get(&self, message_id: &str) -> Result<Option<StoredDelayMessage>, Self::Error> {
        let inner = self.inner.read().map_err(|_| MemoryStoreError::LockPoisoned)?;
        Ok(inner.messages.get(message_id).cloned())
    }

    fn delete(&self, message_id: &str) -> Result<(), Self::Error> {
        let mut inner = self.inner.write().map_err(|_| MemoryStoreError::LockPoisoned)?;
        if let Some(stored) = inner.messages.remove(message_id) {
            inner
                .by_due
                .remove(&(stored.message.execute_at, message_id.to_string()));
        }
        Ok(())
    }

    fn scan_due(
        &self,
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error> {
        let inner = self.inner.read().map_err(|_| MemoryStoreError::LockPoisoned)?;

        // 闭区间 [from, to]：to 自身也该被收进来，它正是"此刻到期"的那些。
        let lo = (from_execute_at, String::new());
        Ok(inner
            .by_due
            .range(lo..)
            .take_while(|((due, _), _)| *due <= to_execute_at)
            .filter(|((_, id), _)| {
                inner
                    .messages
                    .get(id)
                    .is_some_and(|m| m.status == DelayStatus::Pending)
            })
            .take(limit)
            .map(|((due, id), _)| TimerTask::new(id.clone(), *due))
            .collect())
    }

    fn compare_and_swap_status(
        &self,
        message_id: &str,
        expect: DelayStatus,
        next: DelayStatus,
    ) -> Result<bool, Self::Error> {
        let mut inner = self.inner.write().map_err(|_| MemoryStoreError::LockPoisoned)?;
        let stored = inner
            .messages
            .get_mut(message_id)
            .ok_or_else(|| MemoryStoreError::NotFound(message_id.to_string()))?;

        if stored.status != expect {
            return Ok(false);
        }
        stored.status = next;
        stored.updated_at = crate::clock::wall_clock_ms();
        Ok(true)
    }

    fn set_status(&self, message_id: &str, status: DelayStatus) -> Result<(), Self::Error> {
        let mut inner = self.inner.write().map_err(|_| MemoryStoreError::LockPoisoned)?;
        let stored = inner
            .messages
            .get_mut(message_id)
            .ok_or_else(|| MemoryStoreError::NotFound(message_id.to_string()))?;
        stored.status = status;
        stored.updated_at = crate::clock::wall_clock_ms();
        Ok(())
    }

    fn mark_retry(&self, message_id: &str, retry_count: u32) -> Result<(), Self::Error> {
        let mut inner = self.inner.write().map_err(|_| MemoryStoreError::LockPoisoned)?;
        let stored = inner
            .messages
            .get_mut(message_id)
            .ok_or_else(|| MemoryStoreError::NotFound(message_id.to_string()))?;
        stored.retry_count = retry_count;
        stored.status = DelayStatus::Pending;
        stored.updated_at = crate::clock::wall_clock_ms();
        Ok(())
    }

    fn scan_stale_delivering(
        &self,
        now_ms: i64,
        timeout_ms: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>, Self::Error> {
        let inner = self.inner.read().map_err(|_| MemoryStoreError::LockPoisoned)?;
        Ok(inner
            .messages
            .values()
            .filter(|m| m.is_stale_delivering(now_ms, timeout_ms))
            .take(limit)
            .map(|m| TimerTask::from(&m.message))
            .collect())
    }
}
