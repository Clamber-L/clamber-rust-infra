//! 到期处理：从存储取回消息、抢占投递权、投递、删除。
//!
//! 投递目标抽象成 [`MessageSink`]，所以这一层不依赖 Kafka —— `core`
//! 不该知道 rdkafka 的存在，见架构文档 §22。

use crate::message::{DelayMessage, DelayStatus};
use crate::store::DelayStore;
use crate::task::TimerTask;
use crate::worker::{StoreHandle, WorkerGone};

/// 投递目标。由 `kafka-delaymq-kafka` 提供 Kafka 实现。
pub trait MessageSink {
    type Error: std::error::Error + Send + Sync + 'static;

    /// 投递消息，**必须等到对端确认才返回 Ok**。
    ///
    /// 仅把消息放进本地发送队列就返回，会让上层误判投递成功并删除
    /// 持久化记录 —— 之后发送失败的话消息就永久丢了。见架构文档 §13。
    fn send(
        &self,
        message: &DelayMessage,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// 一条到期任务的处理结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// 投递成功，记录已删除。
    Delivered,

    /// 消息已不存在 —— 早前已投递并删除。
    Vanished,

    /// 已被取消，跳过。
    Cancelled,

    /// 未到期，已重新入轮。时间轮由单调时钟推进，与 wall clock 会漂移，
    /// 所以投递前必须复核 `execute_at`。
    Rescheduled,

    /// 投递权被其他 task 抢走 —— 重复入轮的那一个在此退出。
    AlreadyClaimed,

    /// 投递失败，已安排重试。
    Retrying { retry_count: u32 },

    /// 重试耗尽，已转入死信。
    DeadLettered,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError<SE, KE> {
    #[error("store: {0}")]
    Store(SE),

    #[error(transparent)]
    WorkerGone(#[from] WorkerGone),

    #[error("dead letter sink: {0}")]
    DeadLetter(KE),
}

/// 重试与死信策略。
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// 超过该次数转入死信。
    pub max_retries: u32,

    /// 首次重试的退避时长，之后指数增长。
    pub base_backoff_ms: i64,

    /// 退避上限。
    pub max_backoff_ms: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_backoff_ms: 1_000,
            max_backoff_ms: 5 * 60 * 1_000,
        }
    }
}

impl RetryPolicy {
    /// 第 `retry_count` 次重试的退避时长。指数退避，带上限。
    pub fn backoff_ms(&self, retry_count: u32) -> i64 {
        // 移位超过 63 会 panic，而且早就撞上 max_backoff 了
        let shift = retry_count.min(32);
        self.base_backoff_ms
            .saturating_mul(1i64 << shift)
            .min(self.max_backoff_ms)
    }
}

/// 到期处理器。
pub struct Processor<S: DelayStore, K: MessageSink> {
    store: StoreHandle<S::Error>,
    sink: K,
    dead_letter: Option<K>,
    policy: RetryPolicy,
}

impl<S: DelayStore, K: MessageSink> Processor<S, K> {
    pub fn new(store: StoreHandle<S::Error>, sink: K) -> Self {
        Self {
            store,
            sink,
            dead_letter: None,
            policy: RetryPolicy::default(),
        }
    }

    pub fn with_dead_letter(mut self, sink: K) -> Self {
        self.dead_letter = Some(sink);
        self
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// 处理一条到期任务。
    ///
    /// 返回的 `Delivery::Rescheduled` 需要调用方把任务重新放回时间轮 ——
    /// 处理器不持有时间轮，这让它可以独立于调度器测试。
    pub async fn process(
        &self,
        task: &TimerTask,
        now_ms: i64,
    ) -> Result<Delivery, ProcessError<S::Error, K::Error>> {
        let stored = self
            .store
            .get(&task.message_id)
            .await?
            .map_err(ProcessError::Store)?;

        let Some(stored) = stored else {
            return Ok(Delivery::Vanished);
        };

        if stored.status == DelayStatus::Cancelled {
            return Ok(Delivery::Cancelled);
        }

        // 二次校验：时间轮的 tick 由单调时钟推进，与 execute_at 所用的
        // wall clock 会漂移；recovery 与预加载也可能让任务提前到这里。
        if now_ms < stored.message.execute_at {
            return Ok(Delivery::Rescheduled);
        }

        // 抢占投递权。重复入轮的第二个 task 会在这里失败并退出。
        let won = self
            .store
            .compare_and_swap_status(
                &task.message_id,
                DelayStatus::Pending,
                DelayStatus::Delivering,
                now_ms,
            )
            .await?
            .map_err(ProcessError::Store)?;

        if !won {
            return Ok(Delivery::AlreadyClaimed);
        }

        match self.sink.send(&stored.message).await {
            Ok(()) => {
                // 对端已确认，现在才能删
                self.store
                    .delete(&task.message_id)
                    .await?
                    .map_err(ProcessError::Store)?;
                Ok(Delivery::Delivered)
            }

            Err(err) => {
                let retry_count = stored.retry_count + 1;

                if retry_count > self.policy.max_retries {
                    tracing::error!(
                        message_id = %task.message_id,
                        retry_count,
                        error = %err,
                        "retries exhausted, dead-lettering"
                    );

                    if let Some(dlq) = &self.dead_letter {
                        dlq.send(&stored.message)
                            .await
                            .map_err(ProcessError::DeadLetter)?;
                    }

                    self.store
                        .set_status(&task.message_id, DelayStatus::Failed, now_ms)
                        .await?
                        .map_err(ProcessError::Store)?;

                    return Ok(Delivery::DeadLettered);
                }

                tracing::warn!(
                    message_id = %task.message_id,
                    retry_count,
                    error = %err,
                    "delivery failed, scheduling retry"
                );

                // mark_retry 同时把状态放回 Pending，使任务可被重新抢占
                self.store
                    .mark_retry(&task.message_id, retry_count, now_ms)
                    .await?
                    .map_err(ProcessError::Store)?;

                Ok(Delivery::Retrying { retry_count })
            }
        }
    }
}
