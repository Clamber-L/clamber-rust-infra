//! 组装：Kafka + RocksDB + 时间轮 + 处理器。
//!
//! 三个并发任务：
//! ```text
//! consume  delay-topic → RocksDB(+ 条件入轮)
//! schedule 预加载 → 推进时间轮 → 投递
//! preload  滚动拉取远期任务
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kafka_delaymq_core::{
    Clock, Delivery, Processor, Scheduler, StoreHandle, TimerTask,
};
use kafka_delaymq_kafka::{DelayConsumer, KafkaSink};
use kafka_delaymq_storage::{RocksDbStore, StorageError};
use tokio::sync::Mutex;

use crate::config::ServerConfig;

type Store = StoreHandle<StorageError>;

/// 哪个循环先结束。用于避免重复 poll 已完成的 `JoinHandle`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loop {
    Consume,
    Schedule,
    Preload,
}

pub struct DelayServer {
    config: ServerConfig,
    store: Store,
    clock: Clock,
}

impl DelayServer {
    pub fn new(config: ServerConfig) -> Result<Self> {
        config
            .scheduler
            .validate()
            .context("scheduler config is invalid")?;

        let rocks = RocksDbStore::open(&config.data_dir)
            .with_context(|| format!("open rocksdb at {}", config.data_dir.display()))?;

        Ok(Self {
            store: StoreHandle::spawn(rocks),
            clock: Clock::new(),
            config,
        })
    }

    /// 启动并运行到收到关停信号。
    pub async fn run(self) -> Result<()> {
        let consumer = DelayConsumer::new(&self.config.consumer()).context("create consumer")?;

        let sink = KafkaSink::new(&self.config.producer()).context("create producer")?;
        // 关停时要刷出 producer 的本地队列，否则已 ack 但未落地的
        // 消息会丢。processor 会取走 sink 的所有权，所以另开一个
        // 共享同一组 broker 的实例专门用于 flush。
        let flusher = KafkaSink::new(&self.config.producer()).context("create flusher")?;

        let processor = match &self.config.dead_letter_topic {
            Some(topic) => {
                let dlq = KafkaSink::new(&self.config.producer())
                    .context("create dead letter producer")?
                    .with_topic(topic);
                Processor::<RocksDbStore, _>::new(self.store.clone(), sink).with_dead_letter(dlq)
            }
            None => Processor::<RocksDbStore, _>::new(self.store.clone(), sink),
        };

        let scheduler = Scheduler::<RocksDbStore>::new(
            self.store.clone(),
            &self.clock,
            self.config.scheduler,
        )
        .context("create scheduler")?;
        let scheduler = Arc::new(Mutex::new(scheduler));

        // 恢复：重置卡住的 Delivering，重建时间轮
        {
            let mut sched = scheduler.lock().await;
            let stats = sched
                .recover(self.clock.now_ms())
                .await
                .context("recovery failed")?;
            tracing::info!(
                loaded = stats.loaded,
                rejected = stats.rejected,
                "recovery complete"
            );
        }

        // consumer 的所有权交给循环 —— 用 Arc 共享的话 drop 只减引用
        // 计数，librdkafka 客户端不会在运行时存活期间真正析构。
        let mut consume = tokio::spawn(consume_loop(
            consumer,
            self.store.clone(),
            self.clock.clone(),
        ));

        let mut schedule = tokio::spawn(schedule_loop(
            Arc::clone(&scheduler),
            processor,
            self.clock.clone(),
            self.config.tick_interval,
        ));

        let mut preload = tokio::spawn(preload_loop(
            Arc::clone(&scheduler),
            self.clock.clone(),
            Duration::from_millis(self.config.scheduler.preload_interval_ms),
        ));

        // 已经 poll 完成的 JoinHandle 不能再被 poll(会 panic)，所以
        // select! 结束后要记下哪一个已完结，只 abort/join 其余的。
        let mut finished: Option<Loop> = None;

        let outcome = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown signal received");
                Ok(())
            }
            // 任一循环退出都说明出了问题 —— 存储或 Kafka 不可用时
            // 继续运行只会丢消息，应当整体退出。
            result = &mut consume => {
                tracing::error!(?result, "consume loop exited");
                finished = Some(Loop::Consume);
                Err(anyhow::anyhow!("consume loop exited"))
            }
            result = &mut schedule => {
                tracing::error!(?result, "schedule loop exited");
                finished = Some(Loop::Schedule);
                Err(anyhow::anyhow!("schedule loop exited"))
            }
            result = &mut preload => {
                tracing::error!(?result, "preload loop exited");
                finished = Some(Loop::Preload);
                Err(anyhow::anyhow!("preload loop exited"))
            }
        };

        // 关停顺序要紧。librdkafka 的客户端必须在 tokio 运行时仍然
        // 存活时析构，否则进程退出期间会撞上 pthread 竞态并 abort。
        //
        //   1. 停下所有循环，它们持有 consumer / producer 的引用
        //   2. 刷出 producer 的本地队列
        //   3. 析构 Kafka 客户端
        //   4. 关闭 store worker，RocksDB 随之落盘
        if finished != Some(Loop::Consume) {
            consume.abort();
            let _ = consume.await;
        }
        if finished != Some(Loop::Schedule) {
            schedule.abort();
            let _ = schedule.await;
        }
        if finished != Some(Loop::Preload) {
            preload.abort();
            let _ = preload.await;
        }

        if let Err(err) = flusher.flush(self.config.shutdown_flush) {
            tracing::warn!(%err, "producer flush on shutdown failed");
        } else {
            tracing::info!("producer flushed");
        }

        // consumer 与 processor 的所有权都在各自的循环里，abort + await
        // 之后已随任务一起析构。这里只需处理自己持有的 flusher。
        drop(flusher);

        // scheduler 持有一份 store handle 的克隆，必须先放掉，
        // 否则 shutdown 等不到 worker 退出。
        drop(scheduler);

        // store worker 持有 RocksDB。显式等它退出，确保落盘完成 ——
        // 进程直接退出会让 RocksDB 的析构与 LOCK 文件清理来不及跑。
        self.store.shutdown().await;
        tracing::info!("shutdown complete");

        outcome
    }
}

/// delay-topic → RocksDB。
async fn consume_loop(consumer: DelayConsumer, store: Store, clock: Clock) -> Result<()> {
    loop {
        match consumer.recv().await {
            Ok(None) => continue, // poll 超时
            Ok(Some(consumed)) => {
                let now_ms = clock.now_ms();
                let message_id = consumed.message.message_id.clone();

                // 先落库，再提交 offset —— 顺序颠倒会在崩溃时丢消息。
                let outcome = store
                    .put(consumed.message, now_ms)
                    .await
                    .context("store worker gone")?
                    .context("persist message")?;

                if !outcome.is_stored() {
                    // 撞号：上游 ID 生成器配置有问题，告警但不中断。
                    // 重试不会让它变好，静默覆盖会丢已存在的消息。
                    tracing::error!(
                        %message_id,
                        partition = consumed.partition,
                        offset = consumed.offset,
                        "message_id collision — message NOT stored"
                    );
                }

                tracing::debug!(%message_id, ?outcome, "message persisted");
            }
            Err(err) if err.is_skippable() => {
                // 单条坏消息不该堵住整个 partition
                tracing::warn!(%err, "skipping bad record");
            }
            Err(err) => return Err(err).context("consumer failed"),
        }
    }
}

/// 推进时间轮并投递到期消息。
async fn schedule_loop(
    scheduler: Arc<Mutex<Scheduler<RocksDbStore>>>,
    processor: Processor<RocksDbStore, KafkaSink>,
    clock: Clock,
    tick_interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(tick_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut due: Vec<TimerTask> = Vec::new();

    loop {
        ticker.tick().await;
        let now_ms = clock.now_ms();

        due.clear();
        // 锁只在推进时持有，投递在锁外进行 —— 投递要等 broker ack，
        // 握着锁会把预加载也堵住。
        scheduler.lock().await.advance_to(now_ms, &mut due);

        for task in due.drain(..) {
            match processor.process(&task, now_ms).await {
                Ok(Delivery::Rescheduled) => {
                    // 未到期(时钟漂移或提前加载)，放回时间轮
                    let mut sched = scheduler.lock().await;
                    if let Err(err) = sched.reschedule(task.clone()) {
                        tracing::warn!(%err, message_id = %task.message_id, "reschedule failed");
                    }
                }
                Ok(Delivery::Retrying { retry_count }) => {
                    tracing::debug!(message_id = %task.message_id, retry_count, "will retry");
                }
                Ok(outcome) => {
                    tracing::debug!(message_id = %task.message_id, ?outcome, "processed");
                }
                Err(err) => {
                    tracing::error!(%err, message_id = %task.message_id, "processing failed");
                }
            }
        }
    }
}

/// 滚动预加载远期任务。
async fn preload_loop(
    scheduler: Arc<Mutex<Scheduler<RocksDbStore>>>,
    clock: Clock,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let now_ms = clock.now_ms();

        let result = scheduler.lock().await.preload_once(now_ms).await;

        match result {
            Ok(stats) if stats.loaded > 0 => {
                tracing::debug!(loaded = stats.loaded, rejected = stats.rejected, "preloaded");
            }
            Ok(_) => {}
            // 失败不推进窗口，下一轮会重扫同一区间
            Err(err) => tracing::error!(%err, "preload failed"),
        }
    }
}
