//! 单线程 store worker。
//!
//! `DelayStore` 的实现是同步阻塞的(RocksDB 本身如此)，直接在 tokio
//! worker 上调用会占住线程。这里把一个 store 实例交给专属 OS 线程，
//! async 侧通过 channel 下达命令并等回执。
//!
//! 串行化顺带解决了 CAS：`compare_and_swap_status` 是 read-modify-write，
//! RocksDB 没有原生 CAS，但单线程上不存在交错。见架构文档 §16.3。

use std::sync::Arc;
use std::thread;

use tokio::sync::{mpsc, oneshot};

use crate::message::{DelayMessage, DelayStatus, StoredDelayMessage};
use crate::store::{DelayStore, PutOutcome};
use crate::task::TimerTask;

/// worker 已停止 —— 线程 panic 或 handle 已全部 drop。
///
/// 存储不可用时服务应当整体退出，而不是静默降级：一边 commit Kafka
/// offset 一边丢消息比直接崩溃更糟。
#[derive(Debug, thiserror::Error)]
#[error("store worker is not running")]
pub struct WorkerGone;

/// 下达给 worker 的命令。每条自带 oneshot 回执通道。
enum Command<E> {
    Put {
        message: Box<DelayMessage>,
        now_ms: i64,
        reply: oneshot::Sender<Result<PutOutcome, E>>,
    },
    Get {
        message_id: String,
        reply: oneshot::Sender<Result<Option<StoredDelayMessage>, E>>,
    },
    Delete {
        message_id: String,
        reply: oneshot::Sender<Result<(), E>>,
    },
    ScanDue {
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<TimerTask>, E>>,
    },
    Cas {
        message_id: String,
        expect: DelayStatus,
        next: DelayStatus,
        now_ms: i64,
        reply: oneshot::Sender<Result<bool, E>>,
    },
    SetStatus {
        message_id: String,
        status: DelayStatus,
        now_ms: i64,
        reply: oneshot::Sender<Result<(), E>>,
    },
    MarkRetry {
        message_id: String,
        retry_count: u32,
        now_ms: i64,
        reply: oneshot::Sender<Result<(), E>>,
    },
    ScanStaleDelivering {
        now_ms: i64,
        timeout_ms: i64,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<TimerTask>, E>>,
    },
}

/// async 侧的 store 句柄。克隆廉价，所有克隆共享同一个 worker 线程。
pub struct StoreHandle<E> {
    tx: mpsc::UnboundedSender<Command<E>>,
    thread: Arc<thread::JoinHandle<()>>,
}

impl<E> Clone for StoreHandle<E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            thread: Arc::clone(&self.thread),
        }
    }
}

impl<E: Send + 'static> StoreHandle<E> {
    /// 启动 worker 线程并接管 `store`。
    ///
    /// store 的所有权转移进线程 —— 这是 CAS 正确性的类型层面保证：
    /// 实例在线程外不可达，不可能被并发访问。
    pub fn spawn<S>(store: S) -> Self
    where
        S: DelayStore<Error = E>,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<Command<E>>();

        let thread = thread::Builder::new()
            .name("delay-store".to_string())
            .spawn(move || {
                // blocking_recv 在当前线程阻塞等待，不需要运行时。
                while let Some(cmd) = rx.blocking_recv() {
                    // 回执发送失败说明调用方已放弃等待(超时或取消)，
                    // 命令本身已执行完，忽略即可。
                    match cmd {
                        Command::Put {
                            message,
                            now_ms,
                            reply,
                        } => {
                            let _ = reply.send(store.put(&message, now_ms));
                        }
                        Command::Get { message_id, reply } => {
                            let _ = reply.send(store.get(&message_id));
                        }
                        Command::Delete { message_id, reply } => {
                            let _ = reply.send(store.delete(&message_id));
                        }
                        Command::ScanDue {
                            from_execute_at,
                            to_execute_at,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(store.scan_due(
                                from_execute_at,
                                to_execute_at,
                                limit,
                            ));
                        }
                        Command::Cas {
                            message_id,
                            expect,
                            next,
                            now_ms,
                            reply,
                        } => {
                            let _ = reply.send(store.compare_and_swap_status(
                                &message_id,
                                expect,
                                next,
                                now_ms,
                            ));
                        }
                        Command::SetStatus {
                            message_id,
                            status,
                            now_ms,
                            reply,
                        } => {
                            let _ = reply.send(store.set_status(&message_id, status, now_ms));
                        }
                        Command::MarkRetry {
                            message_id,
                            retry_count,
                            now_ms,
                            reply,
                        } => {
                            let _ =
                                reply.send(store.mark_retry(&message_id, retry_count, now_ms));
                        }
                        Command::ScanStaleDelivering {
                            now_ms,
                            timeout_ms,
                            limit,
                            reply,
                        } => {
                            let _ = reply.send(store.scan_stale_delivering(
                                now_ms,
                                timeout_ms,
                                limit,
                            ));
                        }
                    }
                }
            })
            .expect("spawn store worker thread");

        Self {
            tx,
            thread: Arc::new(thread),
        }
    }

    /// worker 线程是否已结束(panic 或正常退出)。
    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }

    async fn dispatch<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, E>>) -> Command<E>,
    ) -> Result<Result<T, E>, WorkerGone> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(make(reply_tx)).map_err(|_| WorkerGone)?;
        // 回执通道被丢弃说明 worker 在处理途中 panic 了。
        reply_rx.await.map_err(|_| WorkerGone)
    }

    pub async fn put(
        &self,
        message: DelayMessage,
        now_ms: i64,
    ) -> Result<Result<PutOutcome, E>, WorkerGone> {
        self.dispatch(|reply| Command::Put {
            message: Box::new(message),
            now_ms,
            reply,
        })
        .await
    }

    pub async fn get(
        &self,
        message_id: impl Into<String>,
    ) -> Result<Result<Option<StoredDelayMessage>, E>, WorkerGone> {
        let message_id = message_id.into();
        self.dispatch(|reply| Command::Get { message_id, reply })
            .await
    }

    pub async fn delete(
        &self,
        message_id: impl Into<String>,
    ) -> Result<Result<(), E>, WorkerGone> {
        let message_id = message_id.into();
        self.dispatch(|reply| Command::Delete { message_id, reply })
            .await
    }

    /// `limit` 既防 OOM，也防队头阻塞 —— worker 是串行的，一次大 scan
    /// 会把其他命令堵在后面。
    pub async fn scan_due(
        &self,
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
    ) -> Result<Result<Vec<TimerTask>, E>, WorkerGone> {
        self.dispatch(|reply| Command::ScanDue {
            from_execute_at,
            to_execute_at,
            limit,
            reply,
        })
        .await
    }

    pub async fn compare_and_swap_status(
        &self,
        message_id: impl Into<String>,
        expect: DelayStatus,
        next: DelayStatus,
        now_ms: i64,
    ) -> Result<Result<bool, E>, WorkerGone> {
        let message_id = message_id.into();
        self.dispatch(|reply| Command::Cas {
            message_id,
            expect,
            next,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn set_status(
        &self,
        message_id: impl Into<String>,
        status: DelayStatus,
        now_ms: i64,
    ) -> Result<Result<(), E>, WorkerGone> {
        let message_id = message_id.into();
        self.dispatch(|reply| Command::SetStatus {
            message_id,
            status,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn mark_retry(
        &self,
        message_id: impl Into<String>,
        retry_count: u32,
        now_ms: i64,
    ) -> Result<Result<(), E>, WorkerGone> {
        let message_id = message_id.into();
        self.dispatch(|reply| Command::MarkRetry {
            message_id,
            retry_count,
            now_ms,
            reply,
        })
        .await
    }

    pub async fn scan_stale_delivering(
        &self,
        now_ms: i64,
        timeout_ms: i64,
        limit: usize,
    ) -> Result<Result<Vec<TimerTask>, E>, WorkerGone> {
        self.dispatch(|reply| Command::ScanStaleDelivering {
            now_ms,
            timeout_ms,
            limit,
            reply,
        })
        .await
    }
}
