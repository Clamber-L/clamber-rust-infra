# Kafka + Rust 时间轮任意延迟消息架构

> 目标：使用 Rust 实现一个类似 RocketMQ 5.x
> 任意时间定时消息的延迟消息服务。
>
> 核心思想：
>
> **Kafka / 持久化存储负责可靠保存消息，Timing Wheel
> 负责高效调度"什么时候执行"。**

------------------------------------------------------------------------

## 1. 目标

系统需要支持：

-   任意延迟时间，例如 3 秒、17 分钟、5 小时、3 天
-   指定绝对执行时间 `execute_at`
-   不依赖业务层 `@Scheduled` / `while + sleep` 扫描
-   JVM/Rust 进程重启后能够恢复未执行消息
-   Kafka Consumer Group 集群部署
-   消息允许重复投递，但不能因为调度器重启而丢失
-   最终消息重新进入 Kafka 普通业务 Topic
-   后续可以扩展为分层时间轮（Hierarchical Timing Wheel）

------------------------------------------------------------------------

# 2. 整体架构

``` text
                         Producer
                            │
                            │ DelayMessage
                            │ execute_at
                            ▼
                    ┌─────────────────┐
                    │ Kafka           │
                    │ delay-topic     │
                    └────────┬────────┘
                             │
                             │ Consumer
                             ▼
              ┌──────────────────────────────┐
              │      Delay Message Service   │
              │            Rust              │
              │                              │
              │  ┌────────────────────────┐  │
              │  │ Kafka Consumer         │  │
              │  └───────────┬────────────┘  │
              │              │               │
              │              ▼               │
              │  ┌────────────────────────┐  │
              │  │ Delay Message Store    │  │
              │  │ RocksDB                │  │
              │  └───────────┬────────────┘  │
              │              │               │
              │              ▼               │
              │  ┌────────────────────────┐  │
              │  │ Timing Wheel           │  │
              │  │ Hierarchical           │  │
              │  └───────────┬────────────┘  │
              │              │               │
              │           到期事件            │
              │              ▼               │
              │  ┌────────────────────────┐  │
              │  │ Delay Processor        │  │
              │  └───────────┬────────────┘  │
              └──────────────┼───────────────┘
                             │
                             │ Kafka Producer
                             ▼
                    ┌─────────────────┐
                    │ business-topic  │
                    └────────┬────────┘
                             │
                             ▼
                         Consumer
```

------------------------------------------------------------------------

# 3. 为什么不是简单的定时任务

传统方案：

``` text
Redis ZSet
    │
    ▼
每秒扫描
    │
    ▼
ZRANGEBYSCORE execute_at <= now
    │
    ▼
执行消息
```

这种方案本质是：

> **主动扫描数据判断是否到期。**

时间轮方案：

``` text
Timing Wheel
     │
     ▼
时间不断向前推进
     │
     ▼
当前 Slot
     │
     ▼
找到到期任务
     │
     ▼
执行
```

它本质是：

> **调度器主动推进时间，不需要业务层不断扫描持久化存储。**

时间轮内部当然仍然需要一个驱动 tick
的执行线程，但这属于调度器自身机制，不是业务代码定时扫描数据库/Redis。

------------------------------------------------------------------------

# 4. 核心设计原则

## 4.1 时间轮不负责持久化

不要把完整消息直接放进时间轮：

``` text
TimingWheel
    └── 完整 Message
```

生产环境可能存在：

``` text
100 万
1000 万
1 亿
```

条延迟消息。

全部放进内存会导致巨大的内存压力。

正确方式：

``` text
TimingWheel
    │
    └── message_id / task index

RocksDB
    │
    └── message_id -> 完整 DelayMessage
```

即：

> **时间轮保存"什么时候执行"，持久化存储保存"执行什么"。**

------------------------------------------------------------------------

# 5. DelayMessage

推荐统一使用绝对时间 `execute_at`。

``` rust
pub struct DelayMessage {
    /// 全局唯一消息 ID
    pub message_id: String,

    /// 业务 Topic
    pub topic: String,

    /// Kafka Partition
    pub partition: i32,

    /// Kafka Offset
    pub offset: i64,

    /// 目标执行时间，Unix milliseconds
    pub execute_at: i64,

    /// 消息内容
    pub payload: Vec<u8>,

    /// Kafka Headers
    pub headers: Vec<(String, String)>,
}
```

不要把系统核心设计成：

``` text
delay_level = 1
delay_level = 2
delay_level = 3
```

而应该使用：

``` text
execute_at = 绝对时间戳
```

这样可以支持：

``` text
3 秒
17 秒
5 分钟
37 分钟
8 小时
3 天
指定时间
```

统一处理。

------------------------------------------------------------------------

# 6. TimerTask

时间轮只需要保存轻量级任务索引：

``` rust
pub struct TimerTask {
    pub message_id: String,

    pub execute_at: i64,

    pub topic: String,

    pub partition: i32,

    pub offset: i64,
}
```

真正的数据仍然存储在 RocksDB。

------------------------------------------------------------------------

# 7. 时间轮

第一版可以先实现单层 Hashed Wheel：

``` text
tick = 100ms
wheel_size = 512

覆盖范围：

100ms × 512
≈ 51.2 秒
```

概念结构：

``` text
                  current_tick
                       │
                       ▼
        ┌────┬────┬────┬────┬────┬────┐
        │ 0  │ 1  │ 2  │ 3  │ 4  │ ...│
        └────┴────┴────┴────┴────┴────┘
                       │
                       ▼
                    Slot 3
                       │
                       ▼
                  TimerTask
```

------------------------------------------------------------------------

# 8. 任意时间：分层时间轮

如果需要支持：

``` text
几秒
几分钟
几小时
几天
甚至更长
```

推荐使用 Hierarchical Timing Wheel。

例如：

``` text
Level 0
tick = 100ms
slots = 512

覆盖：
≈ 51.2 秒


Level 1
tick ≈ 51.2 秒
slots = 512

覆盖：
≈ 7.28 小时


Level 2
tick ≈ 7.28 小时
slots = 512

覆盖：
≈ 155 天
```

消息根据距离当前时间的范围进入不同层。

例如：

``` text
3 秒
    ↓
Level 0

30 秒
    ↓
Level 0

5 分钟
    ↓
Level 1

3 小时
    ↓
Level 1

3 天
    ↓
Level 2
```

------------------------------------------------------------------------

# 9. 时间轮级联

远期任务并不是一直停留在高级时间轮。

例如：

``` text
3 天后
    ↓
Level 2
    ↓
时间推进
    ↓
变成 5 小时
    ↓
Level 1
    ↓
时间推进
    ↓
变成 30 秒
    ↓
Level 0
    ↓
时间到
    ↓
执行
```

这叫：

> Cascade / 时间轮级联。

------------------------------------------------------------------------

# 10. 核心数据流

## 10.1 生产消息

生产者：

``` text
业务系统
   │
   ▼
DelayMessage
   │
   ├── message_id
   ├── topic
   ├── payload
   └── execute_at
   │
   ▼
Kafka delay-topic
```

------------------------------------------------------------------------

## 10.2 延迟服务消费

``` text
Kafka Consumer
      │
      ▼
解析 DelayMessage
      │
      ▼
写入 RocksDB
      │
      ▼
创建 TimerTask
      │
      ▼
加入 Timing Wheel
      │
      ▼
提交 Kafka Offset
```

核心顺序：

``` text
Kafka Message
      ↓
Persistent Store
      ↓
Timing Wheel
      ↓
Offset Commit
```

而不是：

``` text
Kafka Message
      ↓
Offset Commit
      ↓
Timing Wheel
```

后者可能导致消息已经 commit，但进程在进入时间轮之前崩溃。

------------------------------------------------------------------------

# 11. 时间轮触发

时间轮发现任务到期：

``` text
Timing Wheel
      │
      ▼
TimerTask
      │
      ▼
message_id
      │
      ▼
RocksDB
      │
      ▼
获取完整 DelayMessage
      │
      ▼
再次检查 execute_at
      │
      ▼
Kafka Producer
      │
      ▼
business-topic
```

再次检查 `execute_at` 是必要的。

因为：

-   系统时钟可能变化
-   Timer tick 有精度误差
-   任务可能被重新调度
-   进程恢复后可能加载已经到期的任务

------------------------------------------------------------------------

# 12. 到期处理伪代码

``` rust
async fn on_timeout(task: TimerTask) -> Result<()> {

    let message = store
        .get(&task.message_id)
        .await?;

    let Some(message) = message else {
        return Ok(());
    };

    let now = current_time_millis();

    if now < message.execute_at {
        wheel.schedule(
            TimerTask::from(&message)
        );

        return Ok(());
    }

    producer
        .send(
            &message.topic,
            &message.payload
        )
        .await?;

    store
        .delete(&message.message_id)
        .await?;

    Ok(())
}
```

------------------------------------------------------------------------

# 13. Kafka 发送成功之后再删除

不能：

``` rust
producer.send(...);

store.delete(message_id);
```

因为 Producer 可能异步发送。

正确思路：

``` text
Kafka Send
    │
    ├── Success
    │      ↓
    │   删除 Delay Store
    │
    └── Failure
           ↓
       保留消息
           ↓
       重新调度
```

即：

> **只有业务消息成功进入目标 Kafka Topic
> 后，才能删除延迟消息持久化记录。**

------------------------------------------------------------------------

# 14. 重复投递是允许的

这个系统不要追求：

> exactly once

更现实的目标应该是：

> **at-least-once + 幂等。**

例如：

``` text
Timer
  ↓
Kafka send 成功
  ↓
程序突然崩溃
  ↓
RocksDB 中消息还没有删除
  ↓
恢复
  ↓
再次发送
```

结果：

``` text
business-topic
    ├── message-100
    └── message-100
```

所以业务消费者需要：

``` text
message_id
    ↓
幂等判断
    ↓
已经处理 → 忽略
未处理   → 执行
```

------------------------------------------------------------------------

# 15. message_id 是整个系统的核心

建议：

``` rust
message_id: UUID / Snowflake / ULID
```

并保证全局唯一。

它同时用于：

``` text
Kafka Key
RocksDB Key
TimerTask ID
幂等 Key
日志追踪 ID
Metrics 标签
```

推荐：

``` text
message_id
     │
     ├── Delay Store
     ├── Timing Wheel
     ├── Kafka Key
     └── Business Idempotency
```

------------------------------------------------------------------------

# 16. RocksDB 存储设计

第一版可以：

``` text
Key:
message_id

Value:
DelayMessage
```

例如：

``` text
delay_message/
    01JABC...
    01JABD...
    01JABE...
```

但生产级实现建议增加状态。

例如：

``` rust
pub enum DelayStatus {
    Pending,
    Delivering,
    Delivered,
    Failed,
}
```

消息：

``` rust
pub struct StoredDelayMessage {
    pub message: DelayMessage,

    pub status: DelayStatus,

    pub retry_count: u32,

    pub created_at: i64,

    pub updated_at: i64,
}
```

------------------------------------------------------------------------

# 17. Recovery：进程重启恢复

这是持久化时间轮和普通内存时间轮最大的区别。

假设：

``` text
23:00

A -> 23:30
B -> 01:00
C -> 明天 10:00
```

RocksDB 持久化：

``` text
A
B
C
```

进程：

``` text
23:10
```

突然重启。

启动时：

``` text
Rust Service
     │
     ▼
RocksDB
     │
     ▼
读取 Pending Messages
     │
     ├── execute_at <= now
     │       ↓
     │     立即处理
     │
     └── execute_at > now
             ↓
        重新加入时间轮
```

------------------------------------------------------------------------

# 18. Recovery API

可以定义：

``` rust
pub trait DelayStore {

    async fn put(
        &self,
        message: &DelayMessage
    ) -> Result<()>;

    async fn get(
        &self,
        message_id: &str
    ) -> Result<Option<DelayMessage>>;

    async fn delete(
        &self,
        message_id: &str
    ) -> Result<()>;

    async fn load_pending(
        &self
    ) -> Result<Vec<DelayMessage>>;
}
```

启动：

``` rust
async fn recover() -> Result<()> {

    let messages =
        store.load_pending().await?;

    for message in messages {

        if message.execute_at <= now() {
            processor.process(message).await?;
        } else {
            wheel.schedule(
                TimerTask::from(&message)
            );
        }
    }

    Ok(())
}
```

------------------------------------------------------------------------

# 19. Kafka 集群

利用 Kafka Partition 做天然分片。

例如：

``` text
delay-topic

Partition 0
Partition 1
Partition 2
Partition 3
```

服务：

``` text
             Kafka
               │
       ┌───────┼───────┐
       ▼       ▼       ▼
    Server A Server B Server C
       │       │       │
    Wheel A Wheel B Wheel C
```

Kafka Consumer Group 保证：

``` text
一个 Partition
       ↓
同一时刻
       ↓
一个 Consumer 实例
```

因此：

``` text
Partition 0 → Server A
Partition 1 → Server B
Partition 2 → Server C
Partition 3 → Server A
```

每个实例维护自己的时间轮。

------------------------------------------------------------------------

# 20. 一个重要限制：Kafka Partition 与时间轮状态

如果 Partition 发生 Rebalance：

``` text
Server A
    │
    └── Partition 0
            ↓
        Rebalance
            ↓
Server B
    │
    └── Partition 0
```

那么 Server B 必须能够：

``` text
RocksDB / Delay Store
        ↓
重新加载未完成任务
        ↓
重新构建时间轮
```

因此：

> **持久化状态必须能够在 Consumer ownership 发生变化时重新恢复。**

------------------------------------------------------------------------

# 21. 推荐的 Rust Workspace

整个系统可以作为一个 Rust Workspace。

``` text
rust-infra/
│
├── Cargo.toml
│
├── crates/
│   │
│   ├── common/
│   │
│   ├── timing-wheel/
│   │
│   ├── delaymq-core/
│   │
│   ├── delaymq-storage/
│   │
│   └── delaymq-kafka/
│
├── services/
│   │
│   ├── delay-server/
│   │
│   └── pingora-gateway/
│
└── tools/
    │
    ├── delay-cli/
    └── benchmark/
```

------------------------------------------------------------------------

# 22. crate 职责

## timing-wheel

只负责：

``` text
schedule
cancel
advance
cascade
slot
tick
```

不关心：

``` text
Kafka
RocksDB
HTTP
业务消息
```

------------------------------------------------------------------------

## delaymq-core

负责：

``` text
DelayMessage
TimerTask
DelayService
DelayProcessor
业务状态机
```

依赖：

``` text
timing-wheel
delaymq-storage
```

但不应该强依赖 Kafka。

------------------------------------------------------------------------

## delaymq-storage

负责：

``` text
RocksDB
持久化
Recovery
Checkpoint
状态
```

定义：

``` rust
trait DelayStore
```

未来可以实现：

``` text
RocksDbStore
MemoryStore
PostgresStore
```

------------------------------------------------------------------------

## delaymq-kafka

负责：

``` text
Kafka Consumer
Kafka Producer
Kafka Record
Kafka Headers
Offset
Partition
```

核心业务逻辑不应该写在这里。

------------------------------------------------------------------------

## delay-server

负责组合：

``` text
Kafka
+
Storage
+
TimingWheel
+
DelayCore
```

形成最终可运行服务。

------------------------------------------------------------------------

# 23. 推荐依赖关系

``` text
                    delay-server
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   delaymq-kafka   delaymq-core   delaymq-storage
                         │
                         ▼
                  timing-wheel
```

更严格地说：

``` text
delaymq-core
    │
    ├── timing-wheel
    │
    └── DelayStore trait

delaymq-storage
    │
    └── implements DelayStore

delaymq-kafka
    │
    └── Kafka adapter

delay-server
    │
    └── assemble everything
```

------------------------------------------------------------------------

# 24. 不建议一开始做的事情

第一版不要直接实现：

``` text
分布式时间轮
多级 RocksDB 集群
Exactly Once
跨节点时间轮迁移
复杂事务
```

先实现：

``` text
单实例
     ↓
Kafka
     ↓
RocksDB
     ↓
Timing Wheel
     ↓
Kafka
```

然后测试：

``` text
1000 条
10 万条
100 万条
1000 万条
```

再逐渐增加复杂度。

------------------------------------------------------------------------

# 25. 第一阶段推荐实现

``` text
Rust
Tokio
rdkafka
RocksDB
Timing Wheel
Tracing
```

核心流程：

``` text
Producer
   ↓
Kafka delay-topic
   ↓
Kafka Consumer
   ↓
RocksDB
   ↓
Timing Wheel
   ↓
到期
   ↓
Kafka Producer
   ↓
business-topic
```

------------------------------------------------------------------------

# 26. 第二阶段

增加：

``` text
Hierarchical Timing Wheel
       ↓
Cascade
       ↓
Checkpoint
       ↓
Recovery
       ↓
Retry
       ↓
Dead Letter
```

------------------------------------------------------------------------

# 27. 第三阶段

增加生产级能力：

``` text
Metrics
Tracing
Admin API
CLI
动态配置
限流
批量投递
失败重试
消息查询
消息取消
消息重新调度
```

例如：

``` text
POST /delay/messages

GET /delay/messages/{id}

DELETE /delay/messages/{id}

POST /delay/messages/{id}/retry

POST /delay/messages/{id}/reschedule
```

------------------------------------------------------------------------

# 28. 最终目标架构

``` text
                         ┌───────────────┐
                         │   Producer    │
                         └───────┬───────┘
                                 │
                                 ▼
                         ┌───────────────┐
                         │ Kafka         │
                         │ delay-topic   │
                         └───────┬───────┘
                                 │
                                 ▼
                  ┌─────────────────────────────┐
                  │       Rust Delay Server     │
                  │                             │
                  │  Kafka Consumer              │
                  │       │                     │
                  │       ▼                     │
                  │  DelayMessageStore          │
                  │       │                     │
                  │       ▼                     │
                  │  Hierarchical Timing Wheel  │
                  │       │                     │
                  │       ▼                     │
                  │  DelayProcessor             │
                  │       │                     │
                  └───────┼─────────────────────┘
                          │
                          ▼
                     Kafka Producer
                          │
                          ▼
                   business-topic
                          │
                          ▼
                       Consumer
```

------------------------------------------------------------------------

# 29. 最重要的设计结论

整个系统可以浓缩成四句话：

### 1. Kafka 负责消息传输

``` text
Kafka = Transport
```

### 2. RocksDB 负责可靠保存

``` text
RocksDB = Persistence
```

### 3. Timing Wheel 负责时间调度

``` text
Timing Wheel = Scheduler
```

### 4. Kafka Consumer Group 负责横向扩展

``` text
Kafka Partition = Sharding
```

最终形成：

``` text
Kafka
  +
Persistent Store
  +
Hierarchical Timing Wheel
  +
At-Least-Once Delivery
  +
Idempotency
```

这就是整个"Kafka + Rust 时间轮任意延迟消息"系统的核心。

------------------------------------------------------------------------

# 30. 推荐开发顺序

``` text
Step 1
实现单层 Timing Wheel
        ↓
Step 2
编写 Timing Wheel Benchmark
        ↓
Step 3
实现 DelayMessage / TimerTask
        ↓
Step 4
实现 RocksDB DelayStore
        ↓
Step 5
实现 Kafka Consumer
        ↓
Step 6
实现 Kafka Producer
        ↓
Step 7
打通完整延迟消息链路
        ↓
Step 8
实现进程重启 Recovery
        ↓
Step 9
实现重复投递幂等
        ↓
Step 10
实现 Hierarchical Timing Wheel
        ↓
Step 11
实现 Retry / DLQ
        ↓
Step 12
实现 Metrics / Admin API
        ↓
Step 13
进行百万级/千万级消息压力测试
```

------------------------------------------------------------------------

# 31. 最终定位

这个项目不是简单的：

``` text
Kafka + Timer
```

而应该定位成：

> **一个基于 Kafka、RocksDB 和 Hierarchical Timing Wheel 的 Rust
> 任意时间延迟消息服务。**

核心设计思想：

``` text
        “持久化负责记住消息”
                 +
        “时间轮负责记住时间”
                 +
        “Kafka 负责消息传输”
                 +
        “幂等保证至少一次投递下的业务正确性”
```

这套架构后续也可以独立成 Rust Workspace 中的基础设施组件，为其他 Rust
服务提供延迟消息能力。
