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
-   进程重启后能够恢复未执行消息
-   内存占用与**到期速率**相关，而非与**消息总存量**相关
-   Kafka Consumer Group 集群部署，且 rebalance 不丢消息
-   消息允许重复投递，但不能因为调度器重启而丢失
-   最终消息重新进入 Kafka 普通业务 Topic
-   后续可以扩展为分层时间轮（Hierarchical Timing Wheel）

其中第 5 条和第 6 条是原始目标列表缺失的，但它们决定了架构形态，见 §4.2
与 §20。

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
              │  │ CF: delay_message      │  │
              │  │ CF: by_execute_at      │  │
              │  └───────────┬────────────┘  │
              │              │               │
              │      Preloader 滚动拉取       │
              │      execute_at <= now+H     │
              │              ▼               │
              │  ┌────────────────────────┐  │
              │  │ Timing Wheel           │  │
              │  │ 仅 horizon 内的任务      │  │
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

## 4.2 只存索引仍然不够：内存时间轮必须做时间维度的冷热分离

"时间轮只存索引"降低了单条成本，但没有解决总量问题。

按 §6 的 `TimerTask` 估算单条实际占用：

``` text
message_id  String   24 字节(本体) + 26 字节(ULID 堆数据)
topic       String   24 字节(本体) + 堆数据
execute_at  i64       8 字节
partition   i32       4 字节
offset      i64       8 字节
            ──────────────────────────────
            约 130 ~ 150 字节(含 slot 容器开销)
```

于是：

``` text
100 万   ≈ 150 MB      可以接受
1000 万  ≈ 1.5 GB      勉强
1 亿     ≈ 13 GB       放不下
```

**轻量索引乘以 1 亿依然放不下。**

注意这和 §8 的分层时间轮不是同一个问题：

``` text
分层时间轮  解决   时间跨度   (让 3 天后的任务有地方放)
冷热分离    解决   内存总量   (让 1 亿条任务不必同时在内存)
```

分层时间轮的三层都在内存里，1 亿条 task 该占的内存一样占。

正确做法是再加一层时间维度的冷热分离：

``` text
RocksDB  (全量，按 execute_at 排序)
    │
    │  Preloader 滚动预加载
    │  只取 execute_at <= now + horizon
    │
    ▼
Timing Wheel  (内存，只装近期 horizon 内到期的任务)
    │
    ▼
到期执行
```

例如：

``` text
horizon       = 5 分钟
预加载周期    = 1 分钟
内存常驻任务  = 5 分钟内到期的量
```

这样内存占用只和**到期速率**相关，与**总存量**无关：

``` text
1 亿条消息
均匀分布在 30 天
    ↓
5 分钟窗口内约 1.1 万条
    ↓
内存占用 ≈ 1.6 MB
```

这是整个系统能否撑到千万/亿级的关键，预加载依赖 §16 的 `by_execute_at`
时间索引。

------------------------------------------------------------------------

# 5. DelayMessage

推荐统一使用绝对时间 `execute_at`。

``` rust
pub struct DelayMessage {
    /// 全局唯一消息 ID，见 §15
    pub message_id: String,

    /// 投递目标：业务 Topic
    pub target_topic: String,

    /// 来源坐标：delay-topic 的 partition
    /// 仅用于 checkpoint 与问题追踪，不用于回读消息
    pub source_partition: i32,

    /// 来源坐标：delay-topic 的 offset
    /// 仅用于 checkpoint 与问题追踪，不用于回读消息
    pub source_offset: i64,

    /// 目标执行时间，Unix milliseconds(wall clock)
    pub execute_at: i64,

    /// 消息内容，自包含
    pub payload: Vec<u8>,

    /// 透传给业务 Topic 的 Kafka Headers
    pub headers: Vec<(String, String)>,
}
```

这里有一个必须明确的取舍：`payload` 与 `(source_partition,
source_offset)` 在"到期时拿什么去投递"上是两种互斥方案。

``` text
方案 A  自包含
    存完整 payload
    到期直接投递
    ✓ 不依赖 delay-topic retention
    ✓ 投递路径短
    ✗ RocksDB 写入量 = 消息总体积

方案 B  仅存坐标
    只存 (partition, offset)
    到期时回读 delay-topic 取 payload
    ✓ RocksDB 只存索引，体积极小
    ✗ 要求 delay-topic retention > 最大延迟
    ✗ 随机 seek 读 Kafka，到期投递被 IO 放大
```

**本文档选择方案 A。** 因此 `source_partition` / `source_offset`
只作为来源坐标保留，用于 §20 的 checkpoint 和日志追踪，**不**用于回读
payload。

大消息(例如单条 1 MB)场景下方案 A 会让 RocksDB 成为瓶颈，此时应改为
payload 外置对象存储、RocksDB 只存引用，而不是退回方案 B。

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
    /// 指回 RocksDB 的 key
    pub message_id: String,

    /// 到期判断依据，Unix milliseconds
    pub execute_at: i64,
}
```

字段刻意只保留两个。`topic` / `partition` / `offset`
到期后都能从 RocksDB 的 `DelayMessage` 读到，放进 `TimerTask`
只会按 §4.2 的系数放大内存占用。

真正的数据仍然存储在 RocksDB。

------------------------------------------------------------------------

# 7. 时间轮

第一版可以先实现单层 Hashed Wheel：

``` text
tick_ms    = 1000   (1 秒)
wheel_size = 512

单圈覆盖范围：

1s × 512
≈ 8.5 分钟
```

**tick 粒度取 1 秒而非 100ms。** 延迟消息的业务语义是"大约几分钟后"
而不是"精确到百毫秒"，而 tick 粒度直接决定唤醒频率：

``` text
tick = 100ms   →  10 次/秒唤醒，触发抖动 < 100ms
tick = 1s      →   1 次/秒唤醒，触发抖动 < 1s
```

抖动的方向是确定的：因为 schedule 取 ceil(§7.2)，任务只会**晚**触发，
最多晚一个 tick，不会早。1 秒的滞后对延迟消息可以忽略，换来十分之一的
唤醒开销和十分之一的空转扫描。

若将来出现需要秒级以下精度的场景，`tick_ms` 是构造参数，单独开一个
小粒度的轮实例即可，不必改动结构。

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

## 7.1 单层时间轮如何支持超过单圈的延迟

单圈只覆盖 8.5 分钟，但第一版链路(§30 Step 7)必须能接受 3
天的消息，否则链路打不通。

在分层时间轮(§8)实现之前，用 **round counter** 过渡，即 Netty
`HashedWheelTimer` 的做法：

``` rust
struct WheelEntry {
    task: TimerTask,
    /// 还需要转多少圈才到期
    rounds: u64,
}
```

入轮时计算：

``` text
target_tick = ceil_div(execute_at - start_ms, tick_ms)   ← 注意是 ceil
delta       = target_tick - current_tick
slot        = target_tick % wheel_size                    ← 用绝对值取模
rounds      = delta / wheel_size                          ← 用相对值算圈数
```

三个容易写错的地方：

**1. `slot` 用绝对的 `target_tick`，`rounds` 用相对的 `delta`。**
两者取的不是同一个量。若 slot 也叠加 `current_tick`，等于把起点重复
计算了一遍 —— `current_tick == 0` 时恰好正确，轮一旦转起来就错。

**2. `target_tick` 必须用 ceil 而非 floor。**
`tick_ms = 1000` 时 `execute_at` 落在 1500ms，floor 得 tick 1(1000ms)，
比 `execute_at` 早 500ms 触发。早触发不是精度误差而是正确性问题：§12
的二次校验会发现 `now < execute_at` 然后重新调度，而重算出来可能还是
同一个 slot，于是空转。

> **入轮取 ceil，推进取 floor。这个不对称是刻意的，它保证任务永远不会
> 早于 `execute_at` 触发，只会最多晚一个 tick。**

**3. `rounds = delta / wheel_size` 的依据。**
设 `r = delta mod W`，该 slot 首次被访问在 `current_tick + r`，之后每
`W` 个 tick 一次。`delta = r + k*W` 中的 `k` 正是需要跳过的访问次数，
因 `r < W`，整数除法直接给出 `k`。

tick 推进到某个 slot 时：

``` text
遍历 slot 内的 entry
    │
    ├── rounds > 0
    │       ↓
    │   rounds -= 1，保留在 slot
    │
    └── rounds == 0
            ↓
        取出，触发到期
```

另有一个边界：`target_tick < current_tick`(入轮时已过期)的任务
**不能塞进 slot**。轮已经推进过那一格，最早也要等到下一个 tick
边界才会再访问它。这类任务应挂在独立的 `expired` 列表上，由下一次推进
无条件排空 —— "已过期"的语义是"立刻吐出"，而不是"等下一个 tick"。

代价是远期任务每转一圈都要被遍历一次。配合 §4.2 的预加载 horizon
(只有 horizon 内到期的任务才入轮)，`rounds` 实际恒为 0，这个代价可以忽略。
`rounds` 的上限也因此可以用来约束 horizon，见 `max_horizon_ms`。

这也说明 §4.2 的预加载应当先于 §8 的分层时间轮实现，见 §30。

------------------------------------------------------------------------

## 7.2 时钟选择与 tick 补偿

两个必须在写代码前定下来的细节。

**单调时钟推进，wall clock 判断到期。**

`execute_at` 是 Unix 毫秒(wall clock)，而 NTP
校正会让系统时间向前或向后跳变。如果用 wall clock
推进 tick，一次跳变就会打乱 slot 顺序，甚至让 tick 倒退。

``` text
tick 推进      →  Instant      (单调，不受 NTP 影响)
到期判断       →  SystemTime   (wall clock，与 execute_at 同源)
基准校准       →  周期性用 wall clock 重算 wheel_start_at
```

两个时钟之间的漂移由 §11 的二次校验兜住：到期时若
`now < execute_at` 则重新调度。

**tick 必须补偿迟到。**

OS 调度抖动、一次长停顿都会让某次 tick 迟到。迟到后必须一次推进多个
slot，而不是推进一格然后一路欠账：

``` rust
// 错误：每次只推进一格，迟到后永远追不上
interval.tick().await;
wheel.advance_one_tick();
```

不做补偿，时间轮会越跑越慢且永不追上，所有延迟消息系统性偏晚。

**把补偿收进时间轮内部，而不是让调用方算格数。** 让推进接口收一个
**绝对时间**而非"推进几格"：

``` rust
pub fn advance_to(&mut self, now_ms: i64, out: &mut Vec<T>);
```

内部按 `floor_div(now_ms - start_ms, tick_ms)` 算出目标 tick，再
`while current_tick <= target` 循环。补偿自动完成，调用方只需定期把当前
时间递进来，迟到多久都能一次追上：

``` rust
loop {
    ticker.tick().await;
    out.clear();
    wheel.advance_to(clock.now_ms(), &mut out);
    for payload in out.drain(..) { /* 投递 */ }
}
```

这个签名顺带带来三个性质：

``` text
幂等       同一个 now_ms 调两次，第二次循环零次，什么都不做
抗回退     now_ms 变小时同样零次，不 panic 不 rewind current_tick
可测试     时间是入参，测试不需要 sleep，全部确定性且毫秒级跑完
```

最后一条尤其重要：时间轮的测试以 flaky 著称，根源就是习惯了
"sleep 然后断言"。时间从外部注入之后，这类测试全部变成纯函数式的。

**时钟来源**：不要直接把 `SystemTime::now()` 喂给 `advance_to`。
NTP 前跳会触发一次大范围扫掠，后跳则撞上上面的抗回退路径(虽然安全，
但等于白等)。用单调时钟推导出 wall clock：

``` rust
struct Clock {
    start_instant: Instant,
    start_ms: i64,   // 与 start_instant 同时捕获
}
// now_ms = start_ms + start_instant.elapsed().as_millis()
```

这样重新校准变成一个显式动作，而不是随时可能发生的意外。时间轮本身
只收 `i64`，不需要知道时钟策略 —— 两者可以分别测试。

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
tick = 1s
slots = 512

覆盖：
≈ 8.5 分钟


Level 1
tick ≈ 8.5 分钟
slots = 512

覆盖：
≈ 3 天


Level 2
tick ≈ 3 天
slots = 512

覆盖：
≈ 4.2 年
```

消息根据距离当前时间的范围进入不同层。

例如：

``` text
3 秒
    ↓
Level 0

5 分钟
    ↓
Level 0

30 分钟
    ↓
Level 1

3 小时
    ↓
Level 1

30 天
    ↓
Level 2
```

注意分层时间轮解决的是**时间跨度**，不是**内存容量**，见 §4.2。三层都在内存里，
总量问题仍然要靠预加载 horizon 解决。

事实上一旦实现了 §4.2 的预加载，内存时间轮只需要覆盖 horizon
(例如 5 分钟)，而单层 512 slot × 1s 单圈就有 8.5 分钟 —— 已经超过
horizon，`rounds` 恒为 0，分层时间轮的必要性大幅下降。

``` text
无预加载  →  必须分层，覆盖最大延迟(例如 30 天)
有预加载  →  单层即可，只覆盖 horizon(例如 5 分钟)
```

因此本文档把分层时间轮定位为**可选优化**，而非必需能力，见 §30
对实现顺序的调整。

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

注意上图的"加入 Timing Wheel"实际应为"若 `execute_at <= now + horizon`
则加入时间轮，否则只落 RocksDB，等预加载器拉取"，见 §4.2。

------------------------------------------------------------------------

## 10.3 入轮不是幂等的

offset commit 放在最后是对的，但 commit
本身和前面的写入不在一个事务里。崩溃窗口：

``` text
写入 RocksDB   ✓
加入时间轮     ✓
            ←── 崩溃
提交 Offset    ✗
```

重启后同一条消息被再消费一次：

``` text
RocksDB put    同 key 覆盖，无副作用
wheel.schedule 追加，产生第二个 TimerTask
```

于是同一 `message_id` 会到期两次，而 §12 的伪代码会让两个 task
各自走完整的"读取 → 投递 → 删除"流程。

§14 承认重复投递可以接受，但要区分两件事：

``` text
崩溃窗口内偶发重复      可以接受
每次重启都稳定重复      是缺陷
```

两道防线：

``` text
1. schedule 前查重
   维护 message_id -> 是否已在轮中 的集合

2. 状态机 CAS            ← 主要防线
   Pending -> Delivering 的原子转换
   转换失败说明已有投递在进行，直接放弃
```

第 2 条见 §12，它同时也挡住了预加载器与 recovery 可能造成的重复入轮。

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

-   系统时钟可能变化(tick 用单调时钟，`execute_at` 是 wall clock，两者会漂移，见 §7.2)
-   Timer tick 有精度误差
-   任务可能被重新调度
-   进程恢复后可能加载已经到期的任务
-   预加载器与消费路径可能重复入轮(§18.1)

------------------------------------------------------------------------

# 12. 到期处理伪代码

必须用上 §16 的状态机，否则 §10.3
的重复入轮会让两个 task 并发投递同一条消息。

``` rust
async fn on_timeout(task: TimerTask) -> Result<()> {

    let Some(stored) = store
        .get(&task.message_id)
        .await?
    else {
        // 已投递并删除，或已被取消
        return Ok(());
    };

    // 取消是 tombstone 式的，见 §22
    if stored.status == DelayStatus::Cancelled {
        return Ok(());
    }

    // 二次校验，见 §11
    let now = current_time_millis();

    if now < stored.message.execute_at {
        wheel.schedule(
            TimerTask::from(&stored.message)
        );

        return Ok(());
    }

    // 关键：CAS 抢占投递权
    // 并发的第二个 task 会在这里失败并退出
    if !store
        .compare_and_swap_status(
            &task.message_id,
            DelayStatus::Pending,
            DelayStatus::Delivering,
        )
        .await?
    {
        return Ok(());
    }

    // 等待 broker ack，见 §13
    let send_result = producer
        .send(
            &stored.message.target_topic,
            &stored.message.message_id,  // Kafka Key
            &stored.message.payload,
            &stored.message.headers,
        )
        .await;

    match send_result {
        Ok(_) => {
            // ack 之后才删除
            store
                .delete(&task.message_id)
                .await?;
        }
        Err(e) => {
            // 保留消息，回到 Pending 并重新调度
            let retry = stored.retry_count + 1;

            if retry > MAX_RETRY {
                store
                    .set_status(
                        &task.message_id,
                        DelayStatus::Failed,
                    )
                    .await?;

                dlq.send(&stored.message).await?;
            } else {
                store
                    .mark_retry(&task.message_id, retry)
                    .await?;

                wheel.schedule_after(
                    TimerTask::from(&stored.message),
                    backoff(retry),
                );
            }

            tracing::warn!(
                message_id = %task.message_id,
                retry,
                error = %e,
                "delay message send failed"
            );
        }
    }

    Ok(())
}
```

`Delivering` 状态需要配一个超时兜底：进程在 `Delivering`
和 `delete` 之间崩溃，这条消息会永久停在 `Delivering` 而不再被任何
task 触发。recovery(§17)必须把超过阈值的 `Delivering` 重置回
`Pending`，这正是 §14 所说的重复投递来源。

------------------------------------------------------------------------

# 13. Kafka 发送成功之后再删除

不能：

``` rust
producer.send(...);

store.delete(message_id);
```

因为 Producer 可能异步发送。`rdkafka` 的 `FutureProducer::send`
返回的 future 必须 await 到 broker ack，仅仅把消息放进本地
queue 不等于投递成功。

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

但用 `message_id` 作投递时的 Kafka Key 有个副作用：business-topic
的分区由 `hash(message_id)` 决定，而 `message_id`
是随机的，**同一业务实体的多条消息会散落到不同分区，失去顺序保证**。

``` text
需要顺序   →  Key 用业务键(order_id / user_id)
              message_id 放 header 传递
需要打散   →  Key 用 message_id
```

延迟消息本身不保证顺序(到期时间决定投递顺序)，所以多数场景用
`message_id` 没问题。但若下游依赖分区内有序，必须改用业务键。

另外注意 ULID 的时间前缀是**创建时间**而非 `execute_at`，不能用它
替代 §16.1 的时间索引。

------------------------------------------------------------------------

# 16. RocksDB 存储设计

主数据：

``` text
CF: delay_message

Key:
message_id

Value:
StoredDelayMessage
```

状态：

``` rust
pub enum DelayStatus {
    /// 等待到期
    Pending,
    /// 已抢占投递权，正在发送(§12 的 CAS 目标)
    Delivering,
    /// 投递成功但尚未删除，仅在保留历史时使用
    Delivered,
    /// 重试耗尽，已进 DLQ
    Failed,
    /// 已取消，到期时跳过(§22 tombstone 式取消)
    Cancelled,
}
```

`Delivered` 在默认流程中**不会出现** —— §12 投递成功后直接删除记录。
只有在需要保留投递历史(例如 §27 的消息查询要能查到已完成消息)时，
才把 delete 换成置为 `Delivered`，并配合 TTL 或 §24.1 的分片 drop 回收。

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

## 16.1 必须有按 execute_at 排序的二级索引

只有 `message_id -> DelayMessage` 一个 map 是不够的。

`message_id` 是 UUID / Snowflake / ULID，即便 ULID
带时间前缀，那也是**创建时间**而非 `execute_at`：

``` text
消息 A   created 10:00   execute_at 明天 10:00
消息 B   created 10:01   execute_at 10:05

按 message_id 扫描顺序：  A, B
按到期顺序：              B, A
```

**按主 key 扫出来的顺序和到期顺序无关。** 这会让三件事无法实现：

``` text
§4.2  预加载 execute_at <= now + horizon   →  只能全表扫
§17   recovery 找出已到期任务              →  只能全表扫
§27   按时间范围查询消息                   →  只能全表扫
```

所以需要第二个 column family：

``` text
CF: by_execute_at

Key:
execute_at (8 字节, big-endian) || message_id

Value:
(空)
```

**`big-endian` 不是实现细节。** RocksDB
按字节序比较 key，小端编码的时间戳排出来是乱序的：

``` text
execute_at = 1000  小端字节 e8 03 00 00 00 00 00 00
execute_at = 2000  小端字节 d0 07 00 00 00 00 00 00
                            ↑ 字节序比较得到 2000 < 1000，错误

大端字节 00 00 00 00 00 00 03 e8  (1000)
         00 00 00 00 00 00 07 d0  (2000)
                            ↑ 字节序比较正确
```

有了这个 CF，预加载和 recovery 都变成一次 range scan：

``` text
seek(encode_be(now))
    ↓
iterate until execute_at > now + horizon
    ↓
得到按到期顺序排列的 message_id
```

两个 CF 的写入必须放在同一个 `WriteBatch` 里，否则崩溃后会出现索引与主数据
不一致(有索引无消息，或有消息无索引)。

``` rust
let mut batch = WriteBatch::default();

batch.put_cf(cf_message, message_id, &encoded);
batch.put_cf(cf_index, index_key(execute_at, message_id), b"");

db.write(batch)?;
```

删除同理，两个 CF 的 delete 也要在一个 batch 内。

------------------------------------------------------------------------

## 16.2 RocksDB 是同步阻塞 API

`rocksdb` crate 的所有调用都是同步阻塞的。在 `async fn`
里直接调用会占住 tokio 的 worker 线程：

``` rust
// 错误：阻塞 tokio worker
async fn get(&self, id: &str) -> Result<Option<StoredDelayMessage>> {
    let bytes = self.db.get_cf(self.cf_message, id)?;  // 阻塞
    ...
}
```

千万级吞吐下这不是性能优化问题，而是会直接打死运行时 —— 所有
worker 线程都卡在 RocksDB 上，整个服务的异步任务(包括 Kafka
consumer 和 tick 推进)全部停摆。

两种解法：

``` text
方案 1  spawn_blocking 包裹
    每个存储操作丢到 blocking 线程池
    ✓ 改动小
    ✗ 每次调用有任务切换开销

方案 2  同步 trait + 专用线程池        ← 推荐
    DelayStore 定义为同步 trait
    由专门的 storage worker 线程执行
    通过 channel 与 async 世界通信
    ✓ 便于批量合并操作，开销可控
    ✗ 需要自己管理线程池与 channel
```

无论哪种，§18 的 trait 签名都要相应调整。

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
RocksDB  CF: by_execute_at
     │
     ▼
range scan，按到期顺序
     │
     ├── status == Delivering 且超时
     │       ↓
     │   重置为 Pending，见 §12
     │
     ├── execute_at <= now
     │       ↓
     │     立即处理
     │
     └── now < execute_at <= now + horizon
             ↓
        加入时间轮
             ↓
     (更远期的留在 RocksDB，由预加载器负责)
```

**recovery 不能一次性加载全部消息**，这和 §4.2
是同一个约束：1 亿条 pending 消息在启动时全读进内存会直接 OOM。
recovery 只需要处理 `now + horizon` 之前到期的部分。

------------------------------------------------------------------------

# 18. DelayStore API

trait 定义在 `kafka-delaymq-core`，而不是 `kafka-delaymq-storage`，见 §22。

``` rust
pub trait DelayStore: Send + Sync + 'static {

    fn put(
        &self,
        message: &DelayMessage,
    ) -> Result<()>;

    fn get(
        &self,
        message_id: &str,
    ) -> Result<Option<StoredDelayMessage>>;

    fn delete(
        &self,
        message_id: &str,
    ) -> Result<()>;

    /// 按 execute_at 范围扫描，走 CF: by_execute_at
    /// 返回轻量 TimerTask 而非完整消息，limit 限制单批上限
    fn scan_due(
        &self,
        from_execute_at: i64,
        to_execute_at: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>>;

    /// 投递权抢占，返回 false 表示当前状态不等于 expect
    fn compare_and_swap_status(
        &self,
        message_id: &str,
        expect: DelayStatus,
        next: DelayStatus,
    ) -> Result<bool>;

    fn set_status(
        &self,
        message_id: &str,
        status: DelayStatus,
    ) -> Result<()>;

    fn mark_retry(
        &self,
        message_id: &str,
        retry_count: u32,
    ) -> Result<()>;

    /// 找出卡在 Delivering 超过 timeout 的消息，见 §12
    fn scan_stale_delivering(
        &self,
        timeout_ms: i64,
        limit: usize,
    ) -> Result<Vec<TimerTask>>;
}
```

与原设计的四处差异，都是前面各节推导出来的：

``` text
1. 签名同步而非 async
   RocksDB 本身阻塞，见 §16.2
   async 外壳只会骗人，真正的隔离在调用侧

2. load_pending() -> Vec<DelayMessage>
   改为 scan_due(from, to, limit) -> Vec<TimerTask>
   全量加载会 OOM，且不该返回带 payload 的重对象

3. 增加状态转换方法
   CAS 是 §10.3 重复投递的主要防线

4. Send + Sync + 'static bound
   缺少 Send 时 tokio::spawn 无法编译
   同步签名也让 dyn DelayStore 可用，支持 §22 的多实现
```

若坚持 async trait，需要注意 AFIT(Rust 1.75+)产生的 trait
不能直接 `dyn`，而 §22 的 `RocksDbStore` / `MemoryStore` /
`PostgresStore` 多实现通常需要 `dyn DelayStore`，此时要么用
`async-trait`，要么手写 `-> impl Future<Output = ...> + Send`。

启动：

``` rust
fn recover(&self) -> Result<()> {

    let now = current_time_millis();

    // 1. 先修复卡住的 Delivering
    for task in self.store
        .scan_stale_delivering(DELIVERING_TIMEOUT_MS, BATCH)?
    {
        self.store.set_status(
            &task.message_id,
            DelayStatus::Pending,
        )?;
    }

    // 2. 分批加载，只到 horizon 为止
    let mut cursor = 0i64;

    loop {
        let tasks = self.store.scan_due(
            cursor,
            now + HORIZON_MS,
            BATCH,
        )?;

        if tasks.is_empty() {
            break;
        }

        for task in &tasks {
            if task.execute_at <= now {
                self.processor.enqueue(task.clone())?;
            } else {
                self.wheel.schedule(task.clone());
            }
        }

        cursor = tasks.last().unwrap().execute_at;
    }

    Ok(())
}
```

分页游标这里有个容易写错的地方：`execute_at`
不唯一，同一毫秒可能有多条消息。游标用 `last.execute_at + 1`
会跳过同毫秒的剩余消息，用 `last.execute_at` 则会重复处理已处理的部分。

实际实现应让 `scan_due` 的游标是完整的索引 key
(`execute_at || message_id`)而不是裸时间戳，这样才能精确续扫。上面的伪代码
为了可读性做了简化，重复入轮由 §12 的 CAS 兜住。

------------------------------------------------------------------------

# 18.1 预加载器

§4.2 的滚动预加载与 recovery 共用 `scan_due`，是一个常驻任务：

``` rust
async fn preload_loop(&self) {

    let mut ticker = tokio::time::interval(
        Duration::from_millis(PRELOAD_INTERVAL_MS)
    );

    loop {
        ticker.tick().await;

        let now = current_time_millis();

        // 只取下一个预加载窗口内新到期的
        let tasks = self.store.scan_due(
            self.preloaded_until,
            now + HORIZON_MS,
            BATCH,
        );

        match tasks {
            Ok(tasks) => {
                for task in tasks {
                    self.wheel.schedule(task);
                }
                self.preloaded_until = now + HORIZON_MS;
            }
            Err(e) => {
                // 不推进 preloaded_until，下轮重试
                tracing::error!(error = %e, "preload failed");
            }
        }
    }
}
```

关键约束：

``` text
PRELOAD_INTERVAL_MS  <<  HORIZON_MS
```

否则窗口推进赶不上时间流逝，任务会在尚未入轮时就已到期。例如
horizon = 5 分钟、预加载周期 = 1 分钟，留出 5 倍安全余量。

另外 §10.2 的消费路径与预加载器会竞争同一条消息：消息刚写入
RocksDB 且 `execute_at` 落在当前 horizon 内时，消费路径直接入轮，预加载器
也可能扫到它。同样由 §12 的 CAS 兜住。

------------------------------------------------------------------------

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

# 20. 关键约束：本地 RocksDB 与 Consumer Group Rebalance 不兼容

这是整个架构**最危险的一点**，必须在写代码前定下方案。

## 20.1 问题

假设 Partition 0 从 Server A 迁移到 Server B：

``` text
Server A                      Server B
    │                             │
    ├── Partition 0               │
    ├── RocksDB (本地磁盘)         ├── RocksDB (本地磁盘)
    │   └── P0 的未到期消息         │   └── 没有 P0 的数据
    │                             │
    └──────── Rebalance ─────────►├── Partition 0
                                  └── 读自己的 RocksDB → 空
```

Server B 的本地 RocksDB **没有** Partition 0
的数据，那些数据躺在 Server A 的磁盘上。

后果是消息永久丢失：

``` text
P0 的未到期消息
    ↓
offset 已 commit  →  Kafka 不会重投
    ↓
数据在 Server A 磁盘  →  Server B 读不到
    ↓
永久丢失
```

所以这不是"需要注意恢复"，而是**方案在当前形态下跑不通**。
§29 的 "Kafka Partition = Sharding" 成立的前提是节点无状态，
而本系统每个节点都有本地状态。

## 20.2 三种解法

### 方案 A：静态 assign

放弃 consumer group 的自动 rebalance，用 `assign()` 手工固定
partition 与节点的映射。

``` text
Partition 0  →  Server A  (固定)
Partition 1  →  Server B  (固定)
Partition 2  →  Server C  (固定)
```

节点故障时靠运维接管同一份磁盘(StatefulSet + PVC
或固定机器 + 共享存储)。

``` text
✓ 实现最简单，不需要额外机制
✓ 没有状态迁移问题
✗ 失去自动故障转移，节点挂了该分片停摆
✗ 扩缩容需要人工介入
```

### 方案 B：changelog topic

Kafka Streams 的 state store 做法。RocksDB
的每次写同步到一个 compacted topic，rebalance 后新节点从 changelog
重建。

``` text
写入 RocksDB
    ↓
同步写 changelog topic (compacted)
    ↓
Rebalance
    ↓
Server B 从 changelog 回放，重建 RocksDB
    ↓
重建时间轮
```

``` text
✓ 工业界标准解，故障转移完整
✓ RocksDB 退化为纯缓存，可随时重建
✗ 每次写放大一倍(RocksDB + Kafka)
✗ 重建期间该 partition 不可用，大状态下重建很慢
✗ 实现复杂度最高
```

### 方案 C：delay-topic 自己作为 changelog ← 第一版推荐

不额外引入 changelog，直接把 delay-topic 当作事实来源。

``` text
delay-topic retention > 最大延迟时长
    +
周期性 checkpoint 已完成投递的 offset 水位
    ↓
Rebalance 后
    ↓
Server B 从 checkpoint offset 重新消费 P0
    ↓
重建 RocksDB 与时间轮
```

关键在于 offset commit 语义的改变：

``` text
原设计   消息入轮后立刻 commit
         ↓
         崩溃/迁移后无法重建

方案 C   只 commit "该 offset 之前的消息全部已投递完成" 的水位
         ↓
         未投递消息始终可以从 Kafka 重放
```

这意味着 commit 水位由**最早的未投递消息**决定，一条 3
天后到期的消息会把水位卡住 3 天。所以需要独立维护 checkpoint，而非
依赖 Kafka 的 offset commit 本身。

``` text
✓ 不额外引入组件，复用已有的 delay-topic
✓ 故障转移完整，RocksDB 可完全重建
✗ 要求 retention ≥ 最大延迟(3 天延迟就要留 3 天以上)
✗ rebalance 后要重放大量消息，恢复时间与 retention 成正比
✗ 最大延迟受 retention 上限约束，无法真正"任意"
```

## 20.3 结论

``` text
第一版       方案 A(静态 assign)
             最快打通链路，先验证时间轮与存储正确性

生产部署     方案 C
             retention 覆盖业务最大延迟

超长延迟     方案 B
             延迟超过可接受的 retention 时
```

无论选哪个：

> **"每个实例维护自己的时间轮"必须配套说明该实例的持久化状态在
> ownership 变化时如何跟随，否则消息会静默丢失。**

§19 的 "Partition = Sharding" 只在补上这一层之后才成立。

------------------------------------------------------------------------

# 21. 推荐的 Rust Workspace

整个系统可以作为一个 Rust Workspace。

``` text
clamber-rust-infra/
│
├── Cargo.toml
│
├── crates/
│   │
│   ├── kafka-timing-wheel/
│   │
│   ├── kafka-delaymq-core/
│   │
│   ├── kafka-delaymq-storage/
│   │
│   └── kafka-delaymq-kafka/
│
├── service/
│   │
│   └── delay-server/
│
└── tools/
    │
    ├── delay-cli/
    └── benchmark/
```

命名与实际仓库保持一致：crate 带 `kafka-` 前缀，顶层目录是 `service`
(单数)。原文档写的 `timing-wheel/` / `delaymq-core/` / `services/`
与仓库不符，已更正。

`common/` 暂不设立 —— 当前只有 `DelayMessage` / `TimerTask` /
`DelayStore` 需要共享，它们都属于领域模型，放 `kafka-delaymq-core`
即可。等出现与领域无关的通用设施(配置加载、ID 生成、metrics
封装)再抽。

------------------------------------------------------------------------

# 22. crate 职责

## kafka-timing-wheel

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

依赖上只需要 `tokio` / `tracing` / `thiserror`，**不应出现
`rdkafka` 或 `rocksdb`** —— 这两个都是 C/C++ 绑定且编译极慢，混进来会
拖垮增量编译，也会让分层失效。

关于 `cancel` 的实现代价：slot 内部是链表或 `Vec`，按 `message_id`
取消需要 O(n) 扫描 slot，除非额外维护
`message_id -> (level, slot, index)` 索引 —— 而该索引在 cascade
时必须同步更新，很容易写错。

推荐改为 **tombstone 式取消**：不动时间轮，只在 store
里把状态标记为 `Cancelled`，到期时 §12 发现状态不对直接跳过。

``` text
wheel.cancel()        O(n) 扫描 + cascade 时维护索引
store 标记 Cancelled  O(1)，到期时自然跳过   ← 推荐
```

因此 `cancel` 可以从时间轮的职责里去掉。

------------------------------------------------------------------------

## kafka-delaymq-core

负责：

``` text
DelayMessage
TimerTask
DelayStatus
DelayStore trait        ← 定义在这里
DelayService
DelayProcessor
Preloader
业务状态机
```

依赖：

``` text
kafka-timing-wheel
```

**不依赖** `kafka-delaymq-storage`，也不依赖 Kafka。

------------------------------------------------------------------------

## kafka-delaymq-storage

负责：

``` text
RocksDB
Column Family 设计
持久化
Recovery
Checkpoint
```

**实现** core 定义的 trait：

``` rust
impl DelayStore for RocksDbStore
```

依赖：

``` text
kafka-delaymq-core   (为了拿到 trait 与领域模型)
rocksdb
```

未来可以实现：

``` text
RocksDbStore
MemoryStore      (测试用)
PostgresStore
```

------------------------------------------------------------------------

## kafka-delaymq-kafka

负责：

``` text
Kafka Consumer
Kafka Producer
Kafka Record
Kafka Headers
Offset
Partition
Rebalance 回调
```

依赖：

``` text
kafka-delaymq-core   (为了拿到领域模型)
rdkafka
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

关键在于 **依赖方向**：`storage` 依赖 `core`，而不是 `core` 依赖
`storage`。

``` text
                    delay-server
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
  kafka-delaymq-   kafka-delaymq-  kafka-delaymq-
      kafka            storage         core
          │                │             │
          └────────────────┴─────────────┤
                      依赖 core          │
                                         ▼
                              kafka-timing-wheel
```

更严格地说：

``` text
kafka-delaymq-core
    │
    ├── kafka-timing-wheel
    │
    └── 定义 DelayStore trait、DelayMessage、TimerTask
            ↑                    ↑
            │                    │
kafka-delaymq-storage    kafka-delaymq-kafka
    │                        │
    └── impl DelayStore      └── Kafka adapter
        for RocksDbStore

delay-server
    │
    └── 组装：注入 RocksDbStore 作为 dyn DelayStore
```

原文档 §22 写 trait 定义在 `delaymq-storage`，§23 又说 core
依赖 "timing-wheel + DelayStore trait" 且 core "不应强依赖 Kafka"
—— 这组描述自相矛盾：

``` text
若 trait 定义在 storage
    ↓
core 依赖 storage
    ↓
rocksdb 被拖进 core 的依赖树
    ↓
抽象失效，MemoryStore / PostgresStore 的扩展点失去意义
```

这就是依赖倒置原则(DIP)：**抽象属于使用方，不属于实现方。**
`core` 使用 `DelayStore`，所以 trait 归 `core`；`storage`
只提供实现，由 `delay-server` 在组装时注入。

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

## 24.1 压测时会先暴露的瓶颈：逐条 delete 的写放大

§13 要求投递成功后删除，语义上正确，但 RocksDB 的 delete
是写一条 tombstone，而不是就地抹除。

两个 CF 都要删(§16.1)，于是：

``` text
1000 万条消息
    ↓
1000 万次 put × 2 CF  = 2000 万次写
1000 万次 delete × 2 CF = 2000 万次写(tombstone)
    ↓
合计 4000 万次写 + 持续的 compaction 压力
```

tombstone 在 compaction 之前一直占空间，且会拖慢 range
scan —— 而 §4.2 的预加载器**高度依赖** range scan
性能。大量 tombstone 堆积时，`scan_due` 要跳过成片的已删除 key。

按时间分片成多个 CF、整片 drop 比逐条 delete 便宜得多：

``` text
CF: delay_message_2026_10_01
CF: delay_message_2026_10_02
CF: delay_message_2026_10_03
    ↓
某天的消息全部投递完
    ↓
drop_cf()  →  O(1) 直接删文件，无 tombstone
```

逐条 delete 仍然保留(单条投递成功后要立即标记)，但靠分片
drop 回收空间，而不是靠 compaction 消化 tombstone。

这一项在 §30 Step 13 压到 1000 万条时会成为实际瓶颈，设计 CF
时就应预留分片维度。

------------------------------------------------------------------------

# 25. 第一阶段推荐实现

``` text
Rust 2024 edition
Tokio
rdkafka      (librdkafka 绑定，需要系统 C 工具链)
RocksDB      (同步阻塞 API，见 §16.2)
Timing Wheel (单层 + round counter)
Preloader
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
状态机 + CAS
       ↓
Recovery
       ↓
Checkpoint
       ↓
Retry
       ↓
Dead Letter
       ↓
Rebalance 状态跟随(§20 方案 C)
```

注意 Hierarchical Timing Wheel / Cascade
已从第二阶段移出 —— 有了 Preloader 之后单层时间轮就够用，分层只在确认
需要超长 horizon 时才做，见 §8 与 §30 Step 15。

第二阶段真正的重点是**正确性**(状态机、recovery、rebalance)，而不是
时间轮的结构复杂度。

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

三个接口的实现约束：

``` text
DELETE  /messages/{id}
    置 status = Cancelled，不操作时间轮(§22)
    到期时 §12 发现状态不对直接跳过

POST    /messages/{id}/reschedule
    改 execute_at 必须同时更新 by_execute_at 索引：
    删旧索引 key + 写新索引 key + 更新主记录
    三个操作必须在同一个 WriteBatch 内(§16.1)
    时间轮里的旧 task 不必清除，到期时二次校验会重新调度(§11)

GET     /messages/{id}
    若默认流程投递后即删除，已完成消息查不到
    需要可查询则改用 Delivered + TTL，见 §16
```

一个遗漏点：`POST /delay/messages`
是 HTTP 入口，与 §2 的 Kafka 入口并存。两条入口要走同一套写入逻辑
(RocksDB + 索引 + 条件入轮)，否则容易出现 HTTP
写入的消息没进索引这类不一致。推荐 HTTP handler
内部仍然投递到 delay-topic，而不是直接写 store。

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
                  │  DelayMessageStore           │
                  │  RocksDB + execute_at 索引   │
                  │       │                     │
                  │       ▼  Preloader          │
                  │  Timing Wheel (horizon)     │
                  │       │                     │
                  │       ▼                     │
                  │  DelayProcessor             │
                  │  CAS + Retry + DLQ          │
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

但这一条**有前提**：Partition 作为分片单位只在节点无状态时自然成立，
而本系统每个节点持有本地 RocksDB。必须按 §20
选定状态跟随方案(静态 assign / changelog / delay-topic
重放)，否则 rebalance 会静默丢消息。

### 5. 内存只装近期，磁盘装全量

``` text
Timing Wheel = 近期窗口(horizon)
RocksDB      = 全量 + execute_at 索引
```

这是 §4.2 的结论，决定系统能否撑到千万级以上。

最终形成：

``` text
Kafka
  +
Persistent Store (execute_at 索引 + 状态机)
  +
Timing Wheel (近期窗口) + Preloader
  +
At-Least-Once Delivery
  +
Idempotency
```

这就是整个"Kafka + Rust 时间轮任意延迟消息"系统的核心。

------------------------------------------------------------------------

# 30. 推荐开发顺序

动手之前有两个决定必须先做，否则 Step 1 写完就要返工：

``` text
决定 1   DelayStore trait 的归属与依赖方向
         → 已定：trait 在 core，storage 依赖 core，见 §23

决定 2   §20 的 rebalance 状态跟随方案
         → 第一版用静态 assign，生产前升级到方案 C
```

实现顺序：

``` text
Step 0
定义领域模型
DelayMessage / TimerTask / DelayStatus / DelayStore trait
(全部在 kafka-delaymq-core，不依赖 Kafka 与 RocksDB)
        ↓
Step 1
实现单层 Timing Wheel + round counter(§7.1)
含 tick 补偿与单调时钟推进(§7.2)
        ↓
Step 2
编写 Timing Wheel Benchmark
顺带验证 tick 在高负载下不漂移
        ↓
Step 3
实现 MemoryStore
让 core 的逻辑先在内存实现上跑通并可单测
        ↓
Step 4
实现 RocksDB DelayStore
两个 CF(delay_message + by_execute_at，§16.1)
WriteBatch 保证两者一致
spawn_blocking 或专用线程池隔离阻塞(§16.2)
        ↓
Step 5
实现 Preloader(§18.1)
内存只装 horizon 内的任务 —— 这是容量基础，不能推后
        ↓
Step 6
实现 Kafka Consumer
静态 assign，先不碰 rebalance
        ↓
Step 7
实现 Kafka Producer
await 到 broker ack 后才删除(§13)
        ↓
Step 8
打通完整延迟消息链路
此时已能支持任意延迟(靠 round counter + Preloader)
        ↓
Step 9
实现状态机与 CAS(§12)
Pending -> Delivering -> 删除
含 Delivering 超时重置
        ↓
Step 10
实现进程重启 Recovery(§18)
分批扫描，只加载 horizon 内
        ↓
Step 11
实现 Retry / DLQ / tombstone 式 Cancel
        ↓
Step 12
实现 §20 方案 C
checkpoint 水位 + rebalance 后重放重建
        ↓
Step 13
实现 Metrics / Admin API
        ↓
Step 14
百万级/千万级压力测试
重点观测 tombstone 堆积对 scan_due 的影响(§24.1)
        ↓
Step 15
(可选) Hierarchical Timing Wheel
仅在确认需要超长 horizon 时才做，见 §8
```

与原顺序的主要差异：

``` text
Preloader      Step 10 之后  →  Step 5
               它是容量基础，不是优化项

状态机 / CAS   原本缺失      →  Step 9
               §10.3 的重复投递没有它挡不住

MemoryStore    原本缺失      →  Step 3
               让 core 逻辑能脱离 RocksDB 单测

分层时间轮     Step 10       →  Step 15(可选)
               有 Preloader 之后单层就够用

rebalance 方案 原本缺失      →  Step 12
               §20 的丢消息问题必须显式解决
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

------------------------------------------------------------------------

# 32. 待决事项

动手前需要确认的问题，按影响范围排序。

``` text
1. §20 rebalance 方案
   第一版静态 assign 是否可接受？
   生产部署的最大延迟是多少 → 决定 retention 能否覆盖

2. §4.2 horizon 取值
   取决于业务的到期速率分布
   需要先有真实流量特征才能定

3. §5 payload 大小上限
   单条超过多大就必须外置到对象存储

4. §16 是否保留投递历史
   影响 Delivered 状态与 §27 的查询接口

5. §15 business-topic 的 Kafka Key
   用 message_id 还是业务键 → 取决于下游是否依赖分区有序
```

------------------------------------------------------------------------

# 附录：本次文档修订记录

原文档的核心抽象("持久化记住消息、时间轮记住时间")成立，分层时间轮的
容量计算也正确。本次修订补充的是会导致返工的缺口。

``` text
严重
  §20   本地 RocksDB 与 rebalance 不兼容，会静默丢消息
        原文档只说"必须能恢复"，未指出数据不在新节点上

  §4.2  只存索引不解决总量问题，1 亿条 TimerTask 约 13 GB
        原文档把"时间跨度"和"内存容量"当成一个问题
        补充 Preloader 冷热分离

  §16.1 缺少按 execute_at 排序的索引
        message_id 的顺序与到期顺序无关
        导致预加载、recovery、时间范围查询都只能全表扫

  §23   DelayStore trait 归属矛盾，依赖方向反了
        trait 应在 core，storage 依赖 core(依赖倒置)

实现期必踩
  §16.2 RocksDB 同步阻塞，在 async 中直接调用会打死 tokio 运行时
  §7.1  单层时间轮单圈只覆盖 8.5 分钟，第一版链路需要 round counter 过渡
  §7.2  tick 需用单调时钟推进，且必须补偿迟到
  §10.3 入轮不幂等，重启会稳定产生重复 task
  §12   伪代码未使用 §16 定义的状态机，挡不住并发重复投递
  §18   load_pending() 全量加载会 OOM，签名需改为分批 scan
  §13   rdkafka 的 send 必须 await 到 broker ack

设计取舍需明确
  §5    payload 与 (partition, offset) 语义冗余，已明确选方案 A
  §22   cancel 的 O(n) 代价，改为 tombstone 式取消
  §24.1 逐条 delete 的 tombstone 堆积会拖慢 range scan
  §15   message_id 作 Kafka Key 会打散业务顺序

文档一致性
  §21   目录命名与实际仓库不符(已对齐 kafka- 前缀、service 单数)
  §16   Delivered 状态在默认流程中不会出现
  §26   分层时间轮从第二阶段移出，重点改为正确性
  §30   调整实现顺序：Preloader 前移，状态机/MemoryStore 补入
```
