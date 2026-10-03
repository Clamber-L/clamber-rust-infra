# 时间概念速查

> 这套系统里同时存在**四种**不同的"时间"，互相之间不能混用。
> 本文把每一个都钉死：单位、时钟源、谁拥有它、以及混用会出什么事。
>
> 架构背景见 `kafka-rust-timing-wheel-delaymq-architecture.md`，
> 本文只讲时间。

------------------------------------------------------------------------

## 0. 一页速查表

| 名字 | 类型 | 单位 | 时钟源 | 谁拥有 |
|---|---|---|---|---|
| `execute_at` | `i64` | 毫秒 | wall clock | 业务（Producer 指定） |
| `now_ms` | `i64` | 毫秒 | 由单调时钟推导 | `Clock`（服务层） |
| `start_ms` | `i64` | 毫秒 | wall clock，构造时捕获一次 | `TimingWheel` |
| `tick_ms` | `u64` | 毫秒 | — （是跨度，不是时刻） | 配置 |
| `current_tick` | `u64` | tick 数 | — （是计数，不是时刻） | `TimingWheel` |
| `target_tick` | `u64` | tick 数 | — | `schedule` 内部临时量 |
| `rounds` | `u64` | 圈数 | — | `Entry` |
| `horizon_ms` | `u64` | 毫秒 | — （是跨度） | Preloader 配置 |

一句话区分：

``` text
以 _ms 结尾        →  毫秒，可能是「时刻」也可能是「跨度」
以 _at 结尾        →  毫秒，一定是「绝对时刻」
以 _tick / rounds  →  计数，不是时间，乘以 tick_ms 才有时间含义
```

------------------------------------------------------------------------

## 1. 两个根本区分

读代码时先问这两个问题，大部分混乱会自己消失。

### 1.1 时刻 vs 跨度

``` text
时刻 (instant)    这是什么时候        execute_at, now_ms, start_ms
跨度 (duration)   这是多长一段        tick_ms, horizon_ms, delay_ms
```

两者单位相同（毫秒）、类型相近，所以编译器**不会**帮你挡住混用。

``` text
时刻 - 时刻 = 跨度     ✓  execute_at - start_ms = delay_ms
时刻 + 跨度 = 时刻     ✓  now_ms + horizon_ms   = 预加载上界
时刻 + 时刻 = ???      ✗  没有意义，但能编译通过
```

命名上刻意用 `_at` 标记时刻、`_ms` 标记跨度，但 `now_ms` / `start_ms`
是历史遗留的例外（它们是时刻）。看到 `_ms` 要多想一步。

### 1.2 绝对 vs 相对

时间轮内部最容易出错的一组。

``` text
绝对 tick    从 start_ms 起算          current_tick, target_tick
相对 tick    从 current_tick 起算      delta
```

这个区分直接对应 §7.1 的 1 号陷阱：

``` rust
let slot   = (target_tick & mask) as usize;      // 绝对
let rounds = delta / wheel_size as u64;          // 相对
```

**`slot` 用绝对值，`rounds` 用相对值。** 混用的后果：若 slot 也叠加
`current_tick`，起点被重复计算一次。`current_tick == 0` 时恰好正确，
所以这个 bug 能通过所有"新建轮子立刻测试"的用例，一旦轮转起来就错。

------------------------------------------------------------------------

## 2. 逐个定义

### `execute_at` — 业务要求的投递时刻

``` text
类型    i64
单位    Unix 毫秒
时钟    wall clock
来源    Producer 在 DelayMessage 里指定
```

**整个系统唯一的业务真相。** 其他所有时间量都是为了高效地判断
"`execute_at` 到了吗"而存在的中间产物。

一律用绝对时刻，不用 `delay_level` 或"延迟多少秒"。原因见架构文档
§5：绝对时刻能统一表达 3 秒和 3 天，相对延迟在经过队列、重启、重试
之后基准会漂。

``` text
3 秒后    →  execute_at = now + 3_000
3 天后    →  execute_at = now + 259_200_000
指定时刻  →  execute_at = 该时刻的 Unix 毫秒
```

### `now_ms` — 推进时间轮用的当前时刻

``` text
类型    i64
单位    Unix 毫秒
时钟    由单调时钟推导出的 wall clock（关键）
来源    服务层的 Clock，作为 advance_to 的入参
```

它看起来是 wall clock，**但不能直接用 `SystemTime::now()` 取**：

``` rust
struct Clock {
    start_instant: Instant,   // 单调
    start_ms: i64,            // 与 start_instant 同时捕获
}

fn now_ms(&self) -> i64 {
    self.start_ms + self.start_instant.elapsed().as_millis() as i64
}
```

直接用 `SystemTime::now()` 的后果：

``` text
NTP 前跳 1 小时   →  advance_to 一次扫掠 3600 个 tick
                     （安全但毫无必要，且会一次吐出大量任务）
NTP 后跳 1 小时   →  advance_to 变成 no-op 一小时
                     （不 panic，但时间轮停摆）
```

用单调时钟推导，两者都免疫。重新校准变成一个**显式动作**，而不是随时
可能发生的意外。

### `start_ms` — 时间轮的时间原点

``` text
类型    i64
单位    Unix 毫秒
时钟    wall clock，构造时捕获一次，之后不变
来源    TimingWheel::new 的入参
```

定义：**`tick 0` 所对应的 wall clock 时刻。** 它把"绝对时刻"和
"tick 计数"这两个世界连起来：

``` text
tick N 对应的时刻  =  start_ms + N × tick_ms
时刻 T 对应的 tick =  (T - start_ms) / tick_ms
```

三个要点：

- **不可变。** 一旦有任务入轮，改 `start_ms` 会让所有已存在的
  `rounds` 和 slot 位置全部失准。需要重设原点只能新建一个轮。
- **`execute_at < start_ms` 必须拒绝**，返回 `TimeBeforeStart`。
  否则减法为负，而 Rust 的整数除法朝零截断，负数下 ceil/floor
  的行为都会出乎意料。
- 进程重启后 `start_ms` 是新的，这没问题 —— 时间轮是纯内存结构，
  重启后由 recovery（架构文档 §18）从 RocksDB 重建。

### `tick_ms` — 时间轮的时间粒度

``` text
类型    u64
单位    毫秒（这是跨度，不是时刻）
取值    1000（1 秒）
```

**本项目取 1 秒。** 它同时决定两件事：

``` text
唤醒频率      1 次/秒
触发抖动      < 1 秒，且只会偏晚不会偏早
```

"只会偏晚"来自 ceil/floor 的不对称（见 §3）。对延迟消息来说 1 秒的
滞后可以忽略，而相比 100ms 省下十分之九的唤醒和空转扫描。

`tick_ms == 0` 会在第一次除法 panic，构造时 assert 掉。

### `current_tick` — 轮已推进到哪

``` text
类型    u64
单位    tick 计数（不是时间）
```

**定义：下一个待处理的 tick。** 不是"已处理到的 tick" —— 这一个字的
差别决定了循环边界是 `<=` 还是 `<`：

``` rust
while self.current_tick <= target_tick {   // 闭区间，处理 target 自身
```

由此得到时间轮的核心不变量：

> **轮中所有 entry 的 `target_tick >= current_tick`。**

`expired` 列表的存在就是为了维持它：入轮时已过期（`target_tick <
current_tick`）的任务不能塞进 slot，否则违反不变量，且要等到下一个
tick 边界才会被访问。

### `target_tick` — 某个任务该在哪个 tick 触发

``` text
类型    u64
单位    tick 计数
作用域  schedule 内部临时量，不存储
```

``` rust
let target_tick = (execute_at - start_ms).div_ceil(tick_ms);
```

**用 ceil。** 见 §3。

存储时被拆成 `(slot, rounds)` 两部分，`target_tick` 本身不保留 ——
这是时间轮省内存的关键：`slot` 是数组下标（隐含），`rounds` 通常是 0。

### `rounds` — 还要转几圈

``` text
类型    u64
单位    圈数（1 圈 = wheel_size 个 tick）
```

``` rust
let delta  = target_tick - current_tick;   // 相对！
let rounds = delta / wheel_size as u64;
```

为什么 `delta / W` 是对的：设 `r = delta mod W`，该 slot 首次被访问在
`current_tick + r`，之后每 `W` 个 tick 一次。`delta = r + k*W` 中的
`k` 正是需要跳过的访问次数；因 `r < W`，整数除法直接给出 `k`。

每次经过该 slot，`rounds -= 1`；减到 0 才真正触发。

配合 Preloader，`rounds` 在正常路径上**恒为 0**：

``` text
horizon     = 5 分钟
单圈        = 512 × 1s = 8.5 分钟
horizon < 单圈  →  rounds 恒为 0
```

### `horizon_ms` — 内存里装多久的任务

``` text
类型    u64
单位    毫秒（跨度）
取值    待定，见架构文档 §32
```

Preloader 只把 `execute_at <= now_ms + horizon_ms` 的任务灌进时间轮，
其余留在 RocksDB。这是内存占用与消息总量解耦的关键（架构文档 §4.2）：

``` text
内存占用  ∝  到期速率 × horizon
          ⊥  消息总存量
```

约束：`预加载周期 << horizon_ms`，建议留 5 倍余量。否则窗口推进赶不上
时间流逝，任务会在尚未入轮时就已到期。

------------------------------------------------------------------------

## 3. ceil / floor 的不对称

**整篇文档最该记住的一条。**

``` text
schedule   取 ceil    →  宁可晚一个 tick
advance_to 取 floor   →  宁可少推一格
```

两边都朝"保守"的方向取整，合起来保证：

> **任务永远不会早于 `execute_at` 触发，最多晚一个 `tick_ms`。**

举例（`tick_ms = 1000`，`start_ms = 0`，`execute_at = 1500`）：

``` text
用 floor:  target_tick = 1  →  在 1000ms 触发  →  早 500ms  ✗
用 ceil:   target_tick = 2  →  在 2000ms 触发  →  晚 500ms  ✓
```

**早触发不是精度误差，是正确性问题。** 架构文档 §12 的二次校验会发现
`now < execute_at` 然后重新调度，而重算出来可能还是同一个 slot ——
任务在那里空转，每圈被遍历一次却永远触发不了，直到真正到期。

> `never_fires_early_for_non_tick_aligned_deadline` 是这条的回归测试。

------------------------------------------------------------------------

## 4. 四种时间的流转

一条消息从进入到投递，经过的时间量变换：

``` text
Producer
   │  execute_at = 1700000005000      ← wall clock 时刻
   ▼
RocksDB
   │  CF: by_execute_at
   │  key = execute_at(8B big-endian) || message_id
   │                                  ← 仍是 wall clock 时刻
   ▼                                     大端是为了字节序 = 时间序
Preloader
   │  scan_due(from, now_ms + horizon_ms)
   │                                  ← 时刻 + 跨度 = 时刻
   ▼
TimingWheel::schedule
   │  delay_ms    = execute_at - start_ms         时刻 - 时刻 = 跨度
   │  target_tick = ceil(delay_ms / tick_ms)      跨度 / 跨度 = 计数
   │  delta       = target_tick - current_tick    绝对 - 绝对 = 相对
   │  slot        = target_tick % wheel_size      ← 绝对
   │  rounds      = delta / wheel_size            ← 相对
   ▼
TimingWheel::advance_to(now_ms)
   │  target_tick = floor((now_ms - start_ms) / tick_ms)
   │  while current_tick <= target_tick: ...
   ▼
DelayProcessor
   │  if now_ms < execute_at { reschedule }       ← 回到 wall clock 校验
   ▼
Kafka business-topic
```

注意首尾两端都是 `execute_at`（wall clock），中间的 tick / rounds
只是**加速判断的索引**。任何时候对投递时机有疑问，答案都在
`execute_at` 上，不在时间轮里 —— 这也是二次校验能兜住一切时钟问题的
原因。

------------------------------------------------------------------------

## 5. 混用会出什么事

| 写错 | 现象 | 测试 |
|---|---|---|
| `slot` 用了相对值 | `current_tick == 0` 时正确，轮转起来后晚整圈 | `slot_math_is_correct_after_wheel_has_turned` |
| `rounds` 用了绝对值 | 远期任务晚若干圈触发 | 同上 |
| `target_tick` 用 floor | 提前触发，二次校验后空转 | `never_fires_early_for_non_tick_aligned_deadline` |
| `advance_to` 用 ceil | 提前一格处理整个 slot，同上 | 同上 |
| 已过期任务塞进 slot | 要等下一个 tick 边界，最坏晚一圈 | `already_expired_deadline_fires_immediately` |
| 直接用 `SystemTime::now()` | NTP 跳变时扫掠或停摆 | — （属服务层，需另测） |
| 改了 `start_ms` | 所有 `rounds` 与 slot 失准 | — （API 上设为不可变） |
| `current_tick` 语义记反 | 循环边界差一，整体晚/早一个 tick | `fires_at_deadline` |
| 时钟回退未处理 | panic 或 `current_tick` 倒退 | `clock_going_backwards_is_a_noop` |

------------------------------------------------------------------------

## 6. 当前参数

``` text
tick_ms        = 1000           1 秒
wheel_size     = 512            2 的幂，mask = 511
单圈跨度       = 512 秒 ≈ 8.5 分钟
max_horizon_ms = 1000 × 512 × 1024 ≈ 6 天
                （默认上限，可用 with_max_horizon_ms 覆盖）

horizon_ms     = 待定，见架构文档 §32
                 建议 < 8.5 分钟，使 rounds 恒为 0
```

`wheel_size` 取 2 的幂是为了把 `% wheel_size` 写成 `& mask` ——
每个任务每次入轮都要算，值得。512 个空 `Vec` 约 12 KB，可忽略。

`max_horizon_ms` 的用途不是防算术溢出（i64 时间戳推出的 `delay_ms`
溢不出 u64），而是约束 `rounds` 的上限 —— 圈数越大，远期任务被空转
遍历的次数越多。
