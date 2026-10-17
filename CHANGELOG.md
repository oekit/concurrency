# Changelog — oekit/concurrency

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

## [v0.7.0] — 2026-10-18

### feat(async)：panic 收编统一 typed *PanicError（[#12](https://github.com/oekit/concurrency/issues/12)）
- 新增 `async.PanicError{Value, Stack, Task}`——`RunSafeErr`/`RunSafeRet`/`RunSafeRetLabeled` 的
  panic 分支、`syncx.Slot.Err`（新增字段承载，`Panicked` 标 Deprecated 保留）、
  `singleflight.Flight` waiter 错误（`fmt.Errorf %w` 包装，前缀文案保留）统一使用本类型；
  `errors.As` 可辨、携带栈；Error() 文案与历史实现一致，字符串匹配零破坏

### feat(async)：实例级 Runner——库形态 panic 路由（[#13](https://github.com/oekit/concurrency/issues/13)）
- `Runner{Handler}` 携带自己的 PanicHandler（GoSafe/GoSafeLabeled/GoWithContext/RunSafe/RunSafeErr
  方法族 + `RunSafeRetWith[T]` 泛型包级）；零值回退全局 handler，纯增量。
  库被宿主 import 时不再需要（也不能）动全局 SetPanicHandler

### feat(retry)：DelayHint + JitterRatio（[#14](https://github.com/oekit/concurrency/issues/14)）
- `Config.DelayHint func(err) (d, ok)`——服务端退避建议（Retry-After）优先于计算退避，
  仍受 MaxDelay 封顶；ok=false 走计算退避
- `Config.JitterRatio`——按当次退避幅度比例抖动（单边向上 [d, d*(1+ratio))），
  修复固定 Jitter 在高 attempt 档占比趋零导致重试重新同步；>0 时优先于固定 Jitter

### feat(retry/httpx) 新子包：HTTP 可重试性判定（[#15](https://github.com/oekit/concurrency/issues/15)）
- `Retryable(status)`（429/5xx）、`HTTPError{Status, Err}`（Unwrap 保留链）、
  `IsTransportRetryable`（ctx 取消/超时 false）、`Classifier()`（直塞 `Config.Retryable`）、
  `RetryAfterHint()`（直塞 `Config.DelayHint`，配 `NewRetryAfterError`/`NewRetryAfter`）；
  零外部依赖（不 import net/http，状态码以 int 传入）

### feat(budget) 新子包：预算/配额记账原语（[#16](https://github.com/oekit/concurrency/issues/16)）
- `Ledger`：按维度累计消耗，**超限内容存档不丢**（`Consume` → `ErrOverBudget + Ref`，
  `Get(ref)` 取回，`Sweep(olderThan)` 按龄清理）；`WithClock` 注入时钟确定性测试；
  未声明维度不限量；超限消耗不计入。与丢弃式流控面（ratelimit/shedding/breaker）互补

## [v0.6.0] — 2026-10-17

- **feat(async)**：`RunSafeErr` / `RunSafeRet[T]` / `RunSafeRetLabeled`——panic 收编为
  error 透给调用方（「panic→领域 error」场景可采用）
  （[#8](https://github.com/oekit/concurrency/issues/8)）
- **feat(singleflight)**：`Flight.DoChan`——channel 交付结果；配合
  `context.WithoutCancel` 实现「leader 免疫取消 + waiter 各自 ctx 退出」
  （[#10](https://github.com/oekit/concurrency/issues/10)）

## [v0.5.4] — 2026-10-17

- pkg 地板 v0.10.0 → v0.11.0（mathx Upward/jsonrepair UnmarshalLenient，零代码改动）

## [v0.5.3] — 2026-10-17

- fix(deps): pkg 地板 v0.9.3 → v0.10.0（BREAKING 拆分版；本仓仅引用 singleflighttest，零代码改动）

## [v0.5.2] — 2026-10-17

- build: go 最低版本实际降至 1.24.0——golang.org/x/time 锁 v0.14.0（声明 go≤1.24 的最高版）；pkg 地板 v0.8.0 → v0.9.3（v0.5.1 事故修正）

## [v0.5.0] — 2026-10-17

- feat(retry): `Config` 新增指数退避 `Exponential`（`Delay*2^(attempt-1)`，受 `MaxDelay` 封顶、
  溢出饱和不回绕）与可选抖动 `Jitter`（叠加 `[0, Jitter)` 随机量，打散重试风暴）——
  外部 IM 频控（429/5xx）标准应对姿势，消费方内联指数退避可下线收编（#4）
- feat(loop): 新增周期任务循环原语 `loop`——`Run(ctx, Config, fn)` + `Wait`/`Done` 排水。
  语义契约钉死四点：忙时丢 tick（fn 同步不重叠）、单轮 panic 按 RunSafe 收编（循环存活）、
  ctx 取消拦「下一轮」在飞轮次跑完、`FirstDelay` 走 select 可被停机打断（#5）
- async: 新增带任务归属标签的 `RunSafeLabeled`/`GoSafeLabeled`/`GoSafeWithContextLabeled`——
  panic 汇报消息形如 `task=janitor: boom`，运维可按任务维度聚合告警，不再依赖堆栈解析（#6）
- feat(ratelimit): `KeyedLimiter` 增强（#3）：
  - 泛型 key：`KeyedLimiterOf[K comparable]` + `NewKeyedLimiterOf`；`KeyedLimiter` 保留为
    `= KeyedLimiterOf[string]` 别名，旧 API `NewKeyedLimiter(factory, ttl)` 无破坏兼容
  - 容量护栏 `WithMaxEntries`：表满先回收空闲条目，仍满拒绝**未登记** key（已登记不受影响）；
    拒绝建议 `WithOverCapRetryAfter`（默认 60s）；`OverCapHits()` 可观测
  - `AllowWithRetryAfter(key) (wait, ok)`：拒绝时返回下一个可用令牌等待时长，
    内部 `Reserve→Delay→Cancel` **不消耗令牌**（429 + Retry-After 标准姿势）
  - `Replace(key, l)` 速率热更（换桶即新配额）；`TokenBucket.RetryAfter()` 暴露探测能力
- **feat(syncx)**：新增逐槽错误变体 `FanInErr`（`fn func(i int) (T, error)` 返回
  `[]Slot[T]`）——`Slot.Val/Err/Panicked` 三态：fn panic 不再静默零值槽，
  payload 直达调用侧（经新增 `async.ReportPanic` 上报全局 handler，观测面
  统一），合法零值与 panic 空槽可区分，消费方按槽降级免写 recover→占位
  样板。`FanIn` 保留不动（无 error 形态的轻量路径）
  （[#7](https://github.com/oekit/concurrency/issues/7)）
- **test(singleflight)**：接入 `pkg/singleflighttest` 共享语义契约（与
  `pkg/internal/singleflight` 同一套），漂移双红
  （[pkg#4](https://github.com/oekit/pkg/issues/4)）

## [v0.4.0] — 2026-10-16

- feat(batch): 新增攒批执行器 `batch`——`PeriodicalExecutor` + `TaskContainer` 扩展点
  （参考 go-zero core/executors，按需后台协程 + 空闲自退出模型）：满条数 `BulkExecutor` /
  满字节 `ChunkExecutor` / 窗口去重 `LessExecutor`（CAS 裁决恰一次）；Ticker/时钟可注入
  （`WithBatchTicker`/`WithBatchClock`）时序测试完全确定；Execute 统一经 `async.RunSafe`
  隔离 panic，单批失败不影响后续；退出摘牌与取空判定同临界区，任务不滞留无人消费
- async: 新增 `RunSafe`——同步 recover 包装（复用统一 PanicHandler），批量回调等
  "第三方回调不应打死调用方"场景
- feat(timewheel): 新增单级时间轮 `timewheel`（参考 go-zero collection.TimingWheel，
  圈数 circle 表达超轮延迟）：任务收敛为 `func()` 类型安全，触发经 async.RunSafe 隔离；
  Ticker 可注入 + `HasTimer` 串行 barrier 使时序测试完全确定；生命周期 `Close(ctx) error`
  幂等（硬规则），Close 后未触发任务丢弃（持久化/死信/分片需求走 oekit/timertask）
- 补记：shedding（v0.3.0 节已载）实际随本版首发——v0.3.1 tag 之后合入 main，漏发一轮

## [v0.3.1] — 2026-10-16

- 补记：本 tag 仅含依赖/文档微调，无独立功能变更；shedding 等内容记在 v0.3.0 节（见 v0.4.0 补记）。空节占位以保持版本序列完整。

## [v0.3.0] — 2026-10-16

- feat(window): 新增滑动窗口统计容器 `window.RollingWindow`（移植 go-zero
  collection.RollingWindow，算法、惰性重置与时间对齐保持上游语义）：泛型桶
  `BucketInterface[T]` + 默认 `Bucket[T]{Sum, Count}` + `Reduce` + `IgnoreCurrentBucket`
- 时间源可注入 `WithWindowClock`（缺省 `time.Now`，测试确定性）；选项类型按硬规则命名 `WindowOption`
- feat(breaker): 新增 Google SRE 概率式自适应熔断 `breaker`（移植 go-zero
  core/breaker googleBreaker，无状态机）：40×250ms 滑动窗口统计、k=1.5/minK=1.1/
  protection=5、失败占比→丢弃概率、强制探活（缺省 1s）自动恢复、panic 记失败后
  re-panic；统计容器复用 `window.RollingWindow`（自定义桶实现 `BucketInterface[int64]`）
- `ErrCircuitOpen`（逐字 "circuit breaker is open"）为全库熔断规范哨兵；API 全量
  ctx-aware（`Allow(ctx)`/`Do(ctx,…)`），时间源可注入 `WithBreakerClock`，拒绝走可选钩子
  `WithBreakerRejectHandler`（缺省 no-op，零强制 metrics/logx）；`GetBreaker` 按名共享单例
- feat(shedding): 新增 CPU 自适应降载 `shedding`（移植 go-zero core/load
  adaptiveshedder，算法忠实上游）：maxPass×minRt 的 Little's Law 并发上限、
  CPU 过载因子夹 [0.1,1]（至少保留 10% 容量）、avgFlying 滞后 EMA（beta=0.9，
  仅完成时更新）、1s 冷却期；丢弃要求 (CPU 过载 || 冷却期内) && 并发超限同时成立
- CPU 采样纯标准库（/proc/stat 差分 + cgroup v1/v2 配额感知、beta=0.95 指数平滑、
  250ms 周期、毫秒‰ 单位；采样 goroutine 惰性启动 + 每周期 recover 兜底）；
  非 linux polyfill 返回错误 → 降载自动禁用全放行，源错误同按 fail-open；
  CPU 源可注入包级 `SetCPUUsageFunc`（仿 async.SetPanicHandler），时间源可注入
  `WithShedderClock`；`ErrServiceOverloaded` 哨兵；API ctx-aware（`Allow(ctx)`）；
  丢弃走可选钩子 `WithShedderRejectHandler`（缺省 no-op）

## [v0.2.1] — 2026-10-16

- fix(syncx)：FanIn 裸 goroutine 改 async.GoSafe 收编——AGENTS.md「禁裸 go func」
  铁律对齐（v0.2.1 前 fn panic 直接打死宿主进程）。单个 fn panic 被恢复并走全局
  panic handler 落日志，该槽位保持零值、其余槽位与收口不受影响，消费方按「空槽
  降级」处理。新增 TestFanInPanicRecovered；全仓 -race 8 包零竞争。消费者侧动机：
  agentkit 的 rag Hybrid 双路检索等 GoSafe 手写扇出
  形态此前无法安全换用 FanIn。

## [v0.2.0] — 2026-10-15

- feat(gopool)：吸收 cloudwego/gopkg——gopool 自适应协程池（惰性扩容/队列满回退/
  老化退出/Close 排空）；async 措辞中性化（#2）。
  补记：功能段随 v0.3.0 发布批次补录，本小节为合并后的唯一 v0.2.0 记录。

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/concurrency`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。