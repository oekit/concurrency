# Changelog — oekit/concurrency

由 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来。所有 notable 变更记录如下。

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

## [v0.2.0] — 2026-10-16

- feat: v0.2.0 吸收 cloudwego/gopkg——gopool 自适应协程池（惰性扩容/队列满回退/老化退出/Close 排空）；
  async 措辞中性化（#2）。补记：本段随 v0.3.0 发布批次补录

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
  老化退出/Close 排空）

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/concurrency`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。