# concurrency

> 并发与流控原语——GoSafe safe-go/协程池/重试/键控闸门/令牌桶/滑动窗口统计/自适应熔断

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/concurrency@v0.2.0
```

## 这个仓库做什么

`concurrency` 是**并发与流控原语集**：把 Go 并发开发里最容易出事的部分——
裸 goroutine 的 panic、无界重试、无界并发、限流、并发去重——封装成开箱即用且经过生产验证的原语。

## 核心能力

| 包 | 做什么 |
|---|---|
| `async` | **GoSafe 系列** safe-go：`GoSafe`/`GoSafeWithErr`/`GoWithContext`/`GoSafeWithContext`/`GoSafeWithErrChannel`，panic 自动 recover + 可注入 panic 上报。全矩阵禁止裸 `go func` 的替代品 |
| `gopool` | **自适应协程池**：惰性拉起 worker、队列满回退直接 `go`（不丢任务）、空闲按 WorkerMaxAge 老化退出、`Close(ctx)` 优雅排空。高频突发任务的并发数软上限（v0.2.0 吸收自 cloudwego/gopkg） |
| `retry` | `Do(ctx, cfg, fn)` 重试/轮询：次数、退避、可重试判定 |
| `syncx` | 键控闸门（同一 key 串行）、扇出控制 |
| `ratelimit` | 令牌桶限流 |
| `window` | **滑动窗口统计容器**（v0.2.0 移植自 go-zero collection.RollingWindow）：泛型桶 `BucketInterface` + 默认 `Bucket{Sum, Count}`、惰性重置、`Reduce` 聚合、`IgnoreCurrentBucket`；时间源可注入 `WithWindowClock`——熔断/降载的数据源 |
| `breaker` | **自适应熔断**（v0.2.0 移植自 go-zero googleBreaker）：Google SRE 概率式丢弃、无状态机，40×250ms 窗口 + 强制探活自愈，panic 记失败后 re-panic；`ErrCircuitOpen` 全库规范哨兵（httpx/nns-endpoint 别名共用）——下游保护的执行件 |
| `singleflight` | 并发去重（同 key 合并为一次调用） |
| `group` | 并发分组编排 |
| `progress` | 多阶段进度追踪 |

## 快速使用

```go
import (
    "github.com/oekit/concurrency/async"
    "github.com/oekit/concurrency/gopool"
    "github.com/oekit/concurrency/retry"
)

// 替代裸 go func：panic 被 recover，可注入 panic 上报
async.GoSafe(func() { serve() })

// 高频突发任务走池：worker 数软上限，panic 汇入 logx
gopool.SetPanicHandler(func(ctx context.Context, r any, stack []byte) {
    logx.Error("gopool panic", "error", r, "stack", string(stack))
})
gopool.Go(func() { process(item) })

p := gopool.NewPool("ingest", gopool.PoolOption{MaxIdleWorkers: 64})
p.CtxGo(ctx, func() { handle(req) })
defer p.Close(ctx) // 优雅排空

// 带重试的调用
err := retry.Do(ctx, retry.Config{}, func(attempt int) error {
    return callDownstream()
})
```

## 边界

- 需要日志记录 panic → 配合 [logx](https://github.com/oekit/logx) 的 `SetPanicHandler`
- 需要进程内缓存/去重合并的高级组合 → [data](https://github.com/oekit/data)
- 本仓为 L0 基础件：零矩阵内依赖（第三方仅 golang.org/x/time）


## 矩阵总览（23 活跃模块，自动生成）

| 模块 | 内容 | 矩阵内依赖 |
|---|---|---|
| [pkg](https://github.com/oekit/pkg) | 微工具集 18 合 1（errors/validator/stringx/slicex/enumx/convert/encoding/jsonrepair/httpjson/mathx/pathx/pagination/ptrx/timex/uidgen/cache/bufiox/strmap） | — |
| [concurrency](https://github.com/oekit/concurrency) | 并发与流控原语（async/breaker/gopool/group/singleflight/progress/syncx/ratelimit/retry/window） | — |
| [logx](https://github.com/oekit/logx) | 统一日志门面（多后端可替换 + Sink + caller 归属）+ 优雅关停 | — |
| [core](https://github.com/oekit/core) | 应用基建件（governance 治理 schema / config 配置加载 / lifecycle 契约 / network IP 工具 / version） | [logx](https://github.com/oekit/logx) |
| [istio](https://github.com/oekit/istio) | Istio Proxyless——Kitex xDS 动态熔断 | — |
| [metrics](https://github.com/oekit/metrics) | Prometheus 门面 + 弹性指标族 | [core](https://github.com/oekit/core) |
| [tracing](https://github.com/oekit/tracing) | OpenTelemetry 追踪 + trace_id 真相源 | [logx](https://github.com/oekit/logx) |
| [registry](https://github.com/oekit/registry) | 注册发现核心 + Nacos 实现 + Nacos 配置源 + sdbridge | [core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx) |
| [data](https://github.com/oekit/data) | 缓存检查器（TTL/failTTL/inflight 合并）+ LocalCache | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [gorm](https://github.com/oekit/gorm) | SQL/GORM 全家——连接池增强、框架无关 CRUD Service、仓储 | [logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [redis](https://github.com/oekit/redis) | Redis 全家——客户端封装、缓存后端、分布式锁 | [concurrency](https://github.com/oekit/concurrency)、[data](https://github.com/oekit/data)、[logx](https://github.com/oekit/logx) |
| [httpx](https://github.com/oekit/httpx) | HTTP 客户端 + 负载均衡——服务发现/熔断/重试/指标/OTel 透传 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [network](https://github.com/oekit/network) | 轻量网络客户端——WebSocket/NATS/MQ 抽象、连接与流工具、Octopus 推送 | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[tracing](https://github.com/oekit/tracing) |
| [timertask](https://github.com/oekit/timertask) | 分片定时任务（FNV-1a、心跳续约） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [middleware](https://github.com/oekit/middleware) | Hertz 自研标准中间件栈 + contrib 桥接 + hmw | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [nns](https://github.com/oekit/nns) | NNS 服务治理客户端——provider 无关发现/配置/实例缓存 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [nns-endpoint](https://github.com/oekit/nns-endpoint) | NNS 端点工厂——per-service 熔断、换实例重试、gRPC 池 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[httpx](https://github.com/oekit/httpx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry) |
| [bootstrap](https://github.com/oekit/bootstrap) | 应用生命周期引导——bootstrap.Run 骨架 + healthz | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [handler](https://github.com/oekit/handler) | HTTP handler 层——通用 CRUD Handler、参数绑定、版本端点 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[gorm](https://github.com/oekit/gorm)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[middleware](https://github.com/oekit/middleware)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [hertz](https://github.com/oekit/hertz) | Hertz 运行时集成——日志桥接、生命周期、SSE/health/metrics、适配器、OTel | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [kitex](https://github.com/oekit/kitex) | Kitex 生态——日志桥接、治理热更、生命周期、BizError 桥接、kitexotel | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry) |
| [workflow](https://github.com/oekit/workflow) | 组合式流程编排——串行/并行/异步/条件分支 + Hook AOP | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [kitex-examples](https://github.com/oekit/kitex-examples) | Kitex echo 示例——client/server + IDL + 治理配置 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[kitex](https://github.com/oekit/kitex)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
> 依赖严格自底向上；微库已合并为 pkg，旧微库仓已删除。

> 本仓在矩阵中的位置与依赖见上表「矩阵内依赖」列。历史沿革：源自 ekit 单体库
> （git.enjoye.top/enjoydream/ekit v0.34.0，冻结维护），2026-10 拆分为独立 module。

## License

MIT — 见 [LICENSE](LICENSE)
