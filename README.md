# ekit-concurrency

> 并发与流控原语——safe-go goroutine、键控闸门、扇出、令牌桶、重试轮询。零内部依赖。

本仓库是 [ekit](https://git.enjoye.top/enjoydream/ekit)（`git.enjoye.top/enjoydream/ekit` v0.34.0）
拆分出的独立 Go module，属于 **openekit** ekit 子项目矩阵（Round-2 深拆粒度）。

## 安装

```bash
go get github.com/openekit/concurrency@v0.1.0
```

## 包清单

- `async/` — GoSafe 系列（GoSafe/GoSafeWithErr/GoWithContext...），panic recover + 可注入 panic handler
- `group/` — 并发分组编排
- `singleflight/` — 单飞去重
- `progress/` — 多阶段进度追踪
- `syncx/` — 键控闸门（KeyedLimiter）/扇出
- `ratelimit/` — 令牌桶限流
- `retry/` — 重试/轮询

## 矩阵总览（20 活跃仓 + 19 微库，自动生成）

| 仓库 | 内容 | 矩阵内依赖 |
|---|---|---|
| [ekit-bootstrap](https://github.com/openekit/bootstrap) | （见 README） | [ekit-bootstrap](https://github.com/openekit/bootstrap), [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-core](https://github.com/openekit/core), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-nns](https://github.com/openekit/nns), [ekit-registry](https://github.com/openekit/registry), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-cache](https://github.com/openekit/cache) | 进程内缓存微库——LRU O(1)、TOCTOU 修复的 LocalCache | — |
| [ekit-concurrency](https://github.com/openekit/concurrency) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency) |
| [ekit-convert](https://github.com/openekit/convert) | 类型转换微库——字符串/数字/布尔/切片的宽容转换 | — |
| [ekit-core](https://github.com/openekit/core) | Go 后端基础工具库——应用基建件（governance 治理 schema / lifecycle 生命周期契约 / network IP 工具 / version） | [ekit-core](https://github.com/openekit/core) |
| [ekit-data](https://github.com/openekit/data) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-logx](https://github.com/openekit/logx) |
| [ekit-encoding](https://github.com/openekit/encoding) | 编解码微库——JSON/msgpack 等序列化助手 | — |
| [ekit-enumx](https://github.com/openekit/enumx) | 枚举工具微库——确定性顺序的 All/Values/解析 | — |
| [ekit-errors](https://github.com/openekit/errors) | 错误处理微库——Wrap/Cause 链、BizError（业务码/HTTP 码双轨）、sentinel 错误 | — |
| [ekit-gorm](https://github.com/openekit/gorm) | （见 README） | [ekit-errors](https://github.com/openekit/errors), [ekit-gorm](https://github.com/openekit/gorm), [ekit-logx](https://github.com/openekit/logx), [ekit-pagination](https://github.com/openekit/pagination) |
| [ekit-handler](https://github.com/openekit/handler) | （见 README） | [ekit-core](https://github.com/openekit/core), [ekit-errors](https://github.com/openekit/errors), [ekit-gorm](https://github.com/openekit/gorm), [ekit-handler](https://github.com/openekit/handler), [ekit-middleware](https://github.com/openekit/middleware), [ekit-pagination](https://github.com/openekit/pagination), [ekit-tracing](https://github.com/openekit/tracing), [ekit-validator](https://github.com/openekit/validator) |
| [ekit-hertz](https://github.com/openekit/hertz) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-core](https://github.com/openekit/core), [ekit-hertz](https://github.com/openekit/hertz), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-nns](https://github.com/openekit/nns), [ekit-registry](https://github.com/openekit/registry), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-httpjson](https://github.com/openekit/httpjson) | HTTP JSON 微库——DoJSON 带 retry 语义与 Retry-After 尊重 | [ekit-stringx](https://github.com/openekit/stringx) |
| [ekit-httpx](https://github.com/openekit/httpx) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-httpx](https://github.com/openekit/httpx), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-registry](https://github.com/openekit/registry) |
| [ekit-istio](https://github.com/openekit/istio) | （见 README） | — |
| [ekit-jsonrepair](https://github.com/openekit/jsonrepair) | JSON 修复微库——宽容修复链：截断/尾逗号/单引号/非标准值归一 | — |
| [ekit-kitex](https://github.com/openekit/kitex) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-core](https://github.com/openekit/core), [ekit-errors](https://github.com/openekit/errors), [ekit-kitex](https://github.com/openekit/kitex), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-nns](https://github.com/openekit/nns), [ekit-registry](https://github.com/openekit/registry), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-kitex-examples](https://github.com/openekit/kitex-examples) | （见 README） | [ekit-bootstrap](https://github.com/openekit/bootstrap), [ekit-kitex](https://github.com/openekit/kitex), [ekit-kitex-examples](https://github.com/openekit/kitex-examples), [ekit-logx](https://github.com/openekit/logx), [ekit-registry](https://github.com/openekit/registry), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-logx](https://github.com/openekit/logx) | （见 README） | [ekit-logx](https://github.com/openekit/logx) |
| [ekit-mathx](https://github.com/openekit/mathx) | 数学工具微库——安全随机、区间、平均值等评测统计助手 | — |
| [ekit-metrics](https://github.com/openekit/metrics) | （见 README） | [ekit-core](https://github.com/openekit/core), [ekit-metrics](https://github.com/openekit/metrics) |
| [ekit-middleware](https://github.com/openekit/middleware) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-errors](https://github.com/openekit/errors), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-middleware](https://github.com/openekit/middleware), [ekit-pathx](https://github.com/openekit/pathx), [ekit-registry](https://github.com/openekit/registry), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-network](https://github.com/openekit/network) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-logx](https://github.com/openekit/logx), [ekit-tracing](https://github.com/openekit/tracing) |
| [ekit-nns](https://github.com/openekit/nns) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-core](https://github.com/openekit/core), [ekit-logx](https://github.com/openekit/logx), [ekit-nns](https://github.com/openekit/nns), [ekit-registry](https://github.com/openekit/registry) |
| [ekit-nns-endpoint](https://github.com/openekit/nns-endpoint) | NNS 端点工厂——per-service 熔断（4xx 不计失败）、HTTP 换实例重试、gRPC 连接池、弹性指标 | [ekit-httpx](https://github.com/openekit/httpx), [ekit-logx](https://github.com/openekit/logx), [ekit-metrics](https://github.com/openekit/metrics), [ekit-nns](https://github.com/openekit/nns), [ekit-registry](https://github.com/openekit/registry) |
| [ekit-pagination](https://github.com/openekit/pagination) | 分页微库——PageRequest/PageResponse 与换算 | — |
| [ekit-pathx](https://github.com/openekit/pathx) | 路径匹配微库——glob（`vendor/**`、`*.test`）匹配 | — |
| [ekit-ptrx](https://github.com/openekit/ptrx) | 指针工具微库——To/From/安全解引用 | — |
| [ekit-redis](https://github.com/openekit/redis) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-data](https://github.com/openekit/data), [ekit-logx](https://github.com/openekit/logx), [ekit-redis](https://github.com/openekit/redis) |
| [ekit-registry](https://github.com/openekit/registry) | （见 README） | [ekit-core](https://github.com/openekit/core), [ekit-logx](https://github.com/openekit/logx), [ekit-registry](https://github.com/openekit/registry) |
| [ekit-slicex](https://github.com/openekit/slicex) | 切片工具微库——Partition 二分组、GroupBy、去重与映射 | — |
| [ekit-stringx](https://github.com/openekit/stringx) | 字符串工具微库——ID 生成、关键词匹配、缩进、复数化、模板替换 | — |
| [ekit-timertask](https://github.com/openekit/timertask) | 定时任务微库——分片定时任务（FNV-1a hash、心跳续约） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-logx](https://github.com/openekit/logx) |
| [ekit-timex](https://github.com/openekit/timex) | 时间工具微库——格式化/解析/时间戳 + cron 表达式 | — |
| [ekit-tracing](https://github.com/openekit/tracing) | （见 README） | [ekit-logx](https://github.com/openekit/logx) |
| [ekit-uidgen](https://github.com/openekit/uidgen) | ID 生成微库——UUID、雪花、随机串 | — |
| [ekit-validator](https://github.com/openekit/validator) | 参数校验微库——go-playground/validator 封装，ValidationError 归一为 BizError(400) | [ekit-errors](https://github.com/openekit/errors) |
| [ekit-workflow](https://github.com/openekit/workflow) | （见 README） | [ekit-concurrency](https://github.com/openekit/concurrency), [ekit-logx](https://github.com/openekit/logx) |

> `ekit-observability` 已在 Round-2 解散归档；本表由源码 import 自动生成。
> 依赖严格单向：工具微库/L0 → 可观测门面 → registry → data/httpx/middleware → nns/bootstrap → hertz/kitex。

## 当前仓库依赖

详见上方矩阵表中本仓库行。

## 与 ekit 单体仓库的关系

- 代码与 ekit v0.34.0 等价，module 路径 `git.enjoye.top/enjoydream/ekit/...` → `github.com/openekit/concurrency/...`
- 各子仓库独立发版（SemVer）；开发规范沿用 ekit AGENTS.md 约定

## License

见 [LICENSE](LICENSE)
