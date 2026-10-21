# AGENTS.md — oekit/concurrency

> 并发与流控原语（async/gopool/group/singleflight/progress/syncx/ratelimit/retry）+ window 滑动窗口 / breaker 自适应熔断（规范哨兵 ErrCircuitOpen）/ shedding CPU 自适应降载 / batch 攒批执行器（满条数/满字节/窗口去重）/ timewheel 单级时间轮；v0.5.0 起 retry 指数退避+抖动（MaxDelay 封顶/溢出饱和）、loop 周期原语（忙时丢 tick/单轮 panic 收编/FirstDelay 可打断）、async labeled RunSafe（panic 按任务标签上报）、syncx.FanInErr 逐槽错误（panic 槽显式标记）、KeyedLimiter 增强（容量护栏/RetryAfter/速率热更）并接入 pkg/singleflighttest 契约测试；v0.8.0 起 singleflight DoShared 折叠可观测（Result.Waiters）、async panic 统一收编 typed *PanicError（errors.As 可辨、携带栈）+ 实例级 Runner panic 路由、retry DelayHint（服务端 Retry-After 建议优先）+ JitterRatio 比例抖动 + retry/httpx Retryable 可重试性判定；v0.9.0 起 breaker 显式三态状态机 closed/open/half-open——冷却（WithProbeTimeout 缺省 1m）期满首个放行即半开探测、探测成功关闸清窗/失败重开、失联超时自动回收 + AbandonGen 显式放弃，GenBreaker 代绑定（AllowGen/SuccessGen/FailureGen/AbandonGen：代仅在探测放行自增、陈旧代回报整体作废、开放期失败不后移冷却）；v0.10.0 起 WithCooldown 独立业务冷却期（缺省回落 probeTimeout）+ WithConsecutiveFailures 连续失败计数触发（与速率型并联，补低 QPS 信号，对齐 Envoy consecutive_5xx）；v0.11.0 起 ratelimit Lockout 防爆破锁（fail2ban 形态：滑窗失败计数→阈值锁定拒绝 LockDuration→成功 Reset 清零/到期自解，CheckLocked 返回剩余时长）；v0.12.0 起 retry/httpx RetryableWithHeaders（403 + Retry-After/X-RateLimit-Remaining 头感知可重试判定，HeaderSource 零依赖抽象）、syncx FanInLimit 受限并发扇出（固定 worker 防下游过载）+ FanInLimitCtx（v0.14.0，ctx 感知：ctx 已取消未启动项 fn 一次不调、剩余槽快速排空 Canceled、在飞项照常落槽）、ratelimit WithTokenClock 时钟注入；v0.13.0 起 breaker 熔断期收敛到探测级（forcePass 强制探活 + 冷却期满半开探测，其余拒绝——SRE 公式退役为 closed→open 触发器，WithConsecutiveFailures 触发后流量真停）+ State()/State 枚举可观测出口；v0.16.0 起 ratelimit NewKeyedLimiterByFunc[K]——factory func(K) Limiter 按 key 定配额（一个管理器按接口维度分档位，NewKeyedLimiterOf 零破坏随迁；未登记 key 拿 factory 兜底档的坑已在 doc 点名，#31）；v0.17.0 起 KeyedLimiter.Create 护栏内原子登记（表满先回收空闲与 Allow 同路、仍满拒绝计 OverCapHits、已存在不覆盖——热更走 Replace；解决 ByFunc factory 库锁内调用致消费方锁序倒置，#33）
> 当前版本：`v0.18.1` · module：`github.com/oekit/concurrency` · org：[oekit = Open Easy Kit](https://oekit.github.io)

## 本仓包含
`async/`、`batch/`、`breaker/`、`budget/`、`gopool/`、`group/`、`loop/`、`progress/`、`ratelimit/`、`retry/`、`retry/httpx/`、`shedding/`、`singleflight/`、`syncx/`、`timewheel/`、`window/`

## 硬规则速览（全矩阵适用）

### 分层与依赖
- 依赖严格自底向上，禁止反向依赖；本仓的矩阵内依赖见 README「矩阵总览」表
- 微工具集统一在 `github.com/oekit/pkg`；本仓新增基础工具前先确认 pkg 里没有
- `pkg` 是**零第三方依赖仓**（go.mod 零 require，仅标准库，CI 强制）——需要三方能力的基础工具拆独立仓（先例：kitx）

### 错误处理
- 一律使用 `github.com/oekit/pkg/errors`：`errors.Wrap(err, "ctx", kv...)`（nil-safe）、
  `errors.Cause`；业务错误用 BizError（BizCode 与 HTTPCode 双轨，命名工厂 NewNotFoundError 等）
- **类型同一性红线**：同一进程内只允许一套 errors 包。混用旧路径会把 BizError 的 As/Is
  判定打断（Not Found 变 500）
- handler 层统一 `mw.Fail(ctx, c, err)` 出口；所有 error 禁止 `_` 忽略

### 日志
- 门面 `github.com/oekit/logx`：业务服务 `logx.WithService("svc")`；
  **SDK/基础包必须 `logx.Delegating("组件名")` 懒解析**（禁止 Default()/WithService 声明包级 logger）
- kv 结构化风格为默认；`Ctx*` 方法自动携带 trace_id；禁止 printf 风格 `logx.Infof` 作为主要手段
- 后端经 `logx.SetDefault` 替换；`LOG_LEVEL` 环境变量统一生效

### 并发安全
- 禁止裸 `go func`：用 `github.com/oekit/concurrency/async` 的 GoSafe 系列（panic 被 recover）
- 共享状态 mutex / atomic 保护；包级全局可变状态用 `atomic.Pointer[T]`

### 生命周期
- 组件 Close 统一 `Close(ctx) error` 签名 + sync.Once 幂等
- 应用骨架用 `github.com/oekit/bootstrap/bootstrap.Run`；**userFn 返回即服务结束，
  长驻服务必须 `<-app.Context().Done()` 阻塞**

### Option 与命名
- 函数选项模式 `XxxOption`（禁止裸 `Option`，多包 co-import 冲突）
- 导出符号 PascalCase + godoc 注释；包文档放 doc.go

### 版本与发版
- SemVer；0.x 阶段 minor 允许 breaking
- **main 受分支保护，改动一律走 PR，管理员也不例外**（`enforce_admins=true`，
  直推会被 `GH006 Changes must be made through a pull request` 拒掉）
- 发版流程：建分支 → 提交 → 开 PR → CI（`Lint & Test / Lint` + `Lint & Test / Test`）
  绿 → 合入 main → **合入之后**再打 annotated tag 推远端
- tag 不走 PR（`protect-release-tags` ruleset 管的是不可删/不可改，不是审批）；
  tag 必须落在已合入 main 的 commit 上，否则 `go get` 拉到的是没进过 CI 的代码
- module 路径 = `github.com/oekit/<name>`；**改 module 路径必须同步**：
  全部 import、go.mod、README 安装行、logx caller 启发式（若涉及 logx）

### 文档
- 本仓 README 的「矩阵总览」表由脚本生成，结构变化后需重新生成
- 历史完整规范见 ekit 单体仓（git.enjoye.top/enjoydream/ekit，冻结维护）的 AGENTS.md 与 docs/

## 本仓范围

