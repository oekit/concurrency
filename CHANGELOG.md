## [v0.23.0] — 2026-10-28

- fix(singleflight): `DoChan` 的收尾仍是无条件 `Delete` ⇒ `Forget` 后同 key 并发跑两次 fn——`Do` / `DoShared` 两条路径**已修**（收尾用 `CompareAndDelete(key, c)` 判同一、只删自己那条），但 `DoChan` 路径漏修 ⇒ `Forget` 之后旧飞行的收尾会把**新**注册的 call 一起删掉 ⇒ 第三个调用方另起一次 fn ⇒「同 key 只执行一次 fn」的承诺被打破，且两次 fn **并发**跑。后果形态最坏：下游请求翻倍 / 缓存回源击穿 / **非幂等写重复执行**，而且**没有任何报错**——`Result.Shared` 还会把复用结果标成 `Shared=true`，调用方更难察觉。修法与 `Do`/`DoShared` 同一口径：收尾改 `CompareAndDelete`（三条路径的清理语义必须一致，否则该承诺只在部分路径上成立）。补回归用例：仓里此前已有针对 `Do` 的同款时序回归测试，而 `DoChan` 的三个用例**没有一个涉及 Forget** ⇒ 该路径完全无覆盖（#105）
- fix(budget): `Ledger.Consume` 的 `used+n` 先加后比 ⇒ int64 溢出后预算被静默绕过且计数器永久变负——判据 `used+n > cap` 是 int64 上的先加后比，`n` 接近 `math.MaxInt64` 时加法回绕成负数 ⇒ 判据恒为「未超限」⇒ 该笔消耗被**直接放行**并把计数器写成负数 ⇒ 此后**所有**后续消耗都无条件放行。实测（cap=100）：`Consume("bytes", MaxInt64)` 返回 `err=nil`（超限未被拦截！）且 `used = -9223372036854775808` ⇒ 一次 `n ≈ MaxInt64` 的消耗就让这个 Ledger **永久变成「无限制」**。而 `n` 来自上游返回的长度/计数字段，**可被外部影响**（本包用途：LLM 输出预算 / 日志留存配额 / 批量抓取字节配额）；且超限内容**既没被拦截、也没进存档**，调用方却拿到 `err == nil`（=「已通过预算」）⇒ 超额内容被当作合规内容放行。三处修：①判据改成**不溢出的形式** `n > cap-used`（`used<=cap` 时右侧非负，无回绕）；②`used` 改**饱和累加**（钳到 `cap`）⇒ 计数器永不变负，且「`cap-used` 不回绕」这条不变量对后续每次 Consume 都成立；③补 `Used(d Dimension) int64` 观测口——`Ledger` 此前**没有任何出口**能看到计数器，计数器变负这类事故只能靠「突然所有请求都不再被拦」反推。补用例：大数用例（此前 `budget_test.go` 六类用例**没有任何大数用例**）、`used` 永不超过 `cap`、`Used` 反映消耗（#104）
- feat(budget): 补 `Ledger.Used(d Dimension) int64` 观测口——没有它，计数器异常只能靠「突然所有请求都不再被拦」反推（#104）
- build: pkg 地板 v0.22.0 → v0.30.0（跟随 pkg 本轮发布）——带回了共享契约套件的「Forget-竞态」用例（`Forget` → 新 call 仍在飞行 → 旧 call 收尾 → 第三个调用方），此前 `concurrency/singleflight` 跑的是旧套件 ⇒ 该时序**不可观测**；零行为改动

## [v0.22.1] — 2026-10-28

- fix(gopool): 「检查 closed + `wg.Add(1)`」与 `Close` 的「置 closed」不是原子的 ⇒ 提交方可能在 `Close` 的 `wg.Wait()` 期间 Add ⇒ 运行时 panic（"sync: WaitGroup misuse: Add called concurrently with Wait"）。修法：加 `submitMu` 串起这两步（`Close` 持锁只到置位为止，`wg.Wait()` 在锁外，否则「任务里再提交任务」会与 Close 互等）
- fix(singleflight): 清理改用 `CompareAndDelete`——此前 `Delete` 无条件删 key，`Forget` 之后注册的同 key **新** call 会被一并删掉 ⇒ 第三个调用方另起一次 fn，「同 key 只执行一次 fn」的承诺被打破
- fix(breaker): `doReq` 去掉重复的 `markDrop`——`allowGen` 已记过一次，每次拒绝记**两个**丢弃样本而 `Allow` 只记一个 ⇒ `history.total` 在整个熔断窗口里被灌水（它是 SRE 触发器的输入）

## [v0.22.0] — 2026-10-25

- feat(fanout): 新增限并发 + 结果保序 + 错误汇总的批量执行原语 fanout.Map——item 口径、失败项带下标汇总（#97）

## [v0.21.0] — 2026-10-25

- **BREAKING**: ratelimit.Limiter 接口启用 Wait(ctx context.Context) error——此前该行一直处于注释状态：接口上没有阻塞等待这一档，服务端限流中间件要回 X-RateLimit-Remaining 这类响应头拿不到实时令牌数、出站 HTTP 客户端节流要「等到有配额再发请求」也没有对应形态，消费方要么在库外自建第二份令牌桶实现（重复实现 + 各自漂移，上游的时钟注入、Retry-After 探测语义同步不过去），要么退化成「不用上游原语」；启用后**自行实现 Limiter 的调用方需补上 Wait 方法**（接口多了一个方法，旧实现不再满足）——0.x 阶段 minor 允许 breaking；仓内两个实现（TokenBucket / SlidingWindow）均已补齐，全矩阵扫描确认无第三个实现者（middleware 用的是 *TokenBucket）（#92）
- feat(ratelimit): TokenBucket 补剩余配额读出口与阻塞等待档——Tokens() float64 实时剩余令牌读出口（**不消耗配额**，按注入时钟结算，与 Allow/Reserve 同一时间源，假时钟下可确定性断言；与 SlidingWindow.Count() 同维度，构成「限流器可观测面」）；Wait(ctx) / WaitN(ctx, n) 阻塞等待档（语义对齐 x/time/rate 的 Limiter.Wait：等到有令牌返回 nil、ctx 取消返回 ctx.Err()，取消时归还已预留的配额——取消不是消费；n 超过 burst 时永远等不到，返回 ErrBurstTooSmall，不是 ctx 取消类失败、重试无用）；NewTokenBucketFloat(rps float64, burst int, ...) 让消费方不必为传一个数字而额外 import golang.org/x/time/rate（rps 是配置项，让它出现在公开签名上会把三方包类型漏进消费方的 import 面）（#92）
- feat(syncx): KeyedGate 补按 key 即拒档 TryAcquire(key string) (release func(), ok bool)——该 key 档位已满时**立即**返回 ok=false（不排队、不占名额），与同包 Gate.TryAcquire 同款分档，只是按 key 分档位；ok=false 只有一个含义（该 key 档位已满，非阻塞取名额没有别的失败原因），调用方可据此回 409/429 而不是泛化成 500；release 幂等（atomic CAS 实现，与 Acquire/Gate 口径一致）；cap<=0 视为不限恒返回 ok=true。补的是「按 key 即拒档」这个交叉点：Gate 有全局即拒档、KeyedGate 有按 key 排队档，此前消费方防重入只能手搓 map[string]*sync.Mutex + TryLock + sync.Once 兜底幂等归还（每仓各写一遍，once 包装容易漏，漏了就是双重解锁 panic），KeyedGate.Acquire 是排队语义套不上（#93）

## [v0.20.4] — 2026-10-24

- fix(ratelimit): 后台清扫 ticker 间隔下限兜底——ttl/2 在 ttl<2ns 时整除为 0，time.NewTicker(0) panic 被 async.GoSafe 收编后进程不崩但清扫协程当场死亡，空闲条目永不回收（limiters/entries 两张 map 随 key 基数无界增长）、WithMaxEntries 容量护栏永远等不到回收腾位；KeyedLimiterOf 与 LockoutOf 的 cleanup 统一走 cleanupInterval：间隔整除为 0 兜 1s（对齐 pkg/cache 的 LocalCache.cleanup 口径）、ns 级过小间隔兜 1ms 防 ticker 热循环烧核，ttl 语义不变（过期判定仍按原 ttl 走）（#84）

## [v0.20.3] — 2026-10-24

- fix(ratelimit): Lockout 空闲回收不再截短 Window 滑窗语义——回收判据改为「无窗口内有效样本且空闲超 IdleTTL」，窗口内失败样本不再被连同条目丢弃（间隔攻击绕过阈值）；cleanup 与容量护栏回收共用同一判据，与 Redis 孪生实现 redisx/distlock.Lockout 的窗口裁剪口径对齐（#83）

## [v0.20.2] — 2026-10-23

- build: 矩阵内依赖地板对齐（align_floors 不动点）：pkg v0.15.1 → v0.22.0（#79）

## [v0.20.1] — 2026-10-22

- fix(syncx): KeyedGate 的 release 幂等标记改用 atomic CAS——裸 bool 在并发释放下竞态并突破 cap 上限（#70）

## [v0.20.0] — 2026-10-22

- feat(syncx): Gate 补非阻塞取名额 TryAcquire——满额即拒，不排队。Gate 原先是排队语义（Acquire 满额时等 ctx 取消才返回 ctx.Err()），而「重活并发上限用满就该立刻拒绝」这类闸门（图片全量解码、ffmpeg 子进程、模型推理）排队等待会把「内存被并发占满」换成「请求堆积在队列里」，是另一种 DoS 形态；现有原语都对不上——KeyedGate 按 key 隔离同样排队、gopool.Pool 的 MaxIdleWorkers 是常驻 worker 上限不是并发上限（任务队列满即回退为独立 goroutine 直跑，任务永不因并发数被拒）、ratelimit.KeyedLimiter 是速率型令牌桶不是并发闸门，消费方只能手搓裸 channel 信号量。新增 TryAcquire() (release func(), ok bool)：满额立即返回 ok=false，不等待、不占名额；ok=false 只有一个含义（名额已满，非阻塞取名额没有别的失败原因），调用方可据此回 429/503 而不是泛化成 500；release 幂等（与 Acquire 一致，重复调用只归还一次），用 atomic CAS 实现，并发调用安全；cap<=0 视为不限，恒返回 ok=true；与 Acquire 并存、命名区分（Acquire=排队、TryAcquire=即拒）

## [v0.19.0] — 2026-10-21

- feat(syncx): 全局在飞闸门原语 Gate——现有原语都不覆盖「这个进程总共最多 N 个在飞」：KeyedGate 按 key 隔离（用固定 key 硬凑可行，但那是把多档位闸门原语当单档位闸门用，语义与命名都不对）、gopool.Pool 是弹性 worker 池（MaxIdleWorkers 是常驻 worker 上限而非并发上限，任务队列满则回退为独立 goroutine 直跑、任务永不因并发数被拒，与硬配额语义正好相反）、ratelimit.KeyedLimiter 是速率型令牌桶不是并发闸门；Gate 是单一全局配额：整个实例最多 cap 个在飞，超出的调用方排队等待（ctx 取消即返回 ctx.Err()），不会像弹性池那样被偷偷放行；与 KeyedGate API 同形（Acquire 返回 release 函数、ctx 感知、release 幂等）消费方迁移成本低，release 幂等用 atomic CAS 实现（重复调用只归还一次，否则会把名额多放出去、突破配额，并发调用也安全），cap<=0 为不限（Acquire 直通零开销），ctx 为 nil 时按 context.Background() 处理（#56）

## [v0.18.1] — 2026-10-21

- fix(retry): 比例抖动在退避幅度为 0 时 panic（rand.Int63n(0)）——cfg.Delay 零值与 DelayHint 返回 0（服务端 Retry-After: 0）两条生产可达路径下，抖动幅度算成 0 触发 rand.Int63n(0) panic，库内 panic 逃逸到调用方；抖动幅度改为自己钳到 (0, n]，float64→int64 溢出与 delay+抖动溢出一并饱和封顶（#55）

## [v0.18.0] — 2026-10-20

### feat(retry)：`DoValue` 泛型伴生——带返回值的重试（[#48](https://github.com/oekit/concurrency/issues/48)）
- `DoValue[T](ctx, cfg, fn func(attempt int) (T, error)) (T, error)`：`Do` 的泛型伴生，
  供「重试并返回结果」的装饰器型调用方使用（此前这类调用方只能自己重写一遍
  退避+抖动，且常丢掉 `DelayHint` 语义）。
- **`Do` 改为委托给 `DoValue`**——退避序列、抖动比例、`MaxDelay` 封顶、`DelayHint`
  优先、`Retryable` 门控、ctx 取消全部**只有一份实现**。这是本条的关键：若 `DoValue`
  另写一份循环，等于在包内又造出「同一个仓里两份实现」，正是 #48 的根因。
- 契约：成功返回 fn 的结果与 nil；失败返回**零值**与最后一次错误（失败那次的中间
  结果不可用，返回它会诱导调用方误用未成功的产物）；退避等待期间 ctx 取消返回
  零值与 `ctx.Err()`；`cfg.Retryable` 判为不可重试时立即返回。
- 测试：首次成功 / 重试后取成功值 / 耗尽后返回零值 / 不可重试即停 /
  `DelayHint` 优先 / ctx 取消 / 零值配置只跑一次 / **Do 与 DoValue 的尝试次数与
  退避节奏一致性**（同源实现的证据）。

## [v0.17.1] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：pkg v0.11.0 → v0.15.1

## [v0.17.0] — 2026-10-18

### feat(ratelimit)：`KeyedLimiter.Create` 护栏内原子登记（[#33](https://github.com/oekit/concurrency/issues/33)）
- `Create(key K, l Limiter) bool`：显式登记外部构造的限流器——表满先回收空闲条目
  （与 Allow 建桶路径同一 sweep-on-pressure），仍满返回 false 并计一次 `OverCapHits`；
  已登记 key 返回 false **不覆盖**（速率热更走 `Replace`，换桶不该被护栏卡住）；
  `l == nil` 返回 false
- 动机：`NewKeyedLimiterByFunc` 的 factory 在库内锁内被调用——消费方在 factory 里读
  自持锁的速率状态会形成「库锁→消费方锁」与正常路径「消费方锁→库锁」的锁序倒置；
  「每次调用传入当前配置速率」的消费方此前只能手动 `Len()` 预检 + `Replace` 直插
  （护栏漏出库外、且绕过 sweep-on-pressure 回收）。`Create` 与 `Replace` 组合即可表达
  「新 key 原子登记（含护栏与回收）、已登记 key 热更」的完整语义
- 测试：护栏/不覆盖/nil/回收/满拒绝/Replace 不判满 + 并发同 key 恰好一次成功

## [v0.16.0] — 2026-10-18

### feat(ratelimit)：`NewKeyedLimiterByFunc` —— 按 key 定配额（[#31](https://github.com/oekit/concurrency/issues/31)）
- 新增 `NewKeyedLimiterByFunc[K](factory func(K) Limiter, opts ...KeyedOption[K])`：
  `NewKeyedLimiterOf` 的 factory 签名 `func() Limiter` 不接受 key，一个管理器内所有 key
  只能拿到相同配置的限流器。业务上按接口维度分档位（同步类宽松 / 备份类严格 / 导入确认
  最严）是常见形态，此前只能把限流器实例的键从 `(maxReq, window)` 扩成
  `(action, maxReq, window)`，即**每个 action 一个管理器**——那样按 key 隔离、空闲回收、
  容量护栏、`Len()`/`OverCapHits()` 可观测出口这些价值基本被绕过，整体限流状态也没有
  单一治理点
- 实现上把内部 `factory` 字段改为 `func(K) Limiter`，`NewKeyedLimiterOf` 包装一层
  （`func(K) Limiter { return factory() }`）——**对外零破坏**，旧调用方无需改动
- doc 点名 issue 里的坑：未在配额表登记的 key 会拿到 factory 的兜底返回值，等于**静默按
  兜底档限流**。这是 API 形状使然（Go 无法泛化"key 不存在"），已写明缺 key 时应让它
  显式失败或自行判空，别静默降级
- 测试：4 例——逐档耗尽 + 隔离性断言（最严档耗尽后中档/最宽档仍放行，用有序切片 +
  额度记账以规避遍历随机与预检消耗导致的假失败）；锁死旧 `NewKeyedLimiterOf` 语义不变
  （**配置**共享但**计数器按 key 独立**）；factory 拿到的确实是 key 本身；
  空闲回收/容量护栏/`OverCapHits` 在新构造器上仍生效

## [v0.15.0] — 2026-10-18## [v0.15.0] — 2026-10-18

### feat(breaker)：同名共享熔断器配置冲突可观测（[#29](https://github.com/oekit/concurrency/issues/29)）
- **新增 `WithBreakerConflictHandler(func(name, detail string))`**：`GetBreaker(name, opts...)`
  是进程级单例、opts 仅首次创建生效，同名再传**不同** opts 时本次参数被静默丢弃（不报错、
  不告警，表现为「该断的时候没断 / 不该断的时候断了」）。现把这一静默失效变成可观测事件：
  同名已存在 + 本次 opts 非空 + 生效配置指纹与首次不同 → 回调一次 detail 对照串
  （含 name、首次生效配置、本次传入配置与规避建议）。**纯 `GetBreaker(name)` 复用、
  传 nil 选项（=缺省）、传入与首次等价或仅钩子不同的 opts 均不告警**，无误报
- 冲突指纹 `configFingerprint` 只覆盖值语义字段（window/interval/forcePass/probeTimeout/
  cooldown/consecutive），**刻意排除 clock/onReject/onConflict 三个函数字段**——函数值
  不可比较，且钩子属每调用方私有接线，纳入比对会让先注册钩子的调用方被后续每个调用方
  无谓告警
- 每个 name 至多告警一次（`atomic.Bool` CAS 去重，避免热路径每次 GetBreaker 刷屏）；
  **回调在锁外触发**（消费方代码可能反过来调 GetBreaker，`sync.RWMutex` 不可重入）
- 冲突信号走**独立回调**而非复用 `WithBreakerRejectHandler`：拒绝回调语义是「熔断拒绝
  了一次请求」，混入配置冲突会污染按 reject 计数的看板。未注册回调时行为与此前完全
  一致（不产生任何日志/告警），保持本包零强制注入定位
- 文档：`GetBreaker` doc 补命名空间隔离示例（路线 A 参数编进名字 / 路线 B 改用
  `NewBreaker` 显式自建），不再只留一句「opts 仅首次生效」；`NewBreaker` doc 注明它
  无按名共享语义故不存在该问题
- **行为兼容**：共享语义、实例唯一性、并发安全均未变，仍返回首次创建的同一实例

### feat(breaker)：缺省值导出，下游改为引用而非复制魔法数字（[#28](https://github.com/oekit/concurrency/issues/28)）
- 导出 `DefaultWindow`(10s) / `DefaultBuckets`(40) / `DefaultForcePass`(1s) /
  `DefaultProbeTimeout`(1m)——此前全部小写未导出，迁移方只能把魔法数字各自固化一份，
  上游调了缺省、拷贝不会跟着动，而行为差异只在熔断阈值上体现，排查成本很高
- **刻意不导出**冷却期与计数型触发的缺省：二者缺省是「未配置」（cooldown 回落
  `DefaultProbeTimeout`、计数型关闭），值为 0，导出 0 值常量反而是陷阱——
  `WithCooldown(DefaultCooldown)` 会被写成"显式传 0"而 panic。**「用缺省」的正确写法是
  不传该选项**，无可引用的名字；确需非零阈值/冷却属调用方业务取值，应留在调用方配置里，
  不应冒充本包缺省。issue 建议的 `DefaultTripThreshold = 3` / `DefaultCooldown = 5m`
  实为旧 `agentkit/breaker` 的缺省，本包缺省并非如此，照导会凭空打开一条本该关闭的
  计数型触发器
- **订正 issue 正文的一处语义误述**：`WithCooldown(0)` 并非「沿用 probeTimeout」而是
  **panic**（`d <= 0` 一律拒绝，含 0）；回落只发生在配置结构内部的缺省零值上。已在
  `WithCooldown` doc 与 `doc.go` 迁移注记中写明，并加用例锁定该入参校验不被放宽
- 迁移注记（doc.go）：旧库 `NewBreakers(trip, cooldown)` 传 0 = 用缺省，本包选项式 API
  里 cooldown 传 0 = panic、计数型传 0 = 关闭触发——新旧含义相反，直译会编译通过、
  行为却变；本包无 `NewBreakers(trip, cooldown)` 形态，须按选项改写

## [v0.14.0] — 2026-10-18

### feat(syncx)：ctx 感知受限扇出 `FanInLimitCtx`（[#27](https://github.com/oekit/concurrency/issues/27)）
- `FanInLimitCtx[T](ctx, n, limit, fn func(ctx, i) (T, error)) []Slot[T]`——fn 携带 ctx 供
  单项内部使用；引擎级取消语义（三条契约，contract 测试锁定）：ctx 已取消时未启动项
  **fn 一次都不调**、当前及剩余槽一次遍历快速排空为 `context.Canceled`（worker 内缓存
  取消态，排空零额外锁开销）、已在飞 item 跑完照常落槽
- 其余语义与 FanInLimit 一致（panic 进槽 `*async.PanicError` 并上报、limit<=0 = 不设
  上限、n<=0 = nil）；ctx 为 nil 按 Background 处理；家族惯例加变体不改旧签名
  （FanIn/FanInErr/FanInLimit 无 ctx 需求场景继续用）

## [v0.13.1] — 2026-10-18

### fix(retry/httpx)：`RetryableWithHeaders` 头值 OWS 修剪（[#26](https://github.com/oekit/concurrency/issues/26)）
- `X-RateLimit-Remaining`/`Retry-After` 判定前套 `strings.TrimSpace`：net/textproto
  解析路径已按 RFC 剥离空白（主路径零影响），但自造 `http.Header`（测试桩/代理注入/
  配置还原）与非 textproto 客户端的值可能带前导/尾随空白——trim 后统一语义，
  宁可多判一次可重试（有 Retry-After/Reset 兜底）也不误判为永久失败

## [v0.13.0] — 2026-10-18

### fix(breaker)：熔断期流量收敛到探测级——计数型触发不再被公式短路（[#25](https://github.com/oekit/concurrency/issues/25)）
- **行为变更**：open/half-open 态放行策略改为「forcePass 强制探活（缺省 1s）+ 冷却期满
  半开探测，其余一律拒绝（记丢弃样本）」；**不再按 SRE 公式概率丢弃、公式愈合
  （dropRatio≤0）不再恢复放行**——旧版 `dropRatio<=0` 在状态判定之后短路放行，导致
  `WithConsecutiveFailures(3)` 触发后流量照旧（纯失败低样本场景实测第 12 次才开始拒绝，
  配了熔断等于没配）。SRE 公式退役为 closed→open 触发器，「状态」与「流量」不再失配
- 进入/重开 open 时初始化 `lastPass`：forcePass 通道自熔断进入起算，不再因零值失效
  （旧版 lastPass=0 时探活不生效，纯失败场景下熔断期首秒无任何放行通道）
- 评估附注：issue 方案 B（触发时清窗使 dropRatio 转正）数学上不成立——清窗后 total=0、
  dropRatio=(0-5)/1=-5 更负更放行；且仅去掉短路仍不够（概率通道在 dropRatio 自 0 爬升期
  漏过率高达 75%→30%），故采纳方案 A 并收敛到探测级
- **feat(breaker)**：`State()`/State 枚举可观测出口（`StateClosed`/`StateOpen`/
  `StateHalfOpen` + `String()`）——mu 内快照，仅供监控/日志/调试，放行判定仍以
  Allow/Do 返回结果为准。**Breaker 接口新增方法**：外部自行实现该接口的代码需补上
- 测试：#25 复现场景回归（30 连发第 4 次起全拒）+ State 全生命周期 + 窗口滚空不恢复
  流量 + forcePass 探活通道；受行为变更波及的 3 个旧契约测试按新语义重写（全确定，
  不再依赖概率断言）

## [v0.12.0] — 2026-10-18

### feat(retry/httpx)：403 头感知判定 `RetryableWithHeaders`（[#22](https://github.com/oekit/concurrency/issues/22)）
- GitHub 次级限流/滥用检测走 403 而非 429，裸状态码判定会把「等恢复窗口即可成功」的
  限流耗尽误判为永久失败：403 + `Retry-After` → 重试（次级限流）；403 +
  `X-RateLimit-Remaining: 0` → 重试（主限流耗尽）；403 无头 → 权限拒绝不重试；
  其余状态码同 `Retryable`（429/5xx）
- 新增最小 header 抽象 `HeaderSource`（`Get(key) string`）：`http.Header` 天然满足
  （MIME 规范键查找、大小写不敏感），自研客户端 header 类型实现同名方法即接入——
  包保持零依赖（不 import net/http）

### feat(syncx)：受限并发扇出 `FanInLimit`（[#23](https://github.com/oekit/concurrency/issues/23)）
- `FanInLimit[T](n, limit, fn)`：n 槽结果语义与 FanInErr 一致（按 index 序、panic 进槽
  `*async.PanicError` 并上报），内部固定 `min(limit, n)` 个 worker 经原子计数器拉取
  index——goroutine O(w)，供「n 大、fn 是有成本 IO」的 fan-out 防下游过载，收编消费方
  手写 channel 信号量样板；limit<=0 = 不设上限（等价 FanInErr）

### feat(ratelimit)：令牌桶时钟注入 `WithTokenClock`（[#24](https://github.com/oekit/concurrency/issues/24)）
- `NewTokenBucket(rps, burst, opts...)` 可变参选项（既有调用方零改动），`WithTokenClock`
  注入时钟（nil 回落 time.Now）：Allow/AllowN/Reserve/RetryAfter 全链路路由注入时钟
  （ReserveN/DelayFrom/CancelAt 显式时间结算）——假时钟下「1 秒后应恢复配额」与
  「Retry-After 精确回填」确定性断言，无 sleep 无 flake；KeyedLimiter 工厂闭包共享假
  时钟即可整表确定性测试，per-IP 限流手搓实现（map+锁+阈值清理）可整体收编为
  `KeyedLimiterOf` + `WithIdleTTL` + `WithMaxEntries`

## [v0.11.0] — 2026-10-18

### feat(ratelimit)：防爆破锁 `Lockout`——keyed lockout/penalty 原语（[#21](https://github.com/oekit/concurrency/issues/21)）
- fail2ban 形态：窗口内失败事件按 key 累计（滑动窗口，样本自然老化）→ 达阈值切入
  **锁定拒绝态**持续 LockDuration（期间 CheckLocked 返回剩余时长供对外 429 文案）→
  成功操作 `Reset` 清零（亦可用作管理员解封）；到期自动解除，空闲条目回收
- 与家族原语的边界（#21 边界声明核实属实）：`KeyedLimiter` 速率型令牌桶表达不了
  「计数达标后进入持续拒绝态」；`breaker` 是调用方视角的实例级熔断（无 key 隔离）；
  `budget` 是超限记账存档（无拒绝态与 Reset）
- 语义细则：锁定不因后续失败延长；锁定到期后旧失败仍在窗口内时首个新失败即重新锁定
  （重复违规者，对齐 fail2ban findtime）；**锁定条目免于空闲回收**（提前驱逐等于提前
  解锁）；容量护栏 fail-open——表满丢弃未登记 key 的失败样本并计入 `OverCapHits`
  （不因内部容量问题拒绝认证尝试）
- API：泛型 `LockoutOf[K comparable]` + `Lockout = LockoutOf[string]` 别名（对齐
  `KeyedLimiterOf` 惯例）；结构体配置 `LockoutConfig`（Window/Threshold/LockDuration
  必填、非法值 panic 启动即暴露；MaxEntries/IdleTTL/Now 可选，时钟注入对齐
  budget.WithClock）；`RecordFailure`/`CheckLocked` 统一返回 `(locked, lockRemain)` 序
  （issue 草案的两方法相反顺序经实测为 footgun，刻意修正）；`Len`/`Close` 幂等同
  `KeyedLimiterOf`。内存版起步，Redis 后端后续按消费方需求另拆
- 测试 10 例：假时钟确定性（触发/衰减/滑动/老化/Reset/重复违规/不延长/fail-open/
  泛型 key）+ 清扫判定走注入时钟（真实 ticker 只定节奏）+ -race 并发看门

## [v0.10.0] — 2026-10-18

### feat(breaker)：冷却期独立配置 `WithCooldown`（[#19](https://github.com/oekit/concurrency/issues/19)）
- 冷却期（自进入/重开 open 起多久放半开探测）与探测失联时限（挂起探测回收上限，
  `WithProbeTimeout`）拆分为两个独立决策：前者是业务决策（熔断后多久才敢再试），
  后者是调用侧在途时间上界——绑死只能取大值，要么恢复太慢、要么半开探测位被单次
  长尾调用长期占住（典型量级差：业务冷却 5m vs LLM 在途上界 60s）
- `WithCooldown(d)` 纯增量：缺省不设时回落 `probeTimeout`，**与 v0.9.0 行为逐位一致**；
  挂死探测的失联回收始终由 probeTimeout 把守，不受冷却期影响。d <= 0 时 panic

### feat(breaker)：计数型触发 `WithConsecutiveFailures`（[#20](https://github.com/oekit/concurrency/issues/20)）
- 新增「连续失败 n 次即熔断」触发，与速率型**并联**（任一命中即切入 open）；
  缺省 0 = 关闭，不设时**与 v0.9.0 行为逐位一致**
- 速率型回答「最近整体错误率多高」，受 protection=5 保护阈值制约：低 QPS 下连续
  连挂可能永不触发；计数型回答「最近有没有连挂」，补足低 QPS 触发与高成功率
  偶发连挂两类信号（对齐 Envoy consecutive_5xx 语义）
- 计数语义：非陈旧、非探测回报成功清零/失败 +1（FailureGen/panic 路径同样计数）；
  进入 open（任一触发）/ half-open / 关闸时归零，陈旧代回报不计数；
  触发不产生探测（gen/probeGen 不动），openSince 自触发失败起算。n < 0 时 panic

## [v0.9.0] — 2026-10-18

### feat(breaker)：显式三态状态机 + 代绑定——closed/open/half-open（[#17](https://github.com/oekit/concurrency/issues/17)）
- 显式三态取代「窗口统计隐式推导」：错误率超阈值（SRE 公式丢弃概率 > 0，即接受预算耗尽）
  切入 open；open 放行策略 = 原 Google SRE 概率丢弃 + forcePass 强制探活
  （**现有自适应丢弃行为不变**）；冷却（`WithProbeTimeout`，缺省 1m）期满后首个放行请求
  即半开探测——绕过概率公式必达、携带新代 generation
- 探测配对语义：成功关闸（half-open → closed + 清窗，旧失败样本不得触发立即重开）；
  失败重开并重置计时；超 probeTimeout 未回报视为失联自动回收、放行新探测；
  `AbandonGen` 支持调用方显式放弃失联探针
- 代绑定 `GenBreaker` 扩展接口（可选类型断言，`Breaker` 接口零改动）：`AllowGen` /
  `SuccessGen` / `FailureGen` / `AbandonGen`——代仅在探测放行时自增、普通放行携带当前代，
  陈旧代（gen < 当前代）回报整体作废：不计窗口、不迁移（一次调用的结果不会被算进
  另一次调用的账；陈旧探测不再延长冷却）
- 开放期普通失败只记窗口样本、不触碰冷却计时——冷却不被开放期失败后移，恢复时间可预测
- 兼容性：`Allow` / `Do` / `DoWithAcceptable` / `DoWithFallback` / `NewBreaker` / `GetBreaker`
  签名与行为不变（Do 家族经内部 Allow 透传代绑定，命中半开的 Do 调用即探测）；
  `window.RollingWindow` 新增显式 `Reset`（探测关闸清窗复用）。两处行为微变已文档化：
  关闸后全量放行（窗口清空、重新累计）；探测纪元边界的在途回报按陈旧代作废

## [v0.8.0] — 2026-10-18

### feat(singleflight)：`Flight.DoShared` + `Result.Waiters`（[#18](https://github.com/oekit/concurrency/issues/18)）
- `DoShared` 折叠可观测形态，额外返回 Shared（自己是 waiter 而非 leader）；Do 保持 (T, error) 不动
- `Result.Waiters` 新增——本轮折叠掉的并发调用方数（近似值，观测口径）
- 注：#17（breaker 三态状态机）当时评估后挂起，实施大纲已评论（后续轮次处理）

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