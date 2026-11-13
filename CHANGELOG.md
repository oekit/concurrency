## [v0.36.0] — 2026-11-13

- **BREAKING**：删除 syncx.Slot.Panicked 历史兼容字段（v0.7.0 起 Slot.Err 已承载 *async.PanicError，Deprecated 到期退役）。下游迁移：errors.As(slot.Err, &pe) 后读 pe.Value 取 panic payload（携带栈）。FanIn 契约不动。

## [v0.35.1] — 2026-11-13

- fix(group): WaitGroupWrapper 嵌套 Wrap 死锁——submitMu 换 counter+cond 等静止（#296）。Wait 不持锁等计数归零，任务体内 fan-out 派生不再与 Wait 互等（旧方案 AB-BA 挂死无 panic 无日志）；计数增减原子化弃用 WaitGroup，#269 misuse panic 触发面结构性消失。契约变化：Wait 从「等调用时已提交任务」改为「等系统静止」（等待期间新到 Wrap 被计入并等待）。

关联问题单：concurrency#296。

## [v0.35.0] — 2026-11-13

- 热路径与调度语义修复批：breaker open 态快速拒绝（#273）；timewheel 任务执行器可注入、缺省同步语义不变（#274）；ErrCircuitOpen 别名声明对齐现状（#275）；budget 裸 Option 改名 BudgetOption（#282，**BREAKING** 0.x minor）；retry 指数退避溢出边界修复 1<<62（#283）；singleflight 先摘键后唤醒对齐 pkg 参照（#285，窄窗口 join 率 24.9%→0.5%）；godoc 示例 err 字面量收口 logx.ErrKey（#288）；NewTokenBucket 构造期校验 + rps=0 显式不限速档（#291）；retry.Poll interval<=0 构造期 panic（#292）

关联问题单：concurrency#273、concurrency#274、concurrency#275、concurrency#282、concurrency#283、concurrency#285、concurrency#288、concurrency#291、concurrency#292。

## [v0.34.1] — 2026-11-12

- Go 1.26 现代化清扫：计数循环→range N（1.22）129 处、干净 WaitGroup fan-out→wg.Go（1.25）31 处、生产代码 range-int 9 处（progress/ratelimit/window/syncx）；纯等价重构，行为零变更，gofmt/vet/test 与基线一致 + race 抽查全绿。

## [v0.34.0] — 2026-11-12

- 全仓 issue 清零 sweep（#259 索引 13 单）：pkg errors.Recovered 单源收编 + 重复块收敛 + 行为缺陷修复

- refactor(async): panic 收编家族 16 函数 + Runner 六法收敛 guarded/spawnGuarded 单源，panic→error 转换走 org 单源 pkg/errors.Recovered（#261）；GoSafeWithErrChannel panic 分支改发 *PanicError（原裸 %v 丢身份，#268）；PanicError 补 Unwrap，error 型 panic 的错误身份全链不丢（#267）
- refactor(singleflight): Do/DoShared/DoChan 三份 leader/waiter/清理逻辑收敛 doCore/waitCall/runLeader/release，DoShared 退化为 Do+shared 标志（#262）
- refactor(syncx): FanIn 三变体逐槽 recover 收敛 runSlot，Gate/KeyedGate 幂等 release 四连抄收敛 idempotentRelease，FanIn 家族改 sync.WaitGroup.Go（#263）
- refactor(ratelimit): KeyedLimiterOf 五登记入口「容量护栏+安装」收敛 installLocked（#260）；Throttle 僵尸条目写路径每窗口至多一次顺手清扫，不再随历史 key 基数单调增长（#271）
- refactor(batch/timewheel): 逐字重复的 Ticker 五件套收敛 internal/ticker 单源，类型别名转发导入面零破坏（#264）
- refactor(progress): UpdatePhase/Complete 完结回写块收敛 completePhaseLocked，失败终态保护判定单点（#265）
- fix(group): WaitGroupWrapper 的 Wrap 与 Wait 并发触发 runtime WaitGroup misuse panic，submitMu 串行化对齐 gopool 方案（#269）
- fix(loop): 进程信号观察 goroutine opt-out 后泄漏，watchProcessSignals + stopCh 退出路径（不 cancel，已启动循环继续存活的 #218 语义保持）（#270）
- fix(retry): 重试等待 time.After 改 sleepCtx（NewTimer+Stop，提前退出即时释放 timer、不再每轮新分配）（#272）；detail.go 修正不存在的 DoValueLast 文档引用（#266）
- build: Go 地板 1.24.0 → 1.26.0（ci.yml 同步 1.26），依赖 pkg 抬至 v0.45.0（panic 收编单源）

导出面零删除、零签名变化（Unwrap 为新增方法），无 BREAKING。

关联问题单：concurrency#260、concurrency#261、concurrency#262、concurrency#263、concurrency#264、concurrency#265、concurrency#266、concurrency#267、concurrency#268、concurrency#269、concurrency#270、concurrency#271、concurrency#272。

## [v0.33.0] — 2026-11-11

- fix(ratelimit): Limiter.Reserve 收敛为 (time.Duration, error) 双返回，「不可预留」统一规范哨兵 ErrNotReservable（#248 #250）——此前 TokenBucket 回最大 duration（≈292 年）、SlidingWindow 回 -1，两套互斥魔数、接口契约零声明：消费方持 Limiter 接口（KeyedLimiter factory 返回类型）写通用退避，按 d<0 判拒绝的换 TokenBucket 被判成「等 292 年」挂死、按最大值判拒绝的遇 SlidingWindow 把 -1 当立即可用紧循环锤击满窗限流器；判定一律 errors.Is（可穿透 %w 包装），占位值 -1 防误读成 0=立即可用。哨兵契约属 API 面收敛：自行实现 Limiter 的调用方需随签名更新（0.x minor 允许 breaking，同 v0.21.0 启用 Wait 的先例）
- fix(ratelimit): SlidingWindow 补 RetryAfter 不消耗探测（#246）——AllowWithRetryAfter 对滑窗返回真实等待（下一个空位 = 最早一条滑出窗口的时刻，与 Wait 空位计算同口径），不再恒回 wait=0 引发 429 + Retry-After: 0 重试风暴；peekRetryAfter 删除永不可达的 *TokenBucket「Reserve+Cancel 等价探测」死分支（TokenBucket 本就实现 retryProber），不可探测的自定义实现与「限流器本身不可用」探测一律回退 WithOverCapRetryAfter 保守建议而非 0
- fix(ratelimit): KeyedLimiterOf.Replace 对未登记 key 走与 Create 同款 allowNew 容量护栏（#252）——表满先回收空闲条目、仍满拒绝并计一次 OverCapHits，返回 bool 使拒绝可观测（l 为 nil 同样返回 false，对齐 Create 拒 nil 契约）；已登记 key 换桶不受护栏影响（速率热更不被卡，契约「已登记 key 不受影响」原样）。修复前 Replace 是四个登记入口（Get/Create/ReplaceIfChanged/Replace）唯一不查护栏的——速率热更/配置下发为外部输入驱动的新 key 建桶，表可无限涨过 WithMaxEntries，护栏与 OverCapHits/Evictions 观测面双双静默失效
- fix(breaker): forcePass 探活领取原子化（#251）——抽 tryForcePass 用 atomic.Int64.CompareAndSwap 把「判断+占位」收成单步线性化操作，open 态「每隔 d 放行一次」在并发下恰一次：修复前锁外 Load→判断→Store 三段竞态，forcePass 到期瞬间所有已越过阈值的请求同时放行（真并行 12 核实测多赢家率 ≈2.5%/轮），探活风暴直击已过载后端；CAS 失败方重读即知名额已归属他人，循环无活锁
- fix(shedding): adaptiveShedder/nopShedder 的 Allow 入口 nil ctx 兜底为 context.Background（#249）——Shedder 接口契约如实声明，与 retry/loop/syncx/breaker 的 #159 口径统一；此前直接解引用 ctx.Err() nil panic 打死请求 goroutine
- fix(singleflight): Do/DoShared/DoChan 入口 nil ctx 兜底为 context.Background（#249）——DoChan 的 context.WithoutCancel(nil) 与 waiter 路径的 select <-ctx.Done() 均 nil panic；fn 现收到兜底后的 Background，#159 口径清尾
- fix(progress): UpdatePhase 自动完成循环跳过 PhaseFailed 阶段（#247）——失败痕迹（Status/Progress/EndTime）不再被改写成 Completed/100，与 Complete() 的 #193 不变量同口径（同文件测试钉住的断言延伸到第二个入口）；pending 前置照旧自动完成，其余不变量不动

关联问题单：concurrency#246、concurrency#247、concurrency#248、concurrency#249、concurrency#250、concurrency#251、concurrency#252。

## [v0.32.0] — 2026-11-10

- - feat(poll): HandleRetries + DeadLetter 毒丸死信档——可续拉失败重试至上限后交钩子出档，与 Retryable 门正交且门优先（#231）

关联问题单：concurrency#231。

## [v0.31.2] — 2026-11-10

- - build: 依赖地板对齐——pkg v0.41.0 → v0.41.1（floor sync，无代码变更）

## [v0.31.1] — 2026-11-10

- - fix(poll): RunCursor 的 Retryable 门扩到 item 与 advance 错误路径——毒丸 item 不再无退避紧循环（#226）
- - fix(budget): WithClock(nil) 保留缺省时钟，不再首次超限记账即 panic（#227）
- - fix(retry/httpx): HTTPError 显式 0 秒 Retry-After 建议经 hasHint 保留，与 ParseRetryAfter 口径统一（#228）
- - docs(poll): 空批游标推进后等待 IdleDelay 的语义如实成文，测试钉住下界（#229）

关联问题单：concurrency#226、concurrency#227、concurrency#228、concurrency#229。

## [v0.31.0] — 2026-11-09

- feat(loop): DisableBackgroundSignalHandling opt-out + RunBackground 信号抑制声明（#218）
- fix(httpx): ParseRetryAfter 乘法溢出回绕拒绝（#213 #215）
- fix(poll): RunCursor 空批 advance 失败退避升级恢复（#216）
- fix(async): DetachedContext enricher panic 收编（#217）
- fix(timewheel): 非整数格延迟向上取整——不再提前触发（#219）
- fix(batch): exit 通道参数化固化 + 等价性回归钉（#220）
- docs(httpx): 手动跟随重定向示例 ResolveReference+相对 Location pitfall（#214）

关联问题单：concurrency#213、concurrency#214、concurrency#215、concurrency#216、concurrency#217、concurrency#218、concurrency#219、concurrency#220。

## [v0.30.1] — 2026-11-08

- fix(async): Spawn/GoDetached/DetachedContext 保留 parent values——「WithoutCancel 语义内置」表述与实现对齐（concurrency#211）：实现原从 `context.Background()` 派生，parent 的 values 全部丢弃（仅经 SetContextEnricher 补 trace），与 `context.WithoutCancel` 的标准语义（仅去除取消信号、**Value 链原样继承**）相悖——Spawn 的定位是「手拼 channel+WithoutCancel+GoSafe 五件套的收编件」，凡原手拼用了 WithoutCancel（留 values）的场景收编后 values 静默丢失：values 缺失在下游深处往往是 `ScopeFrom(ctx)` 类读取返回 nil 后**静默降级**（不报错、不 panic），极难发现（下游 git-ferry delegate 后台委派实测踩坑：回填结果需要 ctx 里的会话 scope，收编 GoDetached 后拿不到 ⇒ 回填静默不执行，回归测试当场红才暴露）。改为 `context.WithoutCancel(parent)` 派生（go 地板 1.24 直接用 stdlib，不再手写自定义 context 类型）：Done 为 nil、Err 恒 nil、Deadline 不继承、Value 委托 parent——保留/丢弃对照表进 DetachedContext godoc；SetContextEnricher 钩子形态与注册口径不变（只补 trace 一层，与 values 继承正交）；nil parent 兜底 Background 口径不变。用例三对照表逐项钉住：DetachedContext 直测（values 可读且与 `context.WithoutCancel` 逐值一致、cancel parent 后 Err 恒 nil 且 Done 永不触发、带 1h 时限 parent 的 Deadline 不继承）；GoDetached/Spawn 无 enricher 时 fn 内读到 parent values（VALUES-LOST 回归钉住，Spawn 场景叠加主路径 cancel）；清 enricher 后 values 仍在（enricher 只管 trace）

关联问题单：concurrency#211。

## [v0.30.0] — 2026-11-08

- feat(retry/httpx): 新增 URL query 凭据参数剥离 `StripSensitiveQueryParams` / `StripSensitiveQueryParamsWith` + 词表导出 `DefaultSensitiveQueryParams`（#208）——token-in-query 泄漏面（#200 StripSensitiveHeaders 的姊妹篇）：部分平台 SDK 把令牌放 query 而非头部（go-gitea/go-gitee 的 tokenTransport 每一跳 `q.Set("access_token", token)`，包在 http.Client 的 Transport 外层 ⇒ 平台 302 到 CDN/对象存储（Gitee archive/附件端点实测形态）时令牌跟着 query 一起发给第三方主机；头部剥除帮不上忙——令牌根本不在头部；Go 自动跟随也不剥 query——stdlib 只处理头部；手动跟随 #200 同场景同样只顾头部）。纯函数原语：只改传入 `*url.URL` 的 RawQuery，跨宿主判定留给调用方（与 StripSensitiveHeaders 并行调用、CrossHostRedirect 之后逐跳剥离，两条独立泄漏面缺一即漏；同宿主跳转不调用——平台路径跳转的 query 凭据依赖同宿主认证）；内置词表 access_token / private_token / token（下游 go-git-platform transport/client.go v0.118.0 的 sensitiveQueryParams 固化组），经 `DefaultSensitiveQueryParams` 导出副本可组合扩展（无包级可变全局，与头部词表同一哲学）；匹配口径：键大小写不敏感折叠（EqualFold，ACCESS_TOKEN 同属凭据，保守方向多剥）+ 百分号解码键（`%61ccess_token=x` 服务端解码后同键），无下划线驼峰（AccessToken）是不同参数名不误剥；「无变更不重编码」：没剥掉任何参数时 RawQuery 逐字节原样保留（下游实测要点——无变更就 Encode 会扰动参数序与转义形态），发生删除时其余参数按原字节形态保序重接（不经 url.Values.Encode——Encode 会重编全部键值并按字典序重排）；包文档依赖红线更新：标准库外零依赖不变，不 import net/http（URL 形态原语只依赖 net/url）

关联问题单：concurrency#208。

## [v0.29.0] — 2026-11-08

- feat(syncx): 新增按键互斥原语 `KeyedMutex[K]`（#195）——「同名任务/通道/资源同刻只跑一个」的本地单源（收编 git-ferry localGuard 的 sync.Map 三件套手搓：LoadOrStore + 失败还名额 + Delete 回调三步各自易错，失效形态都是慢性的——忘还名额 ⇒ 慢性并发泄漏、Delete 抢跑 ⇒ 误删新执行流的标记）：`WithLock(key, fn)` / `Acquire(key)` 即拒形态（key 忙立刻 false，不排队不占名额，ok=false 唯一含义=该 key 正在执行中）、release 幂等（atomic CAS，Double-release 第二次 no-op 且不误删新持有者的标记）、release 即注销（条目生命周期=持有期，高基数 key 零残留——无需清扫协程、无需 Close，对比 KeyedGate 槽位常驻/KeyedLimiterOf 空闲 TTL 的原因见包文档）；16 段哈希分段锁（maphash.Comparable，Go 1.24）防单 map 热点；与 singleflight（同 key 合并成一次执行）的分工在包文档钉死
- feat(async): 新增带句柄的旁路任务 `Spawn[T]` + `Handle[T]`（Wait/WaitUpTo/Done）（#197）——「主路径旁路发一个任务、限时收结果」的收编件（替代手拼 channel+WithoutCancel+GoSafe 五件套 15 行：缓冲 <1 且主路径提前 return ⇒ sender 永远阻塞、漏 WithoutCancel ⇒ 请求取消连带腰斩旁路任务、channel 元素类型手写 ⇒ 类型错误 run-time 才发现）：脱离 parent 生命周期（WithoutCancel 语义内置）+ 保留 trace（SetContextEnricher）+ panic 收编进结果（*PanicError，errors.As 可辨；汇报 ctx 为富化后台 ctx 并携带 task 标签，#184 新签名口径）；泛型结果类型把编译期对位交还给类型系统；WaitUpTo 的 timedOut 标记把「放弃后结果丢弃」显式化——超时任务继续跑完，迟到结果只对放弃等待的那一路不投递（fan-in 的其它等待方仍拿得到）；ctx 取消（ctx.Err()）与超时（timedOut=true）两条失败路径可区分
- feat(retry/httpx): 新增 `ParseRetryAfter(v, now)`（#198）——Retry-After 头解析入口（RFC 7231 双形态：delay-seconds + HTTP-date；时刻三格式依 IMF-fixdate→RFC 850→asctime 次序尝试，与 net/http.ParseTime 同序同语义，零 net/http 依赖红线不破）：负秒数不可解析（0, false）、过去时刻钳 0（立即重试，不取负）、坏值/空值 (0, false) 走指数退避；bool 口径让「带了 0=立即重试」与「没建议=计算退避」分支清晰；值先做 OWS 修剪（#26 口径）；钳制（MaxDelay 上限）留给调用方——原语只负责「头值→时长」的忠实解析
- feat(retry/httpx): 新增跨宿主重定向敏感头剥离 `StripSensitiveHeaders` / `StripSensitiveHeadersWith` + 判定 `CrossHostRedirect`（#200）——手动跟随重定向的调用方在逐跳 RoundTrip **之前**调用（go-github checkRedirection 同场景：std http.Client 自动跟随会剥离跨宿主 Authorization，手动跟随恰好绕开这层保护；剥离晚了就已经在跳转请求里带出去了）：同宿主跳转原样保留（平台路径跳转依赖同宿主认证），跨宿主（host[:port] 变化、大小写不敏感、scheme 不参与、缺省端口等价形态按不同宿主保守处理）剥离凭据族——Authorization / Proxy-Authorization / Cookie / Private-Token / X-Api-Key / X-Auth-Token，非凭据头保留；词表经 `DefaultSensitiveHeaders` 导出（返回副本，平台特化 Token 头追加后经 With 变体组合，无包级可变全局，与 logredact「规则表公开可组合」同一哲学）；`HeaderEditor`（HeaderSource+Del）零依赖接入 http.Header 与自研客户端

关联问题单：concurrency#195、concurrency#197、concurrency#198、concurrency#200。

## [v0.28.4] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.28.3] — 2026-11-08

- fix(async/supervisor): Group.Reconcile 不再在持有 g.mu 时调用用户回调 idOf——回调里
  读组状态（Len/Running，文档明确承诺安全的用法）不再自死锁；idOf panic 经
  `async.RunSafeRetLabeled` 收编为 `*PanicError` 后在**无锁区**上抛，剩余临界区
  （diff/register）一律 defer Unlock——一次坏回调不再让整组方法永久死锁（#188、#189）
- fix(ratelimit): SlidingWindow.Reserve 预占槽位**互不相同**（预占时刻按
  `timestamps[len-maxCount]+window` 边界推进、同刻腾出的多槽 1ns 错开兜底）；在册预占以
  maxCount 为上限（超限返回 -1 表示不可预留，不再无限透支未来窗口）；Count 只计已落账
  条目（任意时刻 ≤ maxCount）；cleanOld 只扫过期头部、列表恒 ≤ 2*maxCount——40000 次
  连续 Reserve 从 4.5s 的二次方退化回到线性（#190）
- fix(poll): RunCursor 的空批（fetch 成功、0 项、游标不变——长轮询常态）按**成功轮**
  重置失败计数，Exponential 退避不再一次抖动后单调爬到 MaxDelay 钉死不回落（#191）
- fix(budget): Ledger.Consume 存档内容入库即拷贝——调用方复用缓冲区不再事后篡改
  「按需取回完整原文」的取证材料（与 Get 出库侧的防御口径对称）（#192）
- fix(progress): Tracker.Complete 不覆盖 Fail 终态（TaskFailed 不可回退，PhaseFailed
  阶段保留失败痕迹）——`defer t.Complete()` + 错误分支 `t.Fail()` 的常见写法不再把
  失败任务在报表里抹成 100% 成功（#193）
- fix(group): Group.Run 的 interrupt 回调逐个经 `async.RunSafeLabeled` 收编、排空等待
  放 defer——一个 interrupt 自身 panic 不再弃掉其余 Actor 的停机通知与等待（泄漏）（#194）
- docs(changelog): 修复 v0.15.0 版本标题重复粘连（恢复单标题）；恢复被 feature 提交
  误删的 v0.7.0 版本小节（#187，新门禁 oekit/.github#61 口径下自验通过）
- docs: AGENTS.md 治理轮同步（错误处理段改 BizError/stdlib 双轨表述、日志段补
  `logx.ErrKey` 常量约定、Option 段补非法值总则 + 新增「超时默认值」段，Refs matrix-tools#237）

关联问题单：concurrency#187、concurrency#188、concurrency#189、concurrency#190、
concurrency#191、concurrency#192、concurrency#193、concurrency#194。

## [v0.28.2] — 2026-11-08

- **BREAKING** refactor!(async): `PanicHandler` 签名统一为 `func(ctx context.Context, r any, stack []byte)`——与 gopool.PanicHandler 完全一致（#184）。ctx 为任务执行上下文（带 ctx 的 spawn 家族——GoWithContext / GoSafeWithContext / GoSafeWithContextLabeled / RunSafeCtx / Runner.GoWithContext / GoDetached——把调用方 ctx 透传给 handler，日志可按请求聚合）；r 为 recover 原始值（错误上报路径为 fn 返回的 error）；Labeled 家族与 Supervise(WithTask) 的任务标签改经 ctx 携带（新增 `TaskFromContext` 取，默认 handler 输出仍带 `task=<name>:` 前缀），不再拼在消息前缀里。消费方迁移：`func(msg string, stack []byte)` → `func(ctx, r any, stack []byte)`（打印 msg 的位置改打印 r）
- docs(breaker): WithForcePass godoc 钉死 #25 三态语义——探活结果（无论成败）只进窗口统计，不驱动关闸/重开/重置冷却；关闸唯一路径是冷却期满的半开探测成功；「恢复即自动愈合」说法废弃（#185，措辞随 #167 收口后本轮补齐归属）
- docs(ratelimit): SlidingWindow.Wait 注明不预占设计的惊群代价（多等待者同刻醒来抢同一空位，仅一个成功），高并发严格排队场景指路 Reserve 的预占语义（#186）

关联问题单：concurrency#184、concurrency#185、concurrency#186。

## [v0.28.1] — 2026-11-07

- build: 全线依赖地板对齐（16 项，v0.1.27 维护轮）

## [v0.28.0] — 2026-11-07

- fix(breaker): half-open 期 forcePass 放行的流量不再冒充「挂起探测」回报——错误不再驱动熔断状态迁移（#156）
- fix(breaker): report 按放行时签发的探测标记识别探测回报——状态迁移不再被非探测请求劫持，gen-only 入口按代推断（#162）
- fix(breaker): half-open 挂起探测在飞期间关闭 forcePass 通道——force-pass 回报不再被误判为探测回报、真实探测成功不再被吞（#172）
- docs(breaker): WithForcePass 删除「恢复即自动愈合」的过时注释——关闸只由半开探测成功驱动（#167）
- fix(async): Runner 六个方法统一经带 recover 的唯一汇报出口——handler 自身 panic 不再打死进程（#164）
- fix(async): SetPanicHandler(nil) 恢复默认 handler——与 gopool 同名 API 语义对齐，不再静默关闭汇报（#165）
- fix(gopool): 常驻 worker 老化退出前 drain 队列——与 close/临时 worker 两条退出路径对称，积压任务不再无人消费（#163）
- feat(async): GoDetached 脱离请求生命周期、保留 trace 的后台安全 spawn + SetContextEnricher 富化钩子（#155）
- feat(loop): RunBackground 共享进程级信号 ctx + Run 入口已取消 ctx 拒启——defer cancel 脚枪文档化（#154）
- fix(window): RollingWindow 构造期校验 interval>0——首次 Add/Reduce 整型除零 panic 提前到构造期（#158）
- fix(breaker): Allow/Do 系列对 nil ctx 兜底 Background——与 retry/loop/syncx 口径统一（#159）
- docs(singleflight): DoChan 注释改为「leader 恒免疫取消」的真实语义——需取消传播用 Do/DoShared（#160）
- fix(retry): 线性退避裸乘溢出回绕关闭——复用指数路径饱和口径，无界重试不再退化紧循环（#166）
- fix(batch): LessExecutor.DoOrDiscard 首窗先记时间戳再 CAS——同窗口恰有一路执行的契约兑现（#173）
- fix(timewheel): insertLocked ticks 整轮饱和——巨大延迟不再产生负槽位下标打死事件循环，循环死亡时 send/HasTimer 快速失败（#174）
- build: 依赖地板对齐 pkg v0.37.0

关联问题单：concurrency#154、concurrency#155、concurrency#156、concurrency#158、concurrency#159、concurrency#160、concurrency#162、concurrency#163、concurrency#164、concurrency#165、concurrency#166、concurrency#167、concurrency#172、concurrency#173、concurrency#174。

## [v0.27.0] — 2026-11-07

- feat(poll): 新增 `RunCursor[T]`——带单调游标的长轮询批量拉取循环，四个易漂移语义由库钉死（游标只前进 / 处理完才推进 / 失败退避复用 retry 口径 / 单项 panic 收编不杀循环）（#144）
- feat(async/supervisor): 新增 `Group[T]`——按键常驻循环集合与外部目标集持续对齐（新增起 / 消失停并转排水 / 规格变更重启 / 意外死亡探活重启），组内单条循环复用 `async.Supervise`（#143）
- feat(retry): 导出 `Delay(cfg, attempt, err)`——退避的**实跑口径**（真实抖动随机量 / `DelayHint` 覆盖 / `MaxDelay` 封顶），与 `Backoff` 的上界口径互补；`poll` 的续拉退避直接走它，全仓仍只有一份退避公式
- **BREAKING** feat!(fanout): `Option` 更名 `MapOption`——本仓 AGENTS.md 禁止裸 `Option`（多包 co-import 冲突），v0.26.0 引入该别名时违反了这条。只传 `fanout.WithOnItemError(...)` 的调用点不受影响；显式声明该类型变量会编译失败

**#144（游标长轮询）**：IM / 消息平台的收消息循环是同一个形状，逐平台复制——带游标拉一批 → 出错等固定几秒 → 推进游标并持久化 → 逐条处理。复制出来的副本在三处各自漂移：①退避是手写固定值（`time.After(3 * time.Second)`，无指数无抖动 ⇒ 两个平台同时撞故障会按同一节奏把下游再打一遍）；②游标推进时机各写各的（先推进后处理 = 崩溃丢消息，先处理后推进 = 崩溃重复投递，两版混在生产里）；③批内一条坏数据 panic 打死整个循环。

`poll.RunCursor` 把四件事钉死成一份实现：**游标只前进**（非空批必须带回不同游标，否则 `ErrCursorStuck` 当场返回——它不会自愈，退避重试只会无限重复同一批）；**处理完才推进**（缺省档恒为 at-least-once，`OnAdvance` 先落盘成功才算推进，持久化失败就退回旧游标重拉——「凭记忆跳读」比重复处理难查得多）；**失败退避复用 `retry.Config` 全部口径**（指数 / 抖动 / `DelayHint` 服务端建议 / `MaxDelay` 封顶），不可续拉的错误（鉴权失效、游标被服务端作废）带错误返回而不空转；**单项 panic 经 `async.RunSafeCtx` 就地收编**成 `*PanicError` 并汇报全局 `PanicHandler`，循环存活。缺省档下某项失败即停本批（已处理的项会再处理一次，未处理的一项不丢）；`AdvanceOnItemError` 档下必须跑完整批再推进（中途返回会让剩余项再也不会被拉回来）。`IdleDelay` 没有安全缺省——服务端 hold 与不 hold 用同一个数字必有一边是错的，故只对 hold 的接口留 0 并由文档点明。

**#143（按键 reconcile 组）**：`async.Supervise` 管**单条**循环活得好不好，`loop.Run` 管**固定周期**任务，但「目标集来自外部存储、运行中要持续对齐」这一类形态没有原语——通道/连接/租户 worker 池类服务各手写一份 `map[K]entry{cancel, spec, done}` + mutex，锁内 cancel/delete 顺序、snapshot 比对、done 非阻塞探活全是并发正确性敏感点。

`supervisor.Group[T]` 收敛成两个动作：`Reconcile(targets)` 对齐、`Stop`/`Wait` 停机排水。组内单条循环的健壮性**完全复用** `async.Supervise`（panic 收编 + 指数退避 + 失败上限），本包只管集合——两层职责不重叠。三个刻意的判断：①身份取 `IDOf` 返回的 **reconcile 键**而不是主键，哪些字段变了要重启由调用方把字段编进键里决定（拿 `reflect.DeepEqual` 比较规格会在含 mutex / 函数字段时给出假差异 ⇒ 重启风暴，而漏编字段只是少重启一次，两种失效方向不对称）；②移除目标只取消、转排水集合，**不阻塞 Reconcile**（慢排水的循环不能拖住对齐），`Draining()` 暴露排水进度；③用户代码（cancel / start）一律在**锁外**调用——持锁启动会让回调里的读操作卡在不可重入的 `sync.Mutex` 上，失效形态是「整组对齐从此停摆」而不是一次可见的报错。

观测三个非阻塞探针：`Len`（受管目标数）/ `Running`（循环尚未结束的目标数）/ `Draining`（已移除未退出）。`Running < Len` 是「集合对齐了但实际没在服务」的唯一观测口。注意 `Running` 不等于「start 已经跑过」——cancel 早于首跑时 `async.Supervise` 一次都不跑。

**`retry.Delay` 那条**：写 `poll` 时发现「自己实现循环」的调用方拿不到**实跑**退避——`Backoff` 是上界口径（抖动取满），拿来当实跑等待会让抖动退化成确定值，同一批同时失败的实例仍撞在同一点上。故补 `Delay`（实跑口径）并与 `Backoff`（预算口径）在注释里明确分工；`poll` 的续拉退避走它，全仓仍然只有一份退避公式。

**`fanout.Option` 更名**：本仓 AGENTS.md 写着「函数选项模式 `XxxOption`，禁止裸 `Option`（多包 co-import 冲突）」，而 v0.26.0 新增 `WithOnItemError` 时引入的正是裸 `Option`——自己立的规矩自己没守。0.x 阶段 minor 允许 breaking，故直接更名 `MapOption` 而不留别名（留别名等于把违禁名永久化）。调用点只传 `fanout.WithOnItemError(...)` 的不受影响。

关联问题单：concurrency#143、concurrency#144。

## [v0.26.0] — 2026-11-06

- feat(async): 补 `RunSafeCtx` / `RunSafeCtxRet`——在**别人的 goroutine 边界**就地收编 panic，收编成 `*PanicError` 交回调用方，并带 ctx 自己记日志/计数（#146）
- feat(fanout): 补 `WithOnItemError` 逐项失败钩子——`panic` 也走钩子，消费方不必再在 fn 内层手写 `recover` 抢先（#146）
- feat(ratelimit): 补写合并节流 `Throttle[K]`（first-then-suppress）——同 key 窗口内只生效一次，写失败撤销戳可立刻重试，`Suppressed()` 提供「合并生效了没有」的观测口（#145）
- test(ratelimit): 空闲回调用例改**事件驱动**等待——原先「睡 80ms 断言后台清扫已回收」余量只有约 2 倍，实测 ~10% 假红

**#146（人机工学）**：`GoSafe`/`RunSafe` 走全局 PanicHandler（签名不带 ctx ⇒ 日志无法按请求聚合），`GoSafeWithContext` 新开 goroutine（调用方拿不到结局），`RunSafeErr` 收不到 ctx——三档都装不下「在第三方 SDK 自己的读循环 goroutine 里就地收编 panic」。ws 客户端事件回调这类边界上，外层 `Supervise` 覆盖不到，panic 必须在回调内收编，而回调里又确实需要 ctx 才能把这条 panic 记得有用。`RunSafeCtx` 补的正是这一格：同步执行、ctx 透传、panic ⇒ `*PanicError` **返回**给调用方（**同时**照 `RunSafeErr` 口径向全局 handler 汇报一份，全局观测面不因「调用方自己也要记」而漏）；`RunSafeCtxRet` 是带返回值的伴生（panic ⇒ 零值 + `*PanicError`，调用方才能区分「合法零值 + nil」与「崩了」）。

`fanout` 侧同一个根源：库已把逐项 panic 收编成 `*async.PanicError` 并经 `Error.Failed` 返还，但消费方想在**该项失败那一刻**落业务终态（毒丸行标记失败）时，此前只能在 fn 内层 `defer recover()` **抢先于库的收编**（库的 PanicError 通道因此被吃掉），或等整批返回后再 `errors.As` 分拣（长批里终态与进度脱节）。`WithOnItemError` 把「分拣 + 落终态」放回库的收编链路：聚合返回**之前**逐项同步调用（**调用方 goroutine、非 worker ⇒ 不并发**），`err` 覆盖 fn 返回的错误、panic 收编结果、ctx 取消导致未启动项的 `context.Canceled`（与 `Error.Failed` 同一口径）；钩子自己 panic 就地收编上报，不改变该项失败归属。选项走**变参** ⇒ 既有四参调用点零改动。

**#145（写合并节流）**：`KeyedLimiter` 是速率配额（Allow=false 只说明这次没通过），而「last_used_at 类时间戳回写」要的是**窗口内去重合并**——每个请求都该 touch，真落库窗口一次。拿令牌桶硬凑会缺一笔账：抑制了几次没有口径，observability 上看不出合并有没有生效。`Throttle[K]` 补这一格，三条语义是刻意的：①窗口锚在**上次放行时刻**（固定窗口，时钟可注入）；②fn 返回错误或 panic ⇒ **撤销本次戳**，下一个调用立刻可重试——写失败等于「没写」，若仍记成已消费，时间戳会整整一个窗口得不到更新；③fn **不持锁**执行，慢 fn 不阻塞其它 key（同 key 的抑制判定也不等它）。撤销只删「还是自己那一戳」的条目：fn 耗时超过窗口时别的调用可能已写入更新的戳，无条件删会让下一次重复执行。

**改用例的那条**：`TestKeyedLimiter_EvictCallback_PanicAbsorbed` 用「睡 80ms 再断言后台清扫已回收」，而清扫 ticker 的首个命中时刻取决于 ttl、ticker 间隔与调度延迟，插桩实测约 35ms 才命中 ⇒ 余量只有约 2 倍 ⇒ 负载高时实测 20 次跑出 2 次红（`evictions=0 want >=1`）。改事件驱动：回调里 `sync.Once` 发信号（先发信号再 panic，panic 照旧由库收编），3s 超时兜底。改后 40 次连跑零红。这与 v0.23.1 修 async 同款假红是同一条纪律：**等后台协程必须等事件，不要等一个估计出来的时间窗**。

关联问题单：concurrency#145、concurrency#146。

## [v0.25.1] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.25.0] — 2026-11-06

- feat(loop): 新增 `IntervalAfterCompletion` 间隔模式（#140）——`loop.Run` 此前只有「固定节奏 + 忙时丢 tick」一种语义，而周期任务里有很大一类（清理/巡检/对账/认领-投递类 worker）关心的是「两次执行之间至少隔开 Interval」：本轮跑得久，下一轮相应推迟。固定节奏语义下长轮次只丢 tick 不后移，「轮次间隔 ≥ Interval」无保证，消费方只能在 `async.Supervise` 里手搓同款「fn + timer select」（octopus 两处实证：outbox worker / bot 巡检——用 Supervise 只为 panic 收编，重启/退避语义根本没触发）。新增 `Config.IntervalMode`（缺省 `IntervalFixed` 保持既有语义零破坏）：AfterCompletion 模式下间隔从「本轮结束」起算，fn 返回（含 panic 收编）→ 重置计时器 → 等 Interval → 下一轮；ctx 取消拦下一轮、在飞轮次跑完的排水契约不变，轮间等待可被取消打断（timer select 而非 Sleep）。两种模式的判据差异见包文档「间隔模式」节
- feat(ratelimit): KeyedLimiterOf 速率热更下沉——`ReplaceIfChanged` / `AllowWithRate` / `CurrentRate` 三件套（#141）——速率热更入口此前只有无条件 `Replace(key, l)`：按 key 定速率且速率会被外部配置热更（Nacos/DB）的消费方，每次调用都要回答「这个 key 当前生效的速率是不是变了」，而库不暴露 key 的当前速率，消费方只能自维护一张影子 rates 表镜像库内状态（octopus `ensure()` 实证）——影子表与库内真实状态是两份事实，Replace 之外的任何路径都会让它撒谎且无信号；无条件每次 Allow 前 Replace 则每次新建桶（余量清零、分配压力）。修法：条目登记速率元数据（hasRate/rate）为事实单源，三个入口按需取用——`ReplaceIfChanged(key, rate, l)`（比对库内元数据，一致不动桶返回 false 余量保留，变更/未登记才换桶并登记；兼作护栏内原子登记，Create+热更合一）；`AllowWithRate(key, rate, newBucket)`（热路径正解入口：元数据一致直接 Allow 零分配，不一致才锁外惰性建桶原子安装，newBucket 在库锁之外执行——对齐 #33 锁序结论，并发竞态以库内元数据为准只装一方）；`CurrentRate(key)`（元数据出口，观测「这个 key 现在生效的是哪一档」）。`Create`/`Replace` 不登记元数据（doc 声明热更口径统一走 WithRate 家族）；rate<=0 panic（配置错误启动即暴露）、newBucket 返回 nil 不登记（对齐 Get 拒 nil 契约）
- feat(ratelimit): KeyedLimiterOf 空闲回收可观测面——`WithEvictCallback` + `Evictions()`，doc 声明「ttl 必须大于窗口长」不变量（#142）——`WithIdleTTL` 的空闲回收回收的是**整个 Limiter 实例**：对 SlidingWindow 这类窗口语义实现，ttl ≤ window 时 key 每空闲 ttl 被重建、窗口计数永远攒不满 ⇒ 限流/预算闸**静默失效**（不超任何请求、无任何报错，失效方向是安全/预算面放松——「配置错了但一切看起来正常」的经典形态），而该不变量此前只存在于个别调用方的防御性注释里。修法三层：①WithIdleTTL/NewKeyedLimiter/NewKeyedLimiterByFunc（其 SlidingWindow 示例正是危险组合）doc 显式声明不变量与 2×窗口建议值，多档位以最大窗口为准；②`WithEvictCallback` 回调（key, 空闲时长）——后台清扫与表满 sweep-on-pressure 两条路径统一**锁外**同步触发（回调可重入本管理器，panic 经 RunSafeLabeled 收编 task=ratelimit.keyed.evict），消费方在回调里打日志/打点即得观测；③`Evictions()` 累计出口——「回收计数持续增长而限流命中为零」正是静默失效的特征形态

关联问题单：concurrency#140、concurrency#141、concurrency#142。

## [v0.24.0] — 2026-11-05

- feat(async): 新增 `Supervise` 监督循环原语（#137）——GoSafe/GoSafeLabeled 只保进程不保循环：循环体 panic 后 goroutine 死亡、循环静默停摆（消费 worker 停摆=丢消息、巡检循环停摆=新对象永不启动、活性表僵尸条目永占位），消费方三处手写同型防护。监督语义三件套收编为单源：①单轮 panic 收编（转 typed `*PanicError`、errors.As 可辨携带栈，按 GoSafeLabeled 同款格式带 task 标签上报）；②退避重启（`WithRestartBackoff(initial, max)` 指数翻倍封顶 / max<=0 固定档 / initial<=0 立即档，溢出取封顶值不回绕为负；重启等待可被 ctx 取消打断，已取消的 ctx 不首跑）；③连续失败上限 + 钩子（`WithMaxConsecutiveFailures` + `WithOnGiveUp` 告警、`WithOnRestart` 观测，钩子自身 panic 被收编）；`*Supervision` 句柄导出 Rounds/Restarts/ConsecutiveFailures 活性面 + Wait/Err/Done。轮次契约：一次 loop 调用=一轮，返回 nil=优雅结束，error/panic=失败轮；ctx 取消时监督以 ctx.Err() 告终——取消是停机决定，最后一轮业务错误不伪装成结束原因，调用方告警判据 `err != nil && !errors.Is(err, context.Canceled)` 因此可靠。与 loop.Run 的分工：loop.Run 管周期性 tick（单轮 panic 不打断后续轮次），Supervise 管单个长驻循环活得好不好（整轮死亡从头重启，重启即丢 loop 内部状态，有状态循环应状态外置）
- feat(retry): `DetailError` + `WithDetail`/`Detail[T]` 失败伴生数据面（#138）——DoValue「失败恒返零值」防误用契约不动，诊断/分类/审计需要的伴生数据（HTTP 状态码、响应摘要）由 fn 打包进 error：`WithDetail(err, res)` 装配、`Detail[T](err)` 提取最后一次尝试的伴生数据（类型不符 ok=false 不强转）；Error()/Unwrap() 透传原始错误，errors.Is/As 链与既有错误分类零破坏；WithDetail(nil,…) 原样返回 nil，fn 成功短路路径可无判空统一装配
- **BREAKING** fix(retry): `Config.Delay` 负值 panic（#136）——此前 `time.After(负值)` 立即触发，三连败瞬间打完、退避静默归零：失效面正是 retry 包的核心价值（「打散同时失败的重试风暴」），而同函数 MaxDelay 负值有「安全忽略」守卫、Delay 没有且无文档，同函数两种契约。守卫落在退避公式唯一实现处（computedDelay），Do/DoValue/Backoff/TotalBackoff 四个面同口径在 fn 首次调用前暴露（对齐 Lockout「配置错误宁可启动即暴露」哲学；MaxDelay 负值=「安全忽略=不限」没有这类后果，故两者守卫形态不同）；零退避是合法显式配置（设 0），不受影响
- fix(ratelimit): `NewSlidingWindow` 非法值守卫 + maxCount<=0 语义声明（#135）——此前 maxCount<=0 静默构造出**恒拒限流器**，症状（「全被拒」）与「真被打满」在调用方视角不可分辨，值班先去查流量而不会怀疑配置；且与同包 Lockout「非法值 panic」哲学相反（LockoutConfig 注释点名「锁静默失效等于没有防护」）。修法：`window<=0` 与 `maxCount<0` panic（启动即暴露）；`maxCount==0` 按**不限流**处理（恒放行、不记账，对齐同包 WithMaxEntries/LockoutConfig.MaxEntries 的 0=不限口径与宿主配置面直觉——bianque 的 webhook 源限频靠宿主侧守卫 `perMin<=0 return true` 躲坑，守卫应在库侧）；AllowN/Reserve/Wait/Count 全路径同口径
- fix(ratelimit): `KeyedLimiterOf.Get` 慢路径不再登记 factory 返回的 nil 条目（#134）——Create 明确拒 nil、Replace 明确拒 nil，唯独 Get 慢路径照单注册：幽灵条目被后续 Get 快路径持续刷新 lastSeen ⇒ IdleTTL 永不回收（慢性泄漏到进程重启），且占住 WithMaxEntries 名额把真 key 挤出容量护栏（OverCapHits 增长但症状只是「新 key 恒 false」，与限流命中不可分辨）。修后 factory 返回 nil 不注册直接返回 nil，doc 的「配额表缺 key 返回 nil ⇒ Allow 显式失败」建议才真正成立

关联问题单：concurrency#134、concurrency#135、concurrency#136、concurrency#137、concurrency#138。

## [v0.23.4] — 2026-11-03

- feat(retry): 导出退避预算 Backoff/TotalBackoff + 无界重试档 UnboundedAttempts（#128 #129）

关联问题单：concurrency#128、concurrency#129。

## [v0.23.3] — 2026-11-02

- build: 矩阵内依赖地板 `pkg` v0.31.0 → v0.31.1（维护轮全量对齐；本版不含 API 变更）

## [v0.23.2] — 2026-10-29

- build: 依赖地板对齐——pkg 抬到 v0.31.0

## [v0.23.1] — 2026-10-29

- fix(async-test): panic handler 的等待改事件驱动（#112）——去掉 1 秒轮询窗口（CI 偶发红）

TestGoSafeWithContextLabeled 在 CI 上偶发红（--- FAIL (1.00s) / safe_test.go:219: timed out waiting for condition），2026-10-29 实测把一个**只改 README** 的 PR 无端挂起（重跑即绿）。

根因不是「慢」，而是**等错了东西**：panic handler 是在 **recover 之后**才被调用的，而被测 fn 的 defer（close(done)）**先于** recover 执行 ⇒ 主协程从 <-done 醒来时，handler 很可能还没把消息写进 atomic.Value ⇒ 只能靠轮询兜底 ⇒ 而轮询的硬超时窗口**只有 1 秒** ⇒ CI runner 负载高时就偶发红。

修法（事件驱动，零窗口）：新增 waitSignal() 等一个「被触发」的 channel；handler 被调用时 close 一个 channel（sync.Once 保护）；三处同款用例一起改；删掉随之没人用的轮询 helper waitFor（否则 unused linter 判红）。

验证：go test -race -count=30 -run TestGoSafe ./async/... 连跑 30 次无一次红；golangci-lint 0 issues。

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

- feat(syncx): Gate 补非阻塞取名额 TryAcquire（#65）——满额即拒，不排队。Gate 原先是排队语义（Acquire 满额时等 ctx 取消才返回 ctx.Err()），而「重活并发上限用满就该立刻拒绝」这类闸门（图片全量解码、ffmpeg 子进程、模型推理）排队等待会把「内存被并发占满」换成「请求堆积在队列里」，是另一种 DoS 形态；现有原语都对不上——KeyedGate 按 key 隔离同样排队、gopool.Pool 的 MaxIdleWorkers 是常驻 worker 上限不是并发上限（任务队列满即回退为独立 goroutine 直跑，任务永不因并发数被拒）、ratelimit.KeyedLimiter 是速率型令牌桶不是并发闸门，消费方只能手搓裸 channel 信号量。新增 TryAcquire() (release func(), ok bool)：满额立即返回 ok=false，不等待、不占名额；ok=false 只有一个含义（名额已满，非阻塞取名额没有别的失败原因），调用方可据此回 429/503 而不是泛化成 500；release 幂等（与 Acquire 一致，重复调用只归还一次），用 atomic CAS 实现，并发调用安全；cap<=0 视为不限，恒返回 ok=true；与 Acquire 并存、命名区分（Acquire=排队、TryAcquire=即拒）

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
- `Create(key K, l Limiter) bool`：显式登记外部构造的限流器——表满先回收空闲条目（#33）
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
- 新增 `NewKeyedLimiterByFunc[K](factory func(K) Limiter, opts ...KeyedOption[K])`（#31）：
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

## [v0.15.0] — 2026-10-18

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
- 显式三态取代「窗口统计隐式推导」：错误率超阈值（SRE 公式丢弃概率 > 0，即接受预算耗尽）（#17）
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
- docs: CHANGELOG 版本节按倒序重排（#11）——合并重复的 v0.2.0、补 v0.3.1 占位节

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
- **feat(syncx)**：新增逐槽错误变体 `FanInErr`（`fn func(i int) (T, error)` 返回（#9）
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