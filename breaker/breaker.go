package breaker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCircuitOpen 是熔断打开（请求被拒绝）时的全库规范哨兵，错误消息逐字为
// "circuit breaker is open"（与 httpx.ErrCircuitOpen 一致，后者将改为别名本哨兵，
// nns-endpoint 经 httpx 别名链共用同一 sentinel）。判定一律用 errors.Is。
var ErrCircuitOpen = errors.New("circuit breaker is open")

// State 为熔断器状态（可观测出口，#25 附带建议——此前消费方只能靠 Allow 报错反推）。
type State uint8

const (
	// StateClosed 正常放行。
	StateClosed State = iota
	// StateOpen 熔断期：forcePass 节奏的强制探活 + 冷却期满的半开探测，其余拒绝。
	StateOpen
	// StateHalfOpen 半开：挂起探测等待回报（成功关闸/失败重开）。
	StateHalfOpen
)

// String 返回状态名（"closed"/"open"/"half-open"，供日志与监控打点）。
func (s State) String() string {
	switch s {
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// numHistoryReasons 为拒绝回调 reason 保留的最近失败原因条数（对齐上游 errorWindow）。
const numHistoryReasons = 5

type (
	// Promise 是 Allow 放行后返回的承诺：调用方执行真实调用后二选一回填结果——
	// Pass 告知成功、Fail 告知失败。Do 系列方法内部自动回填。
	Promise interface {
		// Pass tells the Breaker that the call is successful.
		Pass()
		// Fail tells the Breaker that the call is failed.
		Fail()
	}

	// Breaker 是熔断器接口。Allow/Do 系列全部 ctx-aware：ctx 已取消时直接返回
	// ctx.Err()，不消耗熔断统计。拒绝时返回裸 ErrCircuitOpen（可 errors.Is）；
	// execute panic 记一次失败样本后原样 re-panic。
	Breaker interface {
		// Name 返回熔断器标识。
		Name() string

		// Allow 判断请求是否放行：放行返回 Promise 供调用方回填 Pass/Fail，
		// 拒绝返回裸 ErrCircuitOpen。
		Allow(ctx context.Context) (Promise, error)

		// Do 放行时执行 execute 并按默认 acceptable（err == nil）回填成败；
		// 拒绝时返回 ErrCircuitOpen。
		Do(ctx context.Context, execute func() error) error

		// DoWithAcceptable 同 Do，acceptable 判定哪些错误仍算成功
		// （即使 err != nil，如把业务码视作可接受结果）。
		DoWithAcceptable(ctx context.Context, execute func() error, acceptable func(error) bool) error

		// DoWithFallback 同 DoWithAcceptable；仅在被熔断拒绝时改走 fallback
		// （入参为 ErrCircuitOpen），execute 自身的错误不经 fallback 原样返回；
		// fallback 为 nil 时拒绝错误原样返回。
		DoWithFallback(ctx context.Context, execute func() error, acceptable func(error) bool,
			fallback func(err error) error) error

		// State 返回当前熔断状态快照（读取瞬间值，仅供监控/日志/调试；
		// 放行判定一律以 Allow/Do 的返回结果为准）。v0.13.0 新增——
		// 外部自行实现 Breaker 接口的代码需同步补上该方法。
		State() State
	}

	// GenBreaker 是带代（generation）的熔断器扩展接口：在 Breaker 之上显式暴露
	// 放行与回报的代绑定，供「需要精确追踪一次放行的归属」的库形态消费方使用
	// （典型：多模型降级链里标记探测、丢弃陈旧探测回报）。本包 *circuitBreaker
	// 同时实现两个接口；持有 Breaker 时按可选扩展惯例类型断言：
	//
	//	if gb, ok := b.(breaker.GenBreaker); ok { ... }
	//
	// 代仅在探测（半开放行）时自增，普通放行携带当前代；陈旧代（gen < 当前代）
	// 的回报整体作废——不计窗口统计、不驱动状态迁移。
	GenBreaker interface {
		Breaker

		// AllowGen 同 Allow，额外返回本次放行的代；拒绝时返回 (nil, 0, ErrCircuitOpen)。
		AllowGen(ctx context.Context) (Promise, uint64, error)

		// SuccessGen 按代回填成功：陈旧代作废；命中挂起探测的成功驱动关闸
		// （half-open → closed + 清窗）。
		SuccessGen(gen uint64)

		// FailureGen 按代回填失败：ctx 已取消时不消耗统计（与 Allow 的 ctx 门对称）；
		// 陈旧代作废（冷却不被陈旧探测延长）；命中挂起探测的失败驱动重开并重置计时；
		// 其余失败只记窗口样本（开放期普通失败不延长冷却）。
		FailureGen(ctx context.Context, gen uint64)

		// AbandonGen 显式放弃一次放行（探针失联的确定性回收）：仅命中当前挂起探测时
		// 生效——腾出探测位（冷却已满时下一放行即刻补放新探测），不触碰冷却计时。
		AbandonGen(gen uint64)
	}
)

// breakerConfig 为 Breaker 的内部配置集合，由 BreakerOption 逐项覆写。
type breakerConfig struct {
	windowSize     int
	windowInterval time.Duration
	forcePass      time.Duration
	probeTimeout   time.Duration
	cooldown       time.Duration
	consecutive    int
	clock          func() time.Time
	onReject       func(name, reason string)
	onConflict     func(name, detail string)
}

// configFingerprint 是 breakerConfig 中**值语义**字段的指纹：用于同名共享熔断器判定
// 「本次 opts 是否与首次创建不同」（#29）。刻意排除三个函数字段（clock / onReject /
// onConflict）——函数值不可比较，且钩子属每调用方的私有接线，差异不构成"参数冲突"
// （否则先注册 reject handler 的调用方会被后续每个调用方无谓地告警一次）。
type configFingerprint struct {
	windowSize     int
	windowInterval time.Duration
	forcePass      time.Duration
	probeTimeout   time.Duration
	cooldown       time.Duration
	consecutive    int
}

// fingerprint 提取可比较的配置指纹。
func (c *breakerConfig) fingerprint() configFingerprint {
	return configFingerprint{
		windowSize:     c.windowSize,
		windowInterval: c.windowInterval,
		forcePass:      c.forcePass,
		probeTimeout:   c.probeTimeout,
		cooldown:       c.cooldown,
		consecutive:    c.consecutive,
	}
}

// String 渲染指纹为可读串，供冲突告警的 detail 直接引用。
func (f configFingerprint) String() string {
	return fmt.Sprintf("window=%d桶/%v, forcePass=%v, probeTimeout=%v, cooldown=%v, consecutive=%d",
		f.windowSize, f.windowInterval, f.forcePass, f.probeTimeout, f.cooldown, f.consecutive)
}

// newBreakerConfig 按缺省值铺底后逐项应用 opts，是 Breaker 配置的唯一构造入口
// （NewBreaker 与 GetBreaker 的冲突指纹共用同一条路径，保证指纹与实际生效配置同源）。
func newBreakerConfig(opts ...BreakerOption) breakerConfig {
	cfg := breakerConfig{
		windowSize:     DefaultBuckets,
		windowInterval: DefaultWindow / DefaultBuckets,
		forcePass:      DefaultForcePass,
		probeTimeout:   DefaultProbeTimeout,
		clock:          time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// BreakerOption 用于定制 Breaker（函数选项模式；按本仓命名硬规则为 XxxOption，
// 禁止裸 Option）。
type BreakerOption func(*breakerConfig)

// WithBreakerWindow 定制统计窗口：size 个桶、单桶时间宽度 interval。
// 缺省 DefaultBuckets 桶 × 250ms（DefaultWindow 窗口，对齐上游）；
// size < 1 或 interval <= 0 时 panic。
func WithBreakerWindow(size int, interval time.Duration) BreakerOption {
	if size < 1 {
		panic("breaker: window size must be greater than 0")
	}
	if interval <= 0 {
		panic("breaker: window interval must be greater than 0")
	}
	return func(c *breakerConfig) {
		c.windowSize = size
		c.windowInterval = interval
	}
}

// WithForcePass 定制强制探活间隔：熔断期内每隔 d 强制放行一次请求探测服务是否恢复，
// 恢复即自动愈合。缺省 DefaultForcePass（对齐上游）；d <= 0 时 panic。
// 该探活属于 open 态放行策略的一部分（探测结果只进窗口统计）；显式三态的半开探测
// 由 WithProbeTimeout 调度，两者互补。
func WithForcePass(d time.Duration) BreakerOption {
	if d <= 0 {
		panic("breaker: force pass duration must be greater than 0")
	}
	return func(c *breakerConfig) {
		c.forcePass = d
	}
}

// WithProbeTimeout 定制探测时限（缺省 DefaultProbeTimeout）：open 态自进入/重开起冷却 d 后
// 放行半开探测（探测成功关闸、失败重开重置计时）；挂起探测超 d 未回报视为失联，回收探测位
// 放行新探测。d <= 0 时 panic。
func WithProbeTimeout(d time.Duration) BreakerOption {
	if d <= 0 {
		panic("breaker: probe timeout must be greater than 0")
	}
	return func(c *breakerConfig) {
		c.probeTimeout = d
	}
}

// WithCooldown 定制冷却期：open 态自进入/重开起冷却 d 后放行半开探测。
// 冷却期（业务决策：熔断后多久才敢再试）与探测失联时限（调用侧在途时间上界，
// WithProbeTimeout）是两个独立决策，缺省不设时回落 probeTimeout——保持既有绑定行为不变，
// 典型量级差场景（冷却 5m + 在途上界 60s）可各自表达。
//
// d <= 0 时 panic（**包括 d == 0**）："不配置"只能用「不传本选项」表达，
// 不存在传 0 走缺省的写法。回落发生在配置结构内部（缺省零值），而非本选项的入参校验——
// 这是与旧 agentkit/breaker 迁移时最容易踩的语义反转之一（#28），详见 doc.go 迁移注记。
func WithCooldown(d time.Duration) BreakerOption {
	if d <= 0 {
		panic("breaker: cooldown must be greater than 0")
	}
	return func(c *breakerConfig) {
		c.cooldown = d
	}
}

// WithConsecutiveFailures 定制「连续失败 n 次即熔断」的计数型触发（缺省 0 = 关闭，
// 保持纯速率型触发不变），与速率型并联：任一命中即切入 open。成功/可接受回报清零计数，
// 进入 open / half-open 时归零，陈旧代回报不计数。速率型受 protection 保护阈值制约，
// 低 QPS 下可能永不触发；计数型补足「最近有没有连挂」这一正交信号（对齐 Envoy
// consecutive_5xx 语义）。n < 0 时 panic。
func WithConsecutiveFailures(n int) BreakerOption {
	if n < 0 {
		panic("breaker: consecutive failures must not be negative")
	}
	return func(c *breakerConfig) {
		c.consecutive = n
	}
}

// WithBreakerClock 注入自定义时间源（缺省 time.Now；nil 视为缺省）。
// 窗口推进与强制探活共用同一时钟，测试注入假时钟即可完全确定地驱动熔断状态。
func WithBreakerClock(clock func() time.Time) BreakerOption {
	return func(c *breakerConfig) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// WithBreakerRejectHandler 注册拒绝回调（缺省 no-op，本包不强制接 logx/metrics，
// 由上层显式接入）。name 为熔断器标识；reason 为最近失败原因（最多 5 条、最新在前、
// 换行拼接；无记录时回退哨兵消息）。拒绝事件在 Allow/Do 系列每次拒绝时触发。
func WithBreakerRejectHandler(handler func(name, reason string)) BreakerOption {
	return func(c *breakerConfig) {
		if handler != nil {
			c.onReject = handler
		}
	}
}

// WithBreakerConflictHandler 注册**同名共享配置冲突**回调（缺省 no-op，#29）。
//
// GetBreaker(name, opts...) 是进程级单例：opts 仅在首次创建该名字时生效。若后续又以
// 同一 name 传入**不同的** opts，本次 opts 会被静默丢弃——不报错也不告警，表现为阈值
// 与传入的不一致（"该断的时候没断 / 不该断的时候断了"），往往只在生产某时段才暴露。
// 本回调把这一静默失效变成可观测事件：
//
//	name 为熔断器标识；detail 为首次生效配置与本次传入配置的对照串。
//
// 触发条件（三者同时满足）：同名已存在、本次 opts 非空、生效配置指纹与首次不同。
// 因此 `GetBreaker(name)` 纯复用、以及传入与首次等价的 opts，都不会误报。
// 每个 name 最多触发一次（热路径去重），回调在锁外触发——回调是消费方代码，
// 可能反过来调 GetBreaker。
//
// 注意：冲突信号走独立回调而非复用 WithBreakerRejectHandler——拒绝回调的语义是
// "熔断拒绝了一次请求"，把配置冲突混进去会污染消费方按 reject 计数的看板。
// 未注册本回调时不产生任何日志或告警（与本包不强制接 logx/metrics 的定位一致），
// 冲突只表现为文档所述的"opts 被静默丢弃"。
func WithBreakerConflictHandler(handler func(name, detail string)) BreakerOption {
	return func(c *breakerConfig) {
		if handler != nil {
			c.onConflict = handler
		}
	}
}

// circuitBreaker 是 Breaker 的公开实现：googleBreaker 核心之上的薄包装，
// 负责 ctx 检查、失败原因收集与拒绝回调分发（对齐上游 loggedThrottle 的职责）。
type circuitBreaker struct {
	name     string
	gb       *googleBreaker
	reasons  *errorReasons
	onReject func(name, reason string)
}

// NewBreaker 返回以 name 标识的 Google SRE 自适应熔断器（显式 closed/open/half-open
// 三态），opts 定制窗口/探活间隔/探测时限/时间源/拒绝回调。每次调用都是独立新实例
// （无按名共享语义，故不存在 GetBreaker 的同名异参问题）。
func NewBreaker(name string, opts ...BreakerOption) Breaker {
	return newCircuitBreaker(name, newBreakerConfig(opts...))
}

// newCircuitBreaker 按已构造好的 cfg 组装公开实现。
func newCircuitBreaker(name string, cfg breakerConfig) *circuitBreaker {
	return &circuitBreaker{
		name:     name,
		gb:       newGoogleBreaker(cfg),
		reasons:  newErrorReasons(),
		onReject: cfg.onReject,
	}
}

// sharedEntry 是共享表的一项：实例本体 + 首建配置指纹 + 冲突回调（#29）。
// sig/onConflict 创建后不再变更，可无锁读取；warned 为冲突去重的 CAS 闸。
type sharedEntry struct {
	Breaker
	sig        configFingerprint
	onConflict func(name, detail string)
	warned     atomic.Bool
}

var (
	breakerMu      sync.RWMutex
	sharedBreakers = make(map[string]*sharedEntry)
)

// GetBreaker 返回按 name 共享的 Breaker（RWMutex 懒创建，上游 breakers.go 模式）。
// 同名返回同一实例（并发安全）；**opts 仅在首次创建该名字时生效**，后续调用既有实例。
//
// ⚠️ 同名 + 不同 opts = 本次 opts 被静默丢弃（#29）。典型踩法：为不同调用方各建一组
// 熔断器，名字共用业务标识（插件名 / 模型名 / 服务名）但阈值不同
// （trip=3, cooldown=2m vs trip=10, cooldown=30s），第二组会沿用第一组的参数，
// 且不报错、不告警。要真正拿到不同参数，有两条路：
//
//	// 路线 A（推荐）：把参数编进名字做命名空间隔离——名字即配置的一部分
//	brkNS := fmt.Sprintf("argus.d%d.plugin.", dispatcherSeq.Add(1))
//	gb := breaker.GetBreaker(brkNS+pluginName, opts...)
//
//	// 路线 B：显式自建，不走共享表（每次都是新实例）
//	gb := breaker.NewBreaker(brkNS+pluginName, opts...)
//
// 不想改名字又怕踩坑，可注册 WithBreakerConflictHandler 让静默失效变成可观测事件——
// 同名异参时回调一次 detail 对照串；不注册则该问题依旧静默。
func GetBreaker(name string, opts ...BreakerOption) Breaker {
	// 无 opts = 纯复用既有实例，不可能构成冲突，走无分配快路径
	if len(opts) == 0 {
		breakerMu.RLock()
		e, ok := sharedBreakers[name]
		breakerMu.RUnlock()
		if ok {
			return e.Breaker
		}
	}

	cfg := newBreakerConfig(opts...)
	fp := cfg.fingerprint()

	breakerMu.Lock()
	e, existed := sharedBreakers[name]
	if !existed {
		e = &sharedEntry{
			Breaker:    newCircuitBreaker(name, cfg),
			sig:        fp,
			onConflict: cfg.onConflict,
		}
		sharedBreakers[name] = e
	}
	breakerMu.Unlock()

	// 冲突告警在锁外触发：回调是消费方代码，可能反过来调 GetBreaker（RWMutex 不可重入）
	if existed && len(opts) > 0 && fp != e.sig {
		e.warnConflict(name, fp)
	}
	return e.Breaker
}

// warnConflict 上报同名异参冲突，每个 name 只报一次（热路径去重，避免每次
// GetBreaker 都刷一遍）；未注册回调时静默跳过。
func (e *sharedEntry) warnConflict(name string, got configFingerprint) {
	if e.onConflict == nil {
		return
	}
	if !e.warned.CompareAndSwap(false, true) {
		return
	}
	e.onConflict(name, fmt.Sprintf("同名熔断器 %q 已存在，本次传入的 opts 被静默丢弃（仅首次创建生效）；"+
		"首次生效配置 = %v，本次传入配置 = %v。请把参数编进名字做命名空间隔离，或改用 NewBreaker 显式自建。",
		name, e.sig, got))
}

// Name 返回熔断器标识。
func (cb *circuitBreaker) Name() string {
	return cb.name
}

// State 返回当前熔断状态快照（mu 内读取；监控/日志/调试用途）。
func (cb *circuitBreaker) State() State {
	cb.gb.mu.Lock()
	defer cb.gb.mu.Unlock()
	switch cb.gb.state {
	case stateOpen:
		return StateOpen
	case stateHalfOpen:
		return StateHalfOpen
	default:
		return StateClosed
	}
}

// Allow 判断请求是否放行：放行返回 Promise 供调用方回填 Pass/Fail，
// 拒绝返回裸 ErrCircuitOpen 并触发拒绝回调；ctx 已取消时返回 ctx.Err()。
func (cb *circuitBreaker) Allow(ctx context.Context) (Promise, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := cb.gb.allow()
	if err != nil {
		cb.reportReject()
	}
	return p, err
}

// AllowGen 同 Allow，额外返回本次放行的代（GenBreaker 扩展接口）；
// 拒绝时返回 (nil, 0, ErrCircuitOpen)。
func (cb *circuitBreaker) AllowGen(ctx context.Context) (Promise, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	p, gen, err := cb.gb.allowGen()
	if err != nil {
		cb.reportReject()
	}
	return p, gen, err
}

// SuccessGen 按代回填成功（GenBreaker 扩展接口）。
func (cb *circuitBreaker) SuccessGen(gen uint64) {
	cb.gb.successGen(gen)
}

// FailureGen 按代回填失败；ctx 已取消时不消耗统计（GenBreaker 扩展接口）。
func (cb *circuitBreaker) FailureGen(ctx context.Context, gen uint64) {
	if err := ctx.Err(); err != nil {
		return
	}
	cb.gb.failureGen(gen)
}

// AbandonGen 显式放弃一次放行（GenBreaker 扩展接口）。
func (cb *circuitBreaker) AbandonGen(gen uint64) {
	cb.gb.abandonGen(gen)
}

// Do 放行时执行 execute 并按默认 acceptable（err == nil）回填成败；
// 拒绝时返回 ErrCircuitOpen。
func (cb *circuitBreaker) Do(ctx context.Context, execute func() error) error {
	return cb.do(ctx, execute, nil, defaultAcceptable)
}

// DoWithAcceptable 同 Do，acceptable 判定哪些错误仍算成功。
func (cb *circuitBreaker) DoWithAcceptable(ctx context.Context, execute func() error,
	acceptable func(error) bool) error {
	return cb.do(ctx, execute, nil, acceptable)
}

// DoWithFallback 同 DoWithAcceptable；仅被熔断拒绝时改走 fallback。
func (cb *circuitBreaker) DoWithFallback(ctx context.Context, execute func() error,
	acceptable func(error) bool, fallback func(err error) error) error {
	return cb.do(ctx, execute, fallback, acceptable)
}

// do 为 Do 系列的公共实现：记录失败原因、分发拒绝回调与应用 fallback。
func (cb *circuitBreaker) do(ctx context.Context, execute func() error,
	fallback func(err error) error, acceptable func(error) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err, rejected := cb.gb.doReq(execute, func(e error) bool {
		ok := acceptable(e)
		if !ok && e != nil {
			cb.reasons.add(e.Error())
		}
		return ok
	})
	if rejected {
		// 按拒绝精确判定（而非对返回值做 errors.Is），execute 自身返回的错误
		// 即使恰好等于哨兵也不会被误判为熔断拒绝
		cb.reportReject()
		if fallback != nil {
			return fallback(err)
		}
	}
	return err
}

// reportReject 把拒绝事件分发到可选回调（缺省 no-op）。
func (cb *circuitBreaker) reportReject() {
	if cb.onReject == nil {
		return
	}
	reason := cb.reasons.String()
	if reason == "" {
		reason = ErrCircuitOpen.Error()
	}
	cb.onReject(cb.name, reason)
}

func defaultAcceptable(err error) bool {
	return err == nil
}

// errorReasons 为固定容量环形缓冲，保留最近 numHistoryReasons 条失败原因，
// 供拒绝回调给出上下文（对齐上游 errorWindow；去掉时间戳前缀——回调消费方可自行打点）。
type errorReasons struct {
	lock    sync.Mutex
	reasons [numHistoryReasons]string
	index   int
	count   int
}

func newErrorReasons() *errorReasons {
	return new(errorReasons)
}

func (ew *errorReasons) add(reason string) {
	ew.lock.Lock()
	ew.reasons[ew.index] = reason
	ew.index = (ew.index + 1) % numHistoryReasons
	ew.count = min(ew.count+1, numHistoryReasons)
	ew.lock.Unlock()
}

// String 倒序（最新在前）换行拼接全部已记原因，无记录时返回空串。
func (ew *errorReasons) String() string {
	ew.lock.Lock()
	defer ew.lock.Unlock()
	reasons := make([]string, 0, ew.count)
	for i := ew.index - 1; i >= ew.index-ew.count; i-- {
		reasons = append(reasons, ew.reasons[(i+numHistoryReasons)%numHistoryReasons])
	}
	return strings.Join(reasons, "\n")
}
