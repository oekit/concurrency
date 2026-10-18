package breaker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// ErrCircuitOpen 是熔断打开（请求被拒绝）时的全库规范哨兵，错误消息逐字为
// "circuit breaker is open"（与 httpx.ErrCircuitOpen 一致，后者将改为别名本哨兵，
// nns-endpoint 经 httpx 别名链共用同一 sentinel）。判定一律用 errors.Is。
var ErrCircuitOpen = errors.New("circuit breaker is open")

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
}

// BreakerOption 用于定制 Breaker（函数选项模式；按本仓命名硬规则为 XxxOption，
// 禁止裸 Option）。
type BreakerOption func(*breakerConfig)

// WithBreakerWindow 定制统计窗口：size 个桶、单桶时间宽度 interval。
// 缺省 40 桶 × 250ms（10s 窗口，对齐上游）；size < 1 或 interval <= 0 时 panic。
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
// 恢复即自动愈合。缺省 1s（对齐上游）；d <= 0 时 panic。
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

// WithProbeTimeout 定制探测时限（缺省 1m）：open 态自进入/重开起冷却 d 后放行半开探测
// （探测成功关闸、失败重开重置计时）；挂起探测超 d 未回报视为失联，回收探测位放行新探测。
// d <= 0 时 panic。
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
// 典型量级差场景（冷却 5m + 在途上界 60s）可各自表达。d <= 0 时 panic。
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

// circuitBreaker 是 Breaker 的公开实现：googleBreaker 核心之上的薄包装，
// 负责 ctx 检查、失败原因收集与拒绝回调分发（对齐上游 loggedThrottle 的职责）。
type circuitBreaker struct {
	name     string
	gb       *googleBreaker
	reasons  *errorReasons
	onReject func(name, reason string)
}

// NewBreaker 返回以 name 标识的 Google SRE 自适应熔断器（显式 closed/open/half-open
// 三态），opts 定制窗口/探活间隔/探测时限/时间源/拒绝回调。
func NewBreaker(name string, opts ...BreakerOption) Breaker {
	cfg := breakerConfig{
		windowSize:     defaultBuckets,
		windowInterval: defaultWindow / defaultBuckets,
		forcePass:      defaultForcePass,
		probeTimeout:   defaultProbeTimeout,
		clock:          time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &circuitBreaker{
		name:     name,
		gb:       newGoogleBreaker(cfg),
		reasons:  newErrorReasons(),
		onReject: cfg.onReject,
	}
}

var (
	breakerMu      sync.RWMutex
	sharedBreakers = make(map[string]Breaker)
)

// GetBreaker 返回按 name 共享的 Breaker（RWMutex 懒创建，上游 breakers.go 模式）。
// 同名返回同一实例（并发安全）；opts 仅在首次创建该名字时生效，后续调用既有实例。
func GetBreaker(name string, opts ...BreakerOption) Breaker {
	breakerMu.RLock()
	b, ok := sharedBreakers[name]
	breakerMu.RUnlock()
	if ok {
		return b
	}

	breakerMu.Lock()
	defer breakerMu.Unlock()
	b, ok = sharedBreakers[name]
	if !ok {
		b = NewBreaker(name, opts...)
		sharedBreakers[name] = b
	}
	return b
}

// Name 返回熔断器标识。
func (cb *circuitBreaker) Name() string {
	return cb.name
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
