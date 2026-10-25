package ratelimit

import (
	"context"
	"errors"
	"time"

	"golang.org/x/time/rate"
)

// ErrBurstTooSmall 表示请求的令牌数超过桶的 burst 上限——该请求**永远等不到**
// （不是 ctx 取消类失败，重试也无用），调用方应按参数错误处理。
var ErrBurstTooSmall = errors.New("ratelimit: requested tokens exceed burst")

// TokenBucket 是令牌桶限流器。
// 基于 golang.org/x/time/rate 实现，支持平滑限流；时间源可注入 WithTokenClock
// （缺省 time.Now），确定性测试注入假时钟即可无 sleep 驱动配额恢复与 Retry-After
// 探测（#24）。
type TokenBucket struct {
	limiter *rate.Limiter
	now     func() time.Time
}

// TokenBucketOption 用于定制 TokenBucket（函数选项模式；按本仓命名硬规则为
// XxxOption，禁止裸 Option）。
type TokenBucketOption func(*TokenBucket)

// WithTokenClock 注入时钟（缺省 time.Now；nil 视为缺省）。放行判定、预留与
// Retry-After 探测全部走该时钟——假时钟下「1 秒后应恢复配额」「Retry-After
// 精确回填」均可确定性地断言，无 sleep 无 flake。
func WithTokenClock(clock func() time.Time) TokenBucketOption {
	return func(tb *TokenBucket) {
		if clock != nil {
			tb.now = clock
		}
	}
}

// NewTokenBucket 创建令牌桶限流器。
//
// 参数:
//   - rps: 每秒允许的请求数（Requests Per Second）
//   - burst: 突发容量（令牌桶最大容量）
//
// 用法:
//
//	limiter := ratelimit.NewTokenBucket(100, 200) // 100 QPS, burst 200
func NewTokenBucket(rps rate.Limit, burst int, opts ...TokenBucketOption) *TokenBucket {
	tb := &TokenBucket{
		limiter: rate.NewLimiter(rps, burst),
		now:     time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(tb)
		}
	}
	return tb
}

// NewTokenBucketFloat 创建令牌桶限流器，rps 以 float64 传入。
//
// 与 NewTokenBucket 等价（内部转 rate.Limit），存在的理由是**消费方不必为了
// 传一个数字而 import golang.org/x/time/rate**——rps 是配置项（配置文件/YAML
// 解出来就是 float64），让它出现在公开 API 签名上会把三方包类型漏进消费方的
// import 面。
//
// 用法:
//
//	limiter := ratelimit.NewTokenBucketFloat(100.5, 200)
func NewTokenBucketFloat(rps float64, burst int, opts ...TokenBucketOption) *TokenBucket {
	return NewTokenBucket(rate.Limit(rps), burst, opts...)
}

// Allow 检查是否允许一个请求。
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN 检查是否允许 N 个请求。
func (tb *TokenBucket) AllowN(n int) bool {
	return tb.limiter.AllowN(tb.now(), n)
}

// Reserve 预留一个令牌，返回需要等待的时间。
func (tb *TokenBucket) Reserve() time.Duration {
	now := tb.now()
	r := tb.limiter.ReserveN(now, 1)
	if r.OK() {
		return r.DelayFrom(now)
	}
	return time.Duration(1<<63 - 1) // 最大 duration，表示不可用
}

// RetryAfter 探测下一个可用令牌的等待时长，不消耗令牌（Reserve→Delay→Cancel，
// 归还按同一注入时钟结算）。ok=false 表示限流器本身不可用（如 burst=0 且 rps=0）。
// 被拒请求不会自我消耗配额——429 + Retry-After 场景的标准姿势。
func (tb *TokenBucket) RetryAfter() (time.Duration, bool) {
	now := tb.now()
	r := tb.limiter.ReserveN(now, 1)
	if !r.OK() {
		return 0, false
	}
	d := r.DelayFrom(now)
	r.CancelAt(now)
	return d, true
}

// Rate 返回限流器的 RPS 配置。
func (tb *TokenBucket) Rate() rate.Limit {
	return tb.limiter.Limit()
}

// Burst 返回限流器的 burst 配置。
func (tb *TokenBucket) Burst() int {
	return tb.limiter.Burst()
}

// Tokens 返回当前剩余令牌数（可能为小数）。
//
// **读出口，不消耗配额**——供服务端限流中间件回 X-RateLimit-Remaining 这类
// 响应头。语义对齐 x/time/rate 的 Limiter.Tokens()。
//
// 按注入时钟（WithTokenClock）结算，与 Allow/Reserve 同一时间源 ⇒ 假时钟下
// 「配额恢复后剩余数变多」可确定性断言，无 sleep 无 flake。
//
// 注意：桶内有**预留未兑现**（Reserve/Wait 在途）时该值可为负——那是「已欠配额」
// 的诚实读数，与 x/time/rate 同口径。回响应头时通常取 max(0, tokens)。
func (tb *TokenBucket) Tokens() float64 {
	return tb.limiter.TokensAt(tb.now())
}

// Wait 阻塞等待直到取到 1 个令牌，或 ctx 取消。
//
// 与 Allow 的区别是**丢弃 vs 等待**：Allow 拿不到配额就立刻拒（调用方回 429），
// Wait 排队等到有配额（出站 HTTP 客户端节流、「等到有配额再发请求」这类场景）。
//
// 语义对齐 x/time/rate 的 Limiter.Wait：等到有令牌返回 nil，ctx 取消返回
// ctx.Err()。ctx 为 nil 时按 context.Background() 处理（永不取消）。
//
// ctx 取消时**归还已预留的配额**（取消不消耗令牌），与 x/time/rate 一致。
func (tb *TokenBucket) Wait(ctx context.Context) error {
	return tb.WaitN(ctx, 1)
}

// WaitN 阻塞等待直到取到 n 个令牌，或 ctx 取消。
//
// n<=0 视为无需令牌，直接返回 nil。n 超过 burst 上限时**永远等不到**，直接返回
// ErrBurstTooSmall（不是 ctx 取消类失败，重试无用）。
//
// 等待时长按注入时钟结算（WithTokenClock），但**实际睡眠走真实时间**——注入时钟
// 只决定「要等多久」，不改变计时器本身。
func (tb *TokenBucket) WaitN(ctx context.Context, n int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n <= 0 {
		return nil
	}
	// 先看 ctx：已取消时不该预留配额（预留了又要归还，白跑一趟）。
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := tb.now()
	r := tb.limiter.ReserveN(now, n)
	if !r.OK() {
		return ErrBurstTooSmall
	}
	delay := r.DelayFrom(now)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// 归还预留的配额——取消不该消耗令牌。
		r.CancelAt(tb.now())
		return ctx.Err()
	}
}
