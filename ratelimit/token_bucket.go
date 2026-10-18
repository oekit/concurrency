package ratelimit

import (
	"time"

	"golang.org/x/time/rate"
)

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
