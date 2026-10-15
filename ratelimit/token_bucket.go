package ratelimit

import (
	"time"

	"golang.org/x/time/rate"
)

// TokenBucket 是令牌桶限流器。
// 基于 golang.org/x/time/rate 实现，支持平滑限流。
type TokenBucket struct {
	limiter *rate.Limiter
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
func NewTokenBucket(rps rate.Limit, burst int) *TokenBucket {
	return &TokenBucket{
		limiter: rate.NewLimiter(rps, burst),
	}
}

// Allow 检查是否允许一个请求。
func (tb *TokenBucket) Allow() bool {
	return tb.limiter.Allow()
}

// AllowN 检查是否允许 N 个请求。
func (tb *TokenBucket) AllowN(n int) bool {
	return tb.limiter.AllowN(time.Now(), n)
}

// Reserve 预留一个令牌，返回需要等待的时间。
func (tb *TokenBucket) Reserve() time.Duration {
	r := tb.limiter.Reserve()
	if r.OK() {
		return r.Delay()
	}
	return time.Duration(1<<63 - 1) // 最大 duration，表示不可用
}

// Rate 返回限流器的 RPS 配置。
func (tb *TokenBucket) Rate() rate.Limit {
	return tb.limiter.Limit()
}

// Burst 返回限流器的 burst 配置。
func (tb *TokenBucket) Burst() int {
	return tb.limiter.Burst()
}
