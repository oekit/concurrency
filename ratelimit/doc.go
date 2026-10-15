// Package ratelimit 提供通用限流器实现。
//
// 支持令牌桶和滑动窗口两种限流算法，以及按 key 隔离的限流器管理。
//
// 用法:
//
//	// 令牌桶限流器
//	limiter := ratelimit.NewTokenBucket(100, 200) // 100 QPS, burst 200
//	if limiter.Allow() {
//	    // 处理请求
//	}
//
//	// 按 key 隔离的限流器（如按用户 ID）
//	keyed := ratelimit.NewKeyedLimiter(func() ratelimit.Limiter {
//	    return ratelimit.NewTokenBucket(10, 20)
//	}, 10*time.Minute)
//	if keyed.Allow("user:123") {
//	    // 处理请求
//	}
package ratelimit
