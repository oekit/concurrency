package ratelimit

import "time"

// Limiter 是通用限流器接口。
type Limiter interface {
	// Allow 检查是否允许一个请求。
	Allow() bool

	// AllowN 检查是否允许 N 个请求。
	AllowN(n int) bool

	// Reserve 预留一个令牌，返回需要等待的时间。
	// 如果返回 0 表示立即可用。
	Reserve() time.Duration

	// Wait 等待直到获取令牌或 ctx 取消。
	// Wait(ctx context.Context) error
}
