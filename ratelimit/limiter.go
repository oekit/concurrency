package ratelimit

import (
	"context"
	"time"
)

// Limiter 是通用限流器接口。
//
// v0.21.0 起启用 Wait(ctx)——此前该行是注释掉的：接口上没有阻塞等待这一档，
// 消费方要么退化成「拿不到就拒」，要么在库外自建第二份令牌桶实现（重复实现 +
// 各自漂移，上游的时钟注入、Retry-After 探测语义同步不过去）。
//
// ⚠️ 启用该方法对**自行实现 Limiter 的调用方**是破坏性变更（接口多了一个方法，
// 旧实现不再满足）——0.x 阶段 minor 允许 breaking。仓内两个实现
// （TokenBucket / SlidingWindow）均已补齐。
type Limiter interface {
	// Allow 检查是否允许一个请求。
	Allow() bool

	// AllowN 检查是否允许 N 个请求。
	AllowN(n int) bool

	// Reserve 预留一个令牌，返回需要等待的时间。
	// 如果返回 0 表示立即可用。
	Reserve() time.Duration

	// Wait 等待直到获取令牌或 ctx 取消。
	//
	// ctx 取消返回 ctx.Err()（且不消耗配额）；ctx 为 nil 时按
	// context.Background() 处理（永不取消）。
	Wait(ctx context.Context) error
}
