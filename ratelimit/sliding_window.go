package ratelimit

import (
	"context"
	"sync"
	"time"
)

// SlidingWindow 是滑动窗口限流器。
// 在指定时间窗口内限制请求次数。
type SlidingWindow struct {
	mu         sync.Mutex
	window     time.Duration
	maxCount   int
	timestamps []time.Time
}

// NewSlidingWindow 创建滑动窗口限流器。
//
// 参数:
//   - window: 时间窗口大小
//   - maxCount: 窗口内最大请求数
//
// 用法:
//
//	limiter := ratelimit.NewSlidingWindow(time.Minute, 100) // 每分钟最多 100 次
func NewSlidingWindow(window time.Duration, maxCount int) *SlidingWindow {
	return &SlidingWindow{
		window:   window,
		maxCount: maxCount,
	}
}

// Allow 检查是否允许一个请求。
func (sw *SlidingWindow) Allow() bool {
	return sw.AllowN(1)
}

// AllowN 检查是否允许 N 个请求。n<=0 视为 0 个请求，直接放行（不占槽位）。
func (sw *SlidingWindow) AllowN(n int) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if n <= 0 {
		return true
	}

	now := time.Now()
	windowStart := now.Add(-sw.window)

	// 清理过期的时间戳
	sw.cleanOld(windowStart)

	// 检查是否超过限制
	if len(sw.timestamps)+n > sw.maxCount {
		return false
	}

	// 记录请求时间戳
	for i := 0; i < n; i++ {
		sw.timestamps = append(sw.timestamps, now)
	}

	return true
}

// Reserve 预留一个请求，返回需要等待的时间。
// 返回 0 表示立即放行（槽位已计入窗口）；返回 d>0 表示需等待 d 后放行，
// 槽位已按 now+d 预占——与 x/time/rate 的 Reserve 语义一致：预留即消费，
// 并发调用者拿到的是互不相同的槽位而非同一个"虚拟许可"。
func (sw *SlidingWindow) Reserve() time.Duration {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-sw.window)

	sw.cleanOld(windowStart)

	if len(sw.timestamps) < sw.maxCount {
		sw.timestamps = append(sw.timestamps, now)
		return 0
	}

	// 需要等到最早的请求过期；预占到期时刻的槽位
	wait := sw.window
	if len(sw.timestamps) > 0 {
		wait = sw.timestamps[0].Add(sw.window).Sub(now)
		if wait < 0 {
			wait = 0
		}
	}
	sw.timestamps = append(sw.timestamps, now.Add(wait))
	return wait
}

// cleanOld 清理过期的时间戳（调用者需持有锁）。
func (sw *SlidingWindow) cleanOld(windowStart time.Time) {
	valid := 0
	for _, ts := range sw.timestamps {
		if ts.After(windowStart) {
			sw.timestamps[valid] = ts
			valid++
		}
	}
	sw.timestamps = sw.timestamps[:valid]
}

// Count 返回当前窗口内的请求数。
func (sw *SlidingWindow) Count() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-sw.window)
	sw.cleanOld(windowStart)

	return len(sw.timestamps)
}

// Reset 重置限流器。
func (sw *SlidingWindow) Reset() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.timestamps = nil
}

// Wait 阻塞等待直到窗口内腾出空位（或 ctx 取消）。
//
// 与 Allow 的区别是**丢弃 vs 等待**：窗口满时 Allow 立刻拒，Wait 等到最早的
// 记录滑出窗口（腾出空位）再放行——「等到有配额再发请求」这类场景。
//
// 语义与 TokenBucket.Wait 对齐：ctx 取消返回 ctx.Err()；ctx 为 nil 时按
// context.Background() 处理（永不取消）。
//
// 等待期间**不预占**槽位：拿到空位的那一刻才计数 ⇒ ctx 取消不会留下
// 「已占位未放行」的幽灵计数（与 Reserve 的「预留即消费」口径不同，这里刻意
// 不预占——等待者还没拿到配额，不该占着坑）。
func (sw *SlidingWindow) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		sw.mu.Lock()
		now := time.Now()
		sw.cleanOld(now.Add(-sw.window))
		if len(sw.timestamps) < sw.maxCount {
			sw.timestamps = append(sw.timestamps, now)
			sw.mu.Unlock()
			return nil
		}
		// 最早的一条滑出窗口的时刻 = 下一个空位到来的时刻。
		wait := sw.timestamps[0].Add(sw.window).Sub(now)
		sw.mu.Unlock()

		if wait <= 0 {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}
