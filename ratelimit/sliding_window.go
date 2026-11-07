package ratelimit

import (
	"context"
	"fmt"
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
//   - window: 时间窗口大小，<=0 直接 panic（对齐 Lockout 的「配置错误宁可启动
//     即暴露」——窗口为 0 的滑窗没有语义）
//   - maxCount: 窗口内最大请求数；**<=0 视为不限流**（恒放行、不记账，对齐
//     同包 WithMaxEntries / LockoutConfig.MaxEntries 的 0=不限口径）——配置面
//     「0=不限」是多数宿主的直觉（#135）。需要「窗口内 0 配额」的恒拒语义
//     不该构造限流器，直接在业务侧拒绝即可。负值 panic：负配额没有任何可辩护
//     语义，「漏个负号」必须启动即暴露，而不是静默变成恒拒限流器（那与「真被
//     打满」在调用方视角不可分辨，值班先去查流量而不会怀疑配置）。
//
// 用法:
//
//	limiter := ratelimit.NewSlidingWindow(time.Minute, 100) // 每分钟最多 100 次
func NewSlidingWindow(window time.Duration, maxCount int) *SlidingWindow {
	if window <= 0 {
		panic("ratelimit: sliding window duration must be greater than 0")
	}
	if maxCount < 0 {
		panic(fmt.Sprintf("ratelimit: sliding window maxCount must not be negative, got %d (0 = unlimited)", maxCount))
	}
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
	if sw.maxCount <= 0 {
		return true // 不限流档（#135）：恒放行、不记账
	}
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
	if sw.maxCount <= 0 {
		return 0 // 不限流档（#135）：立即放行、不记账
	}
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
//
// 该设计的代价是**惊群**（#186，有意取舍）：窗口满、多个 goroutine 同时 Wait
// 时，最早的记录滑出窗口的瞬间所有等待者同时醒来抢同一个空位——只有一个成功，
// 其余回到等待。最终会收敛，但空唤醒次数随等待者数量线性放大。
// 高并发下需要**严格排队**（并发等待者拿到互不相同的槽位、无空唤醒）的场景
// 建议改用 Reserve：它按 now+wait 预占到期槽位，并发调用者天然排队；代价是
// 调用方自行等待期间若放弃（ctx 取消），已预占的槽位会白占到滑出窗口。
func (sw *SlidingWindow) Wait(ctx context.Context) error {
	if sw.maxCount <= 0 {
		return nil // 不限流档（#135）：立即放行、不记账
	}
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
