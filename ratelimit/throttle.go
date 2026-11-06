package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
)

// Throttle 是「写合并节流」原语（first-then-suppress）。
//
// # 与限流器的分工（勿混）
//
// 本包的 Limiter / KeyedLimiter 解决的是**速率配额**：单位时间内允许通过多少次，
// 被拒时调用方拿到 false，然后自己要决定「丢掉、排队还是降级」。
//
// Throttle 解决的是另一件事——**窗口内去重合并**：同一个 key 在一个窗口里**只需
// 生效一次**，后续调用被静默抑制（不排队、不报错、不占配额），窗口过后再放行一次。
// 典型场景：
//
//   - last_used_at / last_seen 类时间戳回写：每个请求都该 touch，真落库 window 一次；
//   - 审计打点、心跳刷新：调用频次是请求量级，外发频次要的是「有人在用」这个事实。
//
// 拿令牌桶硬凑这套语义会缺一笔账：Allow 返回 false 只说明「这次没通过」，没有
// 「抑制了几次」的口径，observability 上看不出「合并生效了没有」。故 Throttle 自带
// Suppressed 计数，且放行时要执行**副作用函数**（不是只返回 bool）。
//
// # 语义
//
//   - Do(ctx, key, fn)：key 在窗口内首次调用执行 fn，返回 (true, err)；
//     窗口内后续同 key 调用直接返回 (false, nil)，**不执行 fn**。
//   - 窗口以**上次放行时刻**为锚点（固定窗口，不是滚动窗口）：放行后经过 window
//     即再次放行。时钟可注入（WithThrottleClock），假时钟下可确定性断言。
//   - fn 返回错误 ⇒ **撤销本次放行的戳**，下一个调用立刻可以重试。理由：写失败
//     等于「没写」，此时把窗口记成已消费会让时间戳整整一个窗口得不到更新。
//   - fn panic ⇒ 经 async.RunSafeCtx 收编为 *PanicError 返回（调用方 errors.As 可辨），
//     与返回 error 同等对待（同样撤销戳）。
//   - window <= 0 ⇒ 不做抑制（每次调用都执行），便于「配置关掉节流」。
//   - 并发安全：fn **不持锁**执行，慢 fn 不会阻塞其它 key。
type Throttle[K comparable] struct {
	window time.Duration
	now    func() time.Time
	max    int

	mu         sync.Mutex
	seen       map[K]time.Time
	suppressed atomic.Uint64
}

// ThrottleOption 配置 Throttle。
type ThrottleOption func(*throttleConfig)

type throttleConfig struct {
	now func() time.Time
	max int
}

// WithThrottleClock 注入时钟（缺省 time.Now）。测试用假时钟可确定性断言窗口边界。
func WithThrottleClock(now func() time.Time) ThrottleOption {
	return func(c *throttleConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// WithThrottleMaxEntries 设 key 条目上限（缺省 0 = 不设上限）。
//
// 超限时先回收已过期条目；仍超限则淘汰**戳最旧**的一条（写节流的 key 通常有热度
// 局部性，最旧的戳最可能已经不再需要）。淘汰是 fail-open 的：被淘汰的 key 下一次
// 调用会重新放行，最坏结果只是多写一次，不会丢写。
func WithThrottleMaxEntries(n int) ThrottleOption {
	return func(c *throttleConfig) {
		if n > 0 {
			c.max = n
		}
	}
}

// NewThrottle 构造写合并节流器。window 是合并窗口，window <= 0 表示不抑制。
func NewThrottle[K comparable](window time.Duration, opts ...ThrottleOption) *Throttle[K] {
	cfg := throttleConfig{now: time.Now}
	for _, fn := range opts {
		if fn != nil {
			fn(&cfg)
		}
	}
	return &Throttle[K]{
		window: window,
		now:    cfg.now,
		max:    cfg.max,
		seen:   make(map[K]time.Time),
	}
}

// Do 在 key 的窗口内首次调用时执行 fn，并返回 (true, err)。
//
// 窗口内后续调用返回 (false, nil)——**这不表示失败**，是「已合并进上一次」。
// 调用方通常不需要区分之外的处置：被抑制就什么都不做即可。
//
// fn 的错误或 panic 会撤销本次放行的戳，使下一个调用立刻可重试（见 Throttle 文档）。
func (t *Throttle[K]) Do(ctx context.Context, key K, fn func(context.Context) error) (ran bool, err error) {
	if t.window <= 0 {
		// 不抑制档：不记戳、不计抑制数，直接把结局交给调用方。
		return true, runThrottled(ctx, fn)
	}

	stamp := t.now()
	t.mu.Lock()
	if last, ok := t.seen[key]; ok && stamp.Sub(last) < t.window {
		t.mu.Unlock()
		t.suppressed.Add(1)
		return false, nil
	}
	t.seen[key] = stamp
	t.evictLocked(stamp)
	t.mu.Unlock()

	if err := runThrottled(ctx, fn); err != nil {
		t.rollback(key, stamp)
		return true, err
	}
	return true, nil
}

// runThrottled 执行副作用并收编 panic——写节流的 fn 常是「打日志/回写库」这类
// 边角工作，它崩了不能打死调用方的业务路径。
func runThrottled(ctx context.Context, fn func(context.Context) error) error {
	_, err := async.RunSafeCtxRet(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// rollback 撤销本次放行的戳。
//
// 只删「还是自己那一戳」的条目：fn 耗时超过窗口时，别的调用可能已经写入更新的戳，
// 无条件删会把那份也删掉，等于让下一次调用重复执行。
func (t *Throttle[K]) rollback(key K, stamp time.Time) {
	t.mu.Lock()
	if cur, ok := t.seen[key]; ok && cur.Equal(stamp) {
		delete(t.seen, key)
	}
	t.mu.Unlock()
}

// evictLocked 维护条目上限（调用方已持锁）。stamp 是本次写入的时刻。
func (t *Throttle[K]) evictLocked(stamp time.Time) {
	if t.max <= 0 || len(t.seen) <= t.max {
		return
	}
	// 先回收已过期条目：它们的下一次调用本来就会重新放行，删掉零代价。
	for k, v := range t.seen {
		if stamp.Sub(v) >= t.window {
			delete(t.seen, k)
		}
	}
	// 仍超限则淘汰戳最旧的一条（fail-open：被淘汰者下次重新放行）。
	for len(t.seen) > t.max {
		var oldestKey K
		var oldestAt time.Time
		first := true
		for k, v := range t.seen {
			if first || v.Before(oldestAt) {
				oldestKey, oldestAt, first = k, v, false
			}
		}
		if first {
			return
		}
		delete(t.seen, oldestKey)
	}
}

// Suppressed 返回累计被抑制的调用次数——这是「合并生效了没有」的观测口，
// 令牌桶的 Allow=false 给不出这一笔。
func (t *Throttle[K]) Suppressed() uint64 { return t.suppressed.Load() }

// Forget 清掉某个 key 的戳，使它的下一次调用立即放行（如账号登录成功后重置）。
func (t *Throttle[K]) Forget(key K) {
	t.mu.Lock()
	delete(t.seen, key)
	t.mu.Unlock()
}

// Sweep 回收已过期条目并返回回收条数。归零口径与 Do 的窗口判定一致
// （stamp.Sub(last) >= window 即过期）。
//
// 不内置后台清扫 goroutine：写节流的 key 基数通常是「活跃实体数」量级，
// 且 WithThrottleMaxEntries 已在写入路径上兜住容量。需要主动归零时调本方法。
func (t *Throttle[K]) Sweep() int {
	stamp := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for k, v := range t.seen {
		if stamp.Sub(v) >= t.window {
			delete(t.seen, k)
			n++
		}
	}
	return n
}

// Len 返回当前记账的 key 条数（含窗口内仍有效的）。
func (t *Throttle[K]) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}
