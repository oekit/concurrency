package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedLimiter_GenericUintKey(t *testing.T) {
	kl := NewKeyedLimiterOf[uint](func() Limiter {
		return NewTokenBucket(2, 2)
	}, WithIdleTTL[uint](time.Minute))
	defer kl.Close()

	if !kl.Allow(42) {
		t.Fatal("channel 42 first should pass")
	}
	if !kl.Allow(42) {
		t.Fatal("channel 42 second should pass (burst 2)")
	}
	if kl.Allow(42) {
		t.Fatal("channel 42 third should reject")
	}
	// 其他 key 独立
	if !kl.Allow(7) {
		t.Fatal("channel 7 independent bucket should pass")
	}
}

func TestKeyedLimiter_MaxEntriesGuards(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(10, 10)
	},
		WithMaxEntries[string](2),
		WithIdleTTL[string](time.Hour), // 防止清理抢先
		WithOverCapRetryAfter[string](60*time.Second),
	)
	defer kl.Close()

	if !kl.Allow("a") || !kl.Allow("b") {
		t.Fatal("registered keys should pass")
	}
	if kl.Allow("c") {
		t.Fatal("unregistered key at capacity should be rejected")
	}
	if kl.Len() != 2 {
		t.Fatalf("len=%d want 2", kl.Len())
	}
	if kl.OverCapHits() == 0 {
		t.Fatal("over-cap hits should be counted")
	}
	// 已登记 key 不受影响
	if !kl.Allow("a") {
		t.Fatal("registered key must keep working at capacity")
	}
	// 拒绝时 Retry-After 返回建议值
	wait, ok := kl.AllowWithRetryAfter("c")
	if ok {
		t.Fatal("c should still be rejected")
	}
	if wait != 60*time.Second {
		t.Fatalf("wait=%v want 60s", wait)
	}
}

func TestKeyedLimiter_MaxEntriesReclaimsIdle(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(10, 10)
	},
		WithMaxEntries[string](2),
		WithIdleTTL[string](20*time.Millisecond),
	)
	defer kl.Close()

	kl.Allow("old1")
	kl.Allow("old2")
	time.Sleep(50 * time.Millisecond) // 超过 idle TTL

	// 表满时应先回收 idle，再放行新 key
	if !kl.Allow("fresh") {
		t.Fatal("should reclaim idle entries and allow fresh key")
	}
}

func TestKeyedLimiter_AllowWithRetryAfter_NoConsume(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(1, 1) // 1 rps, burst 1
	})
	defer kl.Close()

	if wait, ok := kl.AllowWithRetryAfter("k"); !ok || wait != 0 {
		t.Fatalf("first should allow immediately, got ok=%v wait=%v", ok, wait)
	}

	// 第二次应拒绝，且探测到正等待
	wait, ok := kl.AllowWithRetryAfter("k")
	if ok {
		t.Fatal("burst exhausted should reject")
	}
	if wait <= 0 {
		t.Fatalf("probe should return positive wait, got %v", wait)
	}

	// 探测不消耗：再探测仍应拒绝（如果消耗了令牌，探测后可能放行）
	if _, ok := kl.AllowWithRetryAfter("k"); ok {
		t.Fatal("probe must not consume tokens")
	}
}

func TestKeyedLimiter_ReplaceHotReload(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(1, 1)
	})
	defer kl.Close()

	// 消耗默认桶
	kl.Allow("chan")
	if kl.Allow("chan") {
		t.Fatal("default bucket exhausted")
	}

	// 热更换成高配额桶
	kl.Replace("chan", NewTokenBucket(100, 100))
	for i := range 5 {
		if !kl.Allow("chan") {
			t.Fatalf("replaced bucket should allow, i=%d", i)
		}
	}
}

func TestKeyedLimiterOf_CloseIdempotent(t *testing.T) {
	kl := NewKeyedLimiterOf[int](func() Limiter { return NewTokenBucket(1, 1) })
	kl.Close()
	kl.Close() // 必须安全
}

func TestTokenBucket_RetryAfter(t *testing.T) {
	tb := NewTokenBucket(1, 1)
	if d, ok := tb.RetryAfter(); !ok || d != 0 {
		t.Fatalf("fresh bucket should have 0 wait, got %v/%v", d, ok)
	}
	tb.Allow() // 消耗
	d, ok := tb.RetryAfter()
	if !ok {
		t.Fatal("RetryAfter should report ok")
	}
	if d <= 0 {
		t.Fatalf("exhausted should have positive wait, got %v", d)
	}
	// 不消耗
	if tb.Allow() {
		t.Fatal("RetryAfter must not consume")
	}
}

// TestKeyedLimiter_Create_GuardedAndAtomic 钉住 #33：Create 护栏内原子登记——
// 表满先回收空闲（与 Allow 同一 sweep-on-pressure）、仍满拒绝并计 OverCapHits、
// 已存在 key 不覆盖、nil limiter 拒绝、Replace 热更不受护栏影响。
func TestKeyedLimiter_Create_GuardedAndAtomic(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(10, 10)
	},
		WithMaxEntries[string](2),
		WithIdleTTL[string](20*time.Millisecond),
	)
	defer kl.Close()

	if !kl.Create("a", NewTokenBucket(5, 5)) {
		t.Fatal("create a should succeed")
	}
	// 已存在 key：返回 false 且不覆盖（配额仍是 5/s 的桶）
	if kl.Create("a", NewTokenBucket(100, 100)) {
		t.Fatal("create existing key should return false")
	}
	if kl.Get("a").(*TokenBucket).AllowN(6) {
		t.Fatal("existing limiter should not be overwritten (5/s bucket)")
	}
	// nil limiter
	if kl.Create("nil-key", nil) {
		t.Fatal("nil limiter should be rejected")
	}

	// 填满：Create b 成功（len=1 < max=2）；a、b 空闲后 Create c 触发 sweep 回收、成功
	if !kl.Create("b", NewTokenBucket(5, 5)) {
		t.Fatal("create b should succeed")
	}
	time.Sleep(50 * time.Millisecond) // a、b 均超 idle TTL
	if !kl.Create("c", NewTokenBucket(5, 5)) {
		t.Fatal("create c should reclaim idle entries and succeed")
	}
	// 立即 Create d：len=1 < max=2，成功 → 表满（c、d 均 fresh）
	if !kl.Create("d", NewTokenBucket(5, 5)) {
		t.Fatal("create d should succeed (below max)")
	}

	// 表满且无空闲：拒绝并计 OverCapHits
	if kl.Create("e", NewTokenBucket(5, 5)) {
		t.Fatal("create e should fail when table is full with fresh entries")
	}
	time.Sleep(50 * time.Millisecond) // c、d 均老化
	_ = kl.Allow("c")                 // 刷新 c 的 lastSeen（fresh），d 保持空闲
	if !kl.Create("f", NewTokenBucket(5, 5)) {
		t.Fatal("create f should reclaim idle d and succeed")
	}

	// Replace 热更不受护栏影响（直插覆盖已登记 key）
	kl.Replace("f", NewTokenBucket(100, 100))
	if kl.Len() != 2 {
		t.Fatalf("len should stay 2 after Replace, got %d", kl.Len())
	}
	if kl.OverCapHits() < 1 {
		t.Fatalf("guard rejection should count OverCapHits, got %d", kl.OverCapHits())
	}
}

// TestKeyedLimiter_Create_Concurrent 原子性：并发 Create 同一 key 只成功一次，
// 且不会超出护栏。
func TestKeyedLimiter_Create_Concurrent(t *testing.T) {
	kl := NewKeyedLimiterOf[int](func() Limiter {
		return NewTokenBucket(10, 10)
	}, WithMaxEntries[int](8))
	defer kl.Close()

	var success atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if kl.Create(0, NewTokenBucket(1, 1)) {
				success.Add(1)
			}
		})
	}
	wg.Wait()
	if success.Load() != 1 {
		t.Fatalf("concurrent Create on same key should succeed exactly once, got %d", success.Load())
	}
}

// TestKeyedLimiter_ReplaceRespectsMaxEntries 锁定 #252：四个登记入口的容量护栏
// 必须一视同仁——Replace 对**未登记 key**（文档明说「key 不存在时等价登记新桶」，
// 速率热更/配置下发路径正是这么用的）走与 Create 同款 allowNew：表满先回收空闲、
// 仍满拒绝并计 OverCapHits，表不再无限涨过 WithMaxEntries；已登记 key 换桶不受
// 护栏影响（契约「已登记 key 不受影响」，热更不被卡）。修复前 Replace 是唯一
// 不查护栏的登记入口——新 key 直写 map，护栏与 OverCapHits/Evictions 观测面
// 双双静默失效。
func TestKeyedLimiter_ReplaceRespectsMaxEntries(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(10, 1)
	}, WithMaxEntries[string](2), WithIdleTTL[string](time.Hour))
	defer kl.Close()

	if !kl.Create("k1", NewTokenBucket(10, 1)) {
		t.Fatal("护栏内登记 k1 应成功")
	}

	// 护栏内还有空位：新 key 走 Replace 照常登记（正常热更路径不受影响）
	if !kl.Replace("k2", NewTokenBucket(10, 1)) {
		t.Fatal("护栏内有空位时 Replace 新 key 应登记成功")
	}
	if got := kl.Len(); got != 2 {
		t.Fatalf("登记 k2 后 Len=%d, want 2", got)
	}

	// 新 key 走 Replace：表满（len=2=maxEntries）且无空闲条目，护栏必须拒绝
	if kl.Replace("k3", NewTokenBucket(10, 1)) {
		t.Fatal("表满时 Replace 新 key 应被护栏拒绝")
	}
	if got := kl.Len(); got != 2 {
		t.Fatalf("护栏拒绝后 Len=%d, want 2（表不无限涨过 WithMaxEntries）", got)
	}
	if kl.OverCapHits() == 0 {
		t.Fatal("护栏拒绝应计一次 OverCapHits（可观测面不留盲区）")
	}
	if kl.Get("k3") != nil {
		t.Fatal("被护栏拒绝的 key 不应有桶")
	}

	// 已登记 key 换桶：不受护栏影响（速率热更不被卡，契约「已登记 key 不受影响」）
	if !kl.Replace("k1", NewTokenBucket(100, 100)) {
		t.Fatal("已登记 key 的 Replace（换桶）应成功")
	}
	if got := kl.Len(); got != 2 {
		t.Fatalf("换桶后 Len=%d, want 2", got)
	}
	tb, ok := kl.Get("k1").(*TokenBucket)
	if !ok || tb.Burst() != 100 {
		t.Fatalf("k1 应已换成 burst=100 的新桶，got %T", kl.Get("k1"))
	}
}

// TestKeyedLimiter_AllowWithRetryAfter_SlidingWindowProbe 锁定 #246：文档承诺
// 「wait 是下一个可用令牌的等待时长（不消耗令牌）」必须对最常见的 SlidingWindow
// factory 形态成立——修复前滑窗没有 RetryAfter 方法，peekRetryAfter 恒回 0，
// 429 + Retry-After: 0 直接引发重试风暴；「Reserve+Cancel 探测」分支是死代码
// （*TokenBucket 本就实现 retryProber，永远走不到）。
func TestKeyedLimiter_AllowWithRetryAfter_SlidingWindowProbe(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewSlidingWindow(time.Hour, 2)
	})
	defer kl.Close()

	// 打满窗口
	if !kl.Allow("k") {
		t.Fatal("第一笔应放行")
	}
	if !kl.Allow("k") {
		t.Fatal("第二笔应放行")
	}

	wait, ok := kl.AllowWithRetryAfter("k")
	if ok {
		t.Fatal("窗口满应拒绝")
	}
	if wait <= 0 {
		t.Fatalf("滑窗拒绝的探测应返回真实等待（>0，下一个空位 = 最早一条滑出窗口），got %v", wait)
	}

	// 探测不消耗：再探仍应拒绝、账面不增（若探测消耗了配额，Count 会涨到 3
	// 或窗口出现新槽位——两次真实时钟差仅 µs 级，等待时长比对不构成证据）
	if _, ok := kl.AllowWithRetryAfter("k"); ok {
		t.Fatal("探测不得消耗配额（第二次仍应拒绝）")
	}
	if n := kl.Allow("k"); n {
		t.Fatal("探测不得消耗配额（第三次 Allow 仍应被拒）")
	}
	sw := kl.Get("k").(*SlidingWindow)
	if n := sw.Count(); n != 2 {
		t.Fatalf("两次探测后 Count=%d, want 2（探测不落账）", n)
	}
}

// TestKeyedLimiter_AllowWithRetryAfter_NonProbingFallback 锁定 #246 的兜底面：
// 自定义实现未提供 RetryAfter（不可探测）时，AllowWithRetryAfter 回退
// WithOverCapRetryAfter 的保守建议而非 0——「不支持探测的实现返回 0」的旧口径
// 会把 429 变成 Retry-After: 0 重试风暴。这里用一个包内自定义 Limiter 模拟。
type nonProbingLimiter struct{ allow bool }

func (l *nonProbingLimiter) Allow() bool                     { return l.allow }
func (l *nonProbingLimiter) AllowN(n int) bool               { return l.allow }
func (l *nonProbingLimiter) Reserve() (time.Duration, error) { return -1, ErrNotReservable }
func (l *nonProbingLimiter) Wait(ctx context.Context) error  { return nil }

func TestKeyedLimiter_AllowWithRetryAfter_NonProbingFallback(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return &nonProbingLimiter{allow: false}
	}, WithOverCapRetryAfter[string](7*time.Second))
	defer kl.Close()

	wait, ok := kl.AllowWithRetryAfter("k")
	if ok {
		t.Fatal("恒拒实现应拒绝")
	}
	if wait != 7*time.Second {
		t.Fatalf("不可探测实现应回退保守建议（WithOverCapRetryAfter=7s），got %v", wait)
	}
}
