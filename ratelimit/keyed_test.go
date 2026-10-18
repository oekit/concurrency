package ratelimit

import (
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
	for i := 0; i < 5; i++ {
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
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if kl.Create(0, NewTokenBucket(1, 1)) {
				success.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if success.Load() != 1 {
		t.Fatalf("concurrent Create on same key should succeed exactly once, got %d", success.Load())
	}
}
