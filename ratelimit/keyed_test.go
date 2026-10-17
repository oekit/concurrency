package ratelimit

import (
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
