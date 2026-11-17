package ratelimit

import (
	"testing"
	"time"
)

// concurrency#361：KeyedLimiterOf 时钟注入——lastSeen 刷新、空闲过期判定
// （后台清扫 + 表满 sweep-on-pressure 两条路径）全走 WithClock 注入时钟，
// 与 LockoutConfig.Now / WithSlidingWindowClock / WithTokenClock 同族口径
// （#341 三兄弟之外的补齐）：假时钟推进 → 空闲条目被回收，无 sleep。

// TestKeyedLimiterClock_SweepOnPressure 表满 sweep-on-pressure 路径的确定性
// 空闲回收：Clock 推进越过 IdleTTL 后，新 key 的 Get 触发空闲回收腾位——
// 回收观测面（EvictCallback / Evictions）同走注入时钟，整条路径零 sleep。
func TestKeyedLimiterClock_SweepOnPressure(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	var evictedKeys []string
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewSlidingWindow(time.Minute, 10) },
		WithIdleTTL[string](time.Minute),
		WithMaxEntries[string](1),
		WithClock[string](clock.Now),
		WithEvictCallback[string](func(key string, idle time.Duration) {
			evictedKeys = append(evictedKeys, key)
		}))
	defer kl.Close()

	if kl.Get("k1") == nil {
		t.Fatal("k1 首次登记应成功")
	}
	if kl.Get("k1") == nil {
		t.Fatal("k1 快路径应命中（lastSeen 刷新走注入时钟）")
	}
	if kl.Len() != 1 {
		t.Fatalf("Len()=%d, want 1", kl.Len())
	}

	// 推进假时钟越过 IdleTTL：k1 空闲过期；表满护栏回收它给 k2 腾位
	clock.Advance(2 * time.Minute)
	if kl.Get("k2") == nil {
		t.Fatal("k2 应在 sweep-on-pressure 回收 k1 后登记成功")
	}
	if kl.Len() != 1 {
		t.Fatalf("回收后 Len()=%d, want 1", kl.Len())
	}
	if len(evictedKeys) != 1 || evictedKeys[0] != "k1" {
		t.Fatalf("evicted keys = %v, want [k1]", evictedKeys)
	}
	if n := kl.Evictions(); n != 1 {
		t.Fatalf("Evictions()=%d, want 1", n)
	}
	if hits := kl.OverCapHits(); hits != 0 {
		t.Fatalf("sweep 腾位成功不应计 OverCapHits, got %d", hits)
	}
}

// TestKeyedLimiterClock_BackgroundCleanup 后台清扫路径的确定性空闲回收：
// 判定走注入时钟（Clock 推进即过期），清扫节奏仍走真实 ticker（cleanupInterval
// 只定节奏）⇒ 轮询等一轮真实 tick，过期判定本身是假时钟确定的。
func TestKeyedLimiterClock_BackgroundCleanup(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewSlidingWindow(time.Minute, 10) },
		// ttl=2ms ⇒ 清扫节奏 ttl/2=1ms（真实 ticker 够快，测试不等长间隔）
		WithIdleTTL[string](2*time.Millisecond),
		WithClock[string](clock.Now))
	defer kl.Close()

	if kl.Get("k1") == nil {
		t.Fatal("k1 登记应成功")
	}
	clock.Advance(time.Second) // 假时钟越过 IdleTTL：条目已空闲过期
	waitFor(t, 3*time.Second, func() bool { return kl.Len() == 0 })
	if n := kl.Evictions(); n != 1 {
		t.Fatalf("Evictions()=%d, want 1", n)
	}
}

// TestKeyedLimiterClock_NilFallsBackToTimeNow nil 时钟回退缺省 time.Now
// （LockoutConfig.Now 同口径）；不带时钟选项的既有构造形态不变。
func TestKeyedLimiterClock_NilFallsBackToTimeNow(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewTokenBucket(1, 1) },
		WithClock[string](nil))
	if kl.now == nil {
		t.Fatal("nil clock 应回退 time.Now")
	}
	if !kl.Allow("k") {
		t.Fatal("缺省时钟构造应可用")
	}
	kl2 := NewKeyedLimiter(func() Limiter { return NewTokenBucket(1, 1) }, time.Minute)
	defer kl2.Close()
	if !kl2.Allow("k") {
		t.Fatal("不带时钟选项的既有构造形态应可用")
	}
}
