package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestKeyedLimiter_ReplaceIfChanged 元数据比对热更（#141）：同速率不换桶（余量
// 保留），变速率换桶；未登记元数据的既有条目（Create 路径）视为变更。
func TestKeyedLimiter_ReplaceIfChanged(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(1, 1)
	})
	defer kl.Close()

	// 新 key：登记并返回 true
	if !kl.ReplaceIfChanged("k", 100, NewSlidingWindow(time.Second, 100)) {
		t.Fatal("new key should register")
	}
	// 同速率：不换桶返回 false，且余量保留——用 SlidingWindow 的确定性计数做
	// 「耗尽后不换桶」判据（TokenBucket 的持续回填会把耗尽变成竞态）
	if !kl.ReplaceIfChanged("e", 100, NewSlidingWindow(time.Second, 1)) {
		t.Fatal("new key should register")
	}
	if !kl.Allow("e") {
		t.Fatal("fresh bucket should allow")
	}
	if kl.Allow("e") {
		t.Fatal("bucket should be exhausted")
	}
	if kl.ReplaceIfChanged("e", 100, NewSlidingWindow(time.Second, 1)) {
		t.Fatal("same rate must not replace bucket (余量应保留)")
	}
	if kl.Allow("e") {
		t.Fatal("exhausted bucket must survive a no-op ReplaceIfChanged")
	}
	// 变速率：换桶返回 true，新配额生效
	if !kl.ReplaceIfChanged("k", 200, NewSlidingWindow(time.Second, 200)) {
		t.Fatal("changed rate should replace")
	}
	for i := range 5 {
		if !kl.Allow("k") {
			t.Fatalf("new bucket should allow, i=%d", i)
		}
	}
	// 元数据随换桶更新
	if r, ok := kl.CurrentRate("k"); !ok || r != 200 {
		t.Fatalf("CurrentRate=%d,%v want 200,true", r, ok)
	}

	// Create 登记的条目无元数据：ReplaceIfChanged 视为变更
	if !kl.Create("c", NewTokenBucket(1, 1)) {
		t.Fatal("Create should succeed")
	}
	if !kl.ReplaceIfChanged("c", 100, NewTokenBucket(100, 100)) {
		t.Fatal("Create-registered entry (no metadata) should be treated as changed")
	}

	// 拒 nil；rate<=0 panic
	if kl.ReplaceIfChanged("n", 100, nil) {
		t.Fatal("nil limiter must be rejected")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("rate<=0 must panic")
			}
		}()
		kl.ReplaceIfChanged("n", 0, NewTokenBucket(1, 1))
	}()
}

// TestKeyedLimiter_AllowWithRate 热路径零重建 + 变更时惰性重建（#141）。
func TestKeyedLimiter_AllowWithRate(t *testing.T) {
	var builds atomic.Int32
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(1, 1)
	})
	defer kl.Close()

	bucketOf := func(perMin int64) Limiter {
		builds.Add(1)
		return NewTokenBucketFloat(float64(perMin), int(perMin))
	}

	// 同速率连打：只建一次桶
	for range 10 {
		if !kl.AllowWithRate("hot", 100, func() Limiter { return bucketOf(100) }) {
			// 桶配额 1，第二次起被限流是正常语义——关键是不重建
			break
		}
	}
	if builds.Load() != 1 {
		t.Fatalf("builds=%d want 1（同速率热路径不应重建桶）", builds.Load())
	}
	// 变速率：恰好再建一次
	if !kl.AllowWithRate("hot", 200, func() Limiter { return bucketOf(200) }) {
		t.Fatal("new rate bucket should allow")
	}
	if builds.Load() != 2 {
		t.Fatalf("builds=%d want 2（速率变更应恰好重建一次）", builds.Load())
	}
	// newBucket 返回 nil：不登记不崩溃
	if kl.AllowWithRate("ghost", 100, func() Limiter { return nil }) {
		t.Fatal("nil bucket must be rejected")
	}
	if kl.Len() != 1 {
		t.Fatalf("nil bucket must not register, len=%d", kl.Len())
	}
	// 非法参数 panic
	for _, tc := range []struct {
		name string
		rate int64
		nb   func() Limiter
	}{
		{"rate0", 0, func() Limiter { return NewTokenBucket(1, 1) }},
		{"rateNeg", -1, func() Limiter { return NewTokenBucket(1, 1) }},
		{"nilBucketFactory", 100, nil},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s must panic", tc.name)
				}
			}()
			kl.AllowWithRate("x", tc.rate, tc.nb)
		}()
	}
}

// TestKeyedLimiter_AllowWithRate_Guardrail 新 key 受容量护栏约束（已登记 key 的
// 速率热更不受护栏卡——对齐 Replace 的契约）。
func TestKeyedLimiter_AllowWithRate_Guardrail(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(10, 10)
	},
		WithMaxEntries[string](1),
		WithIdleTTL[string](time.Hour),
	)
	defer kl.Close()

	if !kl.AllowWithRate("a", 10, func() Limiter { return NewTokenBucket(10, 10) }) {
		t.Fatal("first key should register")
	}
	if kl.AllowWithRate("b", 10, func() Limiter { return NewTokenBucket(10, 10) }) {
		t.Fatal("unregistered key at capacity should be rejected")
	}
	if kl.OverCapHits() == 0 {
		t.Fatal("over-cap hits should be counted")
	}
	// 已登记 key 变速热更：不被护栏卡
	if !kl.AllowWithRate("a", 20, func() Limiter { return NewTokenBucket(20, 20) }) {
		t.Fatal("existing key rate change must bypass guardrail")
	}
}

// TestKeyedLimiter_AllowWithRate_Concurrent 并发同 key：最终桶状态一致，无双重登记。
func TestKeyedLimiter_AllowWithRate_Concurrent(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter {
		return NewTokenBucket(1000, 1000)
	})
	defer kl.Close()

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 20 {
				kl.AllowWithRate("k", 100, func() Limiter { return NewTokenBucket(100, 100) })
			}
		})
	}
	wg.Wait()

	if r, ok := kl.CurrentRate("k"); !ok || r != 100 {
		t.Fatalf("CurrentRate=%d,%v want 100,true", r, ok)
	}
	if kl.Len() != 1 {
		t.Fatalf("len=%d want 1", kl.Len())
	}
}

// TestKeyedLimiter_CurrentRate 元数据出口：factory/Create/Replace 路径无元数据。
func TestKeyedLimiter_CurrentRate(t *testing.T) {
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewTokenBucket(1, 1) })
	defer kl.Close()

	if _, ok := kl.CurrentRate("missing"); ok {
		t.Fatal("missing key must report no rate")
	}
	if kl.Get("factory") == nil {
		t.Fatal("factory path should register")
	}
	if _, ok := kl.CurrentRate("factory"); ok {
		t.Fatal("factory-registered entry must report no rate (无元数据)")
	}
	kl.Create("created", NewTokenBucket(1, 1))
	if _, ok := kl.CurrentRate("created"); ok {
		t.Fatal("Create-registered entry must report no rate")
	}
	kl.ReplaceIfChanged("created", 7, NewTokenBucket(7, 7))
	if r, ok := kl.CurrentRate("created"); !ok || r != 7 {
		t.Fatalf("CurrentRate=%d,%v want 7,true", r, ok)
	}
}

// TestKeyedLimiter_EvictCallback_BackgroundSweep 空闲回收可观测面（#142）：后台
// 清扫路径触发回调（key + 空闲时长），Evictions 计数。
func TestKeyedLimiter_EvictCallback_BackgroundSweep(t *testing.T) {
	type evict struct {
		key  string
		idle time.Duration
	}
	ch := make(chan evict, 4)
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewSlidingWindow(50*time.Millisecond, 100) },
		WithIdleTTL[string](30*time.Millisecond),
		WithEvictCallback[string](func(key string, idle time.Duration) { ch <- evict{key, idle} }),
	)
	defer kl.Close()

	if !kl.Allow("user:1") {
		t.Fatal("should allow")
	}
	select {
	case e := <-ch:
		if e.key != "user:1" {
			t.Fatalf("evicted key=%s want user:1", e.key)
		}
		if e.idle <= 0 {
			t.Fatalf("idle=%v should be > 0", e.idle)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("eviction callback not called within 3s")
	}
	if kl.Evictions() != 1 {
		t.Fatalf("evictions=%d want 1", kl.Evictions())
	}
	// 回收后窗口语义确实清零（回收=实例重建的实证面）
	// （只验证登记面：key 已不在表里）
	if kl.Len() != 0 {
		t.Fatalf("len=%d want 0 after eviction", kl.Len())
	}
}

// TestKeyedLimiter_EvictCallback_SweepOnPressure 表满 sweep-on-pressure 路径同样
// 触发回调与计数（拒绝路径的回收也要可观测，#142）。
func TestKeyedLimiter_EvictCallback_SweepOnPressure(t *testing.T) {
	type evict struct {
		key  string
		idle time.Duration
	}
	ch := make(chan evict, 4)
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewTokenBucket(10, 10) },
		WithMaxEntries[string](1),
		WithIdleTTL[string](30*time.Millisecond),
		WithEvictCallback[string](func(key string, idle time.Duration) { ch <- evict{key, idle} }),
	)
	defer kl.Close()

	kl.Allow("a") // 登记唯一名额
	time.Sleep(60 * time.Millisecond)
	// b 的建桶路径触发 sweep-on-pressure：回收 a、登记 b 并放行
	if !kl.Allow("b") {
		t.Fatal("b should reclaim idle a and register")
	}
	select {
	case e := <-ch:
		if e.key != "a" {
			t.Fatalf("evicted key=%s want a", e.key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("eviction callback not called on sweep-on-pressure")
	}
	if kl.Evictions() != 1 {
		t.Fatalf("evictions=%d want 1", kl.Evictions())
	}
}

// TestKeyedLimiter_EvictCallback_PanicAbsorbed 回调 panic 被收编，主路径不受影响。
//
// 等回收用**事件驱动**（回调里发信号），不用固定 sleep 等后台清扫：清扫 ticker 的
// 首次命中时刻取决于 ttl、ticker 间隔与调度延迟，原先「睡 80ms 断言已回收」只有
// 约 2 倍余量，CI 负载高时实测 ~10% 概率假红（evictions=0 want >=1）。
func TestKeyedLimiter_EvictCallback_PanicAbsorbed(t *testing.T) {
	async.SetPanicHandler(func(ctx context.Context, r any, stack []byte) { _ = r; _ = stack }) // 静音
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	fired := make(chan struct{})
	var once sync.Once
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewTokenBucket(10, 10) },
		WithIdleTTL[string](20*time.Millisecond),
		WithEvictCallback[string](func(string, time.Duration) {
			once.Do(func() { close(fired) }) // 先发信号再 panic：panic 由库收编
			panic("callback boom")
		}),
	)
	defer kl.Close()

	kl.Allow("a")
	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("eviction callback never fired：清扫协程未在超时内回收空闲条目")
	}

	// 主路径存活：新 key 正常登记放行
	if !kl.Allow("b") {
		t.Fatal("main path must survive callback panic")
	}
	if kl.Evictions() < 1 {
		t.Fatalf("evictions=%d want >=1", kl.Evictions())
	}
}
