package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// concurrency#363：「键控 map + TTL 回收 + 容量护栏 + Close(done)」骨架收编进
// keyedTTLRegistry 后的行为等价回归——既有测试零改动全绿之外，这里把收编前后
// 的语义不变量显式钉住：
//   - 表满 sweep-on-pressure：先回收过期条目腾位再判满，腾位成功不计 OverCapHits；
//   - 回收观测面：EvictCallback 以 (key, 空闲时长) 锁外回调、Evictions 计数
//     （后台清扫与护栏回收两条路径统一）；
//   - 后台清扫：ticker 只定节奏，过期判定一律走注入时钟（#361/#341 同形）；
//   - Close 幂等（可安全重复调用），Close 后登记/查询语义不变。
//
// 判定全部走假时钟（确定性精确值），真实 ticker 只定清扫节奏。

// TestKeyedLimiter_RegistrySkeletonEquivalence KeyedLimiterOf 经 registry 收编
// 后的等价回归：护栏回收路径的 key/idle 载荷、Evictions/OverCapHits 计数、
// 后台清扫、Close 幂等与收编前逐项一致。
func TestKeyedLimiter_RegistrySkeletonEquivalence(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	var mu sync.Mutex
	var evictedKeys []string
	var lastIdle time.Duration
	kl := NewKeyedLimiterOf[string](func() Limiter { return NewSlidingWindow(time.Minute, 10) },
		WithIdleTTL[string](2*time.Millisecond),
		WithMaxEntries[string](1),
		WithClock[string](clock.Now),
		WithEvictCallback[string](func(key string, idle time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			evictedKeys = append(evictedKeys, key)
			lastIdle = idle
		}))
	defer kl.Close()

	// 1) 表满 sweep-on-pressure：k1 空闲过期后 k2 经护栏回收腾位登记成功
	if kl.Get("k1") == nil {
		t.Fatal("k1 首次登记应成功")
	}
	clock.Advance(time.Minute) // k1 空闲超 IdleTTL=2ms；idle 应恰为时钟推进量
	if kl.Get("k2") == nil {
		t.Fatal("k2 应在 sweep-on-pressure 回收 k1 后登记成功")
	}
	if kl.Len() != 1 {
		t.Fatalf("回收后 Len()=%d, want 1", kl.Len())
	}
	mu.Lock()
	if len(evictedKeys) != 1 || evictedKeys[0] != "k1" || lastIdle != time.Minute {
		t.Fatalf("回收载荷 = %v (last idle %v), want [k1] idle=1m（锁内按注入时钟精确采集）",
			evictedKeys, lastIdle)
	}
	mu.Unlock()
	if n := kl.Evictions(); n != 1 {
		t.Fatalf("Evictions()=%d, want 1", n)
	}
	if hits := kl.OverCapHits(); hits != 0 {
		t.Fatalf("sweep 腾位成功不应计 OverCapHits, got %d", hits)
	}

	// 2) 后台清扫：k2 过期后由 registry 清扫循环回收（判定走注入时钟，
	//    与收编前 keyed.go cleanup 同形）
	clock.Advance(time.Minute)
	waitFor(t, 3*time.Second, func() bool { return kl.Evictions() == 2 })
	if kl.Len() != 0 {
		t.Fatalf("后台清扫后 Len()=%d, want 0", kl.Len())
	}
	mu.Lock()
	if len(evictedKeys) != 2 || evictedKeys[1] != "k2" || lastIdle != time.Minute {
		t.Fatalf("后台清扫回收载荷 = %v (last idle %v), want [k1 k2] idle=1m",
			evictedKeys, lastIdle)
	}
	mu.Unlock()

	// 3) Close 幂等：重复调用不 panic；Close 仅停清扫协程，登记/查询照常
	kl.Close()
	kl.Close()
	if kl.Get("k2") == nil {
		t.Fatal("Close 后 Get 仍应可用（仅清扫协程退场，与收编前一致）")
	}
}

// TestLockout_RegistrySkeletonEquivalence LockoutOf 经 registry 收编后的等价
// 回归：护栏 sweep-on-pressure（可回收条目腾位 / 全不可回收拒绝计 OverCapHits）、
// 锁定条目两条回收路径都免驱逐（提前驱逐等于提前解锁）、锁到期且样本老化后
// 正常回收、Close 幂等——判定差异全部由 expired 回调（reclaimable）承载。
func TestLockout_RegistrySkeletonEquivalence(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, func(c *LockoutConfig) {
		c.MaxEntries = 3
		c.LockDuration = 15 * time.Minute // 拉长锁定，覆盖后文观测窗口
		c.IdleTTL = 2 * time.Millisecond  // 清扫节奏 ttl/2=1ms，真实 ticker 快
	})
	defer lo.Close()

	// 铺表：a(T0) 单样本；b(T0+8m) 三样本触发锁定至 T0+23m
	if locked, _ := lo.RecordFailure("a"); locked {
		t.Fatal("a 单样本不得锁定")
	}
	clock.Advance(8 * time.Minute)
	for i := range 3 {
		locked, _ := lo.RecordFailure("b")
		if locked != (i == 2) {
			t.Fatalf("b failure %d locked=%v, want %v", i+1, locked, i == 2)
		}
	}
	if locked, remain := lo.CheckLocked("b"); !locked || remain != 15*time.Minute {
		t.Fatalf("b 第 3 次失败 = (%v, %v), want locked/15m", locked, remain)
	}

	// 1) sweep-on-pressure：推进到 T0+11m——a 样本老化出窗口且空闲超 TTL（可回收），
	//    b 锁定中（免回收）。c/d 直登记（未满不触发回收），e 全不可回收拒绝计
	//    OverCapHits，d 登记路径回收 a 腾位不计。
	clock.Advance(3 * time.Minute) // T0+11m
	if locked, _ := lo.RecordFailure("c"); locked {
		t.Fatal("c 单样本不得锁定")
	}
	if locked, _ := lo.RecordFailure("d"); locked {
		t.Fatal("d 单样本不得锁定")
	}
	if lo.Len() != 3 {
		t.Fatalf("Len()=%d, want 3（d 登记前 sweep 回收 a）", lo.Len())
	}
	if hits := lo.OverCapHits(); hits != 0 {
		t.Fatalf("sweep 腾位成功不应计 OverCapHits, got %d", hits)
	}
	if locked, _ := lo.RecordFailure("e"); locked {
		t.Fatal("e 表满护栏拒绝 (fail-open drop)，不得锁定")
	}
	if locked, remain := lo.CheckLocked("e"); locked {
		t.Fatalf("e 表满护栏拒绝 = (%v, %v), want unlocked (fail-open drop)", locked, remain)
	}
	if hits := lo.OverCapHits(); hits != 1 {
		t.Fatalf("OverCapHits()=%d, want 1（表满且全不可回收时拒绝计一次）", hits)
	}
	if lo.Len() != 3 {
		t.Fatalf("Len()=%d, want 3（拒绝路径不得改表）", lo.Len())
	}
	if locked, remain := lo.CheckLocked("b"); !locked || remain != 12*time.Minute {
		t.Fatalf("b 锁定未被回收路径驱逐 = (%v, %v), want locked/12m", locked, remain)
	}

	// 2) 后台清扫：推进到 T0+21m——c/d 样本老化出窗口且空闲超 TTL ⇒ 回收；
	//    b 空闲也超 TTL 但锁定仍有效（至 T0+23m）⇒ 免驱逐。
	clock.Advance(10 * time.Minute) // T0+21m
	waitFor(t, 3*time.Second, func() bool { return lo.Len() == 1 })
	if locked, remain := lo.CheckLocked("b"); !locked || remain != 2*time.Minute {
		t.Fatalf("锁定条目被后台清扫驱逐: (%v, %v) — eviction must skip locked entries", locked, remain)
	}

	// 3) 锁到期且样本老化后条目正常回收（回收判据不退化为「永不回收」）
	clock.Advance(5 * time.Minute) // T0+26m：锁已到期（T0+23m）、样本已老化
	waitFor(t, 3*time.Second, func() bool { return lo.Len() == 0 })

	// 4) Close 幂等：重复调用不 panic；Close 后登记/查询语义不变
	lo.Close()
	lo.Close()
	if locked, _ := lo.RecordFailure("z"); locked {
		t.Fatal("Close 后 RecordFailure 仍应可用（仅清扫协程退场，与收编前一致）")
	}
}
