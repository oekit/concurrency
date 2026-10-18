package ratelimit

import (
	"testing"
	"time"
)

// TestKeyedLimiterByFunc_PerKeyQuota 锁定 #31 的核心语义：同一个管理器内，
// 不同 key 拿到**各自档位**的配额，而不是一律同一档。
func TestKeyedLimiterByFunc_PerKeyQuota(t *testing.T) {
	// 用有序切片而非 map —— map 遍历顺序随机，会让隔离性断言变得不确定
	type tier struct {
		key    string
		maxReq int
		window time.Duration
	}
	tiers := []tier{
		{"backup/import/confirm", 2, 900 * time.Second}, // 最严档
		{"car/syncAllTripData", 3, 300 * time.Second},   // 中档
		{"car/syncCurrentMonth", 5, 3600 * time.Second}, // 最宽档
	}
	byKey := make(map[string]tier, len(tiers))
	for _, q := range tiers {
		byKey[q.key] = q
	}

	kl := NewKeyedLimiterByFunc[string](func(k string) Limiter {
		q := byKey[k]
		return NewSlidingWindow(q.window, q.maxReq)
	})

	// 逐档耗尽：每档耗尽后立刻确认**下一档尚未耗尽**。
	// 若实现退化成"所有 key 共用一档"，这里必然失败。
	// used 记账：隔离性预检也会消耗额度，后续循环必须接着算，不能从 0 重来。
	used := make(map[string]int, len(tiers))
	allow := func(k string, want bool, ctx string) {
		if got := kl.Allow(k); got != want {
			t.Fatalf("%s：key %q 第 %d 次 = %v, want %v", ctx, k, used[k]+1, got, want)
		}
		if want {
			used[k]++
		}
	}
	for i, q := range tiers {
		for used[q.key] < q.maxReq {
			allow(q.key, true, "配额内应放行")
		}
		allow(q.key, false, "超出配额应拒绝")
		if i+1 < len(tiers) {
			nx := tiers[i+1]
			allow(nx.key, true, "前档耗尽不影响本档")
		}
	}

	if kl.Len() != len(tiers) {
		t.Fatalf("Len() = %d, want %d（一个管理器管住全部 key）", kl.Len(), len(tiers))
	}
}

// TestKeyedLimiterOf_ConfigSharedCountersIndependent 锁定旧 API 语义未变：
// NewKeyedLimiterOf 的 factory 不接受 key，故所有 key 拿到**相同配置的限流器**，
// 但每个 key 各有独立计数器（key 隔离是本包的立身之本）。
// 这是 #31 修复不能破坏的向后兼容契约。
func TestKeyedLimiterOf_ConfigSharedCountersIndependent(t *testing.T) {
	created := 0
	kl := NewKeyedLimiterOf[string](func() Limiter {
		created++
		return NewSlidingWindow(time.Minute, 2)
	})

	// a 与 b 各有自己的 2 次预算
	if !kl.Allow("a") || !kl.Allow("b") {
		t.Fatal("各 key 第 1 次都应放行")
	}
	if !kl.Allow("a") || !kl.Allow("b") {
		t.Fatal("各 key 第 2 次都应放行（计数器按 key 独立）")
	}
	if kl.Allow("a") || kl.Allow("b") {
		t.Fatal("各 key 第 3 次都应拒绝")
	}
	// factory 忽略 key，每个 key 仍各构造一个限流器
	if created != 2 {
		t.Fatalf("factory 调用 %d 次, want 2（每 key 一次）", created)
	}
}

// TestKeyedLimiterByFunc_EachKeyGetsOwnLimiter 断言 factory 拿到的是 key 本身，
// 而不是空值——这是"按 key 定配额"能成立的最小前提。
func TestKeyedLimiterByFunc_EachKeyGetsOwnLimiter(t *testing.T) {
	var seen []string
	kl := NewKeyedLimiterByFunc[string](func(k string) Limiter {
		seen = append(seen, k)
		return NewSlidingWindow(time.Minute, 100)
	})
	for _, k := range []string{"x", "y", "z"} {
		kl.Allow(k)
	}
	if len(seen) != 3 || seen[0] != "x" || seen[1] != "y" || seen[2] != "z" {
		t.Fatalf("factory 收到的 key 序列 = %v, want [x y z]", seen)
	}
}

// TestKeyedLimiterByFunc_OptionsStillApply 选项（空闲回收 / 容量护栏）在新构造器上
// 仍然生效——issue #31 抱怨的绕法正是把这些价值绕过了。
func TestKeyedLimiterByFunc_OptionsStillApply(t *testing.T) {
	kl := NewKeyedLimiterByFunc[string](
		func(k string) Limiter { return NewSlidingWindow(time.Minute, 10) },
		WithMaxEntries[string](2),
	)
	kl.Allow("a")
	kl.Allow("b")
	if kl.Allow("c") { // 表满 → 护栏拒绝未登记 key
		t.Fatalf("容量护栏应拒绝第 3 个 key（OverCapHits=%d）", kl.OverCapHits())
	}
	if kl.OverCapHits() != 1 {
		t.Fatalf("OverCapHits() = %d, want 1", kl.OverCapHits())
	}
	if kl.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", kl.Len())
	}
}
