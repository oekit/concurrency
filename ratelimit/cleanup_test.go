package ratelimit

import (
	"testing"
	"time"
)

// TestCleanupInterval 清扫节奏下限（#84）：
//   - ttl/2 整除为 0（ttl=1ns）⇒ time.NewTicker(0) 会 panic ⇒ 兜到 1s
//   - ttl/2 为正但过小（ns 级）⇒ ticker 会热循环烧核 ⇒ 兜到 1ms
//   - 正常量级 ⇒ 原样返回 ttl/2（清扫节奏语义不变）
func TestCleanupInterval(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"零值兜底", 0, time.Second},
		{"1ns 整除为 0", time.Nanosecond, time.Second},
		{"2ns 间隔 1ns 热循环", 2 * time.Nanosecond, time.Millisecond},
		{"1us 间隔 500ns", time.Microsecond, time.Millisecond},
		{"2ms 间隔 1ms", 2 * time.Millisecond, time.Millisecond},
		{"1s 间隔 500ms", time.Second, 500 * time.Millisecond},
		{"1m 间隔 30s", time.Minute, 30 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := cleanupInterval(c.ttl); got != c.want {
				t.Fatalf("cleanupInterval(%v)=%v, want %v", c.ttl, got, c.want)
			}
		})
	}
}

// TestKeyedLimiter_CleanupSurvivesTinyTTL 回归 #84：ttl=1ns 时 ttl/2=0，
// time.NewTicker(0) panic 被 async.GoSafe 收编——进程不崩，但**后台清扫协程当场死亡**：
// 空闲条目永不回收（limiters 无界增长），容量护栏 WithMaxEntries 也永远等不到腾位。
//
// 断言的是「清扫还在跑」而非「没有 panic」：panic 被 recover 后从外部不可见，
// 但清扫协程一旦死亡，空闲条目就再也回收不掉（修复前本测试必然超时失败）。
func TestKeyedLimiter_CleanupSurvivesTinyTTL(t *testing.T) {
	kl := NewKeyedLimiter(func() Limiter {
		return NewTokenBucket(10, 20)
	}, time.Nanosecond)
	defer kl.Close()

	if !kl.Allow("k") {
		t.Fatal("first request should pass")
	}
	if kl.Len() != 1 {
		t.Fatalf("Len()=%d, want 1", kl.Len())
	}

	// 清扫节奏下限 1s ⇒ 轮询等它真的跑一轮
	waitFor(t, 3*time.Second, func() bool { return kl.Len() == 0 })
}

// TestLockout_CleanupSurvivesTinyIdleTTL 回归 #84：IdleTTL=1ns 时 ttl/2=0，
// time.NewTicker(0) panic ⇒ 后台清扫协程死亡 ⇒ 空闲条目永不回收
// （entries 无界增长，容量护栏永远等不到腾位）。
func TestLockout_CleanupSurvivesTinyIdleTTL(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, func(c *LockoutConfig) {
		c.IdleTTL = time.Nanosecond
	})
	defer lo.Close()

	if locked, _ := lo.RecordFailure("k"); locked {
		t.Fatal("failure 1 must not lock (threshold 3)")
	}
	if lo.Len() != 1 {
		t.Fatalf("Len()=%d, want 1", lo.Len())
	}

	// 推进假时钟到失败样本老化出窗口（#83：窗口内仍有样本的条目免于回收），
	// 此后条目才满足空闲回收条件（now-lastSeen > IdleTTL=1ns）。
	clock.Advance(time.Hour)

	waitFor(t, 3*time.Second, func() bool { return lo.Len() == 0 })
}

// waitFor 轮询等待条件成立（后台清扫走真实 ticker，非确定性 ⇒ 轮询而非固定 sleep）。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
