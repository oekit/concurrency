package ratelimit

import (
	"testing"
	"time"
)

// TokenBucket 时钟注入测试（#24）：假时钟确定性驱动「配额恢复」与
// 「Retry-After 精确探测」——无 sleep 无 flake。复用 lockout_test.go 的
// fakeLockClock（同包测试助手）。

// TestTokenBucket_WithTokenClock 假时钟下 burst 耗尽 → RetryAfter 探测精确
// → 推进注入时钟配额恢复（迁移自 x/time/rate 裸用的消费方可回填测试）。
func TestTokenBucket_WithTokenClock(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucket(10, 2, WithTokenClock(clock.Now)) // 10 rps, burst 2

	if !tb.Allow() {
		t.Fatal("initial burst must allow the 1st request")
	}
	if !tb.Allow() {
		t.Fatal("initial burst must allow the 2nd request")
	}
	if tb.Allow() {
		t.Fatal("burst drained, must reject")
	}

	// Retry-After 探测：rps=10 → 下一令牌恰好 100ms
	d, ok := tb.RetryAfter()
	if !ok {
		t.Fatal("RetryAfter must be available")
	}
	if d != 100*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want exactly 100ms", d)
	}

	// 推进注入时钟：无 sleep、确定性恢复
	clock.Advance(100 * time.Millisecond)
	if !tb.Allow() {
		t.Fatal("quota must recover after 100ms at 10rps (fake clock)")
	}
	if tb.Allow() {
		t.Fatal("only 1 token refilled, second must reject")
	}

	// 再次探测：下一令牌仍是精确 100ms
	if d, _ = tb.RetryAfter(); d != 100*time.Millisecond {
		t.Fatalf("RetryAfter after refill = %v, want exactly 100ms", d)
	}
}

// TestTokenBucket_WithTokenClock_ProbeNotConsume 探测不消耗配额：连续 RetryAfter
// 值稳定，推进到探测点后放行（429 + Retry-After 回填的标准姿势）。
func TestTokenBucket_WithTokenClock_ProbeNotConsume(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucket(5, 1, WithTokenClock(clock.Now)) // 5 rps, burst 1

	if !tb.Allow() {
		t.Fatal("initial burst must allow 1")
	}

	const want = 200 * time.Millisecond
	for i := 0; i < 5; i++ {
		if d, ok := tb.RetryAfter(); !ok || d != want {
			t.Fatalf("probe %d = (%v, %v), want (%v, true) — probing must not consume", i+1, d, ok, want)
		}
	}

	clock.Advance(want)
	if !tb.Allow() {
		t.Fatal("must allow at probed instant")
	}
}

// TestTokenBucket_NilClockFallsBack nil 时钟回落 time.Now（缺省行为不变）。
func TestTokenBucket_NilClockFallsBack(t *testing.T) {
	tb := NewTokenBucket(1000, 10, WithTokenClock(nil))
	if !tb.Allow() {
		t.Fatal("nil clock must fall back to time.Now and allow")
	}
}

// TestTokenBucket_WithTokenClock_KeyedFactory 消费方迁移形态（#24 动机）：
// KeyedLimiter 工厂闭包共享假时钟——per-IP 限流的整表确定性测试。
func TestTokenBucket_WithTokenClock_KeyedFactory(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	kl := NewKeyedLimiter(func() Limiter {
		return NewTokenBucket(1, 1, WithTokenClock(clock.Now))
	}, time.Hour)
	defer kl.Close()

	if !kl.Allow("1.2.3.4") {
		t.Fatal("ip burst must allow once")
	}
	if kl.Allow("1.2.3.4") {
		t.Fatal("ip drained")
	}
	clock.Advance(time.Second) // 1 rps：1s 后恢复 1 令牌（全表确定性，无 sleep）
	if !kl.Allow("1.2.3.4") {
		t.Fatal("ip quota must recover after 1s (fake clock)")
	}
}
