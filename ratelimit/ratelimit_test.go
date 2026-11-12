package ratelimit

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestTokenBucket_Allow(t *testing.T) {
	tb := NewTokenBucket(10, 10) // 10 QPS, burst 10

	// 前 10 个请求应该允许
	for i := range 10 {
		if !tb.Allow() {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// 第 11 个请求应该被拒绝
	if tb.Allow() {
		t.Error("request 11 should be rejected")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(10, 10)

	if !tb.AllowN(5) {
		t.Error("should allow 5 requests")
	}

	if !tb.AllowN(5) {
		t.Error("should allow 5 more requests")
	}

	if tb.AllowN(1) {
		t.Error("should reject when burst is exhausted")
	}
}

func TestTokenBucket_Reserve(t *testing.T) {
	tb := NewTokenBucket(10, 1)

	// 第一个请求应该立即可用
	d, err := tb.Reserve()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if d > 0 {
		t.Errorf("expected 0 duration, got %v", d)
	}

	// 第二个请求需要等待
	d, err = tb.Reserve()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if d <= 0 {
		t.Error("expected positive duration for second request")
	}
}

func TestSlidingWindow_Allow(t *testing.T) {
	sw := NewSlidingWindow(time.Second, 5)

	// 前 5 个请求应该允许
	for i := range 5 {
		if !sw.Allow() {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// 第 6 个请求应该被拒绝
	if sw.Allow() {
		t.Error("request 6 should be rejected")
	}
}

func TestSlidingWindow_Count(t *testing.T) {
	sw := NewSlidingWindow(time.Second, 10)

	sw.Allow()
	sw.Allow()
	sw.Allow()

	if count := sw.Count(); count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

func TestSlidingWindow_Reset(t *testing.T) {
	sw := NewSlidingWindow(time.Second, 5)

	sw.Allow()
	sw.Allow()
	sw.Reset()

	if count := sw.Count(); count != 0 {
		t.Errorf("expected count 0 after reset, got %d", count)
	}
}

func TestKeyedLimiter_Allow(t *testing.T) {
	kl := NewKeyedLimiter(func() Limiter {
		return NewTokenBucket(2, 2)
	}, time.Minute)
	defer kl.Close()

	// key1 的前 2 个请求应该允许
	if !kl.Allow("key1") {
		t.Error("key1 request 1 should be allowed")
	}
	if !kl.Allow("key1") {
		t.Error("key1 request 2 should be allowed")
	}

	// key1 的第 3 个请求应该被拒绝
	if kl.Allow("key1") {
		t.Error("key1 request 3 should be rejected")
	}

	// key2 的请求应该独立
	if !kl.Allow("key2") {
		t.Error("key2 request 1 should be allowed")
	}
}

func TestKeyedLimiter_Len(t *testing.T) {
	kl := NewKeyedLimiter(func() Limiter {
		return NewTokenBucket(10, 10)
	}, time.Minute)
	defer kl.Close()

	kl.Allow("key1")
	kl.Allow("key2")
	kl.Allow("key3")

	if l := kl.Len(); l != 3 {
		t.Errorf("expected length 3, got %d", l)
	}
}

func TestKeyedLimiter_Delete(t *testing.T) {
	kl := NewKeyedLimiter(func() Limiter {
		return NewTokenBucket(10, 10)
	}, time.Minute)
	defer kl.Close()

	kl.Allow("key1")
	kl.Delete("key1")

	if l := kl.Len(); l != 0 {
		t.Errorf("expected length 0 after delete, got %d", l)
	}
}

// TestSlidingWindow_ReserveReservesSlot 回归：Reserve 曾只计算等待时间
// 不追加记录——并发 N 个调用者拿到相同 wait 后同时放行。
// 钉住：预留即消费，后续 Allow 看到槽位被占。
func TestSlidingWindow_ReserveReservesSlot(t *testing.T) {
	sw := NewSlidingWindow(time.Hour, 2) // 窗口内最多 2 个
	if wait, err := sw.Reserve(); err != nil || wait != 0 {
		t.Fatalf("first Reserve should be immediate, got (%v, %v)", wait, err)
	}
	if wait, err := sw.Reserve(); err != nil || wait != 0 {
		t.Fatalf("second Reserve should be immediate, got (%v, %v)", wait, err)
	}
	// 槽位已占满：Allow 必须拒绝（旧实现 Reserve 不占槽，这里会错误放行）
	if sw.Allow() {
		t.Error("Allow after 2 Reserves should be false (slots consumed)")
	}
	// 第三次 Reserve 需要等待
	if wait, err := sw.Reserve(); err != nil || wait <= 0 {
		t.Errorf("third Reserve should wait, got (%v, %v)", wait, err)
	}
}

// TestSlidingWindow_AllowN_NonPositive 钉住 n<=0 显式放行且不占槽位。
func TestSlidingWindow_AllowN_NonPositive(t *testing.T) {
	sw := NewSlidingWindow(time.Hour, 1)
	if !sw.AllowN(0) {
		t.Error("AllowN(0) should be true")
	}
	if !sw.AllowN(-5) {
		t.Error("AllowN(-5) should be true (no-op)")
	}
	if !sw.Allow() {
		t.Error("negative n should not consume slots")
	}
}

// TestLimiter_ReserveNotReservableSentinel 锁定 #248/#250：两个实现（TokenBucket /
// SlidingWindow）对「不可预留」返回**同一规范哨兵** (−1, ErrNotReservable)，
// errors.Is 可判定——消费方持 Limiter 接口（KeyedLimiter factory 的返回类型）写
// 通用退避时，换实现不再静默失效。修复前 TokenBucket 回 time.Duration 最大值
// （≈292 年，按 d<0 判拒绝的通用代码被判成「等 292 年」挂死）、SlidingWindow 回
// 裸 -1（按最大值判拒绝的通用代码把 -1 当立即可用，紧循环锤击满窗限流器），
// 接口契约零声明。
func TestLimiter_ReserveNotReservableSentinel(t *testing.T) {
	// TokenBucket：burst=0（1 个令牌超出 burst 上限——该请求永远等不到）
	// SlidingWindow：在册预占订满整窗额度（maxCount=2：2 个立即放行 + 2 个预占
	// 已达 cap，第 5 笔不可预留）
	newLimiters := map[string]func() Limiter{
		"TokenBucket/burst0": func() Limiter { return NewTokenBucket(10, 0) },
		"SlidingWindow/full": func() Limiter { return NewSlidingWindow(time.Hour, 2) },
	}

	for name, newLimiter := range newLimiters {
		t.Run(name, func(t *testing.T) {
			l := newLimiter()
			for range 4 {
				_, _ = l.Reserve() // 打到不可预留态（TokenBucket 第 1 笔即不可预留）
			}

			// 不可预留：规范哨兵，errors.Is 判定（不要比对魔法数值）
			d, err := l.Reserve()
			if !errors.Is(err, ErrNotReservable) {
				t.Fatalf("%s 不可预留应返回 ErrNotReservable（errors.Is 可判定），got (%v, %v)", name, d, err)
			}
			if d != -1 {
				t.Fatalf("%s 不可预留的占位时长应为 -1（非 0，防误读成「立即可用」），got %v", name, d)
			}

			// 哨兵可被 %w 包装穿透（消费方包装错误链后仍可判定）
			wrapped := fmt.Errorf("retry budget exhausted: %w", err)
			if !errors.Is(wrapped, ErrNotReservable) {
				t.Fatalf("%s 包装后的错误链应仍可判定 ErrNotReservable", name)
			}
		})
	}

	// 成功路径不受影响：预留成功返回 (d>=0, nil)
	tb := NewTokenBucket(10, 1)
	if d, err := tb.Reserve(); err != nil || d != 0 {
		t.Fatalf("TokenBucket 有桶时应 (0, nil)，got (%v, %v)", d, err)
	}
	sw := NewSlidingWindow(time.Hour, 1)
	if d, err := sw.Reserve(); err != nil || d != 0 {
		t.Fatalf("SlidingWindow 有空位时应 (0, nil)，got (%v, %v)", d, err)
	}
}
