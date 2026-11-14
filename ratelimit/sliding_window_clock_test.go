package ratelimit

import (
	"testing"
	"time"
)

// concurrency#341：时钟注入——假时钟下全路径确定性（无 sleep），与
// TokenBucket WithTokenClock / LockoutConfig.Now 同族口径。
func TestSlidingWindowClockInjection(t *testing.T) {
	clock := &fakeLockClock{}
	sw := NewSlidingWindow(time.Minute, 2, WithSlidingWindowClock(clock.Now))

	// 配额用尽
	if !sw.AllowN(1) || !sw.AllowN(1) {
		t.Fatal("前两次应放行")
	}
	if sw.AllowN(1) {
		t.Fatal("窗口满应拒绝")
	}
	if c := sw.Count(); c != 2 {
		t.Fatalf("Count = %d, want 2", c)
	}
	// 推进时钟越过窗口：配额恢复（假时钟，无 sleep）
	clock.Advance(2 * time.Minute)
	if !sw.AllowN(1) {
		t.Fatal("窗口滑过后应恢复放行")
	}
	if d, ok := sw.RetryAfter(); ok && d != 0 {
		t.Fatalf("有空位时 RetryAfter = %v, want 0", d)
	}
	// nil 时钟选项回退缺省
	sw2 := NewSlidingWindow(time.Minute, 1, WithSlidingWindowClock(nil))
	if sw2.now == nil {
		t.Fatal("nil clock 应回退 time.Now")
	}
	// 不带选项的既有构造形态不变
	sw3 := NewSlidingWindow(time.Minute, 1)
	if !sw3.AllowN(1) {
		t.Fatal("缺省构造应可用")
	}
}
