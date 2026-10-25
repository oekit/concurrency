package ratelimit

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

// 限流器可观测面 + 阻塞等待档（#92）：
//
//   - Tokens()：实时剩余配额读出口（X-RateLimit-Remaining 这类响应头用）；
//   - Wait(ctx)：阻塞等待档（出站客户端节流「等到有配额再发请求」用）；
//   - NewTokenBucketFloat：rps 以 float64 传入，消费方不必 import x/time/rate。
//
// 编译期断言：两个仓内实现都满足 Limiter 接口（v0.21.0 起接口启用 Wait）。
var (
	_ Limiter = (*TokenBucket)(nil)
	_ Limiter = (*SlidingWindow)(nil)
)

// 剩余配额读出口不消耗配额，且随消费递减。
func TestTokenBucket_Tokens_ReflectsConsumption(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(10, 2, WithTokenClock(clock.Now)) // 10 rps, burst 2

	if got := tb.Tokens(); got != 2 {
		t.Fatalf("初始剩余配额 = %v, want 2", got)
	}
	if !tb.Allow() {
		t.Fatal("第 1 次 Allow 应放行")
	}
	if got := tb.Tokens(); got != 1 {
		t.Fatalf("消费 1 个后剩余 = %v, want 1", got)
	}
	if !tb.Allow() {
		t.Fatal("第 2 次 Allow 应放行")
	}
	if got := tb.Tokens(); got != 0 {
		t.Fatalf("burst 耗尽后剩余 = %v, want 0", got)
	}
	if tb.Allow() {
		t.Fatal("burst 已耗尽，应拒绝")
	}
	// 被拒的请求不消耗配额——读数仍是 0。
	if got := tb.Tokens(); got != 0 {
		t.Fatalf("被拒后剩余 = %v, want 0", got)
	}
}

// 剩余配额按**注入时钟**恢复（无 sleep、确定性）。
func TestTokenBucket_Tokens_RecoversWithInjectedClock(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(10, 1, WithTokenClock(clock.Now)) // 10 rps, burst 1

	if !tb.Allow() {
		t.Fatal("burst 应放行 1 次")
	}
	if got := tb.Tokens(); got != 0 {
		t.Fatalf("耗尽后剩余 = %v, want 0", got)
	}

	clock.Advance(100 * time.Millisecond) // 10 rps → 100ms 恢复 1 个
	if got := tb.Tokens(); got != 1 {
		t.Fatalf("推进 100ms 后剩余 = %v, want 1", got)
	}
}

// NewTokenBucketFloat 与 NewTokenBucket 等价（消费方不必 import x/time/rate）。
func TestTokenBucket_NewTokenBucketFloat_EqualsNewTokenBucket(t *testing.T) {
	tb := NewTokenBucketFloat(12.5, 7)
	if got := float64(tb.Rate()); got != 12.5 {
		t.Errorf("Rate() = %v, want 12.5", got)
	}
	if got := tb.Burst(); got != 7 {
		t.Errorf("Burst() = %v, want 7", got)
	}
}

// Wait 阻塞到有配额：拿得到令牌时返回 nil，且**消费掉**那个令牌。
func TestTokenBucket_Wait_ConsumesToken(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(1000, 1, WithTokenClock(clock.Now))

	if !tb.Allow() {
		t.Fatal("burst 应放行 1 次")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tb.Wait(ctx); err != nil {
		t.Fatalf("Wait 应成功, err = %v", err)
	}
	// Wait 消费了配额：桶已空，再来一次 Allow 必须被拒。
	if tb.Allow() {
		t.Fatal("Wait 之后配额应已被消费，Allow 应被拒")
	}
}

// Wait 在 ctx 取消时返回 ctx.Err()，且**不消耗配额**（取消不是消费）。
func TestTokenBucket_Wait_ContextCanceledDoesNotConsume(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(1000, 1, WithTokenClock(clock.Now))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tb.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait 应返回 context.Canceled, got %v", err)
	}
	// 配额仍在：取消的等待没有占掉那个令牌。
	if !tb.Allow() {
		t.Fatal("ctx 取消的 Wait 不应消耗配额")
	}
}

// 等待途中 ctx 取消：返回 ctx.Err()，配额归还（不消耗）。
func TestTokenBucket_Wait_CancelDuringWaitReturnsErr(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(1, 1, WithTokenClock(clock.Now)) // 1 rps → 等待 1s

	if !tb.Allow() {
		t.Fatal("burst 应放行 1 次")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := tb.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("等待途中 ctx 取消应返回 DeadlineExceeded, got %v", err)
	}
}

// n 超过 burst：永远等不到 → ErrBurstTooSmall（不是 ctx 取消类失败）。
func TestTokenBucket_WaitN_BurstTooSmall(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(10, 1, WithTokenClock(clock.Now))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tb.WaitN(ctx, 2); !errors.Is(err, ErrBurstTooSmall) {
		t.Fatalf("WaitN(2) 在 burst=1 时应返回 ErrBurstTooSmall, got %v", err)
	}
}

// n<=0 视为无需令牌，直接返回 nil。
func TestTokenBucket_WaitN_NonPositiveIsNoop(t *testing.T) {
	tb := NewTokenBucketFloat(10, 1)
	for _, n := range []int{0, -1} {
		if err := tb.WaitN(context.Background(), n); err != nil {
			t.Fatalf("WaitN(%d) 应返回 nil, got %v", n, err)
		}
	}
}

// Wait 真会等（真实时钟）：延迟量级与 rps 对得上。
func TestTokenBucket_Wait_RealDelay(t *testing.T) {
	tb := NewTokenBucketFloat(100, 1) // 100 rps → 10ms 一个令牌

	if !tb.Allow() {
		t.Fatal("burst 应放行 1 次")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := tb.Wait(ctx); err != nil {
		t.Fatalf("Wait 应成功, err = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Errorf("Wait 应等到令牌恢复, 实际耗时 %v", elapsed)
	}
}

// SlidingWindow.Wait：窗口有位时立即放行。
func TestSlidingWindow_Wait_ImmediateWhenSlotFree(t *testing.T) {
	sw := NewSlidingWindow(time.Minute, 2)

	start := time.Now()
	if err := sw.Wait(context.Background()); err != nil {
		t.Fatalf("有空位时 Wait 应立即成功, err = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("有空位时 Wait 不应等待, 实际耗时 %v", elapsed)
	}
	if got := sw.Count(); got != 1 {
		t.Errorf("Wait 后窗口内计数 = %d, want 1", got)
	}
}

// SlidingWindow.Wait：窗口满时等到最早的记录滑出窗口。
func TestSlidingWindow_Wait_BlocksUntilSlotFrees(t *testing.T) {
	const window = 80 * time.Millisecond
	sw := NewSlidingWindow(window, 1)

	if !sw.Allow() {
		t.Fatal("窗口内首次 Allow 应放行")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := sw.Wait(ctx); err != nil {
		t.Fatalf("Wait 应等到槽位释放, err = %v", err)
	}
	if elapsed := time.Since(start); elapsed < window/2 {
		t.Errorf("Wait 应等到窗口腾空, 实际耗时 %v", elapsed)
	}
	if got := sw.Count(); got != 1 {
		t.Errorf("Wait 后窗口内计数 = %d, want 1", got)
	}
}

// SlidingWindow.Wait：ctx 取消返回 ctx.Err()，且**不留幽灵计数**。
func TestSlidingWindow_Wait_ContextCanceled(t *testing.T) {
	sw := NewSlidingWindow(time.Minute, 1)

	if !sw.Allow() {
		t.Fatal("窗口内首次 Allow 应放行")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := sw.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx 超时 Wait 应返回 DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("ctx 超时后 Wait 应尽快返回, 实际耗时 %v", elapsed)
	}
	// 取消的等待不预占槽位：窗口内计数仍是 1（那条 Allow 的），不是 2。
	if got := sw.Count(); got != 1 {
		t.Errorf("取消的 Wait 不应留下幽灵计数, 窗口内计数 = %d, want 1", got)
	}
}

// Tokens 是实时读出口：不消耗配额（连续读数稳定）。
func TestTokenBucket_Tokens_IsReadOnly(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	tb := NewTokenBucketFloat(10, 2, WithTokenClock(clock.Now))

	first := tb.Tokens()
	for i := 0; i < 10; i++ {
		if got := tb.Tokens(); math.Abs(got-first) > 1e-9 {
			t.Fatalf("读出口不应消耗配额: 第 %d 次读到 %v, 首次 %v", i, got, first)
		}
	}
}
