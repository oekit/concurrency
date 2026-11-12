package ratelimit

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// —— NewTokenBucket 非法配置构造期护栏（#291）——
//
// 修复前 rps/burst 零校验：rps<=0 / burst<=0 静默构造「初始 burst 放完即永久
// 全拒」的限流器（rps=0 还会先把 burst 放完制造 warmup 假象——冒烟期能通、
// 生产全拒），与同接口 SlidingWindow 的 #135 防线（负值 panic、0=unlimited
// 写进 panic 文案）形成契约分裂。本组用例钉住对齐后的构造期行为。

// TestNewTokenBucket_NegativeRpsPanics 负速率没有任何可辩护语义，必须启动即
// 暴露（对齐 #135 的 panic 文案哲学）。
func TestNewTokenBucket_NegativeRpsPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("rps<0 应 panic（启动即暴露）")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "rps") {
			t.Fatalf("panic 消息应点名 rps，got %v", r)
		}
	}()
	NewTokenBucket(-5, 3)
}

// TestNewTokenBucket_NaNRpsPanics 配置面（NewTokenBucketFloat 的 YAML 入口）
// 进来的 NaN 同样当场暴露。
func TestNewTokenBucket_NaNRpsPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("rps=NaN 应 panic")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "NaN") {
			t.Fatalf("panic 消息应点名 NaN，got %v", r)
		}
	}()
	NewTokenBucket(rate.Limit(math.NaN()), 3)
}

// TestNewTokenBucket_NonPositiveBurstPanics x/time/rate 语义下 burst<1 即恒拒
// （构造即全拒，没有合法使用场景）。
func TestNewTokenBucket_NonPositiveBurstPanics(t *testing.T) {
	for _, burst := range []int{0, -1} {
		func() {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("burst=%d 应 panic", burst)
				}
				if msg, ok := r.(string); !ok || !strings.Contains(msg, "burst") {
					t.Fatalf("panic 消息应点名 burst，got %v", r)
				}
			}()
			NewTokenBucket(100, burst)
		}()
	}
}

// TestNewTokenBucket_ZeroRpsUnlimited rps=0 = 不限速档（对齐 SlidingWindow 的
// 0=unlimited，#135/#291）：恒放行、不记账，瞬时批量不受 burst 约束，
// Reserve/RetryAfter/WaitN 与 SlidingWindow 不限流档逐方法同语义——
// KeyedLimiter factory 换实现不再静默翻转行为。
func TestNewTokenBucket_ZeroRpsUnlimited(t *testing.T) {
	tb := NewTokenBucket(0, 3)

	// 远超 burst 的调用量恒放行（修复前 rps=0 会在前 3 次 true 后永久全拒）
	for range 200 {
		if !tb.Allow() {
			t.Fatal("不限流档不得拒绝")
		}
	}
	if !tb.AllowN(50) {
		t.Fatal("不限流档瞬时批量不受 burst 约束（对齐 SlidingWindow 恒放行）")
	}

	// Reserve / RetryAfter / WaitN：立即放行、无等待、恒可用
	if d, err := tb.Reserve(); d != 0 || err != nil {
		t.Fatalf("Reserve = (%v, %v)，want (0, nil)", d, err)
	}
	if d, ok := tb.RetryAfter(); d != 0 || !ok {
		t.Fatalf("RetryAfter = (%v, %v)，want (0, true)", d, ok)
	}
	if err := tb.WaitN(context.Background(), 100); err != nil {
		t.Fatalf("WaitN(100) = %v，want nil（不限流档无令牌可等）", err)
	}

	// Rate 汇报生效值 rate.Inf；与同接口 SlidingWindow 不限流档对照
	if got := tb.Rate(); got != rate.Inf {
		t.Fatalf("Rate() = %v，want rate.Inf", got)
	}
	sw := NewSlidingWindow(time.Minute, 0)
	if !sw.AllowN(50) {
		t.Fatal("对照前提不成立：SlidingWindow 不限流档应恒放行")
	}
	if d, ok := sw.RetryAfter(); d != 0 || !ok {
		t.Fatalf("对照前提不成立：SlidingWindow 不限流档 RetryAfter = (%v, %v)", d, ok)
	}
}

// TestNewTokenBucketFloat_SameGuardsFloatPath 配置面主入口（float64）走同一
// 组护栏（Float 委托 NewTokenBucket，守卫单源）。
func TestNewTokenBucketFloat_SameGuardsFloatPath(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewTokenBucketFloat(-1.5, 3) 应 panic")
		}
	}()
	NewTokenBucketFloat(-1.5, 3)
}
