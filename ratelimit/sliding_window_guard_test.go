package ratelimit

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestNewSlidingWindow_NegativeMaxCountPanics 锁定 #135：负配额没有任何可辩护
// 语义，必须启动即暴露（对齐 Lockout「非法值 panic」哲学）——静默恒拒限流器
// 的症状（「全被拒」）与「真被打满」不可分辨，值班先去查流量而不会怀疑配置。
func TestNewSlidingWindow_NegativeMaxCountPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("maxCount<0 应 panic（启动即暴露）")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "maxCount") {
			t.Fatalf("panic 消息应点名 maxCount，got %v", r)
		}
	}()
	NewSlidingWindow(time.Minute, -3)
}

// TestNewSlidingWindow_NonPositiveWindowPanics 同口径：窗口为 0 的滑窗没有语义，
// 对齐 Lockout 的 Window 守卫。
func TestNewSlidingWindow_NonPositiveWindowPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("window<=0 应 panic（启动即暴露）")
		}
	}()
	NewSlidingWindow(0, 10)
}

// TestSlidingWindow_ZeroMaxCountUnlimited 锁定 #135 推荐形态的另一半：
// maxCount==0 = 不限流（恒放行、不记账），对齐同包 WithMaxEntries /
// LockoutConfig.MaxEntries 的 0=不限口径与宿主配置面直觉。
func TestSlidingWindow_ZeroMaxCountUnlimited(t *testing.T) {
	sw := NewSlidingWindow(time.Minute, 0)

	// 大量请求全部放行——修复前这里是静默恒拒
	for i := 0; i < 1000; i++ {
		if !sw.Allow() {
			t.Fatalf("maxCount=0 是不限流档：第 %d 次 Allow 不应被拒", i+1)
		}
	}
	if !sw.AllowN(500) {
		t.Fatal("不限流档 AllowN 应恒放行")
	}
	// 不记账：Count 恒 0（不限流档没有「窗口内请求数」可言）
	if n := sw.Count(); n != 0 {
		t.Fatalf("不限流档不应记账：Count=%d，want 0", n)
	}
	// Reserve / Wait 同口径立即放行
	if d := sw.Reserve(); d != 0 {
		t.Fatalf("不限流档 Reserve 应立即放行，got %v", d)
	}
	if err := sw.Wait(context.Background()); err != nil {
		t.Fatalf("不限流档 Wait 应立即返回 nil，got %v", err)
	}
}
