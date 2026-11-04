package ratelimit

import (
	"testing"
	"time"
)

// TestKeyedLimiterByFunc_FactoryNilNotRegistered 锁定 #134：Get 慢路径对 factory
// 返回的 nil **不注册**——对齐 Create/Replace 的拒 nil 契约。
//
// 修复前的三个叠加问题（全部由本用例扼守）：
//  1. nil 被注册成条目（Len 不为 0）——三个登记入口两种契约；
//  2. 幽灵条目被后续 Get 快路径持续刷新 lastSeen，IdleTTL 永不回收（慢性泄漏）；
//  3. 幽灵条目占 WithMaxEntries 名额，真 key 被容量护栏拒（OverCapHits 增长，
//     症状与「限流命中」不可分辨）。
func TestKeyedLimiterByFunc_FactoryNilNotRegistered(t *testing.T) {
	kl := NewKeyedLimiterByFunc[string](func(k string) Limiter {
		if k == "ghost" {
			return nil // 配额表缺 key 显式失败（doc 推荐形态）
		}
		return NewSlidingWindow(time.Minute, 10)
	},
		WithIdleTTL[string](time.Minute), WithMaxEntries[string](1))
	defer kl.Close()

	if l := kl.Get("ghost"); l != nil {
		t.Fatalf("factory 返回 nil 时 Get 应返回 nil，got %v", l)
	}
	if n := kl.Len(); n != 0 {
		t.Fatalf("factory 返回 nil 不应注册条目：Len=%d，want 0", n)
	}
	// Allow 对 nil 的显式失败语义不变
	if kl.Allow("ghost") {
		t.Fatal("nil limiter 应显式失败（Allow=false）")
	}

	// 真 key 不再被幽灵条目挤出容量护栏
	if l := kl.Get("real"); l == nil {
		t.Fatalf("真 key 应正常登记（此前被幽灵条目占满护栏拒掉），OverCapHits=%d", kl.OverCapHits())
	}
	if n := kl.Len(); n != 1 {
		t.Fatalf("登记真 key 后 Len=%d，want 1", n)
	}
	if hits := kl.OverCapHits(); hits != 0 {
		t.Fatalf("OverCapHits=%d，want 0（护栏不该被幽灵条目消耗）", hits)
	}

	// 对齐口径自检：Create/Replace 的拒 nil 契约保持不变
	if kl.Create("x", nil) {
		t.Fatal("Create(nil) 应返回 false")
	}
}
