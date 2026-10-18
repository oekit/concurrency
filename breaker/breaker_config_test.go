package breaker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 共享熔断器是进程级全局单例，本文件所有用例必须用互不相同的 name 命名空间，
// 否则用例之间会通过 sharedBreakers 相互串扰。

// ---- #28 导出缺省常量 ----

// TestExportedDefaults_MatchEffectiveConfig 锁定「导出的缺省 == 不传选项时的实际配置」：
// 这是 #28 的核心契约——下游引用 DefaultXxx 时拿到的必须与真实缺省一致。
func TestExportedDefaults_MatchEffectiveConfig(t *testing.T) {
	b := NewBreaker("defaults-probe").(*circuitBreaker)

	if got, want := b.gb.windowSize, DefaultBuckets; got != want {
		t.Errorf("windowSize = %d, want DefaultBuckets(%d)", got, want)
	}
	if got, want := b.gb.forcePass, DefaultForcePass; got != want {
		t.Errorf("forcePass = %v, want DefaultForcePass(%v)", got, want)
	}
	if got, want := b.gb.probeTimeout, DefaultProbeTimeout; got != want {
		t.Errorf("probeTimeout = %v, want DefaultProbeTimeout(%v)", got, want)
	}
	// 缺省窗口总长由 单桶宽度 × 桶数 还原，应精确等于 DefaultWindow
	if got, want := time.Duration(b.gb.windowSize)*(DefaultWindow/DefaultBuckets), DefaultWindow; got != want {
		t.Errorf("window total = %v, want DefaultWindow(%v)", got, want)
	}
	// 冷却期缺省为「未配置」= 0，生效值回落 probeTimeout
	if b.gb.cooldown != 0 {
		t.Errorf("cooldown = %v, want 0 (未配置)", b.gb.cooldown)
	}
	if got, want := time.Duration(b.gb.cooldownNanos()), DefaultProbeTimeout; got != want {
		t.Errorf("effective cooldown = %v, want fallback DefaultProbeTimeout(%v)", got, want)
	}
	// 计数型触发缺省关闭
	if b.gb.consecutive != 0 {
		t.Errorf("consecutive = %d, want 0 (缺省关闭计数型触发)", b.gb.consecutive)
	}
}

// TestWithCooldown_ZeroPanics 锁定「传 0 = 走缺省」在本包**不成立**——
// #28 issue 正文写作「WithCooldown(0) = 沿用 probeTimeout」，实测是 panic。
// 该用例是防止未来把入参校验放宽成"0 走缺省"而误改语义。
func TestWithCooldown_ZeroPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("WithCooldown(0) must panic: 传 0 走缺省在本包不存在，「用缺省」是不传该选项")
		}
	}()
	_ = WithCooldown(0)
}

// TestWithConsecutiveFailures_ZeroDisables 锁定 0 在计数型上的含义：合法，且等于关闭。
func TestWithConsecutiveFailures_ZeroDisables(t *testing.T) {
	b := NewBreaker("consec-zero", WithConsecutiveFailures(0)).(*circuitBreaker)
	if b.gb.consecutive != 0 {
		t.Fatalf("consecutive = %d, want 0", b.gb.consecutive)
	}
	// 关闭态下连发失败不应触发 open（缺省只有速率型，低样本受 protection 保护）
	for i := 0; i < 5; i++ {
		_ = b.Do(context.Background(), func() error { return context.Canceled })
	}
	if s := b.State(); s != StateClosed {
		t.Fatalf("State = %v, want closed (计数型已关闭)", s)
	}
}

// ---- #29 同名共享配置冲突可观测 ----

type conflictRecorder struct {
	mu      sync.Mutex
	details []string
}

func (r *conflictRecorder) handler(name, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.details = append(r.details, name+" | "+detail)
}

func (r *conflictRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.details...)
}

// TestGetBreaker_ConflictHandler_Fires 复现 #29 现场：同名不同 opts 被静默沿用首次配置。
// 本用例锁定「仍返回同一实例」（行为不变）+「但冲突回调被触发」（新增可观测信号）。
func TestGetBreaker_ConflictHandler_Fires(t *testing.T) {
	rec := &conflictRecorder{}

	first := GetBreaker("conflict-basic",
		WithConsecutiveFailures(3),
		WithCooldown(2*time.Minute),
		WithBreakerConflictHandler(rec.handler),
	)
	second := GetBreaker("conflict-basic",
		WithConsecutiveFailures(10),
		WithCooldown(30*time.Second),
	)

	if first != second {
		t.Fatal("同名必须返回同一实例（#29 只加可观测信号，不改共享语义）")
	}
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("conflict handler 触发 %d 次，want 1: %v", len(got), got)
	}
	// detail 必须同时点明 name、首次生效配置与本次传入配置，否则排查时仍需回源码比对
	for _, want := range []string{"conflict-basic", "consecutive=3", "consecutive=10", "cooldown=2m0s", "cooldown=30s"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("detail 缺少 %q:\n%s", want, got[0])
		}
	}
	// 首次生效配置不得被本次 opts 篡改
	if b := first.(*circuitBreaker); b.gb.consecutive != 3 {
		t.Fatalf("consecutive = %d, want 3 (首次配置不得被后续 opts 改写)", b.gb.consecutive)
	}
}

// TestGetBreaker_ConflictHandler_NoFalsePositive 不应告警的场景：纯复用、传入与首次
// 等价的 opts、以及仅钩子不同（指纹刻意排除函数字段）。
func TestGetBreaker_ConflictHandler_NoFalsePositive(t *testing.T) {
	rec := &conflictRecorder{}
	name := "conflict-nofalse"
	GetBreaker(name, WithForcePass(2*time.Second), WithBreakerConflictHandler(rec.handler))

	GetBreaker(name)                                                                      // 纯复用
	GetBreaker(name, WithForcePass(2*time.Second))                                        // 与首次等价
	GetBreaker(name, WithForcePass(2*time.Second), WithProbeTimeout(DefaultProbeTimeout)) // 显式写回缺省仍等价
	// 仅钩子不同、其余值语义字段与首次一致 → 指纹相同，不告警
	GetBreaker(name, WithForcePass(2*time.Second), WithBreakerRejectHandler(func(string, string) {}))

	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("等价配置不应告警，实际触发 %d 次: %v", len(got), got)
	}
}

// TestGetBreaker_Conflict_NilOptionMeansDefaults 锁定「传 nil 选项 = 用缺省」的语义：
// nil 被跳过，等价于按缺省配置构造。当首次创建带了非缺省 opts 时，这是一次真实的静默
// 丢弃（调用方以为拿到缺省、实际拿到首次那套），因此**应当**告警。
func TestGetBreaker_Conflict_NilOptionMeansDefaults(t *testing.T) {
	rec := &conflictRecorder{}
	name := "conflict-nilopt"
	GetBreaker(name, WithForcePass(2*time.Second), WithBreakerConflictHandler(rec.handler))

	GetBreaker(name, nil)

	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("传 nil（=缺省）与首次配置冲突时应告警 1 次，实际 %d 次: %v", len(got), got)
	}
	if !strings.Contains(got[0], "forcePass=1s") {
		t.Errorf("detail 应体现本次按缺省构造（forcePass=1s）:\n%s", got[0])
	}
}

// TestGetBreaker_ConflictHandler_WarnsOnce 同一 name 反复传入异参只报一次，
// 避免热路径上每次 GetBreaker 都刷一遍。
func TestGetBreaker_ConflictHandler_WarnsOnce(t *testing.T) {
	rec := &conflictRecorder{}
	name := "conflict-once"
	GetBreaker(name, WithBreakerConflictHandler(rec.handler))

	for i := 0; i < 20; i++ {
		GetBreaker(name, WithConsecutiveFailures(i+1))
	}
	if got := rec.snapshot(); len(got) != 1 {
		t.Fatalf("触发 %d 次，want 1（按 name 去重）", len(got))
	}
}

// TestGetBreaker_ConflictHandler_NoHandler 锁未注册回调时静默跳过，不 panic。
func TestGetBreaker_ConflictHandler_NoHandler(t *testing.T) {
	GetBreaker("conflict-nohandler", WithForcePass(time.Second))
	_ = GetBreaker("conflict-nohandler", WithForcePass(5*time.Second))
}

// TestGetBreaker_ConflictHandler_Reentrant 回调里反过来调 GetBreaker 不得死锁——
// 冲突派发必须在锁外。go test 默认 10m 超时，死锁会挂到超时。
func TestGetBreaker_ConflictHandler_Reentrant(t *testing.T) {
	var reentered atomic.Bool
	var inner atomic.Value
	name := "conflict-reentrant"

	GetBreaker(name, WithBreakerConflictHandler(func(n, detail string) {
		inner.Store(GetBreaker("conflict-reentrant-inner"))
		reentered.Store(true)
	}))
	_ = GetBreaker(name, WithConsecutiveFailures(5))

	if !reentered.Load() {
		t.Fatal("冲突回调未触发")
	}
	if b, ok := inner.Load().(Breaker); !ok || b == nil {
		t.Fatal("回调内 GetBreaker 未返回实例（可能已死锁或返回 nil）")
	}
}

// TestGetBreaker_Conflict_Concurrent 并发同名 + 不同 opts：实例唯一、冲突至多报一次。
// 配合 -race 覆盖 sharedEntry.warned 的 CAS 与 map 读写。
func TestGetBreaker_Conflict_Concurrent(t *testing.T) {
	var fired atomic.Int32
	name := "conflict-concurrent"
	GetBreaker(name, WithBreakerConflictHandler(func(string, string) { fired.Add(1) }))

	const n = 64
	var wg sync.WaitGroup
	results := make([]Breaker, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 一半纯复用（快路径），一半带不同 opts（冲突路径）
			if i%2 == 0 {
				results[i] = GetBreaker(name)
			} else {
				results[i] = GetBreaker(name, WithConsecutiveFailures(i))
			}
		}(i)
	}
	wg.Wait()

	for i := 1; i < n; i++ {
		if results[i] != results[0] {
			t.Fatalf("goroutine %d 拿到不同实例", i)
		}
	}
	if got := fired.Load(); got != 1 {
		t.Fatalf("冲突回调触发 %d 次，want 1", got)
	}
}

// TestConfigFingerprint_ExcludesHooks 指纹只覆盖值语义字段：仅钩子不同时指纹相同，
// 否则先注册 reject handler 的调用方会被后续每个调用方无谓告警一次。
func TestConfigFingerprint_ExcludesHooks(t *testing.T) {
	base := newBreakerConfig()
	withHooks := newBreakerConfig(
		WithBreakerRejectHandler(func(string, string) {}),
		WithBreakerConflictHandler(func(string, string) {}),
		WithBreakerClock(func() time.Time { return time.Unix(0, 0) }),
	)
	if base.fingerprint() != withHooks.fingerprint() {
		t.Fatal("仅钩子/时钟不同不应改变指纹")
	}
	diff := newBreakerConfig(WithForcePass(2 * time.Second))
	if base.fingerprint() == diff.fingerprint() {
		t.Fatal("forcePass 不同必须改变指纹")
	}
	consec := newBreakerConfig(WithConsecutiveFailures(3))
	if got := consec.fingerprint().String(); !strings.Contains(got, "consecutive=3") {
		t.Fatalf("fingerprint.String() = %q, 缺 consecutive=3", got)
	}
}
