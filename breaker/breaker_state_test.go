package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 状态机测试：假时钟确定性驱动 closed/open/half-open 迁移、代绑定、探测时限、
// 冷却期独立配置（WithCooldown）与计数型触发（WithConsecutiveFailures）。
// 全部断言确定性（探测放行绕过公式必达；窗口打满后概率丢弃的单发漏过概率 ≈ 1.2e-4，
// 仅用于"预期被拒"的统计裕量场景，与既有 openBreaker 口径一致）。

// stateTestStart 假时钟起点：取非零真实量级时刻（lastPass/计时依赖 UnixNano > 0 语义）。
var stateTestStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// newProbeTestBreaker 构造 10 桶 × 10ms 小窗口 + 假时钟 + 指定探测时限的熔断器。
func newProbeTestBreaker(clock *fakeClock, probe time.Duration, opts ...BreakerOption) *circuitBreaker {
	all := append([]BreakerOption{
		WithBreakerWindow(10, 10*time.Millisecond),
		WithBreakerClock(clock.Now),
		WithProbeTimeout(probe),
	}, opts...)
	return NewBreaker("breaker-state-test", all...).(*circuitBreaker)
}

// fillFailures 用连续失败把熔断器打到打开状态（冻结时钟，dropRatio → 1）。
func fillFailures(t *testing.T, cb *circuitBreaker) {
	t.Helper()
	boom := errors.New("boom")
	ctx := context.Background()
	for i := 0; i < 50000; i++ {
		_ = cb.Do(ctx, func() error { return boom })
	}
	if cb.gb.state != stateOpen {
		t.Fatalf("state = %v, want open after sustained failures", cb.gb.state)
	}
}

// TestState_ClosedToOpenThreshold 低错误率保持 closed；持续失败切入 open 并记录 openSince。
func TestState_ClosedToOpenThreshold(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	if b.gb.state != stateClosed {
		t.Fatalf("fresh breaker state = %v, want closed", b.gb.state)
	}
	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("healthy Do err = %v, want nil", err)
	}
	if b.gb.state != stateClosed {
		t.Fatalf("state after healthy call = %v, want closed", b.gb.state)
	}

	fillFailures(t, b)
	if b.gb.openSince != clock.Now().UnixNano() {
		t.Fatalf("openSince = %d, want frozen now %d", b.gb.openSince, clock.Now().UnixNano())
	}
	if b.gb.gen != 0 || b.gb.probeGen != 0 {
		t.Fatalf("gen=%d probeGen=%d, want 0/0 (no probe before deadline)", b.gb.gen, b.gb.probeGen)
	}
}

// TestState_ProbeGrantedAtProbeTimeout 探测截止前不探测；截止后首个放行即半开探测
// （绕过概率公式必达，携带新代）。
func TestState_ProbeGrantedAtProbeTimeout(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	// closed 态普通放行：携带当前代 0，不推进代、不置探测位
	p, gen, err := b.AllowGen(ctx)
	if err != nil || p == nil {
		t.Fatalf("AllowGen = (%v, %v), want pass", p, err)
	}
	if gen != 0 || b.gb.gen != 0 || b.gb.probeGen != 0 {
		t.Fatalf("gen=%d b.gen=%d probeGen=%d, want 0/0/0 on normal grant", gen, b.gb.gen, b.gb.probeGen)
	}
	p.Pass()

	fillFailures(t, b)
	// 冻结时钟：openSince 距今 0 < 1m，不探测（确定性）
	if _, _, err = b.AllowGen(ctx); err != nil && !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("AllowGen before deadline err = %v", err)
	}
	if b.gb.probeGen != 0 {
		t.Fatalf("probeGen = %d, want 0 before probe timeout", b.gb.probeGen)
	}

	// 推进到探测截止：本次放行即探测，必达且携带新代
	clock.Advance(time.Minute)
	p, gen, err = b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v, want nil (probe bypasses formula)", err)
	}
	if gen != 1 || b.gb.probeGen != 1 || b.gb.gen != 1 {
		t.Fatalf("gen=%d probeGen=%d b.gen=%d, want 1/1/1 on probe grant", gen, b.gb.probeGen, b.gb.gen)
	}
	if b.gb.state != stateHalfOpen {
		t.Fatalf("state = %v, want half-open after probe grant", b.gb.state)
	}
	p.Pass()
}

// TestState_ProbeSuccessClosesAndClearsWindow 探测成功关闸：half-open → closed，
// 清空窗口（旧失败样本不得触发立即重开），探测成功样本作为新窗口首个样本。
func TestState_ProbeSuccessClosesAndClearsWindow(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(time.Minute)
	p, _, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v", err)
	}
	p.Pass()

	if b.gb.state != stateClosed || b.gb.probeGen != 0 {
		t.Fatalf("state=%v probeGen=%d, want closed/0 after probe success", b.gb.state, b.gb.probeGen)
	}
	total, failure := sumBucket(b.gb)
	if total != 1 || failure != 0 {
		t.Fatalf("window after close: total=%d failure=%d, want 1/0 (cleared + probe sample)", total, failure)
	}

	// closed 后健康放行不重开（清窗后 dropRatio ≤ 0，阈值不触发）
	for i := 0; i < 50; i++ {
		pp, gen, err := b.AllowGen(ctx)
		if err != nil {
			t.Fatalf("call %d after close err = %v, want pass", i, err)
		}
		if gen != 1 {
			t.Fatalf("call %d gen = %d, want 1 (generation advances only on probe grants)", i, gen)
		}
		pp.Pass()
	}
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed to persist", b.gb.state)
	}

	// 再次持续失败 → 重新 open（状态机循环往复）
	fillFailures(t, b)
}

// TestState_ProbeFailureReopensAndResetsTimer 探测失败重开并重置计时：
// 新冷却从本次失败起算，期满后才放行下一个探测（携带更新的代）。
func TestState_ProbeFailureReopensAndResetsTimer(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(time.Minute)
	p, _, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v", err)
	}
	p.Fail()

	if b.gb.state != stateOpen || b.gb.probeGen != 0 {
		t.Fatalf("state=%v probeGen=%d, want open/0 after probe failure", b.gb.state, b.gb.probeGen)
	}
	resetAt := clock.Now().UnixNano()
	if b.gb.openSince != resetAt {
		t.Fatalf("openSince = %d, want reset to %d", b.gb.openSince, resetAt)
	}
	if _, failure := sumBucket(b.gb); failure == 0 {
		t.Fatal("probe failure must be recorded as failure sample")
	}

	// 新冷却期内不探测（59s < 60s）
	clock.Advance(59 * time.Second)
	if _, _, err = b.AllowGen(ctx); err != nil && !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("AllowGen within new cooldown err = %v", err)
	}
	if b.gb.probeGen != 0 || b.gb.gen != 1 {
		t.Fatalf("probeGen=%d gen=%d, want 0/1 within cooldown", b.gb.probeGen, b.gb.gen)
	}

	// 期满放行下一个探测：代推进为 2
	clock.Advance(time.Second)
	_, gen, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("next probe grant err = %v", err)
	}
	if gen != 2 || b.gb.probeGen != 2 {
		t.Fatalf("gen=%d probeGen=%d, want 2/2 on next probe", gen, b.gb.probeGen)
	}
}

// TestState_OpenPeriodFailureDoesNotExtendCooldown 开放期失败只记样本、不延长冷却：
// openSince 不因普通失败回报后移，冷却期满照常放行探测。
func TestState_OpenPeriodFailureDoesNotExtendCooldown(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	if b.gb.openSince != stateTestStart.UnixNano() {
		t.Fatalf("openSince = %d, want %d", b.gb.openSince, stateTestStart.UnixNano())
	}

	// 开放期两次普通失败回报（当前代 0、非探测）：计入窗口但不触碰 openSince
	before, beforeFailure := sumBucket(b.gb)
	b.FailureGen(ctx, 0)
	b.FailureGen(ctx, 0)
	after, afterFailure := sumBucket(b.gb)
	if after-before != 2 || afterFailure-beforeFailure != 2 {
		t.Fatalf("open-period failures not counted: total %d→%d failure %d→%d",
			before, after, beforeFailure, afterFailure)
	}
	if b.gb.openSince != stateTestStart.UnixNano() {
		t.Fatalf("openSince = %d, want unchanged %d", b.gb.openSince, stateTestStart.UnixNano())
	}

	// 冷却期满（openSince 起算 60s）照常探测——若 openSince 被后移此处将拿不到探测
	clock.Advance(time.Minute)
	_, gen, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v, want nil (cooldown not extended)", err)
	}
	if gen != 1 {
		t.Fatalf("gen = %d, want 1", gen)
	}
}

// TestState_StaleProbeDoesNotStallBreaker 挂死探测不阻塞新探测：超时回收后放行新探测，
// 旧探测迟到回报按陈旧代整体作废（不计窗口、不迁移），新探测成功照常关闸。
func TestState_StaleProbeDoesNotStallBreaker(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(time.Minute)
	p1, gen1, err := b.AllowGen(ctx)
	if err != nil || gen1 != 1 {
		t.Fatalf("first probe = (%v, %d), want grant gen 1", err, gen1)
	}
	_ = p1 // 挂死：永不回报

	// 探测时限到期：回收挂死探测并放行新探测（不阻塞）
	clock.Advance(time.Minute)
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("second probe grant err = %v, want nil (hung probe recycled)", err)
	}
	if gen2 != 2 || b.gb.probeGen != 2 {
		t.Fatalf("gen=%d probeGen=%d, want 2/2", gen2, b.gb.probeGen)
	}

	// 旧探测迟到失败回报：陈旧代整体作废
	before, beforeFailure := sumBucket(b.gb)
	p1.Fail()
	after, afterFailure := sumBucket(b.gb)
	if after != before || afterFailure != beforeFailure {
		t.Fatalf("stale report counted: total %d→%d failure %d→%d", before, after, beforeFailure, afterFailure)
	}
	if b.gb.probeGen != 2 || b.gb.state != stateHalfOpen {
		t.Fatalf("probeGen=%d state=%v, want 2/half-open (stale report must not interfere)", b.gb.probeGen, b.gb.state)
	}

	// 新探测成功 → 关闸
	p2.Pass()
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed (breaker not stalled by stale probe)", b.gb.state)
	}
}

// TestState_StaleProbeGenIgnored 陈旧探测回报不重置冷却：探测失败重开后，
// 旧代迟到的失败回报不得把 openSince 后移（冷却不被陈旧探测延长）。
func TestState_StaleProbeGenIgnored(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(time.Minute)
	p1, _, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v", err)
	}
	p1.Fail() // 探测失败 → 重开，openSince = T0+1m

	resetAt := clock.Now().UnixNano()
	clock.Advance(time.Minute)
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("second probe = (%v, %d), want grant gen 2", err, gen2)
	}

	// 旧探测（gen 1）迟到失败回报：必须被代校验丢弃
	p1.Fail()
	if b.gb.openSince != resetAt {
		t.Fatalf("openSince = %d, want unchanged %d (stale report must not extend cooldown)", b.gb.openSince, resetAt)
	}
	if b.gb.probeGen != 2 || b.gb.state != stateHalfOpen {
		t.Fatalf("probeGen=%d state=%v, want 2/half-open", b.gb.probeGen, b.gb.state)
	}

	// 陈旧成功回报同样作废：不计窗口、不关闸
	p2.Pass()
	before, _ := sumBucket(b.gb)
	p1.Pass()
	after, _ := sumBucket(b.gb)
	if after != before {
		t.Fatalf("stale success counted: total %d→%d", before, after)
	}
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed (current probe success)", b.gb.state)
	}
}

// TestState_AbandonGen 显式放弃探测：立即腾出探测位，冷却已满时下一放行即刻补探测；
// 放弃非探测代为 no-op；放弃不触碰 openSince；被放弃探测的迟到回报按陈旧代作废。
func TestState_AbandonGen(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(time.Minute)
	p1, gen1, err := b.AllowGen(ctx)
	if err != nil || gen1 != 1 {
		t.Fatalf("probe grant = (%v, %d), want gen 1", err, gen1)
	}

	// 放弃非探测代：no-op，探测位不动
	b.AbandonGen(0)
	b.AbandonGen(99)
	if b.gb.probeGen != 1 {
		t.Fatalf("probeGen = %d, want 1 (abandon of non-probe gen must be no-op)", b.gb.probeGen)
	}

	// 显式放弃当前探测：腾出探测位，openSince 不变
	if b.gb.openSince != stateTestStart.UnixNano() {
		t.Fatalf("openSince = %d, want %d before abandon", b.gb.openSince, stateTestStart.UnixNano())
	}
	b.AbandonGen(1)
	if b.gb.probeGen != 0 || b.gb.state != stateOpen {
		t.Fatalf("probeGen=%d state=%v, want 0/open after abandon", b.gb.probeGen, b.gb.state)
	}
	if b.gb.openSince != stateTestStart.UnixNano() {
		t.Fatalf("openSince = %d, want unchanged after abandon", b.gb.openSince)
	}

	// 冷却已满：下一放行即刻补探测（携带新代），旧探测迟到回报作废
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("replacement probe grant err = %v, want nil", err)
	}
	if gen2 != 2 || b.gb.probeGen != 2 {
		t.Fatalf("gen=%d probeGen=%d, want 2/2", gen2, b.gb.probeGen)
	}
	before, _ := sumBucket(b.gb)
	p1.Fail()
	after, _ := sumBucket(b.gb)
	if after != before {
		t.Fatalf("abandoned probe late report counted: total %d→%d", before, after)
	}
	p2.Pass()
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed", b.gb.state)
	}
}

// TestState_FormulaHealDoesNotAutoClose 公式愈合不自动关闸：open 态窗口滚空后全量放行
// （放行策略与历史行为一致），但状态保持 open、探测照常调度——关闸只由探测成功驱动。
func TestState_FormulaHealDoesNotAutoClose(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Second) // 探测时限 1s，短于整窗滚出后的等待
	ctx := context.Background()

	fillFailures(t, b)
	// 探测时限先到（1s）：冷却期内 0 < 1s，不探测；此时窗口尚未滚空
	clock.Advance(500 * time.Millisecond)
	if b.gb.probeGen != 0 {
		t.Fatalf("probeGen = %d, want 0 before deadline", b.gb.probeGen)
	}
	// 整窗滚出（10 桶 × 10ms = 100ms < 500ms，窗口已空）：公式愈合 → 全量放行
	for i := 0; i < 10; i++ {
		p, gen, err := b.AllowGen(ctx)
		if err != nil {
			t.Fatalf("call %d after window expiry err = %v, want pass", i, err)
		}
		if gen != 0 || b.gb.probeGen != 0 {
			t.Fatalf("call %d gen=%d probeGen=%d, want 0/0 (healed formula passes, no probe yet)", i, gen, b.gb.probeGen)
		}
		p.Pass()
	}
	if b.gb.state != stateOpen {
		t.Fatalf("state = %v, want open (formula heal must not auto-close)", b.gb.state)
	}

	// 探测截止到：照常放行探测，成功后显式关闸
	clock.Advance(500 * time.Millisecond)
	_, gen, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("probe grant err = %v", err)
	}
	if gen != 1 {
		t.Fatalf("gen = %d, want 1", gen)
	}
}

// TestState_DoFamilyGenTransparent Do 家族透传代绑定：命中半开的 Do 调用即探测，
// 其结果经代回报驱动迁移——成功关闸、失败重开重置计时。
func TestState_DoFamilyGenTransparent(t *testing.T) {
	boom := errors.New("boom")

	// 成功路径：Do 即探测，nil → 关闸
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()
	fillFailures(t, b)
	clock.Advance(time.Minute)
	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("probe Do err = %v, want nil", err)
	}
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed (Do executed the probe)", b.gb.state)
	}

	// 失败路径：探测 Do 拿到业务错误原样返回，状态重开重置计时
	clock2 := &fakeClock{}
	clock2.Set(stateTestStart)
	b2 := newProbeTestBreaker(clock2, time.Minute)
	fillFailures(t, b2)
	clock2.Advance(time.Minute)
	if err := b2.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("probe Do err = %v, want boom (probe must execute)", err)
	}
	if b2.gb.state != stateOpen || b2.gb.probeGen != 0 {
		t.Fatalf("state=%v probeGen=%d, want open/0 after failed probe Do", b2.gb.state, b2.gb.probeGen)
	}
	if b2.gb.openSince != clock2.Now().UnixNano() {
		t.Fatalf("openSince = %d, want reset to %d", b2.gb.openSince, clock2.Now().UnixNano())
	}
}

// TestState_GenAPIAndCtxGate Gen API 语义：SuccessGen/FailureGen 按代记样本；
// 已取消 ctx 的失败回报不消耗统计；GetBreaker 单例可断言到 GenBreaker。
func TestState_GenAPIAndCtxGate(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()

	var _ GenBreaker = b // *circuitBreaker 必须实现扩展接口
	if _, ok := GetBreaker("gen-api-svc").(GenBreaker); !ok {
		t.Fatal("GetBreaker instance must implement GenBreaker")
	}

	b.SuccessGen(0)
	b.FailureGen(ctx, 0)
	total, failure := sumBucket(b.gb)
	if total != 2 || failure != 1 {
		t.Fatalf("total=%d failure=%d, want 2/1", total, failure)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	b.FailureGen(canceled, 0)
	if total, _ := sumBucket(b.gb); total != 2 {
		t.Fatalf("total = %d, want 2 (canceled ctx must not consume stats)", total)
	}

	// 陈旧代回报整体作废（此时代 0 即当前代——用探测推进制造代差）
	fillFailures(t, b)
	clock.Advance(time.Minute)
	p, gen, err := b.AllowGen(ctx)
	if err != nil || gen != 1 {
		t.Fatalf("probe grant = (%v, %d), want gen 1", err, gen)
	}
	before, _ := sumBucket(b.gb)
	b.SuccessGen(0) // 代 0 < 当前代 1：作废
	after, _ := sumBucket(b.gb)
	if after != before {
		t.Fatalf("stale gen report counted: total %d→%d", before, after)
	}
	p.Pass()
}

// TestWithProbeTimeout_Validation 探测时限非法值 panic，与 WithForcePass 同口径。
func TestWithProbeTimeout_Validation(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("WithProbeTimeout(%v) should panic", d)
				}
			}()
			WithProbeTimeout(d)
		}()
	}
}

// TestWithCooldown_Validation 冷却期非法值 panic，与 WithProbeTimeout 同口径；
// 0 不是合法冷却期（缺省不设即回落 probeTimeout，无需显式 0）。
func TestWithCooldown_Validation(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("WithCooldown(%v) should panic", d)
				}
			}()
			WithCooldown(d)
		}()
	}
}

// TestState_CooldownOverridesProbeTimeout 冷却期独立于探测失联时限（#19）：
// WithCooldown(10ms) < probeTimeout(1m) 时，探测按冷却期放行（10ms 即达，而非 1m）；
// 探测失败重开后冷却重新起算，probeTimeout 不再充当冷却。
func TestState_CooldownOverridesProbeTimeout(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithCooldown(10*time.Millisecond))
	ctx := context.Background()

	fillFailures(t, b)

	// 冷却期 10ms 期满：放行探测（若仍绑定 probeTimeout 此处拿不到探测）
	clock.Advance(10 * time.Millisecond)
	p1, gen1, err := b.AllowGen(ctx)
	if err != nil || gen1 != 1 {
		t.Fatalf("first probe = (%v, %d), want grant gen 1 at cooldown expiry", err, gen1)
	}
	p1.Fail()
	if b.gb.state != stateOpen || b.gb.probeGen != 0 {
		t.Fatalf("state=%v probeGen=%d, want open/0 after probe failure", b.gb.state, b.gb.probeGen)
	}

	// 重开后新冷却期内不探测（刚过 5ms < 10ms）
	clock.Advance(5 * time.Millisecond)
	if _, _, err = b.AllowGen(ctx); err != nil && !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("AllowGen within new cooldown err = %v", err)
	}
	if b.gb.probeGen != 0 {
		t.Fatalf("probeGen = %d, want 0 within new cooldown", b.gb.probeGen)
	}

	// 新冷却期满（重开起 10ms）：放行下一个探测（绑定 probeTimeout 需等 1m）
	clock.Advance(5 * time.Millisecond)
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("second probe = (%v, %d), want grant gen 2 at new cooldown expiry", err, gen2)
	}
	p2.Pass()
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed", b.gb.state)
	}
}

// TestState_ProbeTimeoutStillBoundsHungProbe 探测失联时限仍由 probeTimeout 把守（#19）：
// 冷却早已期满但探测位被挂死探测占住时不得补发新探测；probeTimeout 到期回收后放行替换探测。
func TestState_ProbeTimeoutStillBoundsHungProbe(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithCooldown(10*time.Millisecond))
	ctx := context.Background()

	fillFailures(t, b)
	clock.Advance(10 * time.Millisecond)
	p1, gen1, err := b.AllowGen(ctx)
	if err != nil || gen1 != 1 {
		t.Fatalf("first probe = (%v, %d), want grant gen 1", err, gen1)
	}
	_ = p1 // 挂死：永不回报

	// 冷却早已期满，但探测位被占：不得按冷却期补发新探测（失联回收只认 probeTimeout）
	clock.Advance(10 * time.Millisecond)
	_, _, _ = b.AllowGen(ctx)
	if b.gb.probeGen != 1 {
		t.Fatalf("probeGen = %d, want 1 (hung probe holds the slot; cooldown expiry must not grant replacement)", b.gb.probeGen)
	}

	// probeTimeout 到期（自 probeAt 起 1m）：回收挂死探测并放行替换探测
	clock.Advance(time.Minute - 10*time.Millisecond)
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("replacement probe = (%v, %d), want grant gen 2 after hung-probe recycle", err, gen2)
	}
	p2.Pass()
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed", b.gb.state)
	}
}

// TestState_CooldownLongerThanProbeTimeout 冷却期长于探测时限（#19 动机场景方向）：
// 业务冷却 2m 不被 probeTimeout=1m 缩短——1m 时不探测、2m 才探测；探测失败重开后
// 冷却自重开起重新走满 2m。
func TestState_CooldownLongerThanProbeTimeout(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithCooldown(2*time.Minute))
	ctx := context.Background()

	fillFailures(t, b)

	// probeTimeout（1m）先到但冷却 2m 未满：不探测（v0.9.0 绑定行为会在 1m 放行）
	clock.Advance(time.Minute)
	if _, _, err := b.AllowGen(ctx); err != nil && !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("AllowGen before cooldown err = %v", err)
	}
	if b.gb.probeGen != 0 {
		t.Fatalf("probeGen = %d, want 0 (cooldown 2m governs, probeTimeout must not shorten it)", b.gb.probeGen)
	}

	// 冷却期满：放行探测
	clock.Advance(time.Minute)
	p1, gen1, err := b.AllowGen(ctx)
	if err != nil || gen1 != 1 {
		t.Fatalf("first probe = (%v, %d), want grant gen 1 at 2m cooldown", err, gen1)
	}
	p1.Fail()

	// 重开后冷却重新起算 2m：1m 处不探测
	clock.Advance(time.Minute)
	if _, _, err = b.AllowGen(ctx); err != nil && !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("AllowGen within reopened cooldown err = %v", err)
	}
	if b.gb.probeGen != 0 {
		t.Fatalf("probeGen = %d, want 0 within reopened cooldown", b.gb.probeGen)
	}

	// 重开起 2m 期满：放行下一个探测
	clock.Advance(time.Minute)
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("second probe = (%v, %d), want grant gen 2", err, gen2)
	}
	p2.Pass()
}

// TestWithConsecutiveFailures_Validation 计数型触发阈值校验：负值 panic；
// 0 合法（= 关闭，缺省即 0）。
func TestWithConsecutiveFailures_Validation(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("WithConsecutiveFailures(-1) should panic")
			}
		}()
		WithConsecutiveFailures(-1)
	}()
	WithConsecutiveFailures(0) // 不 panic 即通过
}

// TestState_ConsecutiveFailuresTrip 计数型触发（#20）：连续失败达阈值切入 open——
// 窗口 total=3 < protection=5，速率型不可能先触发，证明触发来自计数型；
// 进入 open 时计数归零、openSince 自触发失败起算。
func TestState_ConsecutiveFailuresTrip(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithConsecutiveFailures(3))
	ctx := context.Background()
	boom := errors.New("boom")
	fail := func() error { return boom }

	if err := b.Do(ctx, fail); !errors.Is(err, boom) {
		t.Fatalf("call 1 err = %v, want boom", err)
	}
	if err := b.Do(ctx, fail); !errors.Is(err, boom) {
		t.Fatalf("call 2 err = %v, want boom", err)
	}
	if b.gb.state != stateClosed || b.gb.consecFails != 2 {
		t.Fatalf("state=%v consecFails=%d, want closed/2 after 2 failures", b.gb.state, b.gb.consecFails)
	}

	if err := b.Do(ctx, fail); !errors.Is(err, boom) {
		t.Fatalf("call 3 err = %v, want boom", err)
	}
	if b.gb.state != stateOpen {
		t.Fatalf("state = %v, want open on 3rd consecutive failure", b.gb.state)
	}
	if b.gb.openSince != clock.Now().UnixNano() {
		t.Fatalf("openSince = %d, want %d (trip failure time)", b.gb.openSince, clock.Now().UnixNano())
	}
	if b.gb.consecFails != 0 {
		t.Fatalf("consecFails = %d, want 0 (reset on entering open)", b.gb.consecFails)
	}
	if b.gb.gen != 0 || b.gb.probeGen != 0 {
		t.Fatalf("gen=%d probeGen=%d, want 0/0 (trip must not grant a probe)", b.gb.gen, b.gb.probeGen)
	}
	total, failure := sumBucket(b.gb)
	if total != 3 || failure != 3 {
		t.Fatalf("window total=%d failure=%d, want 3/3 (samples still recorded)", total, failure)
	}
}

// TestState_ConsecutiveFailuresResetOnSuccess 连续失败计数清零：成功回报（SuccessGen/
// 可接受的 Do）清零计数，重新累计后才可能触发。
func TestState_ConsecutiveFailuresResetOnSuccess(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithConsecutiveFailures(3))
	ctx := context.Background()
	boom := errors.New("boom")

	// FailureGen 路径同样计数
	b.FailureGen(ctx, 0)
	b.FailureGen(ctx, 0)
	if b.gb.consecFails != 2 {
		t.Fatalf("consecFails = %d, want 2 (FailureGen must count)", b.gb.consecFails)
	}
	b.SuccessGen(0)
	if b.gb.consecFails != 0 || b.gb.state != stateClosed {
		t.Fatalf("consecFails=%d state=%v, want 0/closed after success", b.gb.consecFails, b.gb.state)
	}

	// 重新累计：2 次失败不触发，第 3 次触发
	fail := func() error { return boom }
	_ = b.Do(ctx, fail)
	_ = b.Do(ctx, fail)
	if b.gb.state != stateClosed || b.gb.consecFails != 2 {
		t.Fatalf("state=%v consecFails=%d, want closed/2", b.gb.state, b.gb.consecFails)
	}
	_ = b.Do(ctx, fail)
	if b.gb.state != stateOpen {
		t.Fatalf("state = %v, want open on 3rd consecutive failure", b.gb.state)
	}
}

// TestState_ConsecutiveFailuresDisabledByDefault 缺省关闭（#20 兼容性）：
// 不设 WithConsecutiveFailures 时连续失败 3 次不触发（窗口 total=3 < protection=5，
// 速率型同样沉默——行为与 v0.9.0 一致）。
func TestState_ConsecutiveFailuresDisabledByDefault(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute)
	ctx := context.Background()
	boom := errors.New("boom")

	for i := 0; i < 3; i++ {
		if err := b.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("call %d err = %v, want boom", i+1, err)
		}
	}
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed (consecutive trigger is off by default)", b.gb.state)
	}
}

// TestState_ConsecutiveTripProbeCycle 计数型触发的完整生命周期：触发 open → 冷却期满
// 半开探测成功关闸（计数归零）→ 关闸后计数从零起算，需重新累计满阈值才再次触发。
func TestState_ConsecutiveTripProbeCycle(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, time.Minute, WithConsecutiveFailures(3))
	ctx := context.Background()
	boom := errors.New("boom")
	fail := func() error { return boom }

	for i := 0; i < 3; i++ {
		_ = b.Do(ctx, fail)
	}
	if b.gb.state != stateOpen {
		t.Fatalf("state = %v, want open after trip", b.gb.state)
	}

	// 冷却期满：半开探测（Do nil 即探测）成功关闸
	clock.Advance(time.Minute)
	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("probe Do err = %v, want nil", err)
	}
	if b.gb.state != stateClosed || b.gb.consecFails != 0 {
		t.Fatalf("state=%v consecFails=%d, want closed/0 after probe success", b.gb.state, b.gb.consecFails)
	}

	// 关闸后计数从零起算：2 次失败不重开
	_ = b.Do(ctx, fail)
	_ = b.Do(ctx, fail)
	if b.gb.state != stateClosed {
		t.Fatalf("state = %v, want closed (counter restarted from 0 after close)", b.gb.state)
	}

	// 第 3 次失败重新触发
	_ = b.Do(ctx, fail)
	if b.gb.state != stateOpen {
		t.Fatalf("state = %v, want open on fresh 3rd consecutive failure", b.gb.state)
	}
}

// TestState_ConcurrentAllowReport 并发放行与回报的状态机一致性（-race 看门）：
// 多 goroutine 同时 AllowGen/Pass/Fail/AbandonGen + 时钟推进驱动探测调度与回收，
// 全程无数据竞争、无 panic（状态迁移自洽性由代校验保证）。
func TestState_ConcurrentAllowReport(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newProbeTestBreaker(clock, 10*time.Millisecond)
	ctx := context.Background()

	fillFailures(t, b)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 时钟推进线程：驱动探测调度/超时回收/窗口滚动
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clock.Advance(5 * time.Millisecond)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 放行/回报线程：覆盖普通放行、探测放行、探测迁移、显式放弃全路径
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, gen, err := b.AllowGen(ctx)
				if err == nil {
					if i%2 == 0 {
						p.Pass()
					} else {
						p.Fail()
					}
				}
				b.AbandonGen(gen) // 拒绝时 gen=0：非探测代，安全 no-op
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
