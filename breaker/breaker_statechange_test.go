package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 状态迁移观测钩子（#351）测试：closed→open（速率型/计数型触发）、
// open→half-open（开始探测）、half-open→closed（探测成功）、half-open→open
// （探测失败重开）各恰好上报一次；拒绝回调与迁移回调语义分离；ctx 取消、
// 探测失联回收（超时回收 / AbandonGen）不产生迁移事件。

// stateChangeEvent 为一次迁移回调的快照。
type stateChangeEvent struct {
	name string
	from State
	to   State
}

// stateChangeRecorder 记录迁移回调。回调在状态机锁内执行，本记录器只做
// 内存追加，不反向调用 breaker（与 WithBreakerStateChangeHandler 文档约束一致）。
type stateChangeRecorder struct {
	mu     sync.Mutex
	events []stateChangeEvent
}

func (r *stateChangeRecorder) record(name string, from, to State) {
	r.mu.Lock()
	r.events = append(r.events, stateChangeEvent{name: name, from: from, to: to})
	r.mu.Unlock()
}

func (r *stateChangeRecorder) snapshot() []stateChangeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]stateChangeEvent(nil), r.events...)
}

// assertStateChanges 断言迁移事件序列逐项相等（同时校验顺序与 name）。
func assertStateChanges(t *testing.T, got, want []stateChangeEvent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("state change events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("state change event[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestStateChangeHandler_ConsecutiveTripExactlyOnce 计数型触发恰好一次
// closed→open；拒绝回调按「每次被拒」增长，两个钩子语义不混用。
func TestStateChangeHandler_ConsecutiveTripExactlyOnce(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	var rejects atomic.Int64
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(3),
		WithBreakerStateChangeHandler(rec.record),
		WithBreakerRejectHandler(func(name, reason string) { rejects.Add(1) }),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	for i := range 3 {
		if err := b.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("call %d err = %v, want boom", i+1, err)
		}
	}
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
	})
	if got := rejects.Load(); got != 0 {
		t.Fatalf("reject handler calls = %d, want 0 (tripping request itself is not rejected)", got)
	}

	// 熔断后继续打：每次被拒都触发拒绝回调，但不产生新的迁移事件
	for range 10 {
		if err := b.Do(ctx, func() error { return boom }); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("open-state Do err = %v, want ErrCircuitOpen", err)
		}
	}
	if got := rejects.Load(); got != 10 {
		t.Fatalf("reject handler calls = %d, want 10 (per-rejection semantics)", got)
	}
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
	})
}

// TestStateChangeHandler_RateTripExactlyOnce 速率型触发同样恰好一次 closed→open，
// 触发后的持续拒绝不重复上报。
func TestStateChangeHandler_RateTripExactlyOnce(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute, WithBreakerStateChangeHandler(rec.record))

	fillFailures(t, b) // 窗口打满，速率型触发
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
	})

	ctx := context.Background()
	boom := errors.New("boom")
	for range 100 {
		_ = b.Do(ctx, func() error { return boom })
	}
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
	})
}

// TestStateChangeHandler_HalfOpenRoundTrip 三态往返全部上报且顺序正确：
// closed→open（触发）→ open→half-open（探测）→ half-open→closed（恢复）
// → closed→open（再次触发）→ open→half-open → half-open→open（探测失败重开）。
func TestStateChangeHandler_HalfOpenRoundTrip(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(2),
		WithBreakerStateChangeHandler(rec.record),
	)
	ctx := context.Background()
	boom := errors.New("boom")
	fail := func() error { return boom }

	// 第一次触发：计数型 closed→open
	for range 2 {
		_ = b.Do(ctx, fail)
	}
	// open→half-open：冷却期满开始探测
	clock.Advance(time.Minute)
	p, _, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("first probe grant err = %v", err)
	}
	// half-open→closed：探测成功恢复
	p.Pass()

	// 第二次触发：计数从零重新累计，closed→open
	for range 2 {
		_ = b.Do(ctx, fail)
	}
	// open→half-open：再次探测
	clock.Advance(time.Minute)
	p2, _, err := b.AllowGen(ctx)
	if err != nil {
		t.Fatalf("second probe grant err = %v", err)
	}
	// half-open→open：探测失败重开
	p2.Fail()

	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateHalfOpen, to: StateClosed},
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateHalfOpen, to: StateOpen},
	})
}

// TestStateChangeHandler_ParallelFailureTripOnce 并行 N 次失败只报一次迁移：
// 多个请求同时在 closed 态放行并失败，首个回报触发后其余回报不得重复上报。
func TestStateChangeHandler_ParallelFailureTripOnce(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerStateChangeHandler(rec.record),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	const n = 128
	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			start.Wait()
			_ = b.Do(ctx, func() error { return boom })
		})
	}
	start.Done()
	wg.Wait()

	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
	})
	if b.State() != StateOpen {
		t.Fatalf("State() = %v, want StateOpen", b.State())
	}
}

// TestStateChangeHandler_AbandonSilent AbandonGen 显式放弃探测只腾出探测位，
// 不产生 half-open→open 迁移事件；被放弃探测的迟到回报也不得驱动迁移。
func TestStateChangeHandler_AbandonSilent(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerStateChangeHandler(rec.record),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	_ = b.Do(ctx, func() error { return boom }) // closed→open
	clock.Advance(time.Minute)
	p, gen, err := b.AllowGen(ctx) // open→half-open
	if err != nil || gen != 1 {
		t.Fatalf("probe grant = (%v, %d), want gen 1", err, gen)
	}

	b.AbandonGen(gen) // 静默：half-open→open 内部回收
	if b.State() != StateOpen {
		t.Fatalf("State() after abandon = %v, want StateOpen", b.State())
	}
	want := []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
	}
	assertStateChanges(t, rec.snapshot(), want)

	// 被放弃探测的迟到失败回报：非探测样本，不迁移、不上报
	p.Fail()
	assertStateChanges(t, rec.snapshot(), want)

	// 冷却已满：下一放行补放新探测（open→half-open 正常上报），成功关闸
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("replacement probe grant = (%v, %d), want gen 2", err, gen2)
	}
	p2.Pass()
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateHalfOpen, to: StateClosed},
	})
}

// TestStateChangeHandler_ProbeTimeoutReclaimSilent 探测失联超时回收是内部清理：
// 半开 → open 不产生迁移事件（随后补放的新探测照常上报 open→half-open）。
func TestStateChangeHandler_ProbeTimeoutReclaimSilent(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerStateChangeHandler(rec.record),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	_ = b.Do(ctx, func() error { return boom }) // closed→open
	clock.Advance(time.Minute)
	p1, gen1, err := b.AllowGen(ctx) // open→half-open
	if err != nil || gen1 != 1 {
		t.Fatalf("probe grant = (%v, %d), want gen 1", err, gen1)
	}
	clock.Advance(time.Minute) // 超过探测时限：失联回收

	// 本次放行先静默回收旧探测（half-open→open），再补放新探测（open→half-open）
	p2, gen2, err := b.AllowGen(ctx)
	if err != nil || gen2 != 2 {
		t.Fatalf("replacement probe grant = (%v, %d), want gen 2", err, gen2)
	}
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
	})

	// 失联探测的迟到回报按陈旧代作废，不产生迁移
	p1.Fail()
	if b.State() != StateHalfOpen {
		t.Fatalf("State() after stale report = %v, want StateHalfOpen", b.State())
	}
	if got := len(rec.snapshot()); got != 3 {
		t.Fatalf("state change events = %d, want 3 (stale report must not migrate)", got)
	}
	p2.Pass()
}

// TestStateChangeHandler_CtxCancelSilent ctx 取消不消耗统计、不产生迁移回调：
// 取消 ctx 的放行与探测失败回报都被 ctx 门挡下；随后正常的探测成功仍照常上报。
func TestStateChangeHandler_CtxCancelSilent(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerStateChangeHandler(rec.record),
	)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := b.Allow(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Allow(canceled) err = %v, want context.Canceled", err)
	}
	ran := false
	if err := b.Do(canceled, func() error { ran = true; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do(canceled) err = %v, want context.Canceled", err)
	}
	if ran {
		t.Fatal("execute must not run on canceled ctx")
	}
	b.FailureGen(canceled, 0)
	if b.State() != StateClosed {
		t.Fatalf("State() = %v, want StateClosed (canceled ctx must not trip)", b.State())
	}
	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("state change events = %d, want 0", got)
	}

	// 正常触发半开后，取消 ctx 的探测失败回报不得驱动 half-open→open
	ctx := context.Background()
	_ = b.Do(ctx, func() error { return errors.New("boom") }) // closed→open
	clock.Advance(time.Minute)
	p, gen, err := b.AllowGen(ctx) // open→half-open
	if err != nil || gen != 1 {
		t.Fatalf("probe grant = (%v, %d), want gen 1", err, gen)
	}
	b.FailureGen(canceled, gen)
	if b.State() != StateHalfOpen {
		t.Fatalf("State() after canceled report = %v, want StateHalfOpen", b.State())
	}
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
	})
	p.Pass()
	assertStateChanges(t, rec.snapshot(), []stateChangeEvent{
		{name: b.Name(), from: StateClosed, to: StateOpen},
		{name: b.Name(), from: StateOpen, to: StateHalfOpen},
		{name: b.Name(), from: StateHalfOpen, to: StateClosed},
	})
}

// TestStateChangeHandler_NoTransitionNoEvent 未发生迁移（正常成功、未达阈值的
// 失败、拒绝放行）不触发回调；nil handler 等价缺省 no-op。
func TestStateChangeHandler_NoTransitionNoEvent(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &stateChangeRecorder{}
	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(3),
		WithBreakerStateChangeHandler(rec.record),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("healthy Do err = %v", err)
	}
	for range 2 {
		_ = b.Do(ctx, func() error { return boom }) // 2 < 3 阈值：不迁移
	}
	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("state change events = %d, want 0 (no transition)", got)
	}

	// from == to：同一状态重复写入不触发（只报真实迁移）
	b.gb.mu.Lock()
	b.gb.setStateLocked(stateClosed)
	b.gb.mu.Unlock()
	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("state change events = %d, want 0 (from == to must not fire)", got)
	}

	// nil handler：不 panic、不影响状态机
	nop := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerStateChangeHandler(nil),
	)
	_ = nop.Do(ctx, func() error { return boom })
	if nop.State() != StateOpen {
		t.Fatalf("State() = %v, want StateOpen (nil handler must not break transitions)", nop.State())
	}
}
