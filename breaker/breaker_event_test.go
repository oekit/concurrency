package breaker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// 结构化钩子（#376）测试：WithBreakerLabels 的标签随 StateChangeEvent /
// RejectEvent 回传，指标接线无需并行注册表或 name 前缀反解；新钩子与旧钩子
// 并存、各自独立触发；labels 参与 GetBreaker 同名异参冲突指纹。

// eventRecorder 记录结构化事件；回调在状态机锁内执行，只做内存追加、
// 不反向调用 breaker。
type eventRecorder struct {
	mu      sync.Mutex
	states  []StateChangeEvent
	rejects []RejectEvent
}

func (r *eventRecorder) state(e StateChangeEvent) {
	r.mu.Lock()
	r.states = append(r.states, e)
	r.mu.Unlock()
}

func (r *eventRecorder) reject(e RejectEvent) {
	r.mu.Lock()
	r.rejects = append(r.rejects, e)
	r.mu.Unlock()
}

func (r *eventRecorder) stateSnapshot() []StateChangeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]StateChangeEvent(nil), r.states...)
}

func (r *eventRecorder) rejectSnapshot() []RejectEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RejectEvent(nil), r.rejects...)
}

// TestStateChangeEvent_LabelsAndCoexistence 结构化迁移事件携带创建期 labels；
// labels 在选项应用时快照（调用方后续改入参不影响事件）；旧钩子与结构化钩子
// 并存且各触发一次。
func TestStateChangeEvent_LabelsAndCoexistence(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &eventRecorder{}
	legacy := &stateChangeRecorder{}
	labels := map[string]string{"channel_type": "sms", "tenant": "t1"}

	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerLabels(labels),
		WithBreakerStateChangeEvent(rec.state),
		WithBreakerStateChangeHandler(legacy.record),
	)
	// 选项应用后修改入参：已创建熔断器持快照，事件标签不得被污染。
	labels["channel_type"] = "mutated"

	boom := errors.New("boom")
	if err := b.Do(context.Background(), func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("tripping Do err = %v, want boom", err)
	}

	events := rec.stateSnapshot()
	if len(events) != 1 {
		t.Fatalf("state events = %d, want 1: %v", len(events), events)
	}
	ev := events[0]
	if ev.Meta.Name != b.Name() {
		t.Fatalf("event Meta.Name = %q, want %q", ev.Meta.Name, b.Name())
	}
	if ev.From != StateClosed || ev.To != StateOpen {
		t.Fatalf("event transition = %v→%v, want closed→open", ev.From, ev.To)
	}
	if got := ev.Meta.Labels["channel_type"]; got != "sms" {
		t.Fatalf("event label channel_type = %q, want sms (选项应用时的快照)", got)
	}
	if got := ev.Meta.Labels["tenant"]; got != "t1" {
		t.Fatalf("event label tenant = %q, want t1", got)
	}

	// 旧钩子并存：同一迁移各触发一次，载荷一致。
	legacyEvents := legacy.snapshot()
	if len(legacyEvents) != 1 {
		t.Fatalf("legacy state events = %d, want 1", len(legacyEvents))
	}
	if legacyEvents[0].name != ev.Meta.Name ||
		legacyEvents[0].from != ev.From || legacyEvents[0].to != ev.To {
		t.Fatalf("legacy event = %+v, want same transition as structured event %+v", legacyEvents[0], ev)
	}
}

// TestRejectEvent_ReasonAndCoexistence 拒绝事件携带 labels 与拒绝来源枚举：
// open 态常规拒绝为 RejectReasonOpen，half-open 挂起探测期间为
// RejectReasonHalfOpen；Detail 与旧回调 reason 同口径；两个钩子并存。
func TestRejectEvent_ReasonAndCoexistence(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	rec := &eventRecorder{}
	var legacy []string
	var legacyMu sync.Mutex

	b := newProbeTestBreaker(clock, time.Minute,
		WithConsecutiveFailures(1),
		WithBreakerLabels(map[string]string{"channel_type": "sms"}),
		WithBreakerRejectEvent(rec.reject),
		WithBreakerRejectHandler(func(name, reason string) {
			legacyMu.Lock()
			legacy = append(legacy, name+"|"+reason)
			legacyMu.Unlock()
		}),
	)
	ctx := context.Background()
	boom := errors.New("boom")

	// 触发请求本身不算拒绝（既有语义）；随后 open 态拒绝 → RejectReasonOpen。
	if err := b.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("tripping Do err = %v, want boom", err)
	}
	if err := b.Do(ctx, func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open-state Do err = %v, want ErrCircuitOpen", err)
	}

	// 冷却期满放行半开探测；挂起探测在飞时普通请求拒绝 → RejectReasonHalfOpen。
	clock.Advance(time.Minute)
	if _, _, err := b.AllowGen(ctx); err != nil {
		t.Fatalf("probe grant err = %v", err)
	}
	if err := b.Do(ctx, func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("half-open Do err = %v, want ErrCircuitOpen", err)
	}

	events := rec.rejectSnapshot()
	if len(events) != 2 {
		t.Fatalf("reject events = %d, want 2: %v", len(events), events)
	}
	if events[0].Reason != RejectReasonOpen || events[0].Reason.String() != "open" {
		t.Fatalf("first reject reason = %v, want open", events[0].Reason)
	}
	if events[1].Reason != RejectReasonHalfOpen || events[1].Reason.String() != "half-open" {
		t.Fatalf("second reject reason = %v, want half-open", events[1].Reason)
	}
	for i, ev := range events {
		if ev.Meta.Name != b.Name() {
			t.Fatalf("event[%d] Meta.Name = %q, want %q", i, ev.Meta.Name, b.Name())
		}
		if got := ev.Meta.Labels["channel_type"]; got != "sms" {
			t.Fatalf("event[%d] label channel_type = %q, want sms", i, got)
		}
		if !strings.Contains(ev.Detail, "boom") {
			t.Fatalf("event[%d] Detail = %q, want 最近失败原因摘要含 boom", i, ev.Detail)
		}
	}

	// 旧拒绝钩子并存：每次拒绝各触发一次，reason 与结构化 Detail 同口径。
	legacyMu.Lock()
	defer legacyMu.Unlock()
	if len(legacy) != 2 {
		t.Fatalf("legacy reject calls = %d, want 2", len(legacy))
	}
	for i, got := range legacy {
		want := b.Name() + "|" + events[i].Detail
		if got != want {
			t.Fatalf("legacy reject[%d] = %q, want %q", i, got, want)
		}
	}
}

// TestBreakerLabels_ConflictFingerprint labels 参与同名异参冲突指纹：键序不同的
// 等价 labels 不误报，值不同则告警且 detail 点名 labels。
func TestBreakerLabels_ConflictFingerprint(t *testing.T) {
	rec := &conflictRecorder{}
	name := uniqBreakerName("labels-conflict")
	GetBreaker(name,
		WithBreakerLabels(map[string]string{"channel_type": "sms", "tenant": "t1"}),
		WithBreakerConflictHandler(rec.handler),
	)

	// 相同内容、不同插入顺序 → 规范化后等价，不告警。
	GetBreaker(name, WithBreakerLabels(map[string]string{"tenant": "t1", "channel_type": "sms"}))
	if got := rec.snapshot(); len(got) != 0 {
		t.Fatalf("等价 labels 不应告警，实际 %d 次: %v", len(got), got)
	}

	// 同名但 labels 值不同 → 首建生效、后到被丢弃，必须可观测。
	GetBreaker(name, WithBreakerLabels(map[string]string{"channel_type": "email", "tenant": "t1"}))
	got := rec.snapshot()
	if len(got) != 1 {
		t.Fatalf("labels 冲突应告警 1 次，实际 %d 次: %v", len(got), got)
	}
	if !strings.Contains(got[0], "labels=") || !strings.Contains(got[0], "email") {
		t.Fatalf("冲突 detail 应点名 labels 差异:\n%s", got[0])
	}
}
