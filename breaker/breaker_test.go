package breaker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// newSmallClockBreaker 构造 10 桶 × 10ms 小窗口 + 假时钟的熔断器，
// 便于用少量调用快速打满统计。
func newSmallClockBreaker(clock *fakeClock, opts ...BreakerOption) *circuitBreaker {
	all := append([]BreakerOption{
		WithBreakerWindow(10, 10*time.Millisecond),
		WithBreakerClock(clock.Now),
	}, opts...)
	return NewBreaker("breaker-test", all...).(*circuitBreaker)
}

// openBreaker 用连续失败把熔断器打到打开状态（冻结时钟下 forcePass 不到期；
// v0.13.0 起触发后除 forcePass 外一律拒绝——预热循环前几次执行失败即触发，
// 后续全量拒绝，单发"必被拒绝"断言完全确定；纯内存操作，毫秒级完成）。
func openBreaker(t *testing.T, cb *circuitBreaker) {
	t.Helper()
	boom := errors.New("boom")
	ctx := context.Background()
	for i := 0; i < 50000; i++ {
		_ = cb.Do(ctx, func() error { return boom })
	}
}

func TestNewBreaker_NameAndBasicFlow(t *testing.T) {
	b := NewBreaker("order-rpc")
	if b.Name() != "order-rpc" {
		t.Fatalf("Name() = %q, want order-rpc", b.Name())
	}

	ctx := context.Background()
	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("Do success err = %v, want nil", err)
	}
	boom := errors.New("downstream")
	if err := b.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want %v", err, boom)
	}

	p, err := b.Allow(ctx)
	if err != nil || p == nil {
		t.Fatalf("Allow = (%v, %v), want promise and nil error", p, err)
	}
	p.Pass()
}

func TestBreakerOption_Validation(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"window size 0", func() { WithBreakerWindow(0, time.Millisecond) }},
		{"window size -1", func() { WithBreakerWindow(-1, time.Millisecond) }},
		{"window interval 0", func() { WithBreakerWindow(10, 0) }},
		{"window interval negative", func() { WithBreakerWindow(10, -time.Second) }},
		{"force pass 0", func() { WithForcePass(0) }},
		{"force pass negative", func() { WithForcePass(-time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.fn()
		})
	}
}

// TestBreaker_WithBreakerWindow 小窗口快速打满触发熔断；跨过整窗不恢复流量（#25），
// 冷却期满探测成功后才恢复放行——窗口滚动与状态机恢复一并覆盖。
func TestBreaker_WithBreakerWindow(t *testing.T) {
	clock := &fakeClock{}
	clock.Set(stateTestStart)
	b := newSmallClockBreaker(clock)
	openBreaker(t, b)

	ctx := context.Background()
	if b.State() != StateOpen {
		t.Fatalf("State() = %v, want StateOpen", b.State())
	}

	// 推进超过整窗（10 桶 × 10ms）：历史滚空也不恢复流量——熔断期收敛到探测级
	clock.Advance(11 * 10 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if err := b.Do(ctx, func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d after window expiry err = %v, want ErrCircuitOpen", i, err)
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("State() after window expiry = %v, want StateOpen", b.State())
	}

	// 冷却期满（缺省 1m）：首个放行即半开探测，成功关闸后恢复全量放行
	clock.Advance(time.Minute)
	if err := b.Do(ctx, func() error { return nil }); err != nil {
		t.Fatalf("probe Do err = %v, want nil", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("State() after probe success = %v, want StateClosed", b.State())
	}
	for i := 0; i < 5; i++ {
		if err := b.Do(ctx, func() error { return nil }); err != nil {
			t.Fatalf("call %d after close err = %v, want nil", i, err)
		}
	}
}

func TestBreaker_CtxDone(t *testing.T) {
	clock := &fakeClock{}
	b := newSmallClockBreaker(clock)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if p, err := b.Allow(ctx); !errors.Is(err, context.Canceled) || p != nil {
		t.Fatalf("Allow on done ctx = (%v, %v), want (nil, context.Canceled)", p, err)
	}
	ran := false
	if err := b.Do(ctx, func() error { ran = true; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("Do on done ctx err = %v, want context.Canceled", err)
	}
	if ran {
		t.Fatal("execute must not run on done ctx")
	}

	// 已取消的 ctx 不消耗统计：恢复后首个请求正常放行
	total, _ := sumBucket(b.gb)
	if total != 0 {
		t.Fatalf("done ctx should not consume stats, got %d samples", total)
	}
	if err := b.Do(context.Background(), func() error { return nil }); err != nil {
		t.Fatalf("Do after cancel err = %v, want nil", err)
	}
}

// TestBreaker_DoWithAcceptable acceptable 判定为成功的错误不计失败。
func TestBreaker_DoWithAcceptable(t *testing.T) {
	clock := &fakeClock{}
	b := newSmallClockBreaker(clock)
	bizErr := errors.New("biz not found")
	acceptable := func(err error) bool { return errors.Is(err, bizErr) }

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if err := b.DoWithAcceptable(ctx, func() error { return bizErr }, acceptable); !errors.Is(err, bizErr) {
			t.Fatalf("call %d err = %v, want %v", i, err, bizErr)
		}
	}
	// 全部记为成功：无失败样本，dropRatio 恒 ≤ 0，不会拒绝
	total, failure := sumBucket(b.gb)
	if total != 200 || failure != 0 {
		t.Fatalf("total=%d failure=%d, want 200/0", total, failure)
	}
}

// TestBreaker_DoWithFallback fallback 仅在熔断拒绝时触发；业务错误不经 fallback。
func TestBreaker_DoWithFallback(t *testing.T) {
	clock := &fakeClock{}

	// 健康熔断器返回业务错误：原样透传，不走 fallback
	healthy := newSmallClockBreaker(clock)
	bizErr := errors.New("biz")
	called := false
	err := healthy.DoWithFallback(context.Background(), func() error { return bizErr },
		defaultAcceptable, func(err error) error {
			called = true
			return err
		})
	if !errors.Is(err, bizErr) || called {
		t.Fatalf("healthy biz err = %v fallbackCalled=%v, want passthrough without fallback", err, called)
	}

	// 打开的熔断器：拒绝错误进 fallback
	opened := newSmallClockBreaker(clock)
	openBreaker(t, opened)
	fallbackErr := errors.New("degraded")
	var got error
	err = opened.DoWithFallback(context.Background(), func() error {
		t.Fatal("execute must not run when rejected")
		return nil
	}, defaultAcceptable, func(err error) error {
		got = err
		return fallbackErr
	})
	if !errors.Is(got, ErrCircuitOpen) {
		t.Fatalf("fallback input = %v, want ErrCircuitOpen", got)
	}
	if !errors.Is(err, fallbackErr) {
		t.Fatalf("DoWithFallback err = %v, want fallback result", err)
	}

	// fallback 为 nil：拒绝错误原样返回
	err = opened.Do(context.Background(), func() error { return nil })
	if err != ErrCircuitOpen {
		t.Fatalf("Do on open breaker err = %v, want bare ErrCircuitOpen", err)
	}
}

// TestBreaker_RejectHandler 拒绝回调携带熔断器名与最近失败原因，缺省 no-op。
func TestBreaker_RejectHandler(t *testing.T) {
	clock := &fakeClock{}
	var mu sync.Mutex
	var gotName, gotReason string
	calls := 0
	b := newSmallClockBreaker(clock, WithBreakerRejectHandler(func(name, reason string) {
		mu.Lock()
		defer mu.Unlock()
		gotName, gotReason, calls = name, reason, calls+1
	}))
	openBreaker(t, b) // 失败原因 boom 已入环形缓冲

	ctx := context.Background()
	mu.Lock()
	before := calls
	mu.Unlock()
	if err := b.Do(ctx, func() error { return nil }); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	mu.Lock()
	callsAfterDo, name, reason := calls, gotName, gotReason
	mu.Unlock()
	if callsAfterDo != before+1 {
		t.Fatalf("reject handler calls = %d, want %d", callsAfterDo, before+1)
	}
	if name != "breaker-test" {
		t.Fatalf("handler name = %q, want breaker-test", name)
	}
	if reason == "" || !strings.Contains(reason, "boom") {
		t.Fatalf("handler reason = %q, want recorded failure reason boom", reason)
	}

	// Allow 路径同样触发回调（调用期间不得持有 mu，handler 会再次加锁）
	if _, err := b.Allow(ctx); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow err = %v, want ErrCircuitOpen", err)
	}
	mu.Lock()
	if calls != callsAfterDo+1 {
		t.Fatalf("reject handler calls after Allow = %d, want %d", calls, callsAfterDo+1)
	}
	mu.Unlock()

	// nil 回调等价缺省 no-op
	nop := newSmallClockBreaker(clock, WithBreakerRejectHandler(nil))
	openBreaker(t, nop) // 不 panic 即可
}

// TestBreaker_PanicRepanics execute panic 记失败样本后原样 re-panic。
func TestBreaker_PanicRepanics(t *testing.T) {
	clock := &fakeClock{}
	b := newSmallClockBreaker(clock)

	func() {
		defer func() {
			if r := recover(); r != "boom-panic" {
				t.Fatalf("recovered %v, want boom-panic", r)
			}
		}()
		_ = b.Do(context.Background(), func() error { panic("boom-panic") })
	}()

	total, failure := sumBucket(b.gb)
	if total != 1 || failure != 1 {
		t.Fatalf("total=%d failure=%d, want 1/1", total, failure)
	}
}

func TestGetBreaker_Singleton(t *testing.T) {
	a := GetBreaker("svc-a")
	b := GetBreaker("svc-a")
	if a != b {
		t.Fatal("same name must return the same instance")
	}
	if a.Name() != "svc-a" {
		t.Fatalf("Name() = %q, want svc-a", a.Name())
	}
	if c := GetBreaker("svc-b"); c == a {
		t.Fatal("different names must return different instances")
	}

	// opts 仅首次创建生效：对既有名字追加选项不改变实例
	withOpts := GetBreaker("svc-a", WithForcePass(2*time.Second))
	if withOpts != a {
		t.Fatal("opts on existing name must not create a new instance")
	}
}

func TestGetBreaker_Concurrent(t *testing.T) {
	const n = 64
	results := make([]Breaker, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = GetBreaker("concurrent-svc")
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if results[i] != results[0] {
			t.Fatalf("goroutine %d got a different instance", i)
		}
	}
}

func TestErrorReasons_Ring(t *testing.T) {
	ew := newErrorReasons()
	if got := ew.String(); got != "" {
		t.Fatalf("empty reasons String() = %q, want empty", got)
	}
	for i := 0; i < 7; i++ {
		ew.add("reason-" + string(rune('0'+i)))
	}
	got := ew.String()
	// 只保留最近 5 条，最新在前
	want := "reason-6\nreason-5\nreason-4\nreason-3\nreason-2"
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
