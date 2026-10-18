package breaker

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// testInterval 单桶时间宽度；测试用假时钟推进，不真实 sleep。
const testInterval = 250 * time.Millisecond

// fakeClock 可手动推进的假时钟（原子计数，供并发测试安全读写）。
type fakeClock struct {
	nanos atomic.Int64
}

func (c *fakeClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

func (c *fakeClock) Advance(d time.Duration) {
	c.nanos.Add(int64(d))
}

// Set 直接把时钟设到指定时刻（用于给定非零量级的起点）。
func (c *fakeClock) Set(t time.Time) {
	c.nanos.Store(t.UnixNano())
}

// newTestGoogleBreaker 按缺省参数（40×250ms、探测时限 1m）构造注入假时钟的熔断核心。
func newTestGoogleBreaker(clock *fakeClock) *googleBreaker {
	return newGoogleBreaker(breakerConfig{
		windowSize:     defaultBuckets,
		windowInterval: testInterval,
		forcePass:      defaultForcePass,
		probeTimeout:   defaultProbeTimeout,
		clock:          clock.Now,
	})
}

// sumBucket 聚合窗口内全部桶的 Sum/Failure。
func sumBucket(b *googleBreaker) (total, failure int64) {
	b.stat.Reduce(func(bk *bucket) {
		total += bk.Sum
		failure += bk.Failure
	})
	return total, failure
}

// TestGoogleBreaker_LowFailureAllPass 低失败率（≤5%）下 dropRatio 恒 ≤ 0
// （w ≥ minK=1.1，w×accepts ≥ 1.045×total > total-protection），与桶内布局无关，
// 断言完全确定：任何调用都不应被拒绝。
func TestGoogleBreaker_LowFailureAllPass(t *testing.T) {
	clock := &fakeClock{}
	b := newTestGoogleBreaker(clock)
	boom := errors.New("boom")

	for i := 0; i < 400; i++ {
		clock.Advance(testInterval) // 每次跨桶，覆盖窗口滚动路径
		err, rejected := b.doReq(func() error {
			if i%20 == 0 { // 5% 失败率
				return boom
			}
			return nil
		}, defaultAcceptable)
		if rejected {
			t.Fatalf("call %d rejected unexpectedly: %v", i, err)
		}
		wantErr := error(nil)
		if i%20 == 0 {
			wantErr = boom
		}
		if !errors.Is(err, wantErr) || (wantErr != nil && err != wantErr) {
			t.Fatalf("call %d err = %v, want %v", i, err, wantErr)
		}
	}
}

// TestGoogleBreaker_HighFailureDrops 全失败 + 冻结时钟（禁用强制探活）：
// dropRatio 随 total 单调升向 1，必然出现拒绝且后段绝大多数被拒（统计断言、裕量充足）。
func TestGoogleBreaker_HighFailureDrops(t *testing.T) {
	clock := &fakeClock{}
	b := newTestGoogleBreaker(clock)
	boom := errors.New("boom")

	const n = 300
	const tail = 100
	tailDrops, drops := 0, 0
	for i := 0; i < n; i++ {
		ran := false
		err, _ := b.doReq(func() error {
			ran = true
			return boom
		}, defaultAcceptable)
		if !errors.Is(err, boom) && !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d unexpected err %v", i, err)
		}
		if errors.Is(err, ErrCircuitOpen) {
			if err != ErrCircuitOpen {
				t.Fatalf("want bare ErrCircuitOpen, got %v", err)
			}
			if ran {
				t.Fatal("rejected request must not execute")
			}
			drops++
			if i >= n-tail {
				tailDrops++
			}
		}
	}
	if drops == 0 {
		t.Fatal("expected at least one drop under total failure")
	}
	if tailDrops < 80 {
		t.Fatalf("tail drops = %d, want >= 80/%d (dropRatio approaches 1)", tailDrops, tail)
	}
	// 执行过的失败全部落桶（markFailure），拒绝样本也计入统计（markDrop）
	total, failure := sumBucket(b)
	if failure == 0 {
		t.Fatal("executed failures should be recorded as failure samples")
	}
	if total < failure {
		t.Fatalf("total samples = %d < failure samples = %d", total, failure)
	}
}

// TestGoogleBreaker_ForcePass 白盒控制 lastPass 驱动强制探活：
// 距上次放行超过 forcePass 时恰好放行一次（确定性），同刻后续请求回到概率丢弃。
func TestGoogleBreaker_ForcePass(t *testing.T) {
	clock := &fakeClock{}
	// lastPass 语义为“clock 的 UnixNano > 0 才有效”（上游 AtomicDuration 零值同语义），
	// 假时钟须从真实量级的时刻起步，不能停在 epoch 0
	clock.Set(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	b := newTestGoogleBreaker(clock)
	boom := errors.New("boom")

	// 打满失败使 dropRatio → 1（冻结时钟下强制探活不触发）
	for i := 0; i < 50000; i++ {
		_, _ = b.doReq(func() error { return boom }, defaultAcceptable)
	}

	// 未到期不放行：lastPass 距今 500ms < 1s，走概率丢弃（p_pass ≈ 1e-4）
	now := clock.Now().UnixNano()
	b.lastPass.Store(now - int64(500*time.Millisecond))
	if err, _ := b.doReq(func() error { return nil }, defaultAcceptable); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected drop before force pass interval, got %v", err)
	}

	// 到期强制探活：lastPass 距今 2s > 1s，本次必放行（确定性分支）
	b.lastPass.Store(now - int64(2*time.Second))
	ran := false
	if err, rejected := b.doReq(func() error {
		ran = true
		return nil
	}, defaultAcceptable); err != nil || rejected {
		t.Fatalf("force pass expected, got err=%v rejected=%v", err, rejected)
	}
	if !ran {
		t.Fatal("request must execute on force pass")
	}
	if got := b.lastPass.Load(); got != clock.Now().UnixNano() {
		t.Fatalf("force pass should refresh lastPass to now, got %d", got)
	}

	// 同一刻后续请求回到概率丢弃：20 次至多 1 次概率放行（P(≥2) ≈ 1.5e-6）
	passes := 0
	for i := 0; i < 20; i++ {
		if _, rejected := b.doReq(func() error { return boom }, defaultAcceptable); !rejected {
			passes++
		}
	}
	if passes > 1 {
		t.Fatalf("force pass should happen once per interval, got %d passes in 20", passes)
	}
}

// TestGoogleBreaker_PanicMarksFailureAndRepanics panic 记一次失败样本后原样 re-panic。
func TestGoogleBreaker_PanicMarksFailureAndRepanics(t *testing.T) {
	clock := &fakeClock{}
	b := newTestGoogleBreaker(clock)

	func() {
		defer func() {
			r := recover()
			if r != "kaboom" {
				t.Fatalf("panic value = %v, want kaboom", r)
			}
		}()
		_, _ = b.doReq(func() error { panic("kaboom") }, defaultAcceptable)
	}()

	total, failure := sumBucket(b)
	if total != 1 || failure != 1 {
		t.Fatalf("history total=%d failure=%d, want 1/1 (panic recorded as failure)", total, failure)
	}
}

// TestGoogleBreaker_PromiseMarking Allow 放行的 Promise 由调用方回填 Pass/Fail。
func TestGoogleBreaker_PromiseMarking(t *testing.T) {
	clock := &fakeClock{}
	b := newTestGoogleBreaker(clock)

	p, err := b.allow()
	if err != nil {
		t.Fatalf("allow rejected: %v", err)
	}
	p.Fail()

	p2, err := b.allow()
	if err != nil {
		t.Fatalf("allow rejected: %v", err)
	}
	p2.Pass()

	var accepts, total int64
	b.stat.Reduce(func(bk *bucket) {
		accepts += bk.Success
		total += bk.Sum
	})
	if accepts != 1 || total != 2 {
		t.Fatalf("accepts=%d total=%d, want 1/2", accepts, total)
	}
}

// TestGoogleBreaker_WindowExpiryRecovers 整窗滚出后历史清空、丢弃概率归零，
// 服务恢复即自动全量放行（确定性恢复路径）。
func TestGoogleBreaker_WindowExpiryRecovers(t *testing.T) {
	clock := &fakeClock{}
	b := newTestGoogleBreaker(clock)
	boom := errors.New("boom")

	for i := 0; i < 100000; i++ {
		_, _ = b.doReq(func() error { return boom }, defaultAcceptable)
	}
	clock.Advance(defaultBuckets*testInterval + testInterval)

	for i := 0; i < 10; i++ {
		err, rejected := b.doReq(func() error { return nil }, defaultAcceptable)
		if rejected || err != nil {
			t.Fatalf("call %d after window expiry: err=%v rejected=%v, want pass", i, err, rejected)
		}
	}
}

// TestTrueOnProba 边界：0 恒 false，1 恒 true（p < proba 语义）。
func TestTrueOnProba(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if trueOnProba(0) {
			t.Fatal("trueOnProba(0) should never be true")
		}
		if !trueOnProba(1) {
			t.Fatal("trueOnProba(1) should always be true")
		}
	}
}
