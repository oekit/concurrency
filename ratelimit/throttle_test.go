package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// throttleStart 假时钟起点（与 lockoutStart 同量级但不共用，避免跨用例干扰）。
var throttleStart = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func newTestThrottle[K comparable](window time.Duration, clock *fakeLockClock) *Throttle[K] {
	return NewThrottle[K](window, WithThrottleClock(clock.Now))
}

// TestThrottle_FirstThenSuppress 钉住核心契约：窗口内首个放行、后续静默抑制，
// 且被抑制的调用**不执行 fn**、**不报错**。
func TestThrottle_FirstThenSuppress(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Minute, clock)

	var runs atomic.Int32
	fn := func(context.Context) error { runs.Add(1); return nil }

	for i := 0; i < 4; i++ {
		ran, err := th.Do(context.Background(), "k1", fn)
		if err != nil {
			t.Fatalf("第 %d 次调用返回错误: %v", i, err)
		}
		want := i == 0 // 只有第一次放行
		if ran != want {
			t.Fatalf("第 %d 次 ran=%v，want %v", i, ran, want)
		}
	}

	if got := runs.Load(); got != 1 {
		t.Fatalf("fn 只该执行 1 次，got %d", got)
	}
	if got := th.Suppressed(); got != 3 {
		t.Fatalf("抑制计数应为 3，got %d", got)
	}
}

// TestThrottle_WindowElapsesReleasesAgain 钉住窗口以「上次放行时刻」为锚点：
// 满窗口后再次放行一次。
func TestThrottle_WindowElapsesReleasesAgain(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Minute, clock)

	var runs atomic.Int32
	fn := func(context.Context) error { runs.Add(1); return nil }

	if ran, _ := th.Do(context.Background(), "k", fn); !ran {
		t.Fatal("首次应放行")
	}
	clock.Advance(59 * time.Second)
	if ran, _ := th.Do(context.Background(), "k", fn); ran {
		t.Fatal("窗口未满应抑制")
	}
	clock.Advance(time.Second) // 恰好满 60s
	if ran, _ := th.Do(context.Background(), "k", fn); !ran {
		t.Fatal("窗口期满应重新放行")
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("fn 应执行 2 次，got %d", got)
	}
}

// TestThrottle_KeyIsolation 钉住按 key 独立记账：不同 key 互不抑制。
func TestThrottle_KeyIsolation(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[uint](time.Minute, clock)

	var runs atomic.Int32
	fn := func(context.Context) error { runs.Add(1); return nil }

	if ran, _ := th.Do(context.Background(), uint(1), fn); !ran {
		t.Fatal("key=1 首次应放行")
	}
	if ran, _ := th.Do(context.Background(), uint(2), fn); !ran {
		t.Fatal("key=2 是另一个 key，应放行")
	}
	if ran, _ := th.Do(context.Background(), uint(1), fn); ran {
		t.Fatal("key=1 窗口内应抑制")
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("fn 应执行 2 次，got %d", got)
	}
}

// TestThrottle_ErrorRollsBackStamp 钉住：写失败 ⇒ 撤销戳 ⇒ 下一个调用立刻可重试。
//
// 这是本包最容易写错的一条：写失败等于「没写」，若仍记成已消费，时间戳会整整
// 一个窗口得不到更新。
func TestThrottle_ErrorRollsBackStamp(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Minute, clock)

	boom := errors.New("落库失败")
	ran, err := th.Do(context.Background(), "k", func(context.Context) error { return boom })
	if !ran || !errors.Is(err, boom) {
		t.Fatalf("首次应放行并透传原始错误，got ran=%v err=%v", ran, err)
	}

	// 窗口内（时钟未推进）也必须能重试
	ran, err = th.Do(context.Background(), "k", func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("失败后应撤销戳，窗口内即可重试，got ran=%v err=%v", ran, err)
	}
	if got := th.Suppressed(); got != 0 {
		t.Fatalf("两次都应放行，抑制数应为 0，got %d", got)
	}
}

// TestThrottle_PanicRecoveredAndRolledBack 钉住：fn panic 收编为 *PanicError 返回
// （不打死调用方），且与普通错误同等撤销戳。
func TestThrottle_PanicRecoveredAndRolledBack(t *testing.T) {
	async.SetPanicHandler(nil)
	defer async.SetPanicHandler(nil)

	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Minute, clock)

	ran, err := th.Do(context.Background(), "k", func(context.Context) error { panic("touch 崩了") })
	if !ran {
		t.Fatal("panic 也算本次放行")
	}
	var pe *async.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("panic 应收编为 *PanicError，got %T: %v", err, err)
	}

	ran, err = th.Do(context.Background(), "k", func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("panic 后应撤销戳，got ran=%v err=%v", ran, err)
	}
}

// TestThrottle_NonPositiveWindowDisables 钉住：window <= 0 ⇒ 不抑制（配置关掉节流）。
func TestThrottle_NonPositiveWindowDisables(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](0, clock)

	var runs atomic.Int32
	fn := func(context.Context) error { runs.Add(1); return nil }
	for i := 0; i < 3; i++ {
		if ran, _ := th.Do(context.Background(), "k", fn); !ran {
			t.Fatalf("window<=0 时第 %d 次也应放行", i)
		}
	}
	if got := runs.Load(); got != 3 {
		t.Fatalf("应执行 3 次，got %d", got)
	}
	if got := th.Suppressed(); got != 0 {
		t.Fatalf("不抑制档不该计抑制数，got %d", got)
	}
	if got := th.Len(); got != 0 {
		t.Fatalf("不抑制档不该记账，got %d", got)
	}
}

// TestThrottle_MaxEntriesEvicts 钉住容量护栏：条目数不越界，且被淘汰的 key
// fail-open（下一次调用重新放行，最坏只是多写一次）。
func TestThrottle_MaxEntriesEvicts(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := NewThrottle[int](time.Hour, WithThrottleClock(clock.Now), WithThrottleMaxEntries(2))

	for _, k := range []int{1, 2, 3, 4} {
		if ran, _ := th.Do(context.Background(), k, func(context.Context) error { return nil }); !ran {
			t.Fatalf("key=%d 首次应放行", k)
		}
		if got := th.Len(); got > 2 {
			t.Fatalf("条目数应受上限约束，got %d", got)
		}
	}
}

// TestThrottle_SweepAndForget 钉住两个显式出口：Sweep 回收过期条目并返回条数；
// Forget 让指定 key 立刻可再用。
func TestThrottle_SweepAndForget(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Minute, clock)

	fn := func(context.Context) error { return nil }
	for _, k := range []string{"a", "b", "c"} {
		th.Do(context.Background(), k, fn)
	}
	if got := th.Len(); got != 3 {
		t.Fatalf("应有 3 条记账，got %d", got)
	}

	if got := th.Sweep(); got != 0 {
		t.Fatalf("窗口未满不该回收，got %d", got)
	}
	clock.Advance(time.Minute)
	if got := th.Sweep(); got != 3 {
		t.Fatalf("窗口期满应回收 3 条，got %d", got)
	}
	if got := th.Len(); got != 0 {
		t.Fatalf("回收后应为空，got %d", got)
	}

	th.Do(context.Background(), "d", fn)
	th.Forget("d")
	if got := th.Len(); got != 0 {
		t.Fatalf("Forget 后应为空，got %d", got)
	}
	if ran, _ := th.Do(context.Background(), "d", fn); !ran {
		t.Fatal("Forget 后应立刻放行")
	}
}

// TestThrottle_ConcurrentSingleRun 钉住并发下「同 key 只执行一次」：
// 100 个 goroutine 抢同一 key，fn 必须恰好执行 1 次（-race 下同时验数据竞争）。
func TestThrottle_ConcurrentSingleRun(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Hour, clock)

	var runs atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			th.Do(context.Background(), "hot", func(context.Context) error {
				runs.Add(1)
				return nil
			})
		}()
	}
	close(start)
	wg.Wait()

	if got := runs.Load(); got != 1 {
		t.Fatalf("同 key 并发只该执行 1 次，got %d", got)
	}
	if got := th.Suppressed(); got != 99 {
		t.Fatalf("应抑制 99 次，got %d", got)
	}
}

// TestThrottle_SlowFnDoesNotBlockOtherKeys 钉住：fn 不持锁执行——慢 fn 不能把
// 其它 key（以及被抑制的调用）堵在锁上。
func TestThrottle_SlowFnDoesNotBlockOtherKeys(t *testing.T) {
	clock := &fakeLockClock{t: throttleStart}
	th := newTestThrottle[string](time.Hour, clock)

	release := make(chan struct{})
	entered := make(chan struct{})
	slowDone := make(chan struct{})

	go func() {
		defer close(slowDone)
		th.Do(context.Background(), "slow", func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered // slow 的 fn 已在执行（且此处若持锁，就再也走不到下一步）

	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		th.Do(context.Background(), "other", func(context.Context) error { return nil })
	}()

	select {
	case <-otherDone:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("慢 fn 不应阻塞其它 key（fn 必须不持锁执行）")
	}

	// 同 key 的抑制路径也不该被慢 fn 堵住
	suppressed := make(chan struct{})
	go func() {
		defer close(suppressed)
		th.Do(context.Background(), "slow", func(context.Context) error { return nil })
	}()
	select {
	case <-suppressed:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("窗口内抑制判定不应等 fn 结束")
	}

	close(release)
	<-slowDone
}
