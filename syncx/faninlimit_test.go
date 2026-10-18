package syncx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestFanInLimit_CapsConcurrency 受限并发的核心承诺：在飞 fn 数不超过 limit
// （#23——健康评分 fan-out 防 DB 过载），且结果按 index 序全槽填充。
func TestFanInLimit_CapsConcurrency(t *testing.T) {
	const n, limit = 50, 4
	var inFlight, maxInFlight atomic.Int64

	slots := FanInLimit(n, limit, func(i int) (int, error) {
		cur := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if cur <= m || maxInFlight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond) // 占住槽位，放大潜在超发窗口
		inFlight.Add(-1)
		return i, nil
	})

	if got := maxInFlight.Load(); got > limit {
		t.Fatalf("max in-flight fn = %d, want <= %d", got, limit)
	}
	for i, s := range slots {
		if s.Err != nil {
			t.Fatalf("slot %d err = %v, want nil", i, s.Err)
		}
		if s.Val != i {
			t.Fatalf("slot %d val = %d, want %d (input order)", i, s.Val, i)
		}
	}
}

// TestFanInLimit_PanicSlotAndEdges panic 进槽语义与 FanInErr 一致；
// n<=0 → nil；limit<=0 = 不设上限；limit>n 收敛到 n。
func TestFanInLimit_PanicSlotAndEdges(t *testing.T) {
	slots := FanInLimit(3, 2, func(i int) (int, error) {
		if i == 1 {
			panic("boom")
		}
		return i, nil
	})
	var pe *async.PanicError
	if !errors.As(slots[1].Err, &pe) {
		t.Fatalf("slot 1 err = %v, want *async.PanicError", slots[1].Err)
	}
	if slots[1].Panicked == nil || slots[1].Val != 0 {
		t.Fatalf("slot 1 panic payload/zero-val mismatch: %+v", slots[1])
	}
	if slots[0].Val != 0 || slots[2].Val != 2 || slots[0].Err != nil || slots[2].Err != nil {
		t.Fatalf("healthy slots corrupted: %+v %+v", slots[0], slots[2])
	}

	if got := FanInLimit(0, 4, func(i int) (int, error) { return i, nil }); got != nil {
		t.Fatalf("FanInLimit(0,…) = %v, want nil", got)
	}

	// limit<=0：不设上限，等价 FanInErr 行为
	unbounded := FanInLimit(4, 0, func(i int) (int, error) { return i, nil })
	for i, s := range unbounded {
		if s.Val != i || s.Err != nil {
			t.Fatalf("unbounded slot %d = (%v, %v), want (%d, nil)", i, s.Val, s.Err, i)
		}
	}

	// limit>n：收敛到 n，语义不变
	over := FanInLimit(2, 100, func(i int) (int, error) { return i * 10, nil })
	for i, s := range over {
		if s.Val != i*10 {
			t.Fatalf("over-limit slot %d = %v, want %d", i, s.Val, i*10)
		}
	}
}

// TestFanInLimitCtx_PreCanceled 预取消契约（#27）：ctx 已取消时一次 fn 都不调，
// 全部槽快速排空为 context.Canceled。
func TestFanInLimitCtx_PreCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int64
	slots := FanInLimitCtx(ctx, 10, 4, func(ctx context.Context, i int) (int, error) {
		calls.Add(1)
		return i, nil
	})
	if calls.Load() != 0 {
		t.Fatalf("fn calls = %d, want 0 (no work after cancel)", calls.Load())
	}
	for i, s := range slots {
		if !errors.Is(s.Err, context.Canceled) {
			t.Fatalf("slot %d err = %v, want context.Canceled", i, s.Err)
		}
	}
}

// TestFanInLimitCtx_MidwayCancel 中途取消三契约：在飞项跑完正常落槽；
// 未启动项 fn 一次不调、整批一次遍历快速排空为 Canceled。
func TestFanInLimitCtx_MidwayCancel(t *testing.T) {
	const n, limit = 10, 2
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 前两个 item（limit 数量）阻塞在 barrier：保证它们已在飞
	started := make(chan struct{}, limit)
	release := make(chan struct{})
	var lateCalls atomic.Int64
	// 引擎在 goroutine 里跑：主线程负责「等在飞 → 取消 → 放行」的时序编排
	// （引擎返回即 wg.Wait 结束，编排必须发生在它返回之前）
	var slots []Slot[int]
	done := make(chan struct{})
	go func() {
		slots = FanInLimitCtx(ctx, n, limit, func(ctx context.Context, i int) (int, error) {
			if i < limit {
				started <- struct{}{}
				<-release
				return i, nil
			}
			lateCalls.Add(1)
			return i, nil
		})
		close(done)
	}()

	for i := 0; i < limit; i++ {
		<-started // 两个在飞 item 已启动
	}
	cancel()       // 引擎取消
	close(release) // 放行在飞 item（跑完、照常落槽）
	<-done

	for i := 0; i < limit; i++ {
		if slots[i].Err != nil || slots[i].Val != i {
			t.Fatalf("in-flight slot %d = (%v, %v), want normal result", i, slots[i].Val, slots[i].Err)
		}
	}
	for i := limit; i < n; i++ {
		if !errors.Is(slots[i].Err, context.Canceled) {
			t.Fatalf("slot %d err = %v, want context.Canceled (fail-fast drain)", i, slots[i].Err)
		}
	}
	if lateCalls.Load() != 0 {
		t.Fatalf("late fn calls = %d, want 0 (cancel must stop new work)", lateCalls.Load())
	}
}

// TestFanInLimitCtx_PanicAndEdges panic 进槽照旧；n<=0 → nil；limit<=0 不设上限；
// nil ctx 按 Background 处理；fn 携带的 ctx 与传入同一。
func TestFanInLimitCtx_PanicAndEdges(t *testing.T) {
	var mu sync.Mutex
	var gotCtx context.Context
	slots := FanInLimitCtx(context.Background(), 3, 2, func(ctx context.Context, i int) (int, error) {
		mu.Lock()
		gotCtx = ctx
		mu.Unlock()
		if i == 1 {
			panic("boom")
		}
		return i, nil
	})
	var pe *async.PanicError
	if !errors.As(slots[1].Err, &pe) {
		t.Fatalf("slot 1 err = %v, want *async.PanicError", slots[1].Err)
	}
	if slots[1].Panicked == nil || slots[1].Val != 0 {
		t.Fatalf("panic slot mismatch: %+v", slots[1])
	}
	if slots[0].Val != 0 || slots[2].Val != 2 {
		t.Fatalf("healthy slots corrupted: %+v %+v", slots[0], slots[2])
	}
	mu.Lock()
	ctxOk := gotCtx != nil
	mu.Unlock()
	if !ctxOk {
		t.Fatal("fn must receive the caller ctx")
	}

	if got := FanInLimitCtx(context.Background(), 0, 4, func(ctx context.Context, i int) (int, error) { return i, nil }); got != nil {
		t.Fatalf("n=0 = %v, want nil", got)
	}
	// nil ctx：不 panic、正常完成（兜底 Background 是文档化契约，故意传 nil 验证）
	//nolint:staticcheck // SA1012: 故意传 nil 验证引擎的 nil 兜底契约
	ok := FanInLimitCtx(nil, 2, 1, func(ctx context.Context, i int) (int, error) { return i * 2, nil })
	for i, s := range ok {
		if s.Val != i*2 || s.Err != nil {
			t.Fatalf("nil-ctx slot %d = (%v, %v)", i, s.Val, s.Err)
		}
	}
}
