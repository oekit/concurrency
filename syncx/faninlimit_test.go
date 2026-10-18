package syncx

import (
	"errors"
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
