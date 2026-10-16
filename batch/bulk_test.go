package batch

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBulkExecutor_FullTriggersImmediately(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewBulkExecutor(10*time.Millisecond, 3, col.execute,
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a")
	e.Add("b")
	e.Add("c") // 攒满立即触发，无需等周期
	got := collectExec(t, col, 1)
	if len(got) != 3 {
		t.Fatalf("expected immediate flush of 3 tasks, got %d", len(got))
	}
	e.Wait()
}

func TestBulkExecutor_PartialBatchWaitsInterval(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewBulkExecutor(10*time.Millisecond, 100, col.execute,
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a")
	e.Add("b")
	tk.fire() // 不满条数，周期到照常 flush
	got := collectExec(t, col, 1)
	if len(got) != 2 {
		t.Fatalf("expected interval flush of 2 tasks, got %d", len(got))
	}
	e.Wait()
}

func TestBulkExecutor_InvalidParams(t *testing.T) {
	for name, fn := range map[string]func(){
		"zero max":  func() { NewBulkExecutor(time.Second, 0, func([]any) {}) },
		"nil do":    func() { NewBulkExecutor(time.Second, 10, nil) },
		"nil conta": func() { NewPeriodicalExecutor(time.Second, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestChunkExecutor_BytesTrigger(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewChunkExecutor(10*time.Millisecond, 100, col.execute,
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a", 60)
	e.Add("b", 60) // 累计 120 >= 100，立即触发
	got := collectExec(t, col, 1)
	if len(got) != 2 {
		t.Fatalf("expected chunk flush of 2 tasks, got %d", len(got))
	}

	e.Add("x", 40)
	e.Add("y", 30) // 累计 70 < 100，手动排空
	e.Flush()
	got = collectExec(t, col, 1)
	if len(got) != 2 {
		t.Fatalf("expected manual flush of 2 tasks, got %d", len(got))
	}
	e.Wait()
}

func TestChunkExecutor_InvalidParams(t *testing.T) {
	for name, fn := range map[string]func(){
		"zero bytes": func() { NewChunkExecutor(time.Second, 0, func([]any) {}) },
		"nil do":     func() { NewChunkExecutor(time.Second, 100, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestLessExecutor_WindowDedup(t *testing.T) {
	now := time.Unix(0, 0)
	e := NewLessExecutor(time.Minute, func() {}, WithBatchClock(func() time.Time { return now }))

	if !e.DoOrDiscard() {
		t.Fatal("first call should execute")
	}
	now = now.Add(time.Second)
	if e.DoOrDiscard() {
		t.Fatal("within window should discard")
	}
	now = now.Add(time.Minute)
	if !e.DoOrDiscard() {
		t.Fatal("after window should execute")
	}
}

func TestLessExecutor_ConcurrentExactlyOne(t *testing.T) {
	now := time.Unix(0, 0)
	e := NewLessExecutor(time.Minute, func() {}, WithBatchClock(func() time.Time { return now }))

	const n = 16
	var executed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e.DoOrDiscard() {
				executed.Add(1)
			}
		}()
	}
	wg.Wait()
	if executed.Load() != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", executed.Load())
	}
}
