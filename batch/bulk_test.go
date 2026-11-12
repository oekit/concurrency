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
	for range n {
		wg.Go(func() {
			if e.DoOrDiscard() {
				executed.Add(1)
			}
		})
	}
	wg.Wait()
	if executed.Load() != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", executed.Load())
	}
}

// TestLessExecutor_ConcurrentFirstWindowSingleExecution 首窗双执行竞态的
// 压力回归（#173）：每次迭代新建实例（「新建限频器 + 多 goroutine 并发首调」
// 是告警限频的标准形态）、屏障同步后 6 路同时首调，interval 远大于测试时长
// ⇒ 全部落在首窗，同一窗口必须恰有一路执行与一路拿到 true。
// 旧实现在 CAS(ever) 与 Store(lastNano) 的窗口内可双执行（实测 300k 迭代
// 命中 64 次）；修复后窗口在构造上不可达，压力轮次恒为 1。
func TestLessExecutor_ConcurrentFirstWindowSingleExecution(t *testing.T) {
	const (
		iterations = 20000
		callers    = 6
	)
	for i := range iterations {
		var execs, trues atomic.Int32
		e := NewLessExecutor(time.Hour, func() { execs.Add(1) })

		start := make(chan struct{})
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				<-start
				if e.DoOrDiscard() {
					trues.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()

		if got := execs.Load(); got != 1 {
			t.Fatalf("迭代 %d：首窗执行 %d 次，期望恰 1（首窗竞态回归）", i, got)
		}
		if got := trues.Load(); got != 1 {
			t.Fatalf("迭代 %d：首窗返回 true %d 路，期望恰 1", i, got)
		}
	}
}
