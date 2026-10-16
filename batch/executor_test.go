package batch

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualTicker 手动周期信号源：无缓冲 channel，fire 阻塞直至后台接收——
// 每次 fire 严格对应一次已处理的 tick，测试据此完全串行、确定性驱动。
type manualTicker struct{ ch chan time.Time }

func newManualTicker() *manualTicker { return &manualTicker{ch: make(chan time.Time)} }

func (t *manualTicker) C() <-chan time.Time { return t.ch }
func (t *manualTicker) Stop()               {}
func (t *manualTicker) fire()               { t.ch <- time.Now() }

// collector 收集 Execute 交付的批次（缓冲 channel，后台执行不阻塞）。
type collector struct{ ch chan []any }

func newCollector(n int) *collector { return &collector{ch: make(chan []any, n)} }

func (c *collector) execute(tasks []any) { c.ch <- tasks }

// next 等待下一个批次，超时判失败。
func (c *collector) next(t *testing.T) []any {
	t.Helper()
	select {
	case tasks := <-c.ch:
		return tasks
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for execute")
		return nil
	}
}

// drain 清空剩余批次，返回总条数（用于收尾对账）。
func (c *collector) drainAll() int {
	total := 0
	for {
		select {
		case tasks := <-c.ch:
			total += len(tasks)
		default:
			return total
		}
	}
}

// collectExec 批量收集 n 个批次并平铺。
func collectExec(t *testing.T, c *collector, batches int) []any {
	t.Helper()
	var out []any
	for i := 0; i < batches; i++ {
		out = append(out, c.next(t)...)
	}
	return out
}

func TestPeriodicalExecutor_IntervalFlush(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewPeriodicalExecutor(10*time.Millisecond, &bulkContainer{max: 100, do: col.execute},
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a")
	e.Add("b")
	e.Add("c")
	tk.fire()
	got := collectExec(t, col, 1)
	if len(got) != 3 {
		t.Fatalf("expected 3 tasks in one batch, got %d", len(got))
	}
	e.Wait()
}

func TestPeriodicalExecutor_Flush(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewPeriodicalExecutor(10*time.Millisecond, &bulkContainer{max: 100, do: col.execute},
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a")
	e.Add("b")
	e.Flush()
	got := collectExec(t, col, 1)
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks flushed, got %d", len(got))
	}
	e.Wait()
}

func TestPeriodicalExecutor_WaitDrainsAndStops(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewPeriodicalExecutor(time.Hour, &bulkContainer{max: 100, do: col.execute},
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a")
	e.Add("b")
	e.Wait() // 周期未到也应排空积压
	got := collectExec(t, col, 1)
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks after Wait, got %d", len(got))
	}

	// 退出后继续 Add 重新拉起后台，周期照常工作
	e.Add("c")
	tk.fire()
	got = collectExec(t, col, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 task after restart, got %d", len(got))
	}
	e.Wait()
}

func TestPeriodicalExecutor_BackgroundIdleExitAndRestart(t *testing.T) {
	tk := newManualTicker()
	col := newCollector(8)
	e := NewPeriodicalExecutor(time.Millisecond, &bulkContainer{max: 100, do: col.execute},
		WithBatchTicker(func(time.Duration) Ticker { return tk }))

	e.Add("a") // 拉起后台（周期未到不 flush）
	e.mu.Lock()
	running := e.background
	e.mu.Unlock()
	if !running {
		t.Fatal("background should be running after Add")
	}

	// 逐 tick 驱动：首 tick 交付 "a" 并计入一轮空闲（roundTick 执行批次后
	// 会复查一轮），其后累计到 idleExitRounds 即自退出——恰好 idleExitRounds
	// 个 tick；fire 阻塞直至被接收，多发一个就会因无人接收而死锁。
	for i := 0; i < idleExitRounds; i++ {
		tk.fire()
	}
	for i := 0; i < 1000; i++ {
		e.mu.Lock()
		stopped := !e.background
		e.mu.Unlock()
		if stopped {
			break
		}
		runtime.Gosched()
	}
	e.mu.Lock()
	stopped := !e.background
	e.mu.Unlock()
	if !stopped {
		t.Fatal("background should exit after idle rounds")
	}
	if n := col.drainAll(); n != 1 {
		t.Fatalf("expected exactly 1 task delivered before restart, got %d", n)
	}

	// 退出后 Add 重新拉起，周期照常工作
	e.Add("b")
	e.mu.Lock()
	running = e.background
	e.mu.Unlock()
	if !running {
		t.Fatal("background should restart on Add")
	}
	tk.fire()
	got := collectExec(t, col, 1)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected restarted background to deliver [b], got %v", got)
	}
	e.Wait()
}

func TestPeriodicalExecutor_PanicIsolated(t *testing.T) {
	col := newCollector(8)
	var calls atomic.Int32
	c := &panicContainer{col: col, calls: &calls}
	e := NewPeriodicalExecutor(0, c)

	e.Add("a")
	e.Flush() // 首批 Execute panic，被隔离
	e.Add("b")
	e.Flush() // 二批正常交付
	got := collectExec(t, col, 1)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected second batch delivered after panic, got %v", got)
	}
	e.Wait()
}

// panicContainer 首次 Execute panic、其后正常交付，用于验证批次间隔离。
type panicContainer struct {
	col    *collector
	calls  *atomic.Int32
	mu     sync.Mutex
	queued []any
}

func (c *panicContainer) AddTask(task any) bool {
	c.mu.Lock()
	c.queued = append(c.queued, task)
	c.mu.Unlock()
	return false
}

func (c *panicContainer) Execute(tasks []any) {
	if c.calls.Add(1) == 1 {
		panic("boom")
	}
	c.col.execute(tasks)
}

func (c *panicContainer) RemoveAll() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.queued
	c.queued = nil
	return out
}

func TestPeriodicalExecutor_ConcurrentAddFlushNoLoss(t *testing.T) {
	const producers, perProducer = 8, 200
	var mu sync.Mutex
	var total atomic.Int64
	e := NewBulkExecutor(0, 37, func(tasks []any) {
		mu.Lock()
		total.Add(int64(len(tasks)))
		mu.Unlock()
	})

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				e.Add(fmt.Sprintf("p%d-%d", p, i))
				if i%50 == 0 {
					e.Flush()
				}
			}
		}(p)
	}
	wg.Wait()
	e.Wait()
	if got := total.Load(); got != producers*perProducer {
		t.Fatalf("task loss: expected %d executed, got %d", producers*perProducer, got)
	}
}
