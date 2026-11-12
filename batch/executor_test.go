package batch

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
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
	out := make([]any, 0, batches)
	for range batches {
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
	for range idleExitRounds {
		tk.fire()
	}
	for range 1000 {
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
	for p := range producers {
		wg.Go(func() {
			for i := range perProducer {
				e.Add(fmt.Sprintf("p%d-%d", p, i))
				if i%50 == 0 {
					e.Flush()
				}
			}
		})
	}
	wg.Wait()
	e.Wait()
	if got := total.Load(); got != producers*perProducer {
		t.Fatalf("task loss: expected %d executed, got %d", producers*perProducer, got)
	}
}

// gateStopTicker Stop 可挂起的手动信号源：Stop 进入即关闭 stopped（同步点）、
// 阻塞直至 release 放行。defer 次序保证「ticker.Stop() 先于 close(exit)」，
// 挂起的 Stop 恰好把「旧循环已摘牌（background=false 且已解锁）、defer close
// 尚未执行」的竞态窗口（#220）驻留下来供测试在窗口内注入 Add。
type gateStopTicker struct {
	ch      chan time.Time
	stopped chan struct{}
	release chan struct{}
}

func (t *gateStopTicker) C() <-chan time.Time { return t.ch }
func (t *gateStopTicker) fire()               { t.ch <- time.Now() }
func (t *gateStopTicker) Stop() {
	close(t.stopped)
	<-t.release
}

// TestPeriodicalExecutor_OldLoopOnlyClosesOwnExit 旧循环只关自己的 exit 通道
// （#220）：修复前 defer close(e.exit) 重读字段——旧循环摘牌与 defer 执行之间
// 的窗口里并发 Add 覆写 e.exit 拉起新一代循环，旧循环会关掉**新**通道：正在
// Wait 的调用方提前返回（尾部批次丢失）、新循环退出时二次 close panic。
//
// 确定性构造：Stop 挂起把窗口驻留 → 窗口内 Add 拉起第二代 → 放行。修复后
// 第一代关自己启动时固化的通道（exit1 被关闭、exit2 原样）；第二代照常交付、
// 正常退出、全程零 panic。通道同步驱动，-race 稳定、无 sleep 编排。
func TestPeriodicalExecutor_OldLoopOnlyClosesOwnExit(t *testing.T) {
	// PanicHandler 间谍：修复前第二代退出时 close 已被第一代关掉的通道 →
	// 「close of closed channel」被 GoSafe 收编成一条上报
	panics := make(chan string, 8)
	async.SetPanicHandler(func(_ context.Context, r any, _ []byte) { panics <- fmt.Sprint(r) })
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	// 每一代循环从工厂各领一个可挂起的信号源
	tickers := make(chan *gateStopTicker, 4)
	factory := func(time.Duration) Ticker {
		tk := &gateStopTicker{ch: make(chan time.Time), stopped: make(chan struct{}), release: make(chan struct{})}
		tickers <- tk
		return tk
	}
	col := newCollector(8)
	e := NewPeriodicalExecutor(time.Hour, &bulkContainer{max: 100, do: col.execute}, WithBatchTicker(factory))

	e.Add("a") // 拉起第一代循环（后台循环启动时才向工厂领 ticker）
	var gen1 *gateStopTicker
	select {
	case gen1 = <-tickers:
	case <-time.After(2 * time.Second):
		t.Fatal("第一代循环未启动")
	}
	for range idleExitRounds {
		gen1.fire() // 首 tick 交付 "a"，其后累计空闲恰在第 idleExitRounds 轮自退出
	}
	<-gen1.stopped // 同步点：摘牌（background=false）已完成且已解锁，defer close 尚未执行

	// 首 tick 已把 "a" 交付进 collector，先取走再驱动第二代
	if got := collectExec(t, col, 1); len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}

	e.mu.Lock()
	exit1 := e.exit // 第一代自己的退出通道（此刻尚未被覆写）
	e.mu.Unlock()

	// 竞态窗口内 Add：重拉第二代循环（覆写 e.exit）
	e.Add("b")
	e.mu.Lock()
	running := e.background
	exit2 := e.exit
	e.mu.Unlock()
	if !running {
		t.Fatal("摘牌后的窗口 Add 应重拉后台循环")
	}
	if exit2 == exit1 {
		t.Fatal("前置失效：窗口 Add 应分配新 exit 通道")
	}

	// 放行第一代 Stop → 第一代执行 close。修复后关自己的通道（exit1 被关闭、
	// exit2 不受影响）；修复前重读字段关掉 exit2，exit1 永不关闭 ⇒ 超时红灯。
	close(gen1.release)
	select {
	case <-exit1:
	case <-time.After(2 * time.Second):
		t.Fatal("旧循环退出后未关闭自己的 exit 通道——defer 重读了 e.exit 字段（#220）")
	}

	// 第二代照常工作：交付 "b" 并在空闲到期时干净退出
	var gen2 *gateStopTicker
	select {
	case gen2 = <-tickers:
	case <-time.After(2 * time.Second):
		t.Fatal("第二代循环未启动")
	}
	gen2.fire()
	if got := collectExec(t, col, 1); len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}
	for range idleExitRounds - 1 {
		gen2.fire()
	}
	<-gen2.stopped
	close(gen2.release)
	select {
	case <-exit2: // 第二代正常退出，关闭自己的通道
	case <-time.After(2 * time.Second):
		t.Fatal("第二代循环退出后未关闭 exit")
	}

	// 全程零 panic（修复前：第二代 close 已被第一代关掉的通道 → panic 上报）
	select {
	case msg := <-panics:
		t.Fatalf("不应有任何 panic（修复前此处为 close of closed channel）：%s", msg)
	default:
	}
}
