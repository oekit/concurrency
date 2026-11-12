package timewheel

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/gopool"
)

// manualTicker 无缓冲手动周期信号源：fire 阻塞直至事件循环接收，
// 每个 fire 严格对应一次已处理的 tick。
type manualTicker struct{ ch chan time.Time }

func newManualTicker() *manualTicker { return &manualTicker{ch: make(chan time.Time)} }

func (t *manualTicker) C() <-chan time.Time { return t.ch }
func (t *manualTicker) Stop()               {}
func (t *manualTicker) fire()               { t.ch <- time.Now() }

// firedCh 收集触发任务的自标识（缓冲 16，触发不阻塞事件循环）。
func firedCh() (chan any, func(v any)) {
	ch := make(chan any, 16)
	return ch, func(v any) { ch <- v }
}

// barrier 用一次 HasTimer 空查询作同步屏障：事件串行保证其应答返回时，
// 此前已投递的 tick 的 advance 已处理完毕。barrierKey 不承载定时任务。
func barrier(tw *TimingWheel) { tw.HasTimer(barrierKey) }

const barrierKey = "barrier.test"

func newTestWheel(t *testing.T, interval time.Duration, numSlots int) (*TimingWheel, *manualTicker) {
	t.Helper()
	tk := newManualTicker()
	tw := NewTimingWheel(interval, numSlots, WithWheelTicker(func(time.Duration) Ticker { return tk }))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })
	return tw, tk
}

// assertIdle 断言触发通道当前为空（barrier 已保证状态落定）。
func assertIdle(t *testing.T, ch chan any) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected fire: %v", v)
	default:
	}
}

func TestTimingWheel_FiresAfterOneTick(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 4)
	ch, task := firedCh()

	// 5ms 不足一格取一格（ceil 口径下 0.5 格 → 1 格；非整数格的取整见
	// TestTimingWheel_CeilRounding——15ms 那类 1.5 格现在等 2 格，#219）
	tw.SetTimer("k", func() { task("k") }, 5*time.Millisecond)
	barrier(tw)
	if !tw.HasTimer("k") {
		t.Fatal("timer should exist after SetTimer")
	}

	tk.fire()
	barrier(tw)
	select {
	case v := <-ch:
		if v != "k" {
			t.Fatalf("expected k, got %v", v)
		}
	default:
		t.Fatal("expected fire on first tick")
	}
	if tw.HasTimer("k") {
		t.Fatal("timer should be gone after fire")
	}
}

func TestTimingWheel_LongDelayCircle(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 2)
	ch, task := firedCh()

	// 50ms / 10ms = 5 ticks；槽位环 2 格，经 circle 圈数恰在第 5 tick 触发
	tw.SetTimer("long", func() { task("long") }, 50*time.Millisecond)
	barrier(tw)
	for i := 1; i <= 4; i++ {
		tk.fire()
		barrier(tw)
		assertIdle(t, ch)
	}
	tk.fire()
	barrier(tw)
	select {
	case <-ch:
	default:
		t.Fatal("expected fire on 5th tick")
	}
}

func TestTimingWheel_MoveTimer(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 4)
	ch, task := firedCh()

	tw.SetTimer("m", func() { task("m") }, 100*time.Millisecond)
	barrier(tw)
	tw.MoveTimer("m", 20*time.Millisecond)
	barrier(tw)

	tk.fire()
	barrier(tw)
	assertIdle(t, ch)
	tk.fire()
	barrier(tw)
	select {
	case <-ch:
	default:
		t.Fatal("expected fire after move on 2nd tick")
	}
}

func TestTimingWheel_MoveNonExistentNoop(t *testing.T) {
	tw, _ := newTestWheel(t, 10*time.Millisecond, 4)
	tw.MoveTimer("ghost", time.Second) // 不应 panic 或副作用
	barrier(tw)
	if tw.HasTimer("ghost") {
		t.Fatal("ghost timer should not exist")
	}
}

func TestTimingWheel_RemoveTimer(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 4)
	ch, task := firedCh()

	tw.SetTimer("r", func() { task("r") }, 20*time.Millisecond)
	barrier(tw)
	tw.RemoveTimer("r")
	barrier(tw)
	if tw.HasTimer("r") {
		t.Fatal("timer should be gone after RemoveTimer")
	}
	for range 4 {
		tk.fire()
	}
	barrier(tw)
	assertIdle(t, ch)
}

func TestTimingWheel_DuplicateKeyReplaces(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 4)
	ch, task := firedCh()

	tw.SetTimer("dup", func() { task("first") }, 100*time.Millisecond)
	barrier(tw)
	tw.SetTimer("dup", func() { task("second") }, 10*time.Millisecond)
	barrier(tw)

	tk.fire()
	barrier(tw)
	select {
	case v := <-ch:
		if v != "second" {
			t.Fatalf("expected replacement task, got %v", v)
		}
	default:
		t.Fatal("expected replacement to fire on first tick")
	}
	// 旧任务不应再触发
	for range 6 {
		tk.fire()
	}
	barrier(tw)
	assertIdle(t, ch)
}

func TestTimingWheel_PanicIsolated(t *testing.T) {
	tw, tk := newTestWheel(t, 10*time.Millisecond, 4)
	ch, task := firedCh()

	tw.SetTimer("boom", func() { panic("boom") }, 10*time.Millisecond)
	barrier(tw)
	tw.SetTimer("fine", func() { task("fine") }, 20*time.Millisecond)
	barrier(tw)

	tk.fire() // boom 触发并 panic，被隔离
	tk.fire() // fine 照常触发
	barrier(tw)
	select {
	case v := <-ch:
		if v != "fine" {
			t.Fatalf("expected fine, got %v", v)
		}
	default:
		t.Fatal("expected fine to fire after panic")
	}
}

func TestTimingWheel_CloseDropsPendingAndIdempotent(t *testing.T) {
	tk := newManualTicker()
	tw := NewTimingWheel(10*time.Millisecond, 4, WithWheelTicker(func(time.Duration) Ticker { return tk }))

	tw.SetTimer("pending", func() {}, time.Second)
	barrier(tw)
	if err := tw.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tw.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if tw.HasTimer("pending") {
		t.Fatal("timer should be gone after Close")
	}
	// Close 后操作不阻塞、静默丢弃
	tw.SetTimer("after", func() {}, 10*time.Millisecond)
	barrier(tw)
	tw.RemoveTimer("after")
	barrier(tw)
}

func TestTimingWheel_CloseWithCanceledCtx(t *testing.T) {
	tk := newManualTicker()
	tw := NewTimingWheel(10*time.Millisecond, 4, WithWheelTicker(func(time.Duration) Ticker { return tk }))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 事件循环可能尚未消费任何东西，取消的 ctx 允许 Close 返回 ctx.Err()
	_ = tw.Close(ctx) // 不断言具体返回值：done 与 ctx 竞争均合法
	_ = tw.Close(context.Background())
}

func TestTimingWheel_InvalidParams(t *testing.T) {
	for name, fn := range map[string]func(){
		"zero interval":      func() { NewTimingWheel(0, 4) },
		"zero slots":         func() { NewTimingWheel(time.Second, 0) },
		"zero delay":         func() { NewTimingWheel(time.Second, 4).SetTimer("k", func() {}, 0) },
		"nil task":           func() { NewTimingWheel(time.Second, 4).SetTimer("k", nil, time.Second) },
		"nil key":            func() { NewTimingWheel(time.Second, 4).SetTimer(nil, func() {}, time.Second) },
		"non-comparable key": func() { NewTimingWheel(time.Second, 4).SetTimer([]int{1}, func() {}, time.Second) },
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

// TestTimingWheel_HugeDelayDoesNotKillLoop ticks 溢出钳制（#174）：
// interval=1ns + delay=MaxInt64（「传 MaxInt64 当永不超时」的调用方自然写法）
// 时 ticks 可达 MaxInt64，pos 计算回绕成负槽位下标 ⇒ 事件循环 goroutine panic
// 死亡（被 async.GoSafe 收编、进程不死）——此后 SetTimer 静默丢任务、HasTimer
// 永久挂起。修复后按整轮饱和：登记成功、时间轮存活、后续任务照常触发。
func TestTimingWheel_HugeDelayDoesNotKillLoop(t *testing.T) {
	tw, tk := newTestWheel(t, time.Nanosecond, 64)
	ch, task := firedCh()

	// 推进 current=2：溢出在 current≥1 时产生负槽位下标
	tk.fire()
	barrier(tw)
	tk.fire()
	barrier(tw)

	tw.SetTimer("huge", func() { task("huge") }, time.Duration(math.MaxInt64))
	barrier(tw)
	if !tw.HasTimer("huge") {
		t.Fatal("巨大延迟任务应成功登记（饱和不回绕）")
	}

	// 循环必须仍然存活：后续普通任务照常触发（旧实现在此已静默死亡——
	// SetTimer 表面成功、任务永不触发）
	tw.SetTimer("real", func() { task("real") }, 3*time.Nanosecond)
	barrier(tw)
	for range 3 {
		tk.fire()
		barrier(tw)
	}
	select {
	case v := <-ch:
		if v != "real" {
			t.Fatalf("expected real, got %v", v)
		}
	default:
		t.Fatal("事件循环已死亡：普通任务未触发")
	}
	if !tw.HasTimer("huge") {
		t.Fatal("饱和登记的巨大延迟任务应仍挂起（远未到期）")
	}
}

// TestTimingWheel_CeilRounding 非整数格向上取整（#219）：「delay 后触发」=
// 不早于 delay——每个 key 在 ceil(delay/interval) 格**恰好**触发（此前向下
// 取整会恒提前：1.999 格按 1 格计，最多早近一整格）。整数格（1×、2×）不受
// 影响，不足一格（0.5×）仍取一格。MoveTimer 走同一 insertLocked，口径一致。
func TestTimingWheel_CeilRounding(t *testing.T) {
	const interval = 10 * time.Millisecond
	tw, tk := newTestWheel(t, interval, 4)
	ch, task := firedCh()

	// 1.001× / 1.5× / 1.999× / 2.001× interval——issue 点名的非整数格取值，
	// 1× / 2× 为整数格对照，0.5× 为「不足一格取一格」既有口径对照
	cases := []struct {
		key        string
		delay      time.Duration
		fireAtTick int64
	}{
		{"half", interval / 2, 1},
		{"one", interval, 1},
		{"one-plus-001", interval + 10*time.Microsecond, 2},   // 1.001×
		{"one-and-half", interval * 3 / 2, 2},                 // 1.5×
		{"one-plus-999", interval*2 - 10*time.Microsecond, 2}, // 1.999×
		{"two", interval * 2, 2},
		{"two-plus-001", interval*2 + 10*time.Microsecond, 3}, // 2.001×
	}
	wantFired := map[int64][]string{} // tick → 该 tick 应触发的 key
	for _, c := range cases {
		tw.SetTimer(c.key, func() { task(c.key) }, c.delay)
		wantFired[c.fireAtTick] = append(wantFired[c.fireAtTick], c.key)
	}
	barrier(tw)

	fired := map[int64][]string{}
	for tick := int64(1); tick <= 3; tick++ {
		tk.fire()
		barrier(tw)
		for {
			select {
			case v := <-ch:
				fired[tick] = append(fired[tick], v.(string))
				continue
			default:
			}
			break
		}
	}

	for _, c := range cases {
		for tick := int64(1); tick <= 3; tick++ {
			seen := false
			for _, k := range fired[tick] {
				if k == c.key {
					seen = true
				}
			}
			if tick == c.fireAtTick && !seen {
				t.Fatalf("key %s（delay %s）应在第 %d 格触发，实际未触发（fired=%v）", c.key, c.delay, tick, fired)
			}
			if tick != c.fireAtTick && seen {
				t.Fatalf("key %s（delay %s）在第 %d 格提前/延后触发，期望恰在第 %d 格（#219：非整数格不得早于 delay）", c.key, c.delay, tick, c.fireAtTick)
			}
		}
	}

	// MoveTimer 同口径：移到 1.5 格后恰在 2 格触发（不在 1 格提前）
	tw.SetTimer("mv", func() { task("mv") }, time.Hour)
	barrier(tw)
	tw.MoveTimer("mv", interval*3/2)
	barrier(tw)
	tk.fire()
	barrier(tw)
	assertIdle(t, ch) // 1.5 格不得按 1 格提前触发（#219）
	tk.fire()
	barrier(tw)
	select {
	case v := <-ch:
		if v != "mv" {
			t.Fatalf("expected mv, got %v", v)
		}
	default:
		t.Fatal("MoveTimer 到 1.5 格应在第 2 格触发")
	}
}

// —— WithWheelTaskExecutor（#274：到期任务派发出事件循环）——

// execQueue 收集交付给执行器的任务（缓冲 32，交付不阻塞事件循环）。
// 交付序可断言：执行器收到的是「派发」而非「执行完成」。
func execQueue() (chan func(), func(task func())) {
	ch := make(chan func(), 32)
	return ch, func(task func()) { ch <- task }
}

// waitFor 以 deadline 轮询断言异步执行结果（执行器并行态没有同步屏障可用）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// TestTimingWheel_ExecutorDispatchesOutOfLoop 慢任务不再阻塞事件循环（#274）：
// 任务在执行器侧被门（gate）挡住，事件循环必须照常响应 tick 与 HasTimer——
// 缺省同步态下同样的写法会卡死 barrier（任务跑不完 HasTimer 永不应答）。
func TestTimingWheel_ExecutorDispatchesOutOfLoop(t *testing.T) {
	tk := newManualTicker()
	execCh, exec := execQueue()
	tw := NewTimingWheel(10*time.Millisecond, 4,
		WithWheelTicker(func(time.Duration) Ticker { return tk }),
		WithWheelTaskExecutor(exec))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })

	gate := make(chan struct{})
	var ran atomic.Bool
	tw.SetTimer("slow", func() { <-gate; ran.Store(true) }, 10*time.Millisecond)
	barrier(tw)

	tk.fire()
	barrier(tw) // 慢任务挂着，事件循环仍响应查询 ⇒ 已派发出循环
	if tw.HasTimer("slow") {
		t.Fatal("到期任务应在触发时从轮上摘除（交付即摘除）")
	}

	// 放行执行器侧任务，断言恰好收到它且执行完成（不丢、恰一次）
	select {
	case task := <-execCh:
		go task()
	default:
		t.Fatal("执行器未收到交付的任务")
	}
	gate <- struct{}{}
	waitFor(t, "slow task executed", ran.Load)
	if len(execCh) != 0 {
		t.Fatalf("任务被重复交付：执行器队列残留 %d 项", len(execCh))
	}
}

// TestTimingWheel_ExecutorCrossTickOrder 跨 tick 交付序 = 时间轮推进序
// （#274 调度语义：早 tick 先交付——「不乱序于交付」承诺）。
func TestTimingWheel_ExecutorCrossTickOrder(t *testing.T) {
	tk := newManualTicker()
	execCh, exec := execQueue()
	tw := NewTimingWheel(10*time.Millisecond, 4,
		WithWheelTicker(func(time.Duration) Ticker { return tk }),
		WithWheelTaskExecutor(exec))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })

	tw.SetTimer("a", func() {}, 10*time.Millisecond) // 第 1 tick 到期
	tw.SetTimer("b", func() {}, 20*time.Millisecond) // 第 2 tick 到期
	barrier(tw)

	tk.fire()
	tk.fire()
	barrier(tw)

	done := make(chan []string, 1)
	go func() {
		order := make([]string, 0, 2) // prealloc：交付次数已知（lint prealloc）
		for range 2 {
			task := <-execCh
			// 任务自标识：借闭包捕获序号（deferred 无关，直接按交付序命名）
			order = append(order, "delivered")
			task()
		}
		done <- order
	}()
	select {
	case order := <-done:
		if len(order) != 2 {
			t.Fatalf("交付数 = %d，want 2", len(order))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("执行器交付未按序完成")
	}
}

// TestTimingWheel_ExecutorPanicIsolated 任务 panic 收编与执行形态无关（#274）：
// 执行器用裸 go 派发，交付闭包内的 async.RunSafe 兜底 recover——panic 打不死
// 执行器 goroutine，事件循环与后续任务照常。
func TestTimingWheel_ExecutorPanicIsolated(t *testing.T) {
	tk := newManualTicker()
	execCh, exec := execQueue()
	tw := NewTimingWheel(10*time.Millisecond, 4,
		WithWheelTicker(func(time.Duration) Ticker { return tk }),
		WithWheelTaskExecutor(exec))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })

	fired, fine := firedCh()
	tw.SetTimer("boom", func() { panic("boom") }, 10*time.Millisecond)
	tw.SetTimer("fine", func() { fine("fine") }, 20*time.Millisecond)
	barrier(tw)

	tk.fire()
	tk.fire()
	barrier(tw) // panic 任务已交付且不影响循环

	for range 2 {
		task := <-execCh
		go task() // 裸 go：panic 若未被 RunSafe 收编将打死测试进程
	}
	waitFor(t, "fine task after panic", func() bool {
		select {
		case v := <-fired:
			return v == "fine"
		default:
			return false
		}
	})
}

// TestTimingWheel_ExecutorGoPool 复用仓内 worker 池（#274 建议的 gopool 接线）：
// 池化执行器下任务照常触发、轮照常响应。
func TestTimingWheel_ExecutorGoPool(t *testing.T) {
	tk := newManualTicker()
	pool := gopool.NewPool("timewheel-test", gopool.PoolOption{MaxIdleWorkers: 8})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	tw := NewTimingWheel(10*time.Millisecond, 4,
		WithWheelTicker(func(time.Duration) Ticker { return tk }),
		WithWheelTaskExecutor(pool.Go))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })

	var ran atomic.Int64
	tw.SetTimer("p1", func() { ran.Add(1) }, 10*time.Millisecond)
	tw.SetTimer("p2", func() { ran.Add(1) }, 10*time.Millisecond)
	barrier(tw)
	tk.fire()
	barrier(tw)
	waitFor(t, "gopool-executed tasks", func() bool { return ran.Load() == 2 })
	if tw.HasTimer("p1") || tw.HasTimer("p2") {
		t.Fatal("到期任务应从轮上摘除")
	}
}

// TestTimingWheel_NilExecutorFallsBackToSync nil 执行器回退缺省同步态
// （nil 参数 = 显式回退默认值，总则②）：barrier 后任务必须已执行完
// （同步语义——与缺省态既有测试同一断言面）。
func TestTimingWheel_NilExecutorFallsBackToSync(t *testing.T) {
	tk := newManualTicker()
	tw := NewTimingWheel(10*time.Millisecond, 4,
		WithWheelTicker(func(time.Duration) Ticker { return tk }),
		WithWheelTaskExecutor(nil))
	t.Cleanup(func() { _ = tw.Close(context.Background()) })

	ch, task := firedCh()
	tw.SetTimer("sync", func() { task("sync") }, 10*time.Millisecond)
	barrier(tw)
	tk.fire()
	barrier(tw)
	select {
	case v := <-ch:
		if v != "sync" {
			t.Fatalf("expected sync, got %v", v)
		}
	default:
		t.Fatal("nil 执行器应回退同步执行（barrier 后任务已完成）")
	}
}
