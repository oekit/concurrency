package timewheel

import (
	"context"
	"testing"
	"time"
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

	tw.SetTimer("k", func() { task("k") }, 15*time.Millisecond) // 不足一格取一格
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
	for i := 0; i < 4; i++ {
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
	for i := 0; i < 6; i++ {
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
