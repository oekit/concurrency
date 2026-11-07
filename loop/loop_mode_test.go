package loop

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestAfterCompletion_GapFromCompletion 验证 AfterCompletion 语义：下一轮从「本轮
// 结束」起算（间隔 ≥ Interval），而非固定时刻表（#140）。
// 固定节奏下第二轮本应落在 start1+interval（30ms）；本轮耗时 40ms，AfterCompletion
// 下第二轮最早 start1+40ms+interval=70ms——以此为判据区分两种模式。
func TestAfterCompletion_GapFromCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const fnCost = 40 * time.Millisecond
	const interval = 30 * time.Millisecond

	var runs atomic.Int32
	var start1, end1, start2 atomic.Int64
	l := Run(ctx, Config{Interval: interval, IntervalMode: IntervalAfterCompletion, Name: t.Name()}, func(context.Context) {
		if runs.Add(1) == 1 {
			start1.Store(time.Now().UnixNano())
			time.Sleep(fnCost)
			end1.Store(time.Now().UnixNano())
			return
		}
		start2.Store(time.Now().UnixNano())
	})

	// 第二轮最晚 start1 + fnCost + interval + 余量；超时即失败
	deadline := time.Now().Add(fnCost + interval + 2*time.Second)
	for time.Now().Before(deadline) {
		if runs.Load() >= 2 && start2.Load() != 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	l.Wait()

	if runs.Load() < 2 {
		t.Fatalf("期望至少 2 轮，实际 %d", runs.Load())
	}
	got := time.Duration(start2.Load() - end1.Load())
	if got < interval-5*time.Millisecond {
		t.Fatalf("第二轮距本轮结束 %v，期望 ≥ %v（间隔应从本轮结束起算）", got, interval-5*time.Millisecond)
	}
	// 固定节奏时刻表（start1+interval=30ms）下第二轮会在本轮结束（40ms）前就绪，
	// 与「本轮结束后再等」语义互斥——补一个上界防实现退化成固定 ticker。
	if upper := fnCost + interval + 500*time.Millisecond; got > upper {
		t.Fatalf("第二轮距本轮结束 %v，超过合理上界 %v", got, upper)
	}
}

// TestAfterCompletion_PanicRetimesInterval panic 收编后同样重新计时：本轮 panic
// 被 RunSafe 收编、循环存活，下一轮仍从收编完成后再等 Interval（#140）。
func TestAfterCompletion_PanicRetimesInterval(t *testing.T) {
	async.SetPanicHandler(func(ctx context.Context, r any, stack []byte) { _ = r; _ = stack }) // 静音本用例的 panic 汇报
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const interval = 30 * time.Millisecond

	var runs atomic.Int32
	var panicAt, secondAt atomic.Int64
	l := Run(ctx, Config{Interval: interval, IntervalMode: IntervalAfterCompletion, Name: t.Name()}, func(context.Context) {
		if runs.Add(1) == 1 {
			panicAt.Store(time.Now().UnixNano())
			panic("boom")
		}
		secondAt.Store(time.Now().UnixNano())
	})

	deadline := time.Now().Add(interval + 2*time.Second)
	for time.Now().Before(deadline) {
		if runs.Load() >= 2 && secondAt.Load() != 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	l.Wait()

	if runs.Load() < 2 {
		t.Fatalf("panic 后循环应存活并进入下一轮，实际 %d 轮", runs.Load())
	}
	got := time.Duration(secondAt.Load() - panicAt.Load())
	if got < interval-5*time.Millisecond {
		t.Fatalf("panic 收编到第二轮间隔 %v，期望 ≥ %v（收编完成后重新计时）", got, interval-5*time.Millisecond)
	}
}

// TestAfterCompletion_CancelInterruptsWait ctx 取消可打断轮间等待（不腰斩在飞轮次
// 的契约不变——本用例在飞轮次已完成）。
func TestAfterCompletion_CancelInterruptsWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var runs atomic.Int32
	l := Run(ctx, Config{Interval: time.Hour, IntervalMode: IntervalAfterCompletion, Name: t.Name()}, func(context.Context) {
		runs.Add(1)
	})

	// 等首跑完成进入轮间等待
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runs.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	done := make(chan struct{})
	go func() { l.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancel 后 Wait 未及时返回——轮间等待应可被取消打断")
	}
	if runs.Load() != 1 {
		t.Fatalf("取消后不应再有新轮次，实际 %d", runs.Load())
	}
}
