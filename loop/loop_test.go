package loop

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

func TestRunFirstDelayThenInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var n atomic.Int32
	l := Run(ctx, Config{Interval: 20 * time.Millisecond, FirstDelay: 30 * time.Millisecond}, func(context.Context) {
		n.Add(1)
	})

	// FirstDelay 内不应有执行
	time.Sleep(15 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("FirstDelay 内不应执行，实际 %d", n.Load())
	}

	// FirstDelay 后应有首跑 + 后续 tick
	time.Sleep(80 * time.Millisecond)
	if n.Load() < 2 {
		t.Fatalf("期望至少 2 轮，实际 %d", n.Load())
	}

	cancel()
	l.Wait()
}

func TestFirstDelayInterruptible(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	l := Run(ctx, Config{FirstDelay: time.Hour}, func(context.Context) {
		n.Add(1)
	})

	time.Sleep(20 * time.Millisecond)
	cancel()

	// 排水必须在极短时间内返回（不能等 1 小时）
	done := make(chan struct{})
	go func() {
		l.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("首跑延迟未被停机打断，Wait 超时")
	}
	if n.Load() != 0 {
		t.Fatalf("取消后不应执行，实际 %d", n.Load())
	}
}

func TestBusyDropsTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var n atomic.Int32
	l := Run(ctx, Config{Interval: 10 * time.Millisecond}, func(context.Context) {
		n.Add(1)
		time.Sleep(50 * time.Millisecond) // 比 Interval 长 → 必然丢 tick
	})

	time.Sleep(200 * time.Millisecond)
	got := n.Load()
	// 200ms 内若不丢 tick 理论 ~20 轮；丢 tick 后应远小于 10
	if got >= 10 {
		t.Fatalf("忙时未丢 tick：200ms 内执行 %d 轮", got)
	}
	if got < 1 {
		t.Fatal("至少应有首跑")
	}

	cancel()
	l.Wait()
}

func TestPanicDoesNotKillLoop(t *testing.T) {
	var panics atomic.Int32
	async.SetPanicHandler(async.PanicHandler(func(msg string, stack []byte) {
		panics.Add(1)
	}))
	defer async.SetPanicHandler(async.PanicHandler(func(msg string, stack []byte) {
		// 恢复为静默 handler，不影响其他用例
	}))

	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int32
	l := Run(ctx, Config{Interval: 15 * time.Millisecond, Name: "flaky"}, func(context.Context) {
		n.Add(1)
		panic("boom")
	})

	time.Sleep(100 * time.Millisecond)
	cancel()
	l.Wait()

	if n.Load() < 2 {
		t.Fatalf("panic 后循环应存活，实际只执行 %d 轮", n.Load())
	}
	if panics.Load() < 2 {
		t.Fatalf("panic 应被收编汇报，实际 %d 次", panics.Load())
	}
}

func TestCancelStopsNextRoundInFlightCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	inFlight := make(chan struct{})
	release := make(chan struct{})
	var rounds atomic.Int32

	l := Run(ctx, Config{Interval: time.Hour}, func(context.Context) {
		rounds.Add(1)
		if rounds.Load() == 1 {
			close(inFlight)
			<-release // 模拟在飞工作
		}
	})

	<-inFlight
	// 在飞轮次进行中取消
	cancel()
	// 给取消传播一点时间
	time.Sleep(20 * time.Millisecond)

	select {
	case <-l.Done():
		t.Fatal("在飞轮次跑完前不应排水完成")
	default:
	}

	close(release)
	l.Wait()
	if rounds.Load() != 1 {
		t.Fatalf("取消后不应再跑下一轮，实际 %d", rounds.Load())
	}
}

func TestWaitIdempotentAndDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	l := Run(ctx, Config{FirstDelay: 5 * time.Millisecond}, func(context.Context) {})
	select {
	case <-l.Done():
		t.Fatal("首跑延迟内不应已完成")
	default:
	}
	cancel()
	l.Wait()
	l.Wait() // 重复调用安全
	select {
	case <-l.Done():
	default:
		t.Fatal("Wait 返回后 Done 应已关闭")
	}
}

func TestZeroIntervalRunsOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var n atomic.Int32
	_ = Run(ctx, Config{}, func(context.Context) { n.Add(1) })
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 1 {
		t.Fatalf("零 Interval 应只跑首跑轮，实际 %d", n.Load())
	}
}
