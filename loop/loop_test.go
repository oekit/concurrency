package loop

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

func TestRunFirstDelayThenInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const firstDelay = 30 * time.Millisecond
	var n atomic.Int32
	firstAt := make(chan time.Duration, 1)
	start := time.Now()
	l := Run(ctx, Config{Interval: 20 * time.Millisecond, FirstDelay: firstDelay}, func(context.Context) {
		if n.Add(1) == 1 {
			select {
			case firstAt <- time.Since(start):
			default:
			}
		}
	})

	// 「FirstDelay 内不应执行」不能靠「睡 15ms 再取样」来判：time.Sleep 在负载下
	// 会超时到 30ms 之外，取样时刻本身就不确定 ⇒ 断言随时会假红（实测 50 次跑出
	// 「实际 1」）。改为**量首跑的实际时刻**，判据与 FirstDelay 直接比。
	select {
	case d := <-firstAt:
		if d < firstDelay {
			t.Fatalf("首跑早于 FirstDelay：%v < %v", d, firstDelay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("FirstDelay 后仍未首跑")
	}

	// FirstDelay 后至少再有一轮 tick：等**事件**不等等时间窗（超时只作兜底）。
	deadline := time.Now().Add(3 * time.Second)
	for n.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
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
	async.SetPanicHandler(async.PanicHandler(func(ctx context.Context, r any, stack []byte) {
		panics.Add(1)
	}))
	defer async.SetPanicHandler(async.PanicHandler(func(ctx context.Context, r any, stack []byte) {
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

// TestRun_PreCanceledCtxSkipsAndWarns 入口已取消的 ctx：循环连首跑都不执行
// 并留下可定位的 warn 日志（#154）——「短函数 + defer cancel」脚枪的可观测出口，
// 与 FirstDelay 路径的取消语义对齐（此前非 FirstDelay 路径会带死 ctx 静默跑一轮）。
func TestRun_PreCanceledCtxSkipsAndWarns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(os.Stderr); log.SetFlags(log.LstdFlags) })

	var n atomic.Int32
	l := Run(ctx, Config{Interval: time.Hour, Name: "sweeper"}, func(context.Context) {
		n.Add(1)
	})
	l.Wait()

	if n.Load() != 0 {
		t.Fatalf("已取消 ctx 不应执行任何轮次，实际 %d 轮", n.Load())
	}
	out := buf.String()
	if !strings.Contains(out, "sweeper") || !strings.Contains(out, "已取消") {
		t.Fatalf("缺少可定位的退役日志，got %q", out)
	}
}

// TestRunBackground_RunsOnProcessContext RunBackground 的生命周期挂进程级 ctx
// （#154 脚枪的根治形态）：不随调用方函数返回/局部 ctx 取消而退役，持续按
// Interval 运行。
func TestRunBackground_RunsOnProcessContext(t *testing.T) {
	var n atomic.Int32
	_ = RunBackground(Config{Interval: 20 * time.Millisecond, Name: "bg-probe"}, func(context.Context) {
		n.Add(1)
	})

	// 等轮次事件而非等时间窗：凑满 3 轮即证明循环持续存活（超时只作兜底）
	deadline := time.Now().Add(3 * time.Second)
	for n.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.Load() < 3 {
		t.Fatalf("后台循环未持续运行，%d 轮", n.Load())
	}
}
