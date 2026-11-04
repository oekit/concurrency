package async

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestSupervise_RestartAfterPanic 锁定 #137 核心：单轮 panic 收编 + 自动重启 +
// 优雅结束。修复前消费方（octopus 三处手写同型）循环体 panic 后 goroutine 死亡、
// 循环静默停摆。
func TestSupervise_RestartAfterPanic(t *testing.T) {
	var calls atomic.Int64
	sup := Supervise(context.Background(), func(ctx context.Context) error {
		if calls.Add(1) < 3 {
			panic("boom") // 前两轮 panic，第三轮优雅返回
		}
		return nil
	}, WithRestartBackoff(time.Millisecond, time.Millisecond))

	if err := sup.Wait(); err != nil {
		t.Fatalf("第三轮成功后应优雅结束，got %v", err)
	}
	if n := sup.Rounds(); n != 3 {
		t.Fatalf("Rounds=%d，want 3（panic 轮也计入完成轮数）", n)
	}
	if n := sup.Restarts(); n != 2 {
		t.Fatalf("Restarts=%d，want 2", n)
	}
	if n := sup.ConsecutiveFailures(); n != 0 {
		t.Fatalf("优雅结束后连续失败应为 0，got %d", n)
	}
}

// TestSupervise_ErrorRestartThenCancel 恒败循环：失败重启照常进行，ctx 取消后
// 停止重启并以 ctx.Err() 告终（重启等待可被取消打断）。
func TestSupervise_ErrorRestartThenCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var rounds atomic.Int64
	sup := Supervise(ctx, func(ctx context.Context) error {
		rounds.Add(1)
		return errors.New("恒败")
	}, WithRestartBackoff(2*time.Millisecond, time.Millisecond))

	// 等 3 轮失败后再取消（轮询 Rounds，事件驱动上限兜底）
	deadline := time.Now().Add(2 * time.Second)
	for sup.Rounds() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := sup.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后应以 ctx.Err() 告终，got %v", err)
	}
	if got := rounds.Load(); got < 3 {
		t.Fatalf("取消前至少应跑 3 轮，got %d", got)
	}
}

// TestSupervise_GiveUpAfterConsecutiveFailures 连续失败达上限：放弃重启、
// 告警钩子收到 *PanicError、Wait 以该错误告终。
func TestSupervise_GiveUpAfterConsecutiveFailures(t *testing.T) {
	var gaveUp atomic.Int64
	var restartsSeen atomic.Int64
	sup := Supervise(context.Background(), func(ctx context.Context) error {
		panic("常驻错误")
	}, WithRestartBackoff(time.Millisecond, time.Millisecond),
		WithMaxConsecutiveFailures(2),
		WithOnRestart(func(restarts int64, cause error, wait time.Duration) {
			restartsSeen.Store(restarts)
		}),
		WithOnGiveUp(func(cause error) { gaveUp.Add(1) }))

	err := sup.Wait()
	if err == nil {
		t.Fatal("放弃重启后应返回最后的失败原因")
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("失败原因应可 errors.As 为 *PanicError，got %T %v", err, err)
	}
	if n := sup.Rounds(); n != 2 {
		t.Fatalf("上限 2 = 恰好 2 轮失败后放弃，Rounds=%d", n)
	}
	if n := sup.Restarts(); n != 1 {
		t.Fatalf("第 2 轮失败后不再重启，Restarts=%d", n)
	}
	if n := sup.ConsecutiveFailures(); n != 2 {
		t.Fatalf("ConsecutiveFailures 应保留终值 2，got %d", n)
	}
	if got := gaveUp.Load(); got != 1 {
		t.Fatalf("onGiveUp 应恰好调用一次，got %d", got)
	}
	if got := restartsSeen.Load(); got != 1 {
		t.Fatalf("onRestart 只应在唯一一次重启前调用，got %d", got)
	}
}

// TestSupervise_GracefulNil loop 立即优雅返回：监督随即结束、零重启。
func TestSupervise_GracefulNil(t *testing.T) {
	sup := Supervise(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err := sup.Wait(); err != nil {
		t.Fatalf("优雅结束应为 nil，got %v", err)
	}
	if sup.Restarts() != 0 || sup.Rounds() != 1 {
		t.Fatalf("Rounds=1/Restarts=0，got %d/%d", sup.Rounds(), sup.Restarts())
	}
}

// TestSupervise_CancelledBeforeStart ctx 已取消时不启动任何一轮。
func TestSupervise_CancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var calls atomic.Int64
	sup := Supervise(ctx, func(ctx context.Context) error {
		calls.Add(1)
		return nil
	})
	if err := sup.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("未首跑即取消应以 ctx.Err() 告终，got %v", err)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("已取消的 ctx 不应再跑 loop，实际跑了 %d 轮", n)
	}
}

// TestSupervise_HookPanicDoesNotKillSupervision 钩子自身 panic 被收编，
// 监督循环不受影响。
func TestSupervise_HookPanicDoesNotKillSupervision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := Supervise(ctx, func(ctx context.Context) error {
		return errors.New("boom")
	}, WithRestartBackoff(time.Millisecond, 0),
		WithOnRestart(func(restarts int64, cause error, wait time.Duration) {
			panic("hook 自身 panic")
		}))

	// 恒败 + 不限连续失败：只要钩子 panic 不打死监督，轮数就会持续增长
	deadline := time.Now().Add(2 * time.Second)
	for sup.Rounds() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	// Wait 到结束再离开：不留后台监督继续向全局 panic handler 汇报
	// （否则污染同包后续用例的 handler 断言）
	if err := sup.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后应以 ctx.Err() 告终，got %v", err)
	}
	if sup.Rounds() < 3 {
		t.Fatalf("onRestart panic 不应打死监督循环，Rounds=%d", sup.Rounds())
	}
}

// TestSupervise_NilLoopPanics 调用期 fail-fast：nil loop 是编程错误。
func TestSupervise_NilLoopPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil loop 应在 Supervise 调用时 panic")
		}
	}()
	Supervise(context.Background(), nil)
}

// TestRestartBackoff 节奏公式：指数翻倍封顶 / 固定档 / 立即档 / 溢出饱和。
func TestRestartBackoff(t *testing.T) {
	cases := []struct {
		name         string
		k            int64
		initial, max time.Duration
		want         time.Duration
	}{
		{"第1次失败等初值", 1, time.Second, 30 * time.Second, time.Second},
		{"第2次翻倍", 2, time.Second, 30 * time.Second, 2 * time.Second},
		{"封顶", 10, time.Second, 30 * time.Second, 30 * time.Second},
		{"max<=0 固定档", 5, 500 * time.Millisecond, 0, 500 * time.Millisecond},
		{"initial<=0 立即档", 3, 0, time.Second, 0},
		{"initial>=max 直接封顶", 1, time.Minute, time.Second, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := restartBackoff(tc.k, tc.initial, tc.max); got != tc.want {
				t.Fatalf("restartBackoff(%d, %v, %v)=%v，want %v", tc.k, tc.initial, tc.max, got, tc.want)
			}
		})
	}
	// 溢出防护：再翻倍必溢出时取封顶值（真实等待只会更大），不回绕为负
	// （负等待 = 零等待，静默绕过退避）
	if got := restartBackoff(100, time.Hour, time.Duration(1<<62)); got != time.Duration(1<<62) {
		t.Fatalf("指数溢出应封顶为 max，got %v", got)
	}
}
