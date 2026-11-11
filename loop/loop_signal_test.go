//go:build unix

package loop

import (
	"context"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// 本文件测试 #218（RunBackground 的进程级信号副作用 + opt-out）。
//
// 放置于 loop_signal_test.go（排序先于 loop_test.go）：TestRunBackground_RunsOnProcessContext
// 会经 RunBackground 触发 processContext 的 sync.Once，本文件的用例需要在
// 「注册尚未被其他用例消耗」的状态下先行运行才能钉住注册分支的真实行为。

// TestDisableBackgroundSignalHandling 端到端钉住 opt-out 语义（#218）：
//
//  1. 注册分支：processContext 安装了 SIGINT/SIGTERM 监听（stop 非 nil、
//     procCtx 可取消）；
//  2. Disable 撤销注册：自发 SIGTERM 送达测试自身的 handler（证明信号真的
//     发出、进程默认终止被 handler 兜住），但**不再取消 procCtx**（signal.Stop
//     生效、已启动循环继续存活的语义钉点）；
//  3. opt-out 后 RunBackground 照常运行（positive control）；
//  4. opt-out 分支：processSignalContext 此后返回 Background + nil stop
//     （永不注册）。
//
// 不对「进程是否因 SIGTERM 死亡」做断言（无法在单测内断言进程自身死亡）；
// 默认终止行为恢复由「procCtx 不再被取消」间接钉住——测试自身的 handler 全程
// 注册，进程不会真死。
func TestDisableBackgroundSignalHandling(t *testing.T) {
	// ① 注册分支（此刻 flag 尚未置位——本用例最先运行，Once/flag 均未被消耗）
	_ = processContext()
	if procCtxStop == nil {
		t.Fatal("前置失效：processContext 未安装 Notify")
	}
	if procCtx.Done() == nil {
		t.Fatal("注册分支的 procCtx 应有可取消的 Done")
	}

	// 测试自身的 SIGTERM handler：兜住默认终止（进程不死），并证明信号送达
	ours := make(chan os.Signal, 1)
	signal.Notify(ours, syscall.SIGTERM)
	defer signal.Stop(ours)

	// ② opt-out：撤销注册、恢复默认行为
	DisableBackgroundSignalHandling()

	// ③ opt-out 后 RunBackground 照常运行（positive control：循环不依赖信号）
	var n atomic.Int32
	_ = RunBackground(Config{Interval: 10 * time.Millisecond, Name: "optout-probe"}, func(context.Context) {
		n.Add(1)
	})
	deadline := time.Now().Add(3 * time.Second)
	for n.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.Load() < 2 {
		t.Fatalf("opt-out 后 RunBackground 应照常运行，实际 %d 轮", n.Load())
	}

	// 自发 SIGTERM：送达测试 handler（信号确实发出），但不再取消 procCtx
	_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	select {
	case <-ours:
	case <-time.After(2 * time.Second):
		t.Fatal("SIGTERM 未送达测试 handler（测试前提失效）")
	}
	select {
	case <-procCtx.Done():
		t.Fatal("opt-out 后 SIGTERM 不应再取消 procCtx——signal.Stop 未生效（#218）")
	case <-time.After(100 * time.Millisecond):
		// 预期：procCtx 不再是信号接收方（默认终止行为已恢复、循环继续存活）
	}

	// ④ opt-out 分支：此后永不注册
	ctx2, stop2 := processSignalContext()
	if ctx2 != context.Background() {
		t.Fatalf("opt-out 后应返回 Background，got %v", ctx2)
	}
	if stop2 != nil {
		t.Fatal("opt-out 后不应再注册信号（stop 应为 nil）")
	}
}

// TestSignalWatcherExitsOnOptOut #270 回归：opt-out 后信号观察 goroutine 有
// 退出路径——stopCh 关闭后 watchProcessSignals 返回，不再永久阻塞在 select
// （此前 ctx 无人取消、ch 永不关闭，注册过一次的进程泄漏一个 goroutine）。
// ① 单元面：watchProcessSignals 对 stopCh 的直接响应（不依赖进程级 Once
// 状态）+ 信号路径回归防护（退出路径不得遮蔽信号路径）；
// ② 集成面：DisableBackgroundSignalHandling 后进程级观察 goroutine 退出
// （procSignalWatchDone 为 nil 说明注册分支从未建立，跳过）。
func TestSignalWatcherExitsOnOptOut(t *testing.T) {
	// ① stopCh 关闭即退出，且不取消 ctx（#218 语义：已启动循环继续存活）
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	stopCh := make(chan struct{})
	done := watchProcessSignals(ctx, cancel, ch, stopCh)
	close(stopCh)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("opt-out 后观察 goroutine 未退出（泄漏，#270）")
	}
	if ctx.Err() != nil {
		t.Fatal("opt-out 退出路径不应取消 ctx（已启动循环必须存活，#218）")
	}

	// 信号路径仍取消 ctx（回归防护）
	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := make(chan os.Signal, 1)
	done2 := watchProcessSignals(ctx2, cancel2, ch2, make(chan struct{}))
	ch2 <- syscall.SIGTERM
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("信号路径未触发退出")
	}
	if ctx2.Err() == nil {
		t.Fatal("信号应取消 ctx")
	}

	// ② 进程级：注册已发生（前置用例触发过 processContext）则 Disable 后
	// 观察 goroutine 退出
	DisableBackgroundSignalHandling()
	if procSignalWatchDone != nil {
		select {
		case <-procSignalWatchDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Disable 后进程级观察 goroutine 未退出（泄漏，#270）")
		}
	}
}
