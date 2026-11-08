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
