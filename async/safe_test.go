package async

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitSignal 等一个「被触发」的信号（事件驱动）。
//
// 为什么不用轮询：panic handler 是在 **recover 之后**才被调用的，而被测 fn 的
// defer（close(done)）**先于** recover 执行 ⇒ 主协程从 <-done 醒来时，handler
// 很可能还没把消息写进 atomic.Value ⇒ 后面只能靠轮询兜底。
//
// 轮询兜底的代价是**硬超时窗口**：原先只给 1 秒，在 CI runner 负载高时（并发跑
// 多个 job、容器被抢占）就偶发红——2026-10-29 实测把一个**只改 README** 的 PR
// 无端挂起（重跑即绿）。
//
// ⇒ 改成事件驱动：handler 触发时 close 一个 channel ⇒ 用例等那个 channel
// ⇒ 没有「值还没写进去」的窗口，也不依赖调度快慢。
func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待 %s 超时（5s）", what)
	}
}

func TestGoSafe_NoPanic(t *testing.T) {
	ran := make(chan struct{})
	GoSafe(func() {
		close(ran)
	})
	waitSignal(t, ran, "fn 执行")
}

func TestGoSafe_PanicRecovered(t *testing.T) {
	// panic 必须被 recover，否则整个测试进程崩溃退出。
	GoSafe(func() {
		panic("boom")
	})
	// 到达此行即说明进程未崩溃（recover 生效）
	time.Sleep(50 * time.Millisecond)
}

// TestGoSafeWithErrChannel_ClosedChannel 回归测试：调用方提前关闭 channel
// 且 fn panic 时，旧实现会在 defer 内二次 panic（send on closed channel）
// 直接崩溃进程；修复后必须吞掉并经 PanicHandler 记录。
func TestGoSafeWithErrChannel_ClosedChannel(t *testing.T) {
	orig := panicHandler.Load()
	var logged atomic.Bool
	h := PanicHandler(func(msg string, stack []byte) {
		if strings.Contains(msg, "send to error channel") {
			logged.Store(true)
		}
	})
	SetPanicHandler(h)
	defer func() {
		if orig != nil {
			SetPanicHandler(*orig)
		}
	}()

	ch := make(chan error, 1)
	close(ch)
	done := make(chan struct{})
	GoSafeWithErrChannel(ch, func() error {
		defer close(done)
		panic("boom")
	})
	<-done

	// 给 send 的 recover 一点时间执行
	deadline := time.Now().Add(2 * time.Second)
	for !logged.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !logged.Load() {
		t.Error("expected send-on-closed-channel panic to be logged via PanicHandler")
	}
}

// TestGoSafeWithErrChannel_NormalPath 验证正常路径 err 与 panic 都进 channel。
func TestGoSafeWithErrChannel_NormalPath(t *testing.T) {
	ch := make(chan error, 2)

	GoSafeWithErrChannel(ch, func() error { return errors.New("task failed") })
	GoSafeWithErrChannel(ch, func() error { panic("task panic") })

	got := 0
	deadline := time.After(2 * time.Second)
	for got < 2 {
		select {
		case err := <-ch:
			if err == nil {
				t.Error("expected non-nil error")
			}
			got++
		case <-deadline:
			t.Fatalf("expected 2 errors, got %d", got)
		}
	}
}

// TestSetPanicHandler_NilHandler 回归：SetPanicHandler(nil) 后 GoSafe 的
// recover 闭包对 nil func 解引用产生二次 panic——defer 内的新 panic 不会
// 被同一个 defer 恢复，直接打死进程。
func TestSetPanicHandler_NilHandler(t *testing.T) {
	SetPanicHandler(nil)
	done := make(chan struct{})
	GoSafe(func() {
		defer close(done)
		panic("should be absorbed")
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("nil panic handler turned recoverable panic into process crash")
	}
}

// TestSetPanicHandler_HandlerPanics 回归：注入的 handler 自身 panic 时
// 同样必须被收编，不能击穿 GoSafe 的 recover 语义。
func TestSetPanicHandler_HandlerPanics(t *testing.T) {
	SetPanicHandler(func(msg string, stack []byte) { panic("handler boom") })
	defer SetPanicHandler(nil)
	done := make(chan struct{})
	GoSafe(func() {
		defer close(done)
		panic("should be absorbed")
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("panicking handler crashed the process")
	}
}

func TestRunSafeLabeled_PanicMessage(t *testing.T) {
	orig := panicHandler.Load()
	var got atomic.Value
	SetPanicHandler(PanicHandler(func(msg string, stack []byte) {
		got.Store(msg)
	}))
	defer func() {
		if orig != nil {
			SetPanicHandler(*orig)
		}
	}()

	RunSafeLabeled("batch-flush", func() {
		panic("boom")
	})
	msg, _ := got.Load().(string)
	if msg != "task=batch-flush: boom" {
		t.Fatalf("期望 task 前缀消息，得到 %q", msg)
	}
}

func TestGoSafeLabeled_PanicMessage(t *testing.T) {
	orig := panicHandler.Load()
	var got atomic.Value
	stored := make(chan struct{})
	var once sync.Once
	SetPanicHandler(PanicHandler(func(msg string, stack []byte) {
		got.Store(msg)
		once.Do(func() { close(stored) })
	}))
	defer func() {
		if orig != nil {
			SetPanicHandler(*orig)
		}
	}()

	GoSafeLabeled("janitor", func() {
		panic("kaboom")
	})
	// handler 在 recover 之后才被调用 ⇒ 等 handler 真的触发（事件驱动），
	// 而不是「等一个可能还没写进去的原子值」。
	waitSignal(t, stored, "panic handler 被调用")
	msg, ok := got.Load().(string)
	if !ok || msg != "task=janitor: kaboom" {
		t.Fatalf("panic handler 收到的消息 = %q，期望 %q", msg, "task=janitor: kaboom")
	}
}

func TestRunSafeLabeled_EmptyTaskFallsBack(t *testing.T) {
	orig := panicHandler.Load()
	var got atomic.Value
	SetPanicHandler(PanicHandler(func(msg string, stack []byte) {
		got.Store(msg)
	}))
	defer func() {
		if orig != nil {
			SetPanicHandler(*orig)
		}
	}()

	RunSafeLabeled("", func() { panic("plain") })
	msg, _ := got.Load().(string)
	if msg != "plain" {
		t.Fatalf("空 task 应退化为无前缀，得到 %q", msg)
	}
}

func TestGoSafeWithContextLabeled(t *testing.T) {
	orig := panicHandler.Load()
	var got atomic.Value
	stored := make(chan struct{})
	var once sync.Once
	SetPanicHandler(PanicHandler(func(msg string, stack []byte) {
		got.Store(msg)
		once.Do(func() { close(stored) })
	}))
	defer func() {
		if orig != nil {
			SetPanicHandler(*orig)
		}
	}()

	GoSafeWithContextLabeled(context.Background(), "heartbeat", func(ctx context.Context) {
		panic("hb-fail")
	})
	// handler 在 recover 之后才被调用 ⇒ 事件驱动等它触发（不再靠 1 秒轮询兜底）。
	waitSignal(t, stored, "panic handler 被调用")
	msg, ok := got.Load().(string)
	if !ok || msg != "task=heartbeat: hb-fail" {
		t.Fatalf("panic handler 收到的消息 = %q，期望 %q", msg, "task=heartbeat: hb-fail")
	}
}
