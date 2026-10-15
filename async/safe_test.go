package async

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 在超时前轮询 until 返回 true，用于等待 goroutine 完成。
func waitFor(t *testing.T, until func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if until() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestGoSafe_NoPanic(t *testing.T) {
	var ran atomic.Int32
	GoSafe(func() {
		ran.Store(1)
	})
	waitFor(t, func() bool { return ran.Load() == 1 }, time.Second)
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
