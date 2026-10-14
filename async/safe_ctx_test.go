package async

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGoSafeWithErr_ReportsError 钉住：fn 返回的 error 经 PanicHandler 上报
// （AGENTS.md 并发规范记载的 helper，2026-09-15 治理轮补齐实现）。
func TestGoSafeWithErr_ReportsError(t *testing.T) {
	var mu sync.Mutex
	var got string
	old := *panicHandler.Load()
	SetPanicHandler(func(msg string, _ []byte) {
		mu.Lock()
		got = msg
		mu.Unlock()
	})
	t.Cleanup(func() { SetPanicHandler(old) })

	done := make(chan struct{})
	GoSafeWithErr(func() error {
		defer close(done)
		return context.DeadlineExceeded
	})
	<-done
	<-time.After(50 * time.Millisecond) // 等 handler 异步落盘

	mu.Lock()
	defer mu.Unlock()
	if got == "" || !contains(got, "task error") {
		t.Errorf("expected task error reported via panic handler, got %q", got)
	}
}

// TestGoSafeWithErr_PanicRecovered 钉住：fn panic 被收编为 error 上报，进程不崩。
func TestGoSafeWithErr_PanicRecovered(t *testing.T) {
	var mu sync.Mutex
	var got string
	old := *panicHandler.Load()
	SetPanicHandler(func(msg string, _ []byte) {
		mu.Lock()
		got = msg
		mu.Unlock()
	})
	t.Cleanup(func() { SetPanicHandler(old) })

	done := make(chan struct{})
	GoSafeWithErr(func() error {
		defer close(done)
		panic("boom")
	})
	<-done
	<-time.After(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if !contains(got, "boom") {
		t.Errorf("expected panic message reported, got %q", got)
	}
}

// TestGoSafeWithContext_ErrorAndPanic 钉住 GoSafeWithContext 的 ctx 透传、
// error 上报与 panic 收编三条路径。
func TestGoSafeWithContext_ErrorAndPanic(t *testing.T) {
	var ran atomic.Bool
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")

	var mu sync.Mutex
	var msgs []string
	old := *panicHandler.Load()
	SetPanicHandler(func(msg string, _ []byte) {
		mu.Lock()
		msgs = append(msgs, msg)
		mu.Unlock()
	})
	t.Cleanup(func() { SetPanicHandler(old) })

	GoSafeWithContext(ctx, func(ctx context.Context) error {
		if ctx.Value(ctxKey{}) != "v" {
			t.Error("ctx not propagated")
		}
		ran.Store(true)
		return errTest{}
	})
	<-time.After(50 * time.Millisecond)

	done2 := make(chan struct{})
	GoSafeWithContext(ctx, func(ctx context.Context) error {
		defer close(done2)
		panic("ctx boom")
	})
	<-done2
	<-time.After(50 * time.Millisecond)

	if !ran.Load() {
		t.Error("fn should have run")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(msgs) < 2 {
		t.Errorf("expected error + panic both reported, got %v", msgs)
	}
}

// TestGoWithContext_RunsFn 基本路径。
func TestGoWithContext_RunsFn(t *testing.T) {
	var ran atomic.Bool
	done := make(chan struct{})
	GoWithContext(context.Background(), func(ctx context.Context) {
		ran.Store(true)
		close(done)
	})
	<-done
	if !ran.Load() {
		t.Error("fn should have run")
	}
}

type ctxKey struct{}

type errTest struct{}

func (errTest) Error() string { return "test error" }

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
