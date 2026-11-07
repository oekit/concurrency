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
// #184：上报 r 即该 error 值本身（不再是 "task error: ..." 拼接消息）。
func TestGoSafeWithErr_ReportsError(t *testing.T) {
	var mu sync.Mutex
	var got any
	old := *panicHandler.Load()
	SetPanicHandler(func(ctx context.Context, r any, _ []byte) {
		mu.Lock()
		got = r
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
	if got != any(context.DeadlineExceeded) {
		t.Errorf("expected task error reported via panic handler, got %#v", got)
	}
}

// TestGoSafeWithErr_PanicRecovered 钉住：fn panic 被收编为 error 上报，进程不崩。
func TestGoSafeWithErr_PanicRecovered(t *testing.T) {
	var mu sync.Mutex
	var got any
	old := *panicHandler.Load()
	SetPanicHandler(func(ctx context.Context, r any, _ []byte) {
		mu.Lock()
		got = r
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
	if got != any("boom") {
		t.Errorf("expected panic value reported, got %#v", got)
	}
}

// TestGoSafeWithContext_ErrorAndPanic 钉住 GoSafeWithContext 的 ctx 透传、
// error 上报与 panic 收编三条路径；#184：error 与 panic 汇报都携带调用方 ctx。
func TestGoSafeWithContext_ErrorAndPanic(t *testing.T) {
	var ran atomic.Bool
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")

	var mu sync.Mutex
	var rs []any
	var hctxOK []bool
	old := *panicHandler.Load()
	SetPanicHandler(func(hctx context.Context, r any, _ []byte) {
		mu.Lock()
		rs = append(rs, r)
		hctxOK = append(hctxOK, hctx.Value(ctxKey{}) == "v")
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
	if len(rs) < 2 {
		t.Errorf("expected error + panic both reported, got %v", rs)
	}
	for i, ok := range hctxOK {
		if !ok {
			t.Errorf("第 %d 次汇报的 ctx 未携带调用方 ctx 值", i)
		}
	}
	if len(rs) >= 2 && rs[0] != any(errTest{}) {
		t.Errorf("error 路径的 r 应为 errTest 值，got %#v", rs[0])
	}
	if len(rs) >= 2 && rs[1] != any("ctx boom") {
		t.Errorf("panic 路径的 r 应为原始 panic 值，got %#v", rs[1])
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
