package async

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// capturePanic 安装一个收集型 PanicHandler，返回「是否被调用」的读取闭包。
// 与 safe_test.go 的 waitSignal 同一套事件驱动口径：不轮询、不留超时窗口。
// #184 起 handler 收 (ctx, r, stack)，这里把 r 与汇报 ctx 一并存下。
func capturePanic(t *testing.T) func() (r any, hctx context.Context, called bool) {
	t.Helper()
	var mu sync.Mutex
	var got any
	var gotCtx context.Context
	called := make(chan struct{}, 1)
	old := *panicHandler.Load()
	SetPanicHandler(func(ctx context.Context, r any, _ []byte) {
		mu.Lock()
		got = r
		gotCtx = ctx
		mu.Unlock()
		select {
		case called <- struct{}{}:
		default:
		}
	})
	t.Cleanup(func() { SetPanicHandler(old) })
	return func() (any, context.Context, bool) {
		select {
		case <-called:
			mu.Lock()
			defer mu.Unlock()
			return got, gotCtx, true
		default:
			mu.Lock()
			defer mu.Unlock()
			return got, gotCtx, false
		}
	}
}

// TestRunSafeCtx_Normal 钉住：fn 正常返回 ⇒ nil 错误，且不触发全局 handler。
func TestRunSafeCtx_Normal(t *testing.T) {
	read := capturePanic(t)

	ran := false
	if err := RunSafeCtx(context.Background(), func(context.Context) { ran = true }); err != nil {
		t.Fatalf("正常返回应为 nil，got %v", err)
	}
	if !ran {
		t.Fatal("fn 未被调用")
	}
	if r, _, called := read(); called {
		t.Fatalf("正常返回不该触发 PanicHandler，got %v", r)
	}
}

// TestRunSafeCtx_PanicReturnsPanicError 钉住核心契约：panic 收编为 *PanicError
// **返回给调用方**（这是本函数与 GoSafe 系列的分水岭），带 Value 与 Stack。
func TestRunSafeCtx_PanicReturnsPanicError(t *testing.T) {
	capturePanic(t)

	err := RunSafeCtx(context.Background(), func(context.Context) { panic("boom-ctx") })
	if err == nil {
		t.Fatal("panic 必须返回非 nil error")
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("应为 *PanicError，got %T: %v", err, err)
	}
	if pe.Value != "boom-ctx" {
		t.Fatalf("PanicError.Value 应保留原始 payload，got %v", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError.Stack 不能为空（调用方要靠它定位）")
	}
}

// TestRunSafeCtx_PanicAlsoReported 钉住：返回给调用方**同时**向全局 PanicHandler
// 汇报一份——否则「调用方自己记了一笔」会让全局观测面漏掉这类事件。
// #184：汇报 r 为 recover 原始值、ctx 为调用方传入的 ctx（按请求聚合的依据）。
func TestRunSafeCtx_PanicAlsoReported(t *testing.T) {
	read := capturePanic(t)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "req-9")

	_ = RunSafeCtx(ctx, func(context.Context) { panic("boom-report") })

	r, hctx, called := read()
	if !called {
		t.Fatal("panic 必须经全局 PanicHandler 上报一份")
	}
	if r != "boom-report" {
		t.Fatalf("上报的 r 应为原始 payload，got %v", r)
	}
	if v, _ := hctx.Value(key{}).(string); v != "req-9" {
		t.Fatalf("汇报 ctx 应为调用方 ctx，got %v", hctx)
	}
}

// TestRunSafeCtx_ContextPassedThrough 钉住：ctx 原样交给 fn（本函数不做取消判定、
// 不加超时），回调能用它取 trace/租户。
func TestRunSafeCtx_ContextPassedThrough(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "tenant-a")

	var got any
	if err := RunSafeCtx(ctx, func(c context.Context) { got = c.Value(key{}) }); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "tenant-a" {
		t.Fatalf("ctx 应原样透传给 fn，got %v", got)
	}
}

// TestRunSafeCtx_CanceledCtxStillRuns 钉住：ctx 已取消不是本函数的失败语义——
// 收编点必须照常执行 fn（否则 SDK 回调里「先判 ctx 再收尾」的写法会被悄悄跳过）。
func TestRunSafeCtx_CanceledCtxStillRuns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ran := false
	if err := RunSafeCtx(ctx, func(context.Context) { ran = true }); err != nil {
		t.Fatalf("ctx 取消不该让本函数返回错误，got %v", err)
	}
	if !ran {
		t.Fatal("ctx 已取消仍应执行 fn")
	}
}

// TestRunSafeCtx_NilFunc 钉住：fn 为 nil 也收编成 *PanicError，不漏到调用方。
func TestRunSafeCtx_NilFunc(t *testing.T) {
	capturePanic(t)

	err := RunSafeCtx(context.Background(), nil)
	if err == nil {
		t.Fatal("nil fn 应被收编成 error")
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("应为 *PanicError，got %T", err)
	}
}

// TestRunSafeCtxRet_Value 钉住：正常路径原样返回值与错误。
func TestRunSafeCtxRet_Value(t *testing.T) {
	got, err := RunSafeCtxRet(context.Background(), func(context.Context) (int, error) {
		return 42, nil
	})
	if err != nil || got != 42 {
		t.Fatalf("got (%v, %v)，want (42, nil)", got, err)
	}

	want := errors.New("业务失败")
	got, err = RunSafeCtxRet(context.Background(), func(context.Context) (int, error) {
		return 7, want
	})
	if !errors.Is(err, want) || got != 7 {
		t.Fatalf("业务错误必须原样透传，got (%v, %v)", got, err)
	}
}

// TestRunSafeCtxRet_PanicReturnsZero 钉住：panic ⇒ 零值 + *PanicError（调用方
// 才能区分「合法零值 + nil」与「崩了」）。
func TestRunSafeCtxRet_PanicReturnsZero(t *testing.T) {
	capturePanic(t)

	got, err := RunSafeCtxRet(context.Background(), func(context.Context) (int, error) {
		panic("ret-boom")
	})
	if got != 0 {
		t.Fatalf("panic 应返回零值，got %v", got)
	}
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("应为 *PanicError，got %T: %v", err, err)
	}
	if pe.Value != "ret-boom" {
		t.Fatalf("原始 payload 应保留，got %v", pe.Value)
	}
}
