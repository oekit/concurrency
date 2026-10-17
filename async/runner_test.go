package async_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestRunner_OwnHandlerGlobalUntouched 钉住 #13：Runner 用自己的 handler
// 汇报 panic，全局 PanicHandler 不受影响也不被覆盖。
func TestRunner_OwnHandlerGlobalUntouched(t *testing.T) {
	var mu sync.Mutex
	var ownMsgs []string
	r := async.Runner{Handler: func(msg string, stack []byte) {
		mu.Lock()
		ownMsgs = append(ownMsgs, msg)
		mu.Unlock()
	}}

	var globalMsgs []string
	async.SetPanicHandler(func(msg string, stack []byte) {
		mu.Lock()
		globalMsgs = append(globalMsgs, msg)
		mu.Unlock()
	})
	t.Cleanup(func() {
		// 还原默认（log.Printf），避免影响其他用例
		async.SetPanicHandler(nil)
	})

	r.RunSafe(func() { panic("runner-panic") })
	async.RunSafe(func() { panic("global-panic") })

	mu.Lock()
	defer mu.Unlock()
	if len(ownMsgs) != 1 || !strings.Contains(ownMsgs[0], "runner-panic") {
		t.Fatalf("runner handler msgs = %v", ownMsgs)
	}
	if len(globalMsgs) != 1 || !strings.Contains(globalMsgs[0], "global-panic") {
		t.Fatalf("global handler msgs = %v", globalMsgs)
	}
}

func TestRunner_ZeroValue_FallsBackToGlobal(t *testing.T) {
	var mu sync.Mutex
	var globalMsgs []string
	async.SetPanicHandler(func(msg string, stack []byte) {
		mu.Lock()
		globalMsgs = append(globalMsgs, msg)
		mu.Unlock()
	})
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	var r async.Runner // 零值：Handler nil → 回退全局
	r.RunSafe(func() { panic("zero-runner") })

	mu.Lock()
	defer mu.Unlock()
	if len(globalMsgs) != 1 || !strings.Contains(globalMsgs[0], "zero-runner") {
		t.Fatalf("global msgs = %v", globalMsgs)
	}
}

func TestRunner_RunSafeErr_TypedError(t *testing.T) {
	var mu sync.Mutex
	var ownMsgs []string
	r := async.Runner{Handler: func(msg string, stack []byte) {
		mu.Lock()
		ownMsgs = append(ownMsgs, msg)
		mu.Unlock()
	}}

	err := r.RunSafeErr(func() error { panic("runner-boom") })
	var pe *async.PanicError
	if !errors.As(err, &pe) || pe.Value != "runner-boom" {
		t.Fatalf("err = %v, want *PanicError", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ownMsgs) != 1 {
		t.Fatalf("runner handler not invoked: %v", ownMsgs)
	}
}

func TestRunner_RunSafeRetWith(t *testing.T) {
	r := async.Runner{}
	v, err := async.RunSafeRetWith(r, func() (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("ok path: %v %v", v, err)
	}
	v, err = async.RunSafeRetWith(r, func() (int, error) { panic("ret-panic") })
	var pe *async.PanicError
	if err != nil && errors.As(err, &pe) && pe.Value == "ret-panic" && v == 0 {
		return
	}
	t.Fatalf("panic path: %v %v", v, err)
}

func TestRunner_GoFamily(t *testing.T) {
	var mu sync.Mutex
	var ownMsgs []string
	r := async.Runner{Handler: func(msg string, stack []byte) {
		mu.Lock()
		ownMsgs = append(ownMsgs, msg)
		mu.Unlock()
	}}

	done := make(chan struct{}, 3)
	r.GoSafe(func() { panic("go-safe") })
	r.GoSafeLabeled("janitor", func() { panic("go-labeled") })
	r.GoWithContext(context.Background(), func(ctx context.Context) {
		if ctx == nil {
			panic("nil ctx")
		}
		done <- struct{}{}
	})

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(ownMsgs)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("handler msgs = %d, want >= 2", n)
		case <-time.After(10 * time.Millisecond):
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// 两个后台 goroutine 的汇报顺序不确定，按内容断言
	joined := strings.Join(ownMsgs, "|")
	if !strings.Contains(joined, "go-safe") || !strings.Contains(joined, "task=janitor: go-labeled") {
		t.Fatalf("msgs = %v", ownMsgs)
	}
}
