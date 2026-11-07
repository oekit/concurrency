package async_test

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestRunner_HandlerPanicContained Runner 的 handler 自身 panic 必须被就地收编、
// 不再打死进程（#164）——与包级 GoSafe 对 PanicHandler 的防护同口径。
// 同步方法（RunSafe/RunSafeErr/RunSafeRetWith）返回即证明收编发生；
// 异步方法（GoSafe/GoSafeLabeled/GoWithContext）经信号确认 handler 已被调用，
// 进程存活（逃逸的 panic 会当场击穿测试进程）。
func TestRunner_HandlerPanicContained(t *testing.T) {
	called := make(chan string, 8)
	r := async.Runner{Handler: func(msg string, stack []byte) {
		called <- msg
		panic("handler-boom: " + msg) // handler 自身 panic
	}}

	// 同步三件套：不崩、handler 被调、错误收编口径不变
	r.RunSafe(func() { panic("fn-a") })
	if err := r.RunSafeErr(func() error { panic("fn-b") }); err == nil {
		t.Fatal("RunSafeErr 应返回收编的 *PanicError")
	}
	if _, err := async.RunSafeRetWith(r, func() (int, error) { panic("fn-c") }); err == nil {
		t.Fatal("RunSafeRetWith 应返回收编的 *PanicError")
	}

	// 异步三件套：handler 被调用即汇报路径已进入；handler panic 若未收编，
	// goroutine 崩溃会直接终止测试进程
	r.GoSafe(func() { panic("fn-d") })
	r.GoSafeLabeled("task-e", func() { panic("fn-e") })
	r.GoWithContext(context.Background(), func(context.Context) { panic("fn-f") })

	// 收满 6 次调用后按集合断言：异步 goroutine 的 handler 到达顺序不确定，
	// 不能按固定顺序比对
	want := []string{"fn-a", "fn-b", "fn-c", "fn-d", "task=task-e: fn-e", "fn-f"}
	got := make(map[string]bool, len(want))
	for i := 0; i < len(want); i++ {
		select {
		case msg := <-called:
			got[msg] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("handler 调用 %d 未发生（已收 %v）", i, got)
		}
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("handler 未收到 %q 的汇报（已收 %v）", w, got)
		}
	}
}
