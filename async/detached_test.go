package async_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestGoDetached_DecoupledFromParent GoDetached 的 ctx 与 parent 解耦：
// parent 取消（先于、后于 spawn 均覆盖）不影响 fn 的 ctx；nil parent 也可用。
func TestGoDetached_DecoupledFromParent(t *testing.T) {
	// 场景一：spawn 前已取消的 parent——fn 拿到的 ctx 仍可继续
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	async.GoDetached(canceled, "pre-canceled", func(ctx context.Context) {
		done <- ctx.Err()
	})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("detached ctx 不应继承 parent 取消：err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}

	// 场景二：fn 在飞时取消 parent——ctx 不被腰斩（负向断言，只能有界等待）
	parent, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	started := make(chan struct{})
	finished := make(chan error, 1)
	quit := make(chan struct{})
	defer close(quit)
	async.GoDetached(parent, "in-flight", func(ctx context.Context) {
		close(started)
		select {
		case <-ctx.Done(): // 若解耦失败将在这里被唤醒
			finished <- ctx.Err()
		case <-quit:
		}
	})
	<-started
	cancel2()
	select {
	case err := <-finished:
		t.Fatalf("detached ctx 不应随 parent 取消而取消：err=%v", err)
	case <-time.After(100 * time.Millisecond):
		// 预期路径：ctx 未取消，fn 由 quit 收尾退出
	}

	// 场景三：nil parent
	nilDone := make(chan struct{})
	async.GoDetached(nil, "nil-parent", func(ctx context.Context) {
		if ctx == nil || ctx.Err() != nil {
			t.Error("nil parent 应派生 Background ctx")
		}
		close(nilDone)
	})
	select {
	case <-nilDone:
	case <-time.After(2 * time.Second):
		t.Fatal("nil parent 的 GoDetached 未执行")
	}
}

// detachedKey 仅本测试文件使用的 context key（避免导出测试辅助）。
type detachedKey struct{}

// TestGoDetached_EnricherPreservesTrace GoDetached 经 SetContextEnricher 保留
// parent 的追踪信息（trace_id 注入的等价形态：值从 parent 复制到 detached ctx）。
func TestGoDetached_EnricherPreservesTrace(t *testing.T) {
	async.SetContextEnricher(func(parent, bg context.Context) context.Context {
		if v, ok := parent.Value(detachedKey{}).(string); ok {
			return context.WithValue(bg, detachedKey{}, v)
		}
		return bg
	})
	t.Cleanup(func() { async.SetContextEnricher(nil) })

	parent := context.WithValue(context.Background(), detachedKey{}, "trace-abc")
	got := make(chan string, 1)
	async.GoDetached(parent, "traced", func(ctx context.Context) {
		v, _ := ctx.Value(detachedKey{}).(string)
		got <- v
	})
	select {
	case v := <-got:
		if v != "trace-abc" {
			t.Fatalf("detached ctx 未保留 parent 追踪值：got %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}

	// 清除钩子后：退回裸 Background（不含任何值）
	async.SetContextEnricher(nil)
	got2 := make(chan string, 1)
	async.GoDetached(parent, "untraced", func(ctx context.Context) {
		v, _ := ctx.Value(detachedKey{}).(string)
		got2 <- v
	})
	select {
	case v := <-got2:
		if v != "" {
			t.Fatalf("清除钩子后 detached ctx 不应再携带值：got %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}
}

// TestGoDetached_PanicContainedWithTaskLabel GoDetached 的 panic 收编带 task 标签。
func TestGoDetached_PanicContainedWithTaskLabel(t *testing.T) {
	msgs := make(chan string, 8)
	async.SetPanicHandler(func(msg string, stack []byte) {
		msgs <- msg
	})
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	async.GoDetached(context.Background(), "detached-boom", func(context.Context) {
		panic("kaboom")
	})
	select {
	case msg := <-msgs:
		if !strings.Contains(msg, "task=detached-boom") || !strings.Contains(msg, "kaboom") {
			t.Fatalf("panic 汇报应带 task 标签：%q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic 未被收编汇报")
	}
}
