package async_test

import (
	"context"
	"fmt"
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
	async.GoDetached(nil, "nil-parent", func(ctx context.Context) { //nolint:staticcheck // 契约：nil parent 兜底 Background（#155 测试目标）
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

	// 清除钩子后 values 仍在（#211：values 由 WithoutCancel 继承、不依赖
	// enricher——enricher 只补 trace 这一层）
	async.SetContextEnricher(nil)
	got2 := make(chan string, 1)
	async.GoDetached(parent, "untraced", func(ctx context.Context) {
		v, _ := ctx.Value(detachedKey{}).(string)
		got2 <- v
	})
	select {
	case v := <-got2:
		if v != "trace-abc" {
			t.Fatalf("清除钩子后 values 应经 WithoutCancel 保留（#211）：got %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}
}

// ---- #217：enricher panic 收编在 DetachedContext 内部 ----

// TestDetachedContext_EnricherPanicContained enricher（三方 init 注册的现实输入）
// panic 不得同步打进调用方 goroutine：此前 DetachedContext 在调用方 goroutine
// 里裸调钩子，GoDetached/Spawn 都在 go func() 之前构造后台 ctx——panic 直接
// 上抛（HTTP handler 变 500、裸 goroutine 打死进程），与「panic 收编、旁路
// 安全」承诺冲突。修复后就地 RunSafe 收编：回退脱钩面（values 仍保留、观测
// 注入丢失）并上报 PanicHandler，调用方安全。
func TestDetachedContext_EnricherPanicContained(t *testing.T) {
	reports := make(chan string, 8)
	async.SetPanicHandler(func(_ context.Context, r any, _ []byte) {
		reports <- fmt.Sprint(r)
	})
	t.Cleanup(func() { async.SetPanicHandler(nil) })
	async.SetContextEnricher(func(parent, bg context.Context) context.Context {
		panic("enricher-boom")
	})
	t.Cleanup(func() { async.SetContextEnricher(nil) })

	parent := context.WithValue(context.Background(), detachedKey{}, "scope-217")

	// DetachedContext 自身不 panic，回退脱钩面且 values 保留
	d := async.DetachedContext(parent)
	if v, ok := d.Value(detachedKey{}).(string); !ok || v != "scope-217" {
		t.Fatalf("enricher panic 后应回退脱钩 ctx（values 保留）, got (%q, %v)", v, ok)
	}

	// GoDetached 调用方安全，fn 正常执行
	fnDone := make(chan struct{})
	async.GoDetached(parent, "detached-217", func(ctx context.Context) {
		if _, ok := ctx.Value(detachedKey{}).(string); !ok {
			t.Error("回退 ctx 应保留 values")
		}
		close(fnDone)
	})
	select {
	case <-fnDone:
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}

	// Spawn 调用方安全，句柄正常返回结果
	h := async.Spawn(parent, "spawn-217", func(ctx context.Context) (string, error) {
		v, _ := ctx.Value(detachedKey{}).(string)
		return v, nil
	})
	val, err, timedOut := h.WaitUpTo(context.Background(), 2*time.Second)
	if timedOut || err != nil || val != "scope-217" {
		t.Fatalf("Spawn 结果 = (%q, %v, %v)，期望正常返回", val, err, timedOut)
	}

	// 每次 DetachedContext 派生恰好上报一次（直调 + GoDetached + Spawn 共 3 次）
	for i := 0; i < 3; i++ {
		select {
		case msg := <-reports:
			if !strings.Contains(msg, "enricher-boom") {
				t.Fatalf("enricher panic 应上报 PanicHandler，got %q", msg)
			}
		default:
			t.Fatalf("enricher panic 上报缺失（第 %d 次）——观测面漏接", i+1)
		}
	}
	// 恰好三次，无多余上报
	select {
	case msg := <-reports:
		t.Fatalf("enricher panic 上报多余: %q", msg)
	default:
	}
}

// ---- #211：DetachedContext 保留 parent values（WithoutCancel 标准语义）----

// TestDetachedContext_KeepsParentValues 三对照收口（直测 DetachedContext）：
// values 可读（与 context.WithoutCancel 逐值一致）、cancel parent 不影响
// （Done 为 nil / Err 恒 nil）、时限不继承（后台任务不受请求超时牵连）。
func TestDetachedContext_KeepsParentValues(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	parent = context.WithValue(parent, detachedKey{}, "session-scope")
	defer cancel()

	d := async.DetachedContext(parent)

	// 对照一：values 可读，且与 stdlib WithoutCancel 同语义逐值一致
	if v, ok := d.Value(detachedKey{}).(string); !ok || v != "session-scope" {
		t.Fatalf("detached ctx 应继承 parent values, got (%q, %v)", v, ok)
	}
	if want := context.WithoutCancel(parent).Value(detachedKey{}); d.Value(detachedKey{}) != want {
		t.Fatalf("应与 context.WithoutCancel 同语义, got %v want %v", d.Value(detachedKey{}), want)
	}

	// 对照二：cancel parent 不影响 detached ctx（Done 为 nil、Err 恒 nil）
	cancel()
	if err := d.Err(); err != nil {
		t.Fatalf("detached ctx 不应随 parent 取消, err=%v", err)
	}
	select {
	case <-d.Done():
		t.Fatal("detached ctx 的 Done 应为 nil（永不取消）")
	default:
	}

	// 对照三：时限不继承（parent 带 1h 超时，detached 侧不带）
	if dl, ok := d.Deadline(); ok {
		t.Fatalf("detached ctx 不应继承 parent 时限, got %v", dl)
	}
}

// TestGoDetached_KeepsParentValues 无 enricher 时 fn 内照常读到 parent 的
// values——手拼 WithoutCancel 场景（回填结果需要会话 scope）收编后不再
// 静默丢 values（git-ferry delegate 实证形态）。
func TestGoDetached_KeepsParentValues(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	parent = context.WithValue(parent, detachedKey{}, "scope-211")
	got := make(chan string, 1)
	async.GoDetached(parent, "scoped", func(ctx context.Context) {
		v, _ := ctx.Value(detachedKey{}).(string)
		got <- v
	})
	select {
	case v := <-got:
		if v != "scope-211" {
			t.Fatalf("fn 内应读到 parent values（VALUES-LOST 回归）, got %q", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GoDetached 未执行")
	}
}

// TestGoDetached_PanicContainedWithTaskLabel GoDetached 的 panic 收编带 task 标签
// （#184：标签经汇报 ctx 携带，TaskFromContext 取；r 为 recover 原始值）。
func TestGoDetached_PanicContainedWithTaskLabel(t *testing.T) {
	msgs := make(chan string, 8)
	async.SetPanicHandler(func(ctx context.Context, r any, stack []byte) {
		msgs <- TaskFromContextLabel(ctx, r)
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

// TaskFromContextLabel 测试侧小工具：把标签与 r 拼回可断言文案。
func TaskFromContextLabel(ctx context.Context, r any) string {
	task := async.TaskFromContext(ctx)
	if task == "" {
		return fmt.Sprint(r)
	}
	return "task=" + task + ": " + fmt.Sprint(r)
}
