package singleflight

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// TestFlight_PanicWaiterErrorTyped 钉住 #12：fn panic 时 waiter 拿到的错误
// errors.As 可辨 *async.PanicError（前缀文案保留），首个 caller 仍收到原始 panic。
func TestFlight_PanicWaiterErrorTyped(t *testing.T) {
	f := New[string]()
	key := "panic-typed"

	go func() {
		defer func() { _ = recover() }() // 首个 caller 收到 re-panic
		_, _ = f.Do(context.Background(), key, func(ctx context.Context) (string, error) {
			time.Sleep(50 * time.Millisecond) // 给 main 时间注册为 waiter
			panic("do-not-do-this")
		})
	}()

	time.Sleep(10 * time.Millisecond)
	_, err := f.Do(context.Background(), key, func(ctx context.Context) (string, error) {
		return "", errors.New("should-not-run")
	})

	var pe *async.PanicError
	if !errors.As(err, &pe) || pe.Value != "do-not-do-this" {
		t.Fatalf("waiter err = %v, want *PanicError", err)
	}
	if !strings.HasPrefix(err.Error(), "singleflight: fn panic:") {
		t.Fatalf("prefix text broken: %q", err.Error())
	}
}

// TestFlight_DoShared 钉住 #18：DoShared 返回 Shared 折叠标记——
// 首个 caller false，并发 waiter true；Do 保持 (T, error) 不动。
func TestFlight_DoShared(t *testing.T) {
	f := New[string]()
	key := "shared-key"
	started := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_, _, _ = f.DoShared(context.Background(), key, func(ctx context.Context) (string, error) {
			close(started)
			<-release
			return "leader-result", nil
		})
	}()

	<-started
	sharedDone := make(chan struct{})
	go func() {
		v, shared, err := f.DoShared(context.Background(), key, func(ctx context.Context) (string, error) {
			return "should-not-run", nil
		})
		if err != nil || v != "leader-result" || !shared {
			t.Errorf("waiter: v=%q shared=%v err=%v", v, shared, err)
		}
		close(sharedDone)
	}()

	time.Sleep(20 * time.Millisecond)
	close(release)
	<-sharedDone
}

// TestFlight_ResultWaiters 钉住 #18 附带：DoChan 的 Result.Waiters 反映
// 「这一轮折叠掉的调用方数」（leader 记 1，每个 waiter 计入）。
func TestFlight_ResultWaiters(t *testing.T) {
	f := New[string]()
	key := "waiters-key"
	started := make(chan struct{})
	release := make(chan struct{})

	resCh := f.DoChan(context.Background(), key, func(ctx context.Context) (string, error) {
		close(started)
		<-release
		return "leader", nil
	})

	<-started
	waiterDone := make(chan int, 1)
	go func() {
		res := <-f.DoChan(context.Background(), key, func(ctx context.Context) (string, error) {
			return "should-not-run", nil
		})
		waiterDone <- res.Waiters
	}()

	time.Sleep(20 * time.Millisecond)
	close(release)
	got := <-waiterDone
	res := <-resCh
	if res.Waiters < 1 {
		t.Fatalf("leader Waiters = %d, want >= 1（折叠掉的 waiter 数）", res.Waiters)
	}
	_ = got
}

// TestFlight_NilCtxTreatedAsBackground 锁定 #249（#159 口径清尾）：nil ctx =
// context.Background()。修复前 waiter 路径 select <-ctx.Done()、DoChan 的
// context.WithoutCancel(nil) 直接 nil panic 打死请求 goroutine。
func TestFlight_NilCtxTreatedAsBackground(t *testing.T) {
	sf := New[string]()

	// leader 路径：fn 收到的 ctx 已兜底为 Background（fn 解引用 ctx 不 panic）
	entered := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	var leaderCtx context.Context
	go func() {
		defer close(leaderDone)
		val, err := sf.Do(nil, "k", func(ctx context.Context) (string, error) { //nolint:staticcheck // 契约：nil ctx 按 Background 处理（#249 测试目标）
			leaderCtx = ctx
			close(entered)
			<-release // 挂起 leader：让下方 waiter 确定性走到等待分支
			return "leader", nil
		})
		if err != nil || val != "leader" {
			t.Errorf("leader = (%q, %v), want (leader, nil)", val, err)
		}
	}()
	<-entered // leader 已登记 call 条目：下一次同 key 调用必为 waiter 路径

	// waiter 路径：修复前 select <-ctx.Done() nil panic
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		val, err := sf.Do(nil, "k", func(context.Context) (string, error) { //nolint:staticcheck // 契约：nil ctx 按 Background 处理（#249 测试目标）
			return "should-not-run", nil
		})
		if err != nil || val != "leader" {
			t.Errorf("waiter = (%q, %v), want (leader, nil)", val, err)
		}
	}()

	// 等 waiter 确定性注册进等待分支：leader 的 call 条目在 fn 返回前不会被
	// 清理（fn 挂在 <-release 上），waiter 命中条目后 waiters.Add(1)——读到
	// 计数 ≥1 即 waiter 已走到等待分支，此时才放行 leader。包内测试直读
	// call.waiters，不 sleep 猜时序。
	for registered := false; !registered; {
		if v, ok := sf.calls.Load("k"); ok {
			registered = v.(*call[string]).waiters.Load() >= 1
		}
		if !registered {
			runtime.Gosched()
		}
	}

	close(release)
	<-leaderDone
	<-waiterDone
	if leaderCtx == nil {
		t.Error("fn 应收到兜底后的 Background，而非 nil ctx")
	}
	if got := leaderCtx.Err(); got != nil {
		t.Errorf("Background.Err() = %v, want nil", got)
	}
}

// TestFlight_NilCtxDoSharedAndDoChan 同口径覆盖 DoShared 与 DoChan 两个入口。
func TestFlight_NilCtxDoSharedAndDoChan(t *testing.T) {
	sf := New[int]()

	val, shared, err := sf.DoShared(nil, "k", func(ctx context.Context) (int, error) { //nolint:staticcheck // 契约：nil ctx 按 Background 处理（#249 测试目标）
		if ctx == nil {
			t.Error("DoShared fn 应收到兜底后的 Background")
		}
		return 42, nil
	})
	if err != nil || val != 42 || shared {
		t.Fatalf("DoShared(nil) = (%v, %v, %v), want (42, false, nil)", val, shared, err)
	}

	res := <-sf.DoChan(nil, "k", func(ctx context.Context) (int, error) { //nolint:staticcheck // 契约：nil ctx 按 Background 处理（#249 测试目标）
		if ctx == nil {
			t.Error("DoChan fn 应收到兜底后的 Background")
		}
		return 7, nil
	})
	if res.Err != nil || res.Val != 7 || res.Shared {
		t.Fatalf("DoChan(nil) = %+v, want {Val:7, Shared:false, Err:nil}", res)
	}
}

// concurrency#340：waiter 路径 Waiters 不含自己（与 leader 视角同口径）。
func TestDoChanWaitersExcludesSelf(t *testing.T) {
	var f Flight[int]
	start := make(chan struct{})
	leader := f.DoChan(context.Background(), "k", func(context.Context) (int, error) {
		<-start
		return 42, nil
	})
	w := f.DoChan(context.Background(), "k", func(context.Context) (int, error) {
		t.Fatal("waiter 不应执行 fn")
		return 0, nil
	})
	close(start)
	lr := <-leader
	wr := <-w
	if lr.Shared || lr.Waiters != 1 {
		t.Fatalf("leader: Shared=%v Waiters=%d, want false/1", lr.Shared, lr.Waiters)
	}
	if !wr.Shared || wr.Waiters != 0 {
		t.Fatalf("waiter: Waiters=%d, want 0(不含自己,#340)", wr.Waiters)
	}
}
