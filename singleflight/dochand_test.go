package singleflight

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoChan_SharedResult(t *testing.T) {
	f := New[string]()
	started := make(chan struct{})
	fn := func(ctx context.Context) (string, error) {
		close(started)
		time.Sleep(30 * time.Millisecond)
		return "v", nil
	}
	ch1 := f.DoChan(context.Background(), "k", fn)
	<-started
	ch2 := f.DoChan(context.Background(), "k", fn)
	r1, r2 := <-ch1, <-ch2
	if r1.Val != "v" || r2.Val != "v" {
		t.Fatalf("r1=%+v r2=%+v", r1, r2)
	}
	if !r1.Shared && !r2.Shared {
		t.Fatalf("one should be shared: %v %v", r1.Shared, r2.Shared)
	}
}

func TestDoChan_WaiterCtxExit(t *testing.T) {
	f := New[int]()
	block := make(chan struct{})
	defer close(block)
	go func() {
		_, _ = f.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
			<-block
			return 1, nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	wctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r := <-f.DoChan(wctx, "k", func(ctx context.Context) (int, error) {
		t.Fatal("waiter must not run fn")
		return 0, nil
	})
	if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Fatalf("r=%+v", r)
	}
}

func TestDoChan_LeaderImmuneWithWithoutCancel(t *testing.T) {
	f := New[string]()
	callCtx, cancel := context.WithCancel(context.Background())
	res := f.DoChan(context.WithoutCancel(callCtx), "k", func(ctx context.Context) (string, error) {
		select {
		case <-time.After(80 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	cancel()
	r := <-res
	if r.Err != nil || r.Val != "done" {
		t.Fatalf("leader should be immune, got %+v", r)
	}
}

// TestDoChan_Forget_DoesNotDeleteNewerCall 回归测试（#105）：DoChan 路径的收尾
// 曾是无条件 `f.calls.Delete(key)`，而 Do/DoShared 早已改成 CompareAndDelete
// ⇒「同 key 只执行一次 fn」的承诺**只在部分路径上成立**。
//
// 时序（与 Do 路径的同款回归用例一字不差的骨架）：
//
//	G1 注册 c1（慢 fn）→ Forget("k") 删掉 c1 → G2 注册 c2（仍在飞行）→ G1 结束 →
//
//	旧 call 的收尾 Delete("k") 把 **c2** 一起删掉 → G3 于是又起了第三次 fn
//
// ⇒ 两次 fn **并发**跑（下游请求翻倍 / 缓存回源击穿 / 非幂等写重复执行），
// 且没有任何报错——Result.Shared 还会把复用结果标成 Shared=true，调用方更难察觉。
func TestDoChan_Forget_DoesNotDeleteNewerCall(t *testing.T) {
	f := New[int]()

	var calls atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})

	// G1：注册 c1（慢 fn）。
	ch1 := f.DoChan(context.Background(), "k", func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(firstStarted)
		<-releaseFirst
		return 1, nil
	})
	<-firstStarted

	// Forget：c1 从 map 里摘掉。
	f.Forget("k")

	// G2：注册 c2（同 key，**仍在飞行中**的慢 fn）。
	ch2 := f.DoChan(context.Background(), "k", func(ctx context.Context) (int, error) {
		calls.Add(1)
		close(secondStarted)
		<-releaseSecond
		return 2, nil
	})
	<-secondStarted

	// G1 结束：它的收尾**不得**删掉还在飞行中的 c2。
	close(releaseFirst)
	time.Sleep(50 * time.Millisecond)
	<-ch1 // G1 的结果已交付 ⇒ 其收尾（清理）必然已完成

	// G3 到达：c2 仍在飞行中 ⇒ 应**加入**它，而不是另起一次 fn。
	ch3 := f.DoChan(context.Background(), "k", func(ctx context.Context) (int, error) {
		calls.Add(1)
		return 3, nil
	})
	// G3 应**加入**仍在飞行中的 c2 ⇒ 它会等到 c2 结束才返回。
	// （若 G1 的收尾误删了 c2，G3 会另起一次 fn 并**立即**返回。）
	select {
	case <-ch3:
		t.Fatal("G3 立即返回了：它没有加入仍在飞行中的 c2（G1 的收尾误删了 c2）")
	case <-time.After(200 * time.Millisecond):
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("fn 执行次数 = %d，期望 2（G1 的收尾误删了仍在飞行中的 G2 ⇒ G3 又起了一次）", got)
	}
	close(releaseSecond)
	<-ch2
	<-ch3
}
