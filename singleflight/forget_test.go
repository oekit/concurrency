package singleflight

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// callFlavour 抽象三条调用路径的差异：Do / DoShared 阻塞到 fn 返回，
// DoChan 阻塞到结果投递。语义上三者对「同 key 只执行一次 fn」的承诺相同，
// 因此 Forget 的回归护栏必须**三条路径都跑**——历史上正是漏了 DoChan
// （收尾仍是无条件 Delete）⇒ Forget 后同 key 并发跑两次 fn。
type callFlavour struct {
	name string
	call func(f *Flight[int], ctx context.Context, key string, fn func(context.Context) (int, error))
}

var callFlavours = []callFlavour{
	{
		name: "Do",
		call: func(f *Flight[int], ctx context.Context, key string, fn func(context.Context) (int, error)) {
			_, _ = f.Do(ctx, key, fn)
		},
	},
	{
		name: "DoShared",
		call: func(f *Flight[int], ctx context.Context, key string, fn func(context.Context) (int, error)) {
			_, _, _ = f.DoShared(ctx, key, fn)
		},
	},
	{
		name: "DoChan",
		call: func(f *Flight[int], ctx context.Context, key string, fn func(context.Context) (int, error)) {
			<-f.DoChan(ctx, key, fn)
		},
	},
}

// TestFlight_Forget_DoesNotDeleteNewerCall 回归测试：Forget 之后，本次调用
// 结束时的清理**不得**把同 key 的**新** call 一起删掉。
//
// 缺陷成因：清理是无条件 `f.calls.Delete(key)`。序列：
//
//	G1 注册 c1（慢 fn）→ Forget("k") 删掉 c1 → G2 注册 c2 → G1 结束 →
//
// Delete("k") 把 **c2** 一起删掉 → G3 于是又起了第三次 fn
//
// ⇒「同 key 只执行一次 fn」的承诺被打破。x/sync/singleflight 用
// `if g.m[key] == c` 判同一。
//
// **三条路径都要跑**：Do / DoShared 的收尾早已改成 CompareAndDelete，
// 但 DoChan 曾漏修（#105）⇒ 单测只覆盖一条路径时，同型缺陷可以在
// 另一条路径上静默留存。参数化是这类缺陷最便宜的防线。
func TestFlight_Forget_DoesNotDeleteNewerCall(t *testing.T) {
	for _, flavour := range callFlavours {
		t.Run(flavour.name, func(t *testing.T) {
			f := New[int]()

			var calls atomic.Int32
			firstStarted := make(chan struct{})
			releaseFirst := make(chan struct{})
			secondStarted := make(chan struct{})
			releaseSecond := make(chan struct{})

			// G1：慢 fn，注册 c1。
			go func() {
				flavour.call(f, context.Background(), "k", func(ctx context.Context) (int, error) {
					calls.Add(1)
					close(firstStarted)
					<-releaseFirst
					return 1, nil
				})
			}()
			<-firstStarted

			// Forget：c1 从 map 里摘掉。
			f.Forget("k")

			// G2：注册 c2（同 key，**仍在飞行中**的慢 fn）。
			secondDone := make(chan struct{})
			go func() {
				defer close(secondDone)
				flavour.call(f, context.Background(), "k", func(ctx context.Context) (int, error) {
					calls.Add(1)
					close(secondStarted)
					<-releaseSecond
					return 2, nil
				})
			}()
			<-secondStarted

			// G1 结束：它的清理**不得**删掉还在飞行中的 c2。
			close(releaseFirst)
			time.Sleep(50 * time.Millisecond)

			// G3 到达：c2 仍在飞行中 ⇒ 应**加入**它，而不是另起一次 fn。
			thirdDone := make(chan struct{})
			go func() {
				defer close(thirdDone)
				flavour.call(f, context.Background(), "k", func(ctx context.Context) (int, error) {
					calls.Add(1)
					return 3, nil
				})
			}()
			// G3 应**加入**仍在飞行中的 c2 ⇒ 它会等到 c2 结束才返回。
			// （若 G1 的清理误删了 c2，G3 会另起一次 fn 并**立即**返回。）
			select {
			case <-thirdDone:
				t.Fatal("G3 立即返回了：它没有加入仍在飞行中的 c2（G1 的清理误删了 c2）")
			case <-time.After(200 * time.Millisecond):
			}

			if got := calls.Load(); got != 2 {
				t.Fatalf("fn 执行次数 = %d，期望 2（G1 的清理误删了仍在飞行中的 G2 ⇒ G3 又起了一次）", got)
			}
			close(releaseSecond)
			<-secondDone
			<-thirdDone
		})
	}
}

// TestFlight_Forget_AllowsRerun 回归护栏：Forget 后同 key 可重新执行 fn
// （既有口径）。
func TestFlight_Forget_AllowsRerun(t *testing.T) {
	f := New[int]()
	var calls atomic.Int32
	fn := func(ctx context.Context) (int, error) {
		calls.Add(1)
		return 1, nil
	}
	_, _ = f.Do(context.Background(), "k", fn)
	f.Forget("k")
	_, _ = f.Do(context.Background(), "k", fn)
	if got := calls.Load(); got != 2 {
		t.Fatalf("fn 执行次数 = %d，期望 2（Forget 后应可重入）", got)
	}
}

// TestFlight_ConcurrentDedup 回归护栏：同 key 并发只执行一次 fn（既有口径）。
func TestFlight_ConcurrentDedup(t *testing.T) {
	f := New[int]()
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return 1, nil
			})
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn 执行次数 = %d，期望 1", got)
	}
}
