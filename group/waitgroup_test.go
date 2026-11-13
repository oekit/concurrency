package group

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestWaitGroupWrapper_ConcurrentWrapWait #269 回归：Wrap 与 Wait 并发不得
// 触发 runtime「WaitGroup misuse: Add called concurrently with Wait」panic
// （该 panic 是 runtime.Throw，测试进程直接死亡——修复前本测试稳定崩溃）。
// submitMu 串行化后：Wait 进行中到达的 Wrap 阻塞至 Wait 返回，随后照常执行。
//
// 竞态窗口展开方式：首个 Wrap 先落位（Add 已发生），随后生产者持续 Add、
// 消费者循环 Wait——反复进出 Wait 的注册/唤醒路径，撞上「counter 归零的
// 唤醒窗口内又来 Add」的 misuse 判定。单次 Wait 测不到（Wait 可能在任何
// Add 之前就跑完）。
func TestWaitGroupWrapper_ConcurrentWrapWait(t *testing.T) {
	for range 50 {
		w := NewWaitGroup(context.Background())
		firstDone := make(chan struct{})
		stop := make(chan struct{})
		var workers sync.WaitGroup

		w.Wrap(func(ctx context.Context) error {
			close(firstDone)
			return nil
		})
		<-firstDone // 首个 Add 已落位，竞态窗口从这里有原料

		// 生产者：持续 Wrap（与 Wait 并发）
		workers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.Wrap(func(ctx context.Context) error { return nil })
				runtime.Gosched()
			}
		})

		// 消费者：循环 Wait，反复进出注册/唤醒路径
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.Wait()
				runtime.Gosched()
			}
		}()

		time.Sleep(2 * time.Millisecond) // 让并发窗口展开
		close(stop)
		workers.Wait()
		<-waitDone
		w.Wait()
	}
}

// TestWaitGroupWrapper_NestedWrapDuringWait（#296 回归锚点）：任务体内
// 嵌套 Wrap 派生子任务 + 外层 Wait——旧 submitMu 方案是 AB-BA 硬死锁
// （Wait 持锁等任务、任务体内 Wrap 等锁，无 panic 无日志进程挂死）。
// 新 counter+cond 方案 Wait 等静止不持锁：嵌套派生照常入计数并被等。
func TestWaitGroupWrapper_NestedWrapDuringWait(t *testing.T) {
	ctx := context.Background()
	wg := NewWaitGroup(ctx)

	done := make(chan error, 1)
	go func() {
		done <- wg.Wait()
	}()

	wg.Wrap(func(ctx context.Context) error {
		// 任务体内嵌套派生子任务（fan-out 形态）。
		wg.Wrap(func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("★ #296 嵌套 Wrap + 外层 Wait 死锁（旧 submitMu 方案的 AB-BA 挂死）")
	}
}

// TestWaitGroupWrapper_WaitQuiescence 等静止契约（#296 语义变化钉）：
// Wait 期间新到达的 Wrap 被计入并等待——计数归零才返回。
func TestWaitGroupWrapper_WaitQuiescence(t *testing.T) {
	ctx := context.Background()
	wg := NewWaitGroup(ctx)

	var order []string
	var mu sync.Mutex
	wg.Wrap(func(ctx context.Context) error {
		time.Sleep(30 * time.Millisecond) // 保证 Wait 已进入等待
		wg.Wrap(func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "late")
			mu.Unlock()
			return nil
		})
		return nil
	})

	if err := wg.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 1 || order[0] != "late" {
		t.Fatalf("★ 等静止契约：Wait 应等待期间新到的 Wrap, order=%v", order)
	}
}
