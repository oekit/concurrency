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
