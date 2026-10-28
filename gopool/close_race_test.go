package gopool

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestPool_ConcurrentSubmitAndClose_NoWaitGroupMisuse 回归测试：并发提交 +
// Close 不得触发运行时的 WaitGroup 误用 panic。
//
// 缺陷成因：CtxGo 先读 `closed` 标志、再 `wg.Add(1)`，两步**不是原子的**；
// Close 置 closed 后就 `wg.Wait()`。窗口内（读到 closed==false 与进入 Wait
// 之间）提交方会 `wg.Add(1)` ⇒ 运行时 panic
// "sync: WaitGroup misuse: Add called concurrently with Wait"，崩在**提交方**
// 的 goroutine 上。
//
// 修法：把「检查 closed + wg.Add(1)」与 Close 的「置 closed」放进同一把锁，
// 于是「Add 发生在 Wait 之前」恒成立（WaitGroup 的硬要求）；Wait 本身在锁外，
// 避免「任务里再提交任务」与 Close 互等。
func TestPool_ConcurrentSubmitAndClose_NoWaitGroupMisuse(t *testing.T) {
	for round := 0; round < 20; round++ {
		p := NewPool("t")
		var wg sync.WaitGroup
		stop := make(chan struct{})

		// 持续提交，制造「读到 closed==false 之后 Close 就进入 Wait」的窗口。
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					p.Go(func() {})
				}
			}()
		}

		time.Sleep(time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = p.Close(ctx)
		cancel()
		close(stop)
		wg.Wait()
	}
}
