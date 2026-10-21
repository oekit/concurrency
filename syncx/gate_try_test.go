package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TryAcquire 的核心语义：满额**立即拒**，不排队、不占名额。
func TestGate_TryAcquire_FullRejectsImmediately(t *testing.T) {
	g := NewGate(2)

	rel1, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("第 1 次 TryAcquire 应成功")
	}
	rel2, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("第 2 次 TryAcquire 应成功")
	}

	// 满额：必须**立即**返回，不能等。
	start := time.Now()
	rel3, ok := g.TryAcquire()
	if ok {
		t.Fatalf("满额时 TryAcquire 应返回 ok=false")
	}
	if rel3 != nil {
		t.Errorf("满额时不应返回 release 函数")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("满额时 TryAcquire 应立即返回, 实际耗时 %v", elapsed)
	}

	rel1()
	rel2()
}

// 归还后名额可再取——TryAcquire 不是一次性消费。
func TestGate_TryAcquire_ReleaseFreesSlot(t *testing.T) {
	g := NewGate(1)

	rel, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("第 1 次 TryAcquire 应成功")
	}
	if _, got := g.TryAcquire(); got {
		t.Fatalf("满额时 TryAcquire 应返回 ok=false")
	}

	rel()

	rel2, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("归还后 TryAcquire 应成功")
	}
	rel2()
}

// release 幂等：重复调用只归还一次，不多放名额、也不阻塞。
func TestGate_TryAcquire_ReleaseIdempotent(t *testing.T) {
	g := NewGate(1)

	rel, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("TryAcquire 应成功")
	}

	done := make(chan struct{})
	go func() {
		rel()
		rel()
		rel()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("重复 release 阻塞了（幂等保护失效）")
	}

	// 名额只应被归还一次：还能取到一个，且取走后仍无余量。
	rel2, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("归还后 TryAcquire 应成功")
	}
	if _, got := g.TryAcquire(); got {
		t.Fatalf("名额被多放了：TryAcquire 应返回 ok=false")
	}
	rel2()
}

// cap<=0 视为不限：TryAcquire 恒成功。
func TestGate_TryAcquire_Unlimited(t *testing.T) {
	for _, c := range []int{0, -1} {
		g := NewGate(c)
		for i := 0; i < 100; i++ {
			rel, ok := g.TryAcquire()
			if !ok {
				t.Fatalf("cap=%d 时 TryAcquire 应恒成功（第 %d 次）", c, i)
			}
			rel()
		}
	}
}

// 并发下在飞数不超过 cap：TryAcquire 是**硬配额**，不像弹性 worker 池那样
// 把超出的任务偷偷放行成独立 goroutine。
func TestGate_TryAcquire_CapsConcurrency(t *testing.T) {
	const cap = 3
	g := NewGate(cap)

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
		granted  atomic.Int64
		wg       sync.WaitGroup
	)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, ok := g.TryAcquire()
			if !ok {
				return // 满额即拒
			}
			granted.Add(1)
			cur := inFlight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inFlight.Add(-1)
			rel()
		}()
	}
	wg.Wait()

	if peak.Load() > cap {
		t.Errorf("在飞峰值 %d 超过 cap %d", peak.Load(), cap)
	}
	if granted.Load() == 0 {
		t.Errorf("应有请求成功取到名额")
	}
}

// TryAcquire 与 Acquire 语义分明：一个即拒、一个排队。
func TestGate_TryAcquire_VsAcquire(t *testing.T) {
	g := NewGate(1)
	rel, ok := g.TryAcquire()
	if !ok {
		t.Fatalf("TryAcquire 应成功")
	}
	defer rel()

	// Acquire 在满额时排队等 ctx 取消……
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx); err == nil {
		t.Fatalf("满额时 Acquire 应等 ctx 取消后返回错误")
	}

	// ……而 TryAcquire 立刻拒，不等。
	start := time.Now()
	if _, got := g.TryAcquire(); got {
		t.Fatalf("满额时 TryAcquire 应返回 ok=false")
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("TryAcquire 不应等待, 实际耗时 %v", elapsed)
	}
}
