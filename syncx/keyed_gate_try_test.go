package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// KeyedGate.TryAcquire 的核心语义（#93）：该 key 档位已满**立即拒**，不排队、
// 不占名额；与同包 Gate.TryAcquire 同款分档，只是按 key 分档位。

// 同一 key 满额即拒：必须**立即**返回，不能等。
func TestKeyedGate_TryAcquire_FullRejectsImmediately(t *testing.T) {
	g := NewKeyedGate(1)

	rel, ok := g.TryAcquire("account:42")
	if !ok {
		t.Fatalf("第 1 次 TryAcquire 应成功")
	}

	start := time.Now()
	rel2, ok := g.TryAcquire("account:42")
	if ok {
		t.Fatalf("该 key 满额时 TryAcquire 应返回 ok=false")
	}
	if rel2 != nil {
		t.Errorf("满额时不应返回 release 函数")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("满额时 TryAcquire 应立即返回, 实际耗时 %v", elapsed)
	}

	rel()
}

// 不同 key 互不影响：档位是**按 key** 分的。
func TestKeyedGate_TryAcquire_KeyIsolation(t *testing.T) {
	g := NewKeyedGate(1)

	rel, ok := g.TryAcquire("account:42")
	if !ok {
		t.Fatalf("account:42 首次 TryAcquire 应成功")
	}
	defer rel()

	if _, ok := g.TryAcquire("account:43"); !ok {
		t.Fatalf("account:43 的档位独立，不应受 account:42 占用影响")
	}
	if _, ok := g.TryAcquire("model:gpt"); !ok {
		t.Fatalf("model:gpt 的档位独立，不应受 account:42 占用影响")
	}
}

// 归还后名额可再取——TryAcquire 不是一次性消费。
func TestKeyedGate_TryAcquire_ReleaseFreesSlot(t *testing.T) {
	g := NewKeyedGate(1)

	rel, ok := g.TryAcquire("k")
	if !ok {
		t.Fatalf("首次 TryAcquire 应成功")
	}
	if _, got := g.TryAcquire("k"); got {
		t.Fatalf("满额时 TryAcquire 应返回 ok=false")
	}

	rel()

	rel2, ok := g.TryAcquire("k")
	if !ok {
		t.Fatalf("归还后 TryAcquire 应成功")
	}
	rel2()
}

// release 幂等：重复调用只归还一次，不多放名额、也不阻塞。
// 幂等标记用 atomic CAS（与 Acquire / Gate 一致）——裸 bool 在并发释放下是
// data race，且两者都可能读到「未归还」而各执行一次 <-ch，名额被归还两次，
// 该 key 的在飞数突破 cap 上限。
func TestKeyedGate_TryAcquire_ReleaseIdempotent(t *testing.T) {
	g := NewKeyedGate(1)

	rel, ok := g.TryAcquire("k")
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
	rel2, ok := g.TryAcquire("k")
	if !ok {
		t.Fatalf("归还后 TryAcquire 应成功")
	}
	if _, got := g.TryAcquire("k"); got {
		t.Fatalf("名额被多放了：TryAcquire 应返回 ok=false")
	}
	rel2()
}

// cap<=0 视为不限：TryAcquire 恒成功。
func TestKeyedGate_TryAcquire_Unlimited(t *testing.T) {
	for _, c := range []int{0, -1} {
		g := NewKeyedGate(c)
		for i := 0; i < 100; i++ {
			rel, ok := g.TryAcquire("k")
			if !ok {
				t.Fatalf("cap=%d 时 TryAcquire 应恒成功（第 %d 次）", c, i)
			}
			rel()
		}
	}
}

// cap 按 key 生效：同一 key 最多 cap 个在飞，第 cap+1 个被拒。
func TestKeyedGate_TryAcquire_CapIsPerKey(t *testing.T) {
	const cap = 2
	g := NewKeyedGate(cap)

	var releases []func()
	for i := 0; i < cap; i++ {
		rel, ok := g.TryAcquire("k")
		if !ok {
			t.Fatalf("第 %d 次 TryAcquire 应成功（cap=%d）", i+1, cap)
		}
		releases = append(releases, rel)
	}
	if _, ok := g.TryAcquire("k"); ok {
		t.Fatalf("该 key 在飞已达 cap=%d，第 %d 次应被拒", cap, cap+1)
	}

	for _, rel := range releases {
		rel()
	}
	if _, ok := g.TryAcquire("k"); !ok {
		t.Fatalf("全部归还后 TryAcquire 应成功")
	}
}

// 与 Acquire 语义分明：一个即拒、一个排队。
func TestKeyedGate_TryAcquire_VsAcquire(t *testing.T) {
	g := NewKeyedGate(1)

	rel, ok := g.TryAcquire("k")
	if !ok {
		t.Fatalf("TryAcquire 应成功")
	}
	defer rel()

	// Acquire 在满额时排队等 ctx 取消……
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx, "k"); err == nil {
		t.Fatalf("满额时 Acquire 应等 ctx 取消后返回错误")
	}

	// ……而 TryAcquire 立刻拒，不等。
	start := time.Now()
	if _, got := g.TryAcquire("k"); got {
		t.Fatalf("满额时 TryAcquire 应返回 ok=false")
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("TryAcquire 不应等待, 实际耗时 %v", elapsed)
	}
}

// 并发下**每个 key** 的在飞数不超过 cap：TryAcquire 是**硬配额**，不像弹性
// worker 池那样把超出的任务偷偷放行成独立 goroutine。
func TestKeyedGate_TryAcquire_CapsConcurrency(t *testing.T) {
	const (
		cap      = 3
		keys     = 4
		rounders = 50
	)
	g := NewKeyedGate(cap)

	var (
		inFlight [keys]atomic.Int64
		peak     [keys]atomic.Int64
		granted  atomic.Int64
		wg       sync.WaitGroup
	)
	for k := 0; k < keys; k++ {
		for i := 0; i < rounders; i++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				key := string(rune('a' + k))
				rel, ok := g.TryAcquire(key)
				if !ok {
					return // 满额即拒
				}
				granted.Add(1)
				cur := inFlight[k].Add(1)
				for {
					p := peak[k].Load()
					if cur <= p || peak[k].CompareAndSwap(p, cur) {
						break
					}
				}
				time.Sleep(time.Millisecond)
				inFlight[k].Add(-1)
				rel()
			}(k)
		}
	}
	wg.Wait()

	for k := 0; k < keys; k++ {
		if peak[k].Load() > cap {
			t.Errorf("key %c 在飞峰值 %d 超过 cap %d", 'a'+k, peak[k].Load(), cap)
		}
	}
	if granted.Load() == 0 {
		t.Errorf("应有请求成功取到名额")
	}
}
