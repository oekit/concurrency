package syncx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestKeyedGateLimitsPerKey(t *testing.T) {
	g := NewKeyedGate(1)
	release, err := g.Acquire(context.Background(), "m1")
	if err != nil {
		t.Fatalf("首个名额应立即获得：%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx, "m1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("同键超限应超时，得到 %v", err)
	}
	release()
	release() // 幂等
	if _, err := g.Acquire(context.Background(), "m1"); err != nil {
		t.Fatalf("释放后应可再取：%v", err)
	}
}

func TestKeyedGateIndependentKeys(t *testing.T) {
	g := NewKeyedGate(1)
	r1, err := g.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer r1()
	r2, err := g.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatalf("异键互不影响：%v", err)
	}
	defer r2()
}

func TestKeyedGateZeroCapPassthrough(t *testing.T) {
	g := NewKeyedGate(0)
	release, err := g.Acquire(context.Background(), "x")
	if err != nil || release == nil {
		t.Fatalf("cap<=0 应直通：%v", err)
	}
	release()
}

func TestKeyedGateQueuedAcquireAfterRelease(t *testing.T) {
	g := NewKeyedGate(1)
	r1, _ := g.Acquire(context.Background(), "k")
	go func() {
		time.Sleep(10 * time.Millisecond)
		r1()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r2, err := g.Acquire(ctx, "k")
	if err != nil {
		t.Fatalf("排队者应在释放后获得名额：%v", err)
	}
	r2()
}

// 并发调用同一个 release 不应重复归还名额（#70）。
//
// 回归缺陷：release 的幂等标记此前是裸 bool，两个 goroutine 并发调用时
// 构成 data race，且都可能读到"未归还"而各执行一次 <-ch —— 多出来的那次
// <-ch 永久阻塞（channel 已空、无人再发送），名额账目随之错乱：它会偷走
// 后续 Acquire 的令牌，于是 cap=1 的上限被突破（可同时有 2 个在飞）。
//
// 断言取「归还一次」的账目：归还后还能取 cap 个，第 cap+1 个才阻塞。
func TestKeyedGate_ConcurrentRelease(t *testing.T) {
	const cap = 2
	g := NewKeyedGate(cap)

	release, err := g.Acquire(context.Background(), "k")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release()
		}()
	}
	// 归还名额时若存在竞态，多出的 <-ch 会永久阻塞——这里给出界等待，
	// 让失败表现为「超时」而不是整包测试挂死。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("并发 release 阻塞：多余的 <-ch 永久等待（幂等标记存在竞态）")
	}

	// 只归还了一次 → 还能取 cap 个，第 cap+1 个才阻塞。
	var releases []func()
	for i := 0; i < cap; i++ {
		rel, err := g.Acquire(context.Background(), "k")
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx, "k"); err == nil {
		t.Error("并发 release 导致配额被突破：应阻塞")
	}
	for _, rel := range releases {
		rel()
	}
}

// release 幂等：重复调用只归还一次（否则会把名额多放出去，突破配额）。
func TestKeyedGate_ReleaseIsIdempotent(t *testing.T) {
	const cap = 2
	g := NewKeyedGate(cap)

	release, err := g.Acquire(context.Background(), "k")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
	release()
	release()

	// 名额只被归还一次 → 还能取 cap 个，第 cap+1 个才阻塞。
	var releases []func()
	for i := 0; i < cap; i++ {
		rel, err := g.Acquire(context.Background(), "k")
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx, "k"); err == nil {
		t.Error("多余 release 导致配额被突破：应阻塞")
	}
	for _, rel := range releases {
		rel()
	}
}

// nil ctx 按 context.Background() 处理（与 Gate 同口径，不 panic）。
func TestKeyedGate_NilContextTreatedAsBackground(t *testing.T) {
	g := NewKeyedGate(1)
	release, err := g.Acquire(nil, "k") //nolint:staticcheck // 契约：nil ctx 按 Background 处理
	if err != nil {
		t.Fatalf("nil ctx 不应报错：%v", err)
	}
	release()
}

func TestFanInPreservesOrder(t *testing.T) {
	out := FanIn(5, func(i int) int {
		time.Sleep(time.Duration(5-i) * time.Millisecond) // 后 index 先完成
		return i * i
	})
	for i, v := range out {
		if v != i*i {
			t.Fatalf("结果应按 index 归位：out[%d]=%d", i, v)
		}
	}
}

func TestFanInRunsConcurrently(t *testing.T) {
	var inFlight, peak int64
	var mu sync.Mutex
	FanIn(8, func(i int) int {
		cur := atomic.AddInt64(&inFlight, 1)
		mu.Lock()
		if cur > peak {
			peak = cur
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt64(&inFlight, -1)
		return i
	})
	if peak < 2 {
		t.Fatalf("期望并发执行，峰值在飞=%d", peak)
	}
}

func TestFanInEmpty(t *testing.T) {
	if out := FanIn[int](0, func(i int) int { return i }); out != nil {
		t.Fatalf("n<=0 应返回 nil，得到 %v", out)
	}
}

func TestFanInPanicRecovered(t *testing.T) {
	// 单个 fn panic：GoSafe 恢复（不炸进程、不波及其余槽位与收口），该槽保持
	// 零值——消费方按「空槽降级」处理。v0.2.1 前为裸 go，此场景直接 crash。
	got := FanIn(3, func(i int) int {
		if i == 1 {
			panic("lane boom")
		}
		return i * 10
	})
	if len(got) != 3 || got[0] != 0 || got[1] != 0 || got[2] != 20 {
		t.Fatalf("panic 槽应保持零值且其余照常: %+v", got)
	}
}
