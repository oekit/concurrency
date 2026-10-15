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
