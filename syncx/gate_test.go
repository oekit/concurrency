package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGate_Unlimited(t *testing.T) {
	g := NewGate(0)
	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
	// 不限配额时反复取用不会阻塞。
	for i := 0; i < 100; i++ {
		rel, err := g.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		rel()
	}
}

func TestGate_NegativeCapUnlimited(t *testing.T) {
	g := NewGate(-1)
	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
}

// 硬配额的核心语义：在飞数不超过 cap，超出的调用方**排队等待**（而不是像
// 弹性 worker 池那样被偷偷放行成独立 goroutine）。
func TestGate_CapsConcurrency(t *testing.T) {
	const cap = 3
	g := NewGate(cap)

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
		wg       sync.WaitGroup
	)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := g.Acquire(context.Background())
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer release()
			cur := inFlight.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
		}()
	}
	wg.Wait()

	if p := peak.Load(); p > cap {
		t.Errorf("峰值在飞数 %d 超过配额 %d", p, cap)
	}
}

// 名额用尽时排队者等 ctx 取消 → 返回 ctx.Err()，不泄漏名额。
func TestGate_AcquireRespectsContextCancel(t *testing.T) {
	g := NewGate(1)

	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, cerr := g.Acquire(ctx); cerr == nil {
		t.Fatal("ctx 已取消时应返回错误")
	} else if cerr != context.Canceled {
		t.Errorf("应返回 context.Canceled，实际 %v", cerr)
	}

	// 取消的等待者没有占住名额：归还第一个后仍能取到。
	release()
	rel2, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("名额泄漏：Acquire: %v", err)
	}
	rel2()
}

// release 幂等：重复调用只归还一次（否则会把名额多放出去，突破配额）。
func TestGate_ReleaseIsIdempotent(t *testing.T) {
	const cap = 2
	g := NewGate(cap)

	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
	release()
	release()

	// 名额只被归还一次 → 还能取 cap 个，第 cap+1 个才阻塞。
	var releases []func()
	for i := 0; i < cap; i++ {
		rel, err := g.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx); err == nil {
		t.Error("多余 release 导致配额被突破：应阻塞")
	}
	for _, rel := range releases {
		rel()
	}
}

// 并发调用 release 不应重复归还（race 下也不能多放名额）。
func TestGate_ConcurrentRelease(t *testing.T) {
	const cap = 4
	g := NewGate(cap)

	release, err := g.Acquire(context.Background())
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
	wg.Wait()

	// 只归还了一次 → 还能取 cap 个，第 cap+1 个才阻塞。
	var releases []func()
	for i := 0; i < cap; i++ {
		rel, err := g.Acquire(context.Background())
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		releases = append(releases, rel)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := g.Acquire(ctx); err == nil {
		t.Error("并发 release 导致配额被突破：应阻塞")
	}
	for _, rel := range releases {
		rel()
	}
}

func TestGate_NilContextTreatedAsBackground(t *testing.T) {
	g := NewGate(1)
	release, err := g.Acquire(nil) //nolint:staticcheck // 契约：nil ctx 按 Background 处理
	if err != nil {
		t.Fatalf("Acquire(nil): %v", err)
	}
	release()
}

// 排队中的等待者被 ctx 取消后，不影响后续正常获取。
func TestGate_CancelledWaiterDoesNotBlockOthers(t *testing.T) {
	g := NewGate(1)
	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, aerr := g.Acquire(ctx)
		done <- aerr
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	if werr := <-done; werr == nil {
		t.Fatal("被取消的等待者应返回错误")
	}
	release()

	rel2, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatalf("取消的等待者泄漏了名额：Acquire: %v", err)
	}
	rel2()
}
