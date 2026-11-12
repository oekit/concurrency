package syncx_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/syncx"
)

func TestKeyedMutexAcquireExclusive(t *testing.T) {
	m := syncx.NewKeyedMutex[string]()
	release, ok := m.Acquire("task-a")
	if !ok {
		t.Fatal("acquire on free key must succeed")
	}
	if release == nil {
		t.Fatal("successful acquire must return non-nil release")
	}

	// 同 key 已持有 ⇒ 即拒，且不产生第二个 release 句柄
	r2, ok := m.Acquire("task-a")
	if ok {
		t.Fatal("acquire on held key must fail immediately")
	}
	if r2 != nil {
		t.Fatal("failed acquire must return nil release")
	}

	// 不同 key 互不干扰（分段锁下各自持有）
	r3, ok := m.Acquire("task-b")
	if !ok {
		t.Fatal("acquire on another free key must succeed")
	}
	_ = r3

	if got := m.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}

	release()
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after release = %d, want 1", got)
	}
}

func TestKeyedMutexReleaseReacquire(t *testing.T) {
	m := syncx.NewKeyedMutex[string]()
	release, ok := m.Acquire("k")
	if !ok {
		t.Fatal("acquire must succeed")
	}
	release()

	// release 后同 key 可再持有（条目已注销，等价全新状态）
	release2, ok := m.Acquire("k")
	if !ok {
		t.Fatal("reacquire after release must succeed")
	}
	release2()

	if got := m.Len(); got != 0 {
		t.Fatalf("Len after all releases = %d, want 0 (entries reclaimed)", got)
	}
}

func TestKeyedMutexReleaseIdempotent(t *testing.T) {
	m := syncx.NewKeyedMutex[string]()
	release, _ := m.Acquire("k")
	release()
	// Double-release 第二次必须 no-op：既不 panic，也不能把
	// 后续新持有者的标记误删（④ 号失效形态）
	release()

	// 新执行流拿走标记
	release2, ok := m.Acquire("k")
	if !ok {
		t.Fatal("reacquire must succeed")
	}
	release() // 迟到的旧 release 再次误调
	if got := m.Len(); got != 1 {
		t.Fatalf("stale release must not drop the new holder, Len = %d, want 1", got)
	}
	release2()

	// 并发 Double-release 也必须安全（两个 goroutine 同时释放同一句柄）
	release3, _ := m.Acquire("k")
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			release3()
		})
	}
	wg.Wait()
	if got := m.Len(); got != 0 {
		t.Fatalf("concurrent releases must drop the mark exactly once, Len = %d, want 0", got)
	}
}

func TestKeyedMutexWithLock(t *testing.T) {
	m := syncx.NewKeyedMutex[string]()
	var runs int

	if !m.WithLock("k", func() { runs++ }) {
		t.Fatal("WithLock on free key must run fn and return true")
	}
	if runs != 1 {
		t.Fatalf("fn ran %d times, want 1", runs)
	}

	// key 忙 ⇒ fn 一次不调、立即 false
	release, _ := m.Acquire("k")
	if m.WithLock("k", func() { runs++ }) {
		t.Fatal("WithLock on held key must return false")
	}
	if runs != 1 {
		t.Fatalf("fn must not run on busy key, ran %d times", runs)
	}
	release()

	// WithLock 之后标记必然已释放（defer release）
	if _, ok := m.Acquire("k"); !ok {
		t.Fatal("key must be free after WithLock returns")
	}
}

func TestKeyedMutexWithLockReleasesOnPanic(t *testing.T) {
	m := syncx.NewKeyedMutex[string]()
	func() {
		defer func() {
			r := recover()
			if r != "boom" {
				t.Fatalf("WithLock must not swallow fn panic, got %v", r)
			}
		}()
		m.WithLock("k", func() { panic("boom") })
	}()
	// panic 展开经过 defer release，标记已注销
	if got := m.Len(); got != 0 {
		t.Fatalf("panic path must release the mark, Len = %d, want 0", got)
	}
	if _, ok := m.Acquire("k"); !ok {
		t.Fatal("key must be re-acquirable after fn panic")
	}
}

func TestKeyedMutexMutualExclusionConcurrent(t *testing.T) {
	const (
		workers  = 32
		attempts = 200
	)

	m := syncx.NewKeyedMutex[string]()
	var (
		inside atomic.Int64 // 临界区同时在飞数（互斥 ⇒ 任何时刻 ≤ 1）
		maxNow atomic.Int64
		enter  atomic.Int64 // 成功进入临界区的次数
		reject atomic.Int64 // 即拒次数
	)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range attempts {
				ok := m.WithLock("shared", func() {
					now := inside.Add(1)
					for {
						max := maxNow.Load()
						if now <= max || maxNow.CompareAndSwap(max, now) {
							break
						}
					}
					if now > 1 {
						t.Errorf("mutual exclusion broken: %d holders inside", now)
					}
					time.Sleep(time.Microsecond)
					inside.Add(-1)
				})
				if ok {
					enter.Add(1)
				} else {
					reject.Add(1)
				}
			}
		})
	}
	wg.Wait()

	if got := maxNow.Load(); got > 1 {
		t.Fatalf("max concurrent holders = %d, want 1", got)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 after all done", got)
	}
	if enter.Load()+reject.Load() != workers*attempts {
		t.Fatalf("enter+reject = %d, want %d", enter.Load()+reject.Load(), workers*attempts)
	}
	t.Logf("enter=%d reject=%d (contention visible)", enter.Load(), reject.Load())
}

func TestKeyedMutexPerKeyIsolationConcurrent(t *testing.T) {
	// 多 key 并发：每个 key 内部互斥，不同 key 之间可并行（分段锁不互相卡）
	const keys = 8
	m := syncx.NewKeyedMutex[int]()
	var inside [keys]atomic.Int64
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			for j := range 100 {
				k := (w + j) % keys
				_ = m.WithLock(k, func() {
					if inside[k].Add(1) > 1 {
						t.Errorf("key %d: mutual exclusion broken", k)
					}
					inside[k].Add(-1)
				})
			}
		})
	}
	wg.Wait()
}

func TestKeyedMutexGenericKeyTypes(t *testing.T) {
	// key 任意 comparable：int / 结构体键都要能工作
	mi := syncx.NewKeyedMutex[int]()
	if _, ok := mi.Acquire(42); !ok {
		t.Fatal("int key acquire must succeed")
	}

	type taskRef struct {
		Repo string
		ID   int
	}
	ms := syncx.NewKeyedMutex[taskRef]()
	ref := taskRef{Repo: "oekit/concurrency", ID: 195}
	release, ok := ms.Acquire(ref)
	if !ok {
		t.Fatal("struct key acquire must succeed")
	}
	if _, ok := ms.Acquire(ref); ok {
		t.Fatal("equal struct key must be mutually exclusive")
	}
	other := taskRef{Repo: "oekit/concurrency", ID: 197}
	if _, ok := ms.Acquire(other); !ok {
		t.Fatal("different struct key must acquire independently")
	}
	release()
}

func TestKeyedMutexNoCrossInstanceLeak(t *testing.T) {
	// 两个实例的键空间完全独立（seed 独立、集合独立）
	a := syncx.NewKeyedMutex[string]()
	b := syncx.NewKeyedMutex[string]()
	if _, ok := a.Acquire("k"); !ok {
		t.Fatal("instance a acquire must succeed")
	}
	if _, ok := b.Acquire("k"); !ok {
		t.Fatal("instance b must not see instance a's hold")
	}
}

func BenchmarkKeyedMutexAcquireRelease(b *testing.B) {
	m := syncx.NewKeyedMutex[string]()
	b.Run("serial", func(b *testing.B) {
		for range b.N {
			release, ok := m.Acquire("k")
			if !ok {
				b.Fatal("unexpected busy")
			}
			release()
		}
	})
	b.Run("parallel-16-keys", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := fmt.Sprintf("k%d", i%16)
				i++
				release, ok := m.Acquire(key)
				if !ok {
					continue
				}
				release()
			}
		})
	})
}
