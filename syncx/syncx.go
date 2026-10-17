// Package syncx 并发编排原语（stdlib-only）：按键在飞闸门 + 按序扇出收集。
// 收编自消费方手写形态——LLM 按模型名限在飞（429 风暴治理）与「按 index 写
// 结果免锁」的并行扇出样板。
package syncx

import (
	"context"
	"sync"

	"github.com/oekit/concurrency/async"
)

// KeyedGate 按键在飞上限闸门（并发安全；channel 令牌先到先得；键集由调用方
// 保证有界——如按模型名/租户名，键量级恒小不回收）。
type KeyedGate struct {
	mu    sync.Mutex
	cap   int
	slots map[string]chan struct{}
}

// NewKeyedGate 构造；cap<=0 = 不限（Acquire 直通零开销）。
func NewKeyedGate(cap int) *KeyedGate {
	return &KeyedGate{cap: cap, slots: map[string]chan struct{}{}}
}

func (g *KeyedGate) slot(key string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch, ok := g.slots[key]
	if !ok {
		ch = make(chan struct{}, g.cap)
		g.slots[key] = ch
	}
	return ch
}

// Acquire 取一个键下在飞名额；ctx 取消（排队等待中）返回 ctx.Err()。
// 返回的 release 归还名额，幂等可重复调用。
func (g *KeyedGate) Acquire(ctx context.Context, key string) (func(), error) {
	if g.cap <= 0 {
		return func() {}, nil
	}
	ch := g.slot(key)
	select {
	case ch <- struct{}{}:
		released := false
		return func() {
			if released {
				return
			}
			released = true
			<-ch
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// FanIn 并行执行 fn(0..n-1)，各 goroutine 只写自己的 results[i] 槽位（免锁），
// 全部完成后按 index 序返回。fn 经 async.GoSafe 收编（AGENTS.md「禁裸 go func」，
// v0.2.1 前为裸 go、fn panic 会打死宿主进程）：单个 fn panic 被恢复并走全局
// panic handler 落日志，该槽位保持零值、其余槽位与收口不受影响——消费方按
// 「空槽降级」处理（零值歧义由调用方区分）。n<=0 返回空切片。
func FanIn[T any](n int, fn func(i int) T) []T {
	if n <= 0 {
		return nil
	}
	results := make([]T, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		async.GoSafe(func() {
			defer wg.Done()
			results[i] = fn(i)
		})
	}
	wg.Wait()
	return results
}

// Slot 是 FanInErr 的逐槽结果：Val/Err 为 fn 的正常返回；Panicked 非 nil 表示
// fn panic（已恢复并经 async.ReportPanic 上报全局 handler），此时 Val 为零值、
// Err 为 nil——T 的合法零值与 panic 空槽由此可区分，消费方按槽降级不再需要
// 自写 recover→占位样板。
type Slot[T any] struct {
	Val      T
	Err      error
	Panicked any
}

// FanInErr 逐槽错误变体：并行执行 fn(0..n-1)（fn 可返回 error），全部完成后
// 按 index 序返回逐槽结果。fn 正常返回 → Val/Err 如实记录；fn panic → 该槽
// Panicked=payload、Val 零值、Err nil——不再静默（FanIn 契约保留不动，供无
// error 形态的轻量路径）。n<=0 返回空切片。
func FanInErr[T any](n int, fn func(i int) (T, error)) []Slot[T] {
	if n <= 0 {
		return nil
	}
	slots := make([]Slot[T], n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		async.GoSafe(func() {
			defer wg.Done()
			func() {
				defer func() {
					if r := recover(); r != nil {
						slots[i].Panicked = r
						async.ReportPanic(r)
					}
				}()
				v, err := fn(i)
				slots[i].Val, slots[i].Err = v, err
			}()
		})
	}
	wg.Wait()
	return slots
}
