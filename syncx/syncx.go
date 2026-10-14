// Package syncx 并发编排原语（stdlib-only）：按键在飞闸门 + 按序扇出收集。
// 收编自消费方手写形态——LLM 按模型名限在飞（429 风暴治理）与「按 index 写
// 结果免锁」的并行扇出样板。
package syncx

import (
	"context"
	"sync"
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
// 全部完成后按 index 序返回。fn 内 panic 不接（与裸 go 语义一致，由调用方的
// panic 守卫约定承担）。n<=0 返回空切片。
func FanIn[T any](n int, fn func(i int) T) []T {
	if n <= 0 {
		return nil
	}
	results := make([]T, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			results[i] = fn(i)
		}(i)
	}
	wg.Wait()
	return results
}
