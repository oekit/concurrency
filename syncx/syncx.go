// Package syncx 并发编排原语（stdlib-only）：在飞闸门（按键分档位 / 全局单一
// 配额）+ 按序扇出收集。
// 收编自消费方手写形态——LLM 按模型名限在飞（429 风暴治理）、「这个进程总共
// 最多 N 个在飞」的手搓信号量，与「按 index 写结果免锁」的并行扇出样板。
package syncx

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
)

// KeyedGate 按键在飞上限闸门（并发安全；channel 令牌先到先得；键集由调用方
// 保证有界——如按模型名/租户名，键量级恒小不回收）。
//
// 按 key 分档位（"每个模型最多 N 个在飞"）。要的是**单一全局配额**
// （"整个进程总共最多 N 个在飞"）用 Gate——两者语义不同，不要用固定 key 硬凑。
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

// Slot 是 FanInErr 的逐槽结果：Val/Err 为 fn 的正常返回；fn panic 时
// Err 承载 *async.PanicError（errors.As 可辨、携带栈，#12）且 Val 为零值。
// Panicked 保留原始 payload（兼容字段，Deprecated——改用 errors.As(Slot.Err)）。
type Slot[T any] struct {
	Val T
	// Err 正常错误，或 panic 收编的 *async.PanicError。
	Err error
	// Panicked 历史兼容字段：panic 原始 payload。新代码改用
	// errors.As(&async.PanicError) 读 Err。
	//
	// Deprecated: v0.7.0 起由 Slot.Err 承载。
	Panicked any
}

// FanInErr 逐槽错误变体：并行执行 fn(0..n-1)（fn 可返回 error），全部完成后
// 按 index 序返回逐槽结果。fn 正常返回 → Val/Err 如实记录；fn panic → 该槽
// Err=*async.PanicError（v0.7.0 起统一收编，errors.As 可辨、携带栈；Panicked
// 同写原始 payload，已 Deprecated）、Val 零值，并经 ReportPanic 上报——不再
// 静默（FanIn 契约保留不动，供无 error 形态的轻量路径）。n<=0 返回空切片。
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
						slots[i].Err = async.NewPanicError(r)
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

// FanInLimit 是 FanInErr 的受限并发变体（#23）：n 槽结果语义不变，内部固定
// w = min(limit, n) 个 worker 经原子计数器拉取 index 执行——goroutine O(w)，
// 供「n 可达数十以上、fn 是有成本 IO（DB/远端查询）」的 fan-out 限并发，
// 防下游过载（收编消费方手写的 channel 信号量样板）。其余语义与 FanInErr
// 一致：按 index 序返回逐槽结果、panic 进该槽 Err（*async.PanicError）并
// ReportPanic 上报。limit<=0 = 不设上限（等价 FanInErr 行为）。
func FanInLimit[T any](n, limit int, fn func(i int) (T, error)) []Slot[T] {
	if n <= 0 {
		return nil
	}
	if limit <= 0 || limit > n {
		limit = n
	}
	slots := make([]Slot[T], n)
	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)
	wg.Add(limit)
	for w := 0; w < limit; w++ {
		async.GoSafe(func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							slots[i].Panicked = r
							slots[i].Err = async.NewPanicError(r)
							async.ReportPanic(r)
						}
					}()
					v, err := fn(i)
					slots[i].Val, slots[i].Err = v, err
				}()
			}
		})
	}
	wg.Wait()
	return slots
}

// FanInLimitCtx 是 FanInLimit 的 ctx 感知变体（#27）：fn 携带 ctx 供单项内部
// 使用；引擎级取消语义——worker 每轮拉取 index 前先查 ctx.Err()（worker 内
// 缓存，取消后零额外锁开销），命中即不再启动新工作（fn 一次都不调），当前及
// 剩余槽一次遍历快速排空为该错误（context.Canceled）；已在飞 item 跑完、结果
// 照常落槽。其余语义与 FanInLimit 一致（panic 进槽并上报、limit<=0 = 不设上限、
// n<=0 = nil；ctx 为 nil 时按 context.Background() 处理）。
func FanInLimitCtx[T any](ctx context.Context, n, limit int, fn func(ctx context.Context, i int) (T, error)) []Slot[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	if n <= 0 {
		return nil
	}
	if limit <= 0 || limit > n {
		limit = n
	}
	slots := make([]Slot[T], n)
	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)
	wg.Add(limit)
	for w := 0; w < limit; w++ {
		async.GoSafe(func() {
			defer wg.Done()
			var cancelErr error // worker 内缓存：取消后一次锁调用，排空零额外开销
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				if cancelErr == nil {
					cancelErr = ctx.Err()
				}
				if cancelErr != nil {
					// 引擎级 fail-fast：不再启动新工作，剩余槽快速排空
					slots[i].Err = cancelErr
					continue
				}
				func() {
					defer func() {
						if r := recover(); r != nil {
							slots[i].Panicked = r
							slots[i].Err = async.NewPanicError(r)
							async.ReportPanic(r)
						}
					}()
					v, err := fn(ctx, i)
					slots[i].Val, slots[i].Err = v, err
				}()
			}
		})
	}
	wg.Wait()
	return slots
}
