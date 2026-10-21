package syncx

import (
	"context"
	"sync/atomic"
)

// Gate 是**全局在飞上限闸门**：整个实例最多 cap 个在飞，排队者等 ctx 取消
// 即返回 ctx.Err()。与 KeyedGate 的区别：Gate 是单一全局配额（不分 key），
// KeyedGate 按 key 分档位（按模型名/租户名分别限流）。
//
// 两种取用语义并存、命名区分：
//
//   - Acquire(ctx)：满额时**排队**等名额；
//   - TryAcquire()：满额时**立即拒**（不等待、不占名额）——重活的并发上限
//     用满就该立刻拒，让调用方回 429 / 503，而不是把请求堆在队列里。
//
// 为什么需要这一档：现有原语都不覆盖"这个进程总共最多 N 个在飞"这类闸门。
//
//   - KeyedGate 按 key 隔离，用固定 key 硬凑可行，但那是把"多档位闸门"原语
//     当"单档位闸门"用，语义与命名都不对；
//   - gopool.Pool 是弹性 worker 池，**MaxIdleWorkers 是常驻 worker 上限，
//     不是并发上限**——任务队列满时回退为独立 goroutine 直跑，任务永不因
//     并发数被拒（实测 MaxIdleWorkers=2 时实际并发仍是 10）；它与"硬配额"
//     的语义正好相反；
//   - ratelimit.KeyedLimiter 是速率型（令牌桶），不是并发闸门。
//
// 适用场景：保护下游 DB / 控制子进程数 / 限制外部 API 并发。
//
// 并发安全；release 幂等（可重复调用，只归还一次）。
type Gate struct {
	cap int
	sem chan struct{}
}

// NewGate 构造；cap<=0 = 不限（Acquire 直通零开销）。
func NewGate(cap int) *Gate {
	if cap <= 0 {
		return &Gate{}
	}
	return &Gate{cap: cap, sem: make(chan struct{}, cap)}
}

// Acquire 取一个在飞名额；ctx 取消（排队等待中）返回 ctx.Err()。
// 返回的 release 归还名额，幂等可重复调用。
//
// ctx 为 nil 时按 context.Background() 处理（永不取消）。
func (g *Gate) Acquire(ctx context.Context) (func(), error) {
	if g.cap <= 0 {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case g.sem <- struct{}{}:
		var released atomic.Bool
		return func() {
			if !released.CompareAndSwap(false, true) {
				return
			}
			<-g.sem
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryAcquire 尝试取一个在飞名额，**不等待**：名额已满时**立即**返回 ok=false。
//
// 与 Acquire 的区别是**排队 vs 即拒**，两者并存、命名区分：
//
//   - Acquire(ctx)：满额时**排队**等名额，等 ctx 取消才返回 ctx.Err()；
//   - TryAcquire()：满额时**立刻拒**，一个名都不占。
//
// 什么时候要即拒：Acquire 的排队语义会把「并发被占满」换成「请求堆在队列里」——
// 那是另一种 DoS 形态。重活（图片全量解码、ffmpeg 子进程、模型推理）的并发
// 上限用满就该立刻拒，让调用方回 429 / 503，而不是让队列越堆越长、把内存
// 或子进程数推到别处爆掉。
//
// ok=false 只有一个含义：**名额已满**（非阻塞取名额没有别的失败原因），
// 调用方可据此返回 429 / 503，而不是泛化成 500。
//
// 返回的 release 归还名额，幂等可重复调用（与 Acquire 一致）。
// cap<=0（不限）时恒返回 ok=true——不限额的闸门没有「满」这回事。
//
// 用法:
//
//	release, ok := gate.TryAcquire()
//	if !ok {
//	    return nil, ErrTooManyInFlight // 429 / 503
//	}
//	defer release()
func (g *Gate) TryAcquire() (release func(), ok bool) {
	if g.cap <= 0 {
		return func() {}, true
	}
	select {
	case g.sem <- struct{}{}:
		var released atomic.Bool
		return func() {
			if !released.CompareAndSwap(false, true) {
				return
			}
			<-g.sem
		}, true
	default:
		return nil, false
	}
}
