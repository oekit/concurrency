package singleflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
)

// Flight 对同一个 key 的并发调用只执行一次 fn。
// 零值可用，无需初始化。
type Flight[T any] struct {
	calls sync.Map
}

// New 创建一个 Flight 实例。
func New[T any]() *Flight[T] {
	return &Flight[T]{}
}

type call[T any] struct {
	ready chan struct{}
	val   T
	err   error
	// waiters 折叠掉的并发调用方数。原子递增：摘键（CompareAndDelete）与
	// 唤醒（close ready）之间仍可能有迟到 waiter 注册——计数为近似值
	// （观测口径，非精确账目）。
	waiters atomic.Int32
}

// —— leader/waiter/清理骨架单源（#262）——
//
// Do / DoShared / DoChan 三入口的登记、等待、收尾逻辑收敛为 doCore / waitCall /
// runLeader / release 四件：waiter 分支与 panic 清理此前三处重复，#105 的
// CompareAndDelete 契约注释也复制了三份——清理语义改动从 3 处收敛到 1 处。

// doCore LoadOrStore 登记或加入同 key 的在飞调用（#262 共同骨架）。
// loaded=false：本调用方是 leader，返回的 c 待执行 fn（经 runLeader 收尾）；
// loaded=true：本调用方是 waiter，返回的 existing 正在飞行（经 waitCall 等待），
// 并已登记 waiters 计数。
func (f *Flight[T]) doCore(key string) (c *call[T], loaded bool) {
	c = &call[T]{ready: make(chan struct{})}
	actual, ok := f.calls.LoadOrStore(key, c)
	if !ok {
		return c, false
	}
	c = actual.(*call[T])
	c.waiters.Add(1)
	return c, true
}

// waitCall waiter 等待 leader 完成（#262 共同骨架）：leader 完成返回
// (val, err, true)；ctx 先取消返回 (零值, ctx.Err(), false)——仅本调用方
// 放弃等待，不影响 leader。
func waitCall[T any](ctx context.Context, c *call[T]) (T, error, bool) {
	select {
	case <-c.ready:
		return c.val, c.err, true
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err(), false
	}
}

// release 先摘除在飞记录、后关闭 ready 唤醒全部 waiter（#285：顺序即契约，
// 对齐 pkg/internal/singleflight 与 golang.org/x/sync/singleflight）。
// 反过来（先 close 后摘键）会留一个窗口：ready 已关、key 还在 ⇒ 新到调用
// LoadOrStore 命中的是**已结束**的那次 flight ⇒ 不执行 fn 直接拿到上一次的
// 结果——陈旧结果被当成新鲜结果返回；且唤醒风暴会把 leader 的摘键推迟到
// 毫秒级（同款探针实测：close-first 侧 join 率 24.9% / maxAge 6ms，摘键侧
// 0.5% / 92µs，差 50×——见 TestNarrowWindow_NoStaleJoin）。摘键用
// CompareAndDelete 而非 Delete（#105 契约唯一注释点）：Delete 是**无条件**
// 删除 key，而 Forget(key) 之后可能已经注册了同 key 的**新** call ⇒ 本次清理
// 会把那条新记录一起删掉 ⇒ 第三个调用方会另起一次 fn ⇒「同 key 只执行一次
// fn」的承诺被打破（x/sync/singleflight 用 `if g.m[key] == c` 判同一）。
// 三条入口的清理语义必须一致，否则该承诺只在部分路径上成立。
func (f *Flight[T]) release(key string, c *call[T]) {
	f.calls.CompareAndDelete(key, c)
	close(c.ready)
}

// runLeader 执行 leader 的 fn 并统一收尾（#262 唯一的 panic 清理实现）：
//   - fn 正常完成：release 后返回 false（结果已在 c.val/c.err）；
//   - fn panic：先把 panic 收编为 *async.PanicError 存进 c.err（waiter 拿到
//     error 而非零值+nil 的伪成功——调用方无法区分「合法零值」与「执行失败」
//     会静默污染下游缓存/状态），release 后按 repanic 分流：
//     repanic=true 把**原始 panic** 重抛传播给首个 caller（Do/DoShared：fn
//     在调用方 goroutine 同步执行，与 x/sync/singleflight 语义一致，保留原始
//     调用栈）；repanic=false 吞掉（DoChan：fn 在内部 goroutine 执行，goroutine
//     边界无处传播，重抛会打死进程——结果由调用方经 channel 送达）。
func (f *Flight[T]) runLeader(key string, c *call[T], repanic bool, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("singleflight: fn panic: %w", async.NewPanicError(r))
			f.release(key, c)
			panicked = true
			if repanic {
				panic(r)
			}
			return
		}
		f.release(key, c)
	}()
	fn()
	return false
}

// do 是 Do 与 DoShared 的共同实现（#262）：DoShared 退化为 Do + shared 标志。
// 返回 shared=true 表示本调用方是 waiter（与他人共享了同一次 fn）。
func (f *Flight[T]) do(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) (T, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c, loaded := f.doCore(key)
	if loaded {
		val, err, ok := waitCall(ctx, c)
		if !ok {
			var zero T
			return zero, false, ctx.Err()
		}
		return val, true, err
	}
	// leader（首个执行 fn 的 goroutine）：fn 正常返回还是 panic 都经
	// runLeader 收尾；panic 重抛传播给首个 caller。
	if f.runLeader(key, c, true, func() {
		val, err := fn(ctx)
		c.val, c.err = val, err
	}) {
		// panic 已重抛，走不到这里（防御分支）
		var zero T
		return zero, false, c.err
	}
	return c.val, false, c.err
}

// Do 执行 fn，同一个 key 的并发调用共享结果。
//
// 如果多个 goroutine 同时调用 Do 且 key 相同：
//   - 只有第一个 goroutine（caller）实际执行 fn
//   - 其他 goroutine（waiter）阻塞等待结果
//   - caller 完成后所有 waiter 同时获得返回值
//
// panic 处理（与 golang.org/x/sync/singleflight 语义一致）：
//   - 若 fn panic，panic 向首个 caller 自然传播（保留原始调用栈）
//   - waiter 被唤醒并拿到由 panic 转化的 error（而非零值+nil 的伪成功——
//     调用方无法区分"合法零值"与"执行失败"，会静默污染下游缓存/状态）
//   - map 中的 key 记录会被清理，不会因 panic 残留导致后续调用死锁
//
// ctx 为 nil 时按 context.Background() 处理（永不取消）——与 retry/loop/
// syncx/breaker 的 nil ctx 口径一致（#159/#249）。
func (f *Flight[T]) Do(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) (T, error) {
	val, _, err := f.do(ctx, key, fn)
	return val, err
}

// Forget 移除指定 key 的进行中调用记录。
// 之后对该 key 的 Do 调用会发起新的 fn 执行。
func (f *Flight[T]) Forget(key string) {
	f.calls.Delete(key)
}

// Result 是 DoChan 的单次结果。
type Result[T any] struct {
	Val    T
	Err    error
	Shared bool // true 表示与其他调用方共享了同一次 fn
	// Waiters 是本轮折叠掉的**其他**调用方数（不含自己，>=0——leader 与
	// waiter 两条路径同口径：leader 从未自增 waiters，waiter 读 Load()-1
	// 扣掉自己；concurrency#340 此前 waiter 路径含自己，单 waiter 恒 1、
	// 埋点多算且两路径不可比）。排障时可直接看出「这一轮合并了几个调用方」
	// （#18）。
	Waiters int
}

// DoShared 是 Do 的折叠可观测形态（#18）：额外返回 Shared——true 表示
// 本次调用与其他调用方共享了同一次 fn（自己是 waiter 而非 leader）。
// 用于埋点区分「本轮真采集」与「复用了并发中的那一轮」；Do 保持 (T, error)
// 不动，不破坏现有调用点。
// ctx 为 nil 时按 context.Background() 处理（与 Do 同口径，#159/#249）。
func (f *Flight[T]) DoShared(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) (T, bool, error) {
	return f.do(ctx, key, fn)
}

// DoChan 与 Do 语义相同，但结果经 channel 返回，调用方可用 select 组合：
//
//	// leader 恒免疫取消（内部已 WithoutCancel）+ waiter 各自 ctx 退出
//	resCh := f.DoChan(ctx, key, dial)
//	select {
//	case r := <-resCh:
//	    ...
//	case <-ctx.Done():
//	    return ctx.Err() // 仅本调用方放弃等待，不影响 leader 的 fn
//	}
//
// fn 始终在内部 goroutine 执行；返回的 channel 缓冲 1，调用方放弃读取
// 不会泄漏 goroutine。leader 的 fn **恒免疫取消**：内部固定以
// context.WithoutCancel(ctx) 执行——即使传入可取消 ctx，fn 也不会被调用方的
// 放弃等待腰斩（合并语义：同 key 的其他调用方还在等这次结果）。
// 需要「调用方取消即中止执行」的取消传播用 Do/DoShared（fn 直接收到调用方 ctx）。
// ctx 为 nil 时按 context.Background() 处理（WithoutCancel(nil) 会 panic，
// 与 Do/DoShared 同口径兜底，#159/#249）。
func (f *Flight[T]) DoChan(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) <-chan Result[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	innerCtx := context.WithoutCancel(ctx)
	ch := make(chan Result[T], 1)

	c, loaded := f.doCore(key)
	if loaded {
		go func() {
			select {
			case <-c.ready:
				// concurrency#340：doCore 已 Add(1)（含自己），扣掉——
				// 「不含自己」与 leader 视角同口径。
				w := int(c.waiters.Load()) - 1
				if w < 0 {
					w = 0
				}
				ch <- Result[T]{Val: c.val, Err: c.err, Shared: true, Waiters: w}
			case <-ctx.Done():
				ch <- Result[T]{Err: ctx.Err()}
			}
		}()
		return ch
	}

	// leader goroutine：repanic=false——goroutine 边界无处传播 panic，
	// 收编错误经 channel 送达（runLeader #262 单源；正常与 panic 都在
	// release 之后发送，waiter 唤醒先于结果投递）。
	go func() {
		f.runLeader(key, c, false, func() { c.val, c.err = fn(innerCtx) })
		ch <- Result[T]{Val: c.val, Err: c.err, Waiters: int(c.waiters.Load())}
	}()
	return ch
}
