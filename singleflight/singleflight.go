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
	// waiters 折叠掉的并发调用方数。原子递增：close(ready) 与 Delete 之间
	// 仍可能有迟到 waiter 注册——计数为近似值（观测口径，非精确账目）。
	waiters atomic.Int32
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
func (f *Flight[T]) Do(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) (T, error) {
	c := &call[T]{
		ready: make(chan struct{}),
	}

	actual, loaded := f.calls.LoadOrStore(key, c)
	existing := actual.(*call[T])

	if loaded {
		// waiter：等待 caller 完成或 context 取消
		existing.waiters.Add(1)
		select {
		case <-existing.ready:
			return existing.val, existing.err
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}

	// caller（首个执行 fn 的 goroutine）
	// defer 保证：无论 fn 正常返回还是 panic，都关闭 ready 唤醒所有 waiter 并清理 map。
	// fn panic 时先把 panic 转成 error 存给 waiter，再 re-panic 传播给首个 caller。
	// 清理用 CompareAndDelete 而非 Delete：Delete 是**无条件**删除 key，
	// 而 Forget(key) 之后可能已经注册了同 key 的**新** call ⇒ 本次清理会把
	// 那条新记录一起删掉 ⇒ 第三个调用方会另起一次 fn ⇒ 「同 key 只执行一次
	// fn」的承诺被打破（x/sync/singleflight 用 `if g.m[key] == c` 判同一）。
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("singleflight: fn panic: %w", async.NewPanicError(r))
			close(c.ready)
			f.calls.CompareAndDelete(key, c)
			panic(r)
		}
		close(c.ready)
		f.calls.CompareAndDelete(key, c)
	}()

	c.val, c.err = fn(ctx)
	return c.val, c.err
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
	// Waiters 是本轮折叠掉的并发调用方数（leader 视角：不含自己，>=0）。
	// 排障时可直接看出「这一轮合并了几个调用方」（#18）。
	Waiters int
}

// DoShared 是 Do 的折叠可观测形态（#18）：额外返回 Shared——true 表示
// 本次调用与其他调用方共享了同一次 fn（自己是 waiter 而非 leader）。
// 用于埋点区分「本轮真采集」与「复用了并发中的那一轮」；Do 保持 (T, error)
// 不动，不破坏现有调用点。
func (f *Flight[T]) DoShared(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) (T, bool, error) {
	c := &call[T]{ready: make(chan struct{})}

	actual, loaded := f.calls.LoadOrStore(key, c)
	existing := actual.(*call[T])

	if loaded {
		existing.waiters.Add(1)
		select {
		case <-existing.ready:
			return existing.val, true, existing.err
		case <-ctx.Done():
			var zero T
			return zero, false, ctx.Err()
		}
	}

	// 清理用 CompareAndDelete 而非 Delete：Delete 是**无条件**删除 key，
	// 而 Forget(key) 之后可能已经注册了同 key 的**新** call ⇒ 本次清理会把
	// 那条新记录一起删掉 ⇒ 第三个调用方会另起一次 fn ⇒ 「同 key 只执行一次
	// fn」的承诺被打破（x/sync/singleflight 用 `if g.m[key] == c` 判同一）。
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("singleflight: fn panic: %w", async.NewPanicError(r))
			close(c.ready)
			f.calls.CompareAndDelete(key, c)
			panic(r)
		}
		close(c.ready)
		f.calls.CompareAndDelete(key, c)
	}()

	val, err := fn(ctx)
	c.val, c.err = val, err
	return val, false, err
}

// DoChan 与 Do 语义相同，但结果经 channel 返回，调用方可用 select 组合：
//
//	// leader 免疫取消 + waiter 各自 ctx 退出
//	resCh := f.DoChan(context.WithoutCancel(ctx), key, dial)
//	select {
//	case r := <-resCh:
//	    ...
//	case <-ctx.Done():
//	    return ctx.Err() // 仅本 waiter 退出，不影响 leader
//	}
//
// fn 始终在内部 goroutine 执行；返回的 channel 缓冲 1，调用方放弃读取
// 不会泄漏 goroutine。leader 的 fn 是否免疫取消由调用方传入的 ctx 决定
// （配合 context.WithoutCancel）。
func (f *Flight[T]) DoChan(ctx context.Context, key string, fn func(ctx context.Context) (T, error)) <-chan Result[T] {
	innerCtx := context.WithoutCancel(ctx)
	ch := make(chan Result[T], 1)

	c := &call[T]{ready: make(chan struct{})}
	actual, loaded := f.calls.LoadOrStore(key, c)
	existing := actual.(*call[T])

	if loaded {
		existing.waiters.Add(1)
		go func() {
			select {
			case <-existing.ready:
				ch <- Result[T]{Val: existing.val, Err: existing.err, Shared: true, Waiters: int(existing.waiters.Load())}
			case <-ctx.Done():
				ch <- Result[T]{Err: ctx.Err()}
			}
		}()
		return ch
	}

	// 清理用 CompareAndDelete 而非 Delete：Delete 是**无条件**删除 key，
	// 而 Forget(key) 之后可能已经注册了同 key 的**新** call ⇒ 本次清理会把
	// 那条新记录一起删掉 ⇒ 第三个调用方会另起一次 fn ⇒ 「同 key 只执行一次
	// fn」的承诺被打破（x/sync/singleflight 用 `if g.m[key] == c` 判同一）。
	// 与 Do/DoShared 同一口径——三条路径的清理语义必须一致，否则「同 key 只
	// 执行一次」这个承诺只在部分路径上成立（#105）。
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.err = fmt.Errorf("singleflight: fn panic: %w", async.NewPanicError(r))
				close(c.ready)
				f.calls.CompareAndDelete(key, c)
				ch <- Result[T]{Err: c.err}
				return
			}
			close(c.ready)
			f.calls.CompareAndDelete(key, c)
			ch <- Result[T]{Val: c.val, Err: c.err, Waiters: int(c.waiters.Load())}
		}()
		c.val, c.err = fn(innerCtx)
	}()
	return ch
}
