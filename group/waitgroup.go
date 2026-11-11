package group

import (
	"context"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// WaitGroupWrapper 是 context 感知的 WaitGroup 封装。
//
// 特性：
//   - 每个 goroutine 携带 context（支持超时/取消）
//   - panic 自动恢复并转换为 error（不会把 panic 当成成功）
//   - 第一个 error 自动取消 context（级联取消兄弟 goroutine）
//   - Wait() 返回第一个 error
//
// 并发纪律（#269）：Wrap 与 Wait 并发安全——Wait 进行中到达的 Wrap 会在
// submitMu 上阻塞至 Wait 返回后执行（此时 ctx 已取消、其结果不再被该次
// Wait 汇总）。最自然的契约仍是「全部 Wrap 先于 Wait」；此前的实现无任何
// 闸，Wrap 与 Wait 并发正是 runtime WaitGroup misuse panic 的触发形态
// （Add 与 Wait 并发），同仓 gopool 已用 submitMu 方案解决，本封装对齐。
//
// 用法：
//
//	wg := group.NewWaitGroup(ctx)
//	wg.Wrap(func(ctx context.Context) error { return doWork(ctx) })
//	wg.Wrap(func(ctx context.Context) error { return doMore(ctx) })
//	if err := wg.Wait(); err != nil { ... }
type WaitGroupWrapper struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	// submitMu 串行化 Wrap 与 Wait（#269）：锁内 Add / 锁内 Wait，
	// Wait 期间不再可能出现 Add——runtime WaitGroup misuse panic 的
	// 触发条件从构造上消除（对齐 gopool 的 submitMu 方案）。
	submitMu sync.Mutex

	errOnce sync.Once
	err     error
}

// Wrap 启动一个被追踪的 goroutine。
// goroutine 会继承 WaitGroup 的 context。
// 如果 cb 返回非 nil error 或发生 panic，context 会被取消（影响其他兄弟 goroutine），
// panic 会被转换为 error 并通过 Wait 返回（不会丢失或被当成成功）。
func (w *WaitGroupWrapper) Wrap(cb func(ctx context.Context) error) {
	w.submitMu.Lock()
	defer w.submitMu.Unlock()
	w.wg.Add(1)
	// async.GoSafe:满足"禁止裸 go func"的项目约定;内层 cb 的 panic 已由
	// GoSafeWithErrChannel 转为 error,外层兜底防未来改动引入无 recover 路径
	async.GoSafe(func() {
		defer w.wg.Done()
		// 复用 async.GoSafeWithErrChannel 将 panic 转为 error；
		// errCh 容量为 1，足以容纳 cb 的单次返回或 panic。
		errCh := make(chan error, 1)
		async.GoSafeWithErrChannel(errCh, func() error {
			return cb(w.ctx)
		})
		err := <-errCh
		if err != nil {
			w.errOnce.Do(func() {
				w.err = err
				if w.cancel != nil {
					w.cancel()
				}
			})
		}
	})
}

// Wait 阻塞直到所有 Wrap 启动的 goroutine 退出。
// 返回第一个非 nil error，如果全部成功则返回 nil。
// 与 Wrap 并发调用安全（#269，见类型文档的并发纪律）。
func (w *WaitGroupWrapper) Wait() error {
	w.submitMu.Lock()
	defer w.submitMu.Unlock()
	w.wg.Wait()
	if w.cancel != nil {
		w.cancel()
	}
	return w.err
}

// NewWaitGroup 创建一个 WaitGroupWrapper。
// ctx 为父 context，支持通过 WithTimeout 设置全局超时。
func NewWaitGroup(ctx context.Context, opts ...RunOption) *WaitGroupWrapper {
	rOpts := &runOptions{}
	for _, opt := range opts {
		opt(rOpts)
	}
	g := &WaitGroupWrapper{}
	if rOpts.timeout > 0 {
		g.ctx, g.cancel = context.WithTimeout(ctx, rOpts.timeout)
	} else {
		g.ctx, g.cancel = context.WithCancel(ctx)
	}
	return g
}

// RunOption 配置 WaitGroupWrapper 的可选参数。
type RunOption func(opts *runOptions)

type runOptions struct {
	timeout time.Duration
}

// WithTimeout 为 WaitGroup 的 context 设置超时。
func WithTimeout(timeout time.Duration) RunOption {
	return func(opts *runOptions) {
		opts.timeout = timeout
	}
}
