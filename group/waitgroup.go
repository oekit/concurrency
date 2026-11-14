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
// 并发纪律（#269/#296）：
//   - Wrap 与 Wait 并发安全：计数增减是本类型 mu 的临界区，不再使用
//     sync.WaitGroup——「Add 与 Wait 并发」的 runtime misuse panic 触发面
//     从构造上消除（#269 的 submitMu 修复曾把并发问题换成嵌套死锁，见下）；
//   - **任务体内可安全嵌套 Wrap**（#296）：Wait 等待期间不持有任何锁，
//     任务体派生子任务（fan-out/分治/pipeline stage）照常入计数；
//   - Wait 的契约是**等系统静止**：阻塞至计数归零——含等待期间新到达的
//     Wrap 与任务体内嵌套派生（与旧「外部 Wrap 阻塞至 Wait 返回」不同：
//     新到任务被计入并等待，持续派发下 Wait 不返回属调用方未静止）。
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

	// counter+cond 替代 sync.WaitGroup（#269/#296）：
	//   - Wait 在 cond 上等归零、**不持有任何锁** ⇒ 任务体内嵌套 Wrap 不会
	//     与 Wait 互等（旧 submitMu 方案的 AB-BA 死锁：Wait 持锁等任务、
	//     任务体内 Wrap 等锁）；
	//   - 计数增减在 mu 临界区原子完成 ⇒ 无 WaitGroup「Add 不得与 Wait
	//     并发」的规则面，misuse panic 结构性消失。
	mu      sync.Mutex
	cond    *sync.Cond // counter 归零时 Broadcast 唤醒 Wait
	counter int

	errOnce sync.Once
	err     error
}

// Wrap 启动一个被追踪的 goroutine。
// goroutine 会继承 WaitGroup 的 context。
// 如果 cb 返回非 nil error 或发生 panic，context 会被取消（影响其他兄弟 goroutine），
// panic 会被转换为 error 并通过 Wait 返回（不会丢失或被当成成功）。
func (w *WaitGroupWrapper) Wrap(cb func(ctx context.Context) error) {
	w.mu.Lock()
	w.counter++
	w.mu.Unlock()
	// async.GoSafe:满足"禁止裸 go func"的项目约定;内层 cb 的 panic 已由
	// GoSafeWithErrChannel 转为 error,外层兜底防未来改动引入无 recover 路径
	async.GoSafe(func() {
		defer func() {
			w.mu.Lock()
			w.counter--
			if w.counter == 0 {
				w.cond.Broadcast() // 唤醒等静止的 Wait
			}
			w.mu.Unlock()
		}()
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

// Wait 阻塞直到系统静止（计数归零，见类型文档的等静止契约）。
// 返回第一个非 nil error，如果全部成功则返回 nil。
// 与 Wrap 并发调用安全（#269），任务体内嵌套 Wrap 安全（#296）。
func (w *WaitGroupWrapper) Wait() error {
	w.mu.Lock()
	for w.counter > 0 {
		w.cond.Wait() // 不持锁等待：任务体内嵌套 Wrap 可正常入计数
	}
	w.mu.Unlock()
	if w.cancel != nil {
		w.cancel()
	}
	return w.err
}

// NewWaitGroup 创建一个 WaitGroupWrapper。
// ctx 为父 context，支持通过 WithTimeout 设置全局超时。
func NewWaitGroup(ctx context.Context, opts ...RunOption) *WaitGroupWrapper {
	if ctx == nil {
		// #329：nil ctx = Background（#159 口径）——此前 WithCancel(nil) 直接 panic
		ctx = context.Background()
	}
	rOpts := &runOptions{}
	for _, opt := range opts {
		opt(rOpts)
	}
	g := &WaitGroupWrapper{}
	g.cond = sync.NewCond(&g.mu)
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
