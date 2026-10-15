package gopool

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// PanicHandler panic 恢复处理函数：ctx 为提交任务时的上下文（可能为
// context.Background），r 为 recover 值，stack 为调用栈。
type PanicHandler func(ctx context.Context, r any, stack []byte)

// PoolOption 池参数。零值字段取对应默认值。
type PoolOption struct {
	// MaxIdleWorkers 常驻 worker 上限；超出部分即用即走（取完存量任务立即退出）。
	// <=0 取默认 1000。
	MaxIdleWorkers int
	// WorkerMaxAge worker 空闲老化时长，超龄退出。<=0 取默认 time.Minute。
	WorkerMaxAge time.Duration
	// TaskChanBuffer 任务队列长度；队列满时回退为直接 `go` 执行。
	// <=0 取默认 1000。
	TaskChanBuffer int
}

// DefaultPoolOption 返回带默认值的 PoolOption。
func DefaultPoolOption() PoolOption {
	return PoolOption{
		MaxIdleWorkers: 1000,
		WorkerMaxAge:   time.Minute,
		TaskChanBuffer: 1000,
	}
}

type task struct {
	ctx context.Context
	f   func() // nil = ticker 唤醒信号（no-op，不计入 WaitGroup）
}

// Pool goroutine 池。零值不可用，经 NewPool 构造；Pool 并发安全。
type Pool struct {
	name string
	opt  PoolOption

	tasks   chan task
	workers atomic.Int32
	// clock 共享时钟（unix 毫秒；0 表示 ticker 未运行）：worker 借它做老化
	// 检查，避免每个 worker 挂独立 timer
	clock  atomic.Int64
	maxage int64 // 毫秒

	panicHandler atomic.Pointer[PanicHandler]

	wg        sync.WaitGroup // 在池路径提交的任务计数（Close 排空用）
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewPool 创建名为 name 的池；opts 为空或字段零值时取默认值。
func NewPool(name string, opts ...PoolOption) *Pool {
	opt := DefaultPoolOption()
	if len(opts) > 0 {
		if opts[0].MaxIdleWorkers > 0 {
			opt.MaxIdleWorkers = opts[0].MaxIdleWorkers
		}
		if opts[0].WorkerMaxAge > 0 {
			opt.WorkerMaxAge = opts[0].WorkerMaxAge
		}
		if opts[0].TaskChanBuffer > 0 {
			opt.TaskChanBuffer = opts[0].TaskChanBuffer
		}
	}
	p := &Pool{
		name:    name,
		opt:     opt,
		tasks:   make(chan task, opt.TaskChanBuffer),
		maxage:  opt.WorkerMaxAge.Milliseconds(),
		closeCh: make(chan struct{}),
	}
	p.SetPanicHandler(nil) // 装载默认处理器
	return p
}

// Go 在池中后台执行 f。
func (p *Pool) Go(f func()) {
	p.CtxGo(context.Background(), f)
}

// CtxGo 在池中后台执行 f；panic 时把 ctx 传给 PanicHandler。
// 队列满时回退为独立 goroutine 执行（任务不丢、依然 recover）。
// wg 计数先于入队，保证 Close 的排空等待不漏任务。
func (p *Pool) CtxGo(ctx context.Context, f func()) {
	if p.closed.Load() {
		// Close 后提交的任务不再入池：旁路执行，不排空等待范围
		goSafe(func() { p.recoverRun(ctx, f) })
		return
	}
	p.wg.Add(1)
	select {
	case p.tasks <- task{ctx: ctx, f: f}:
	default:
		go p.runTask(ctx, f)
		return
	}
	// len==0 说明入队即被空闲 worker 取走；非空说明全部 worker 忙，扩容
	if len(p.tasks) == 0 {
		return
	}
	go p.runWorker()
}

// SetPanicHandler 设置 panic 处理器；nil 恢复默认（标准库 log）。
func (p *Pool) SetPanicHandler(h PanicHandler) {
	if h == nil {
		def := PanicHandler(func(ctx context.Context, r any, stack []byte) {
			log.Printf("[gopool:%s] panic recovered: %v\n%s", p.name, r, string(stack))
		})
		p.panicHandler.Store(&def)
		return
	}
	p.panicHandler.Store(&h)
}

// CurrentWorkers 返回当前存活 worker 数。
func (p *Pool) CurrentWorkers() int { return int(p.workers.Load()) }

// Closed 返回池是否已关闭。
func (p *Pool) Closed() bool { return p.closed.Load() }

// Close 优雅排空：置关闭标记（此后提交走旁路）、唤醒全部 worker，
// 并等待所有已提交任务执行完成；ctx 到期返回其错误。
// 调用方应先停止提交再 Close；Close 可重复调用。
func (p *Pool) Close(ctx context.Context) error {
	p.closed.Store(true)
	p.closeOnce.Do(func() { close(p.closeCh) })
	done := make(chan struct{})
	goSafe(func() {
		p.wg.Wait()
		close(done)
	})
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runTask 执行单任务并归减任务计数。
func (p *Pool) runTask(ctx context.Context, f func()) {
	defer p.wg.Done()
	p.recoverRun(ctx, f)
}

// recoverRun 执行 f 并 recover panic。
func (p *Pool) recoverRun(ctx context.Context, f func()) {
	defer func() {
		if r := recover(); r != nil {
			h := p.panicHandler.Load()
			// 处理器自身 panic 就地收编：此处已在 recover 闭包内，
			// 再 panic 不会被同一个 defer 恢复，会直接打死进程
			defer func() {
				if rr := recover(); rr != nil {
					log.Printf("[gopool:%s] panic handler itself panicked: %v", p.name, rr)
				}
			}()
			(*h)(ctx, r, debug.Stack())
		}
	}()
	f()
}

// runWorker worker 主循环：超出常驻额度的 worker 取完存量任务即退；
// 常驻 worker 借共享时钟做老化检查，超龄或池关闭（存量清空后）退出。
func (p *Pool) runWorker() {
	id := p.workers.Add(1)
	defer p.workers.Add(-1)

	if id > int32(p.opt.MaxIdleWorkers) {
		// 临时 worker：清空队列后直接退出，不等待
		p.drainTasks()
		return
	}

	createdAt := time.Now().UnixMilli()
	for {
		select {
		case t := <-p.tasks:
			if t.f != nil {
				p.runTask(t.ctx, t.f)
			}
			// f == nil 是 ticker 唤醒信号：不执行任务，但要落到下面的查龄逻辑
		case <-p.closeCh:
			// 关闭后队列可能仍有存量：清完再退，保证 Close 排空可达
			p.drainTasks()
			return
		}
		now := p.clock.Load()
		if now == 0 {
			// ticker 未运行：CAS 抢占启动，输家拿本地时间兜底查龄
			now = time.Now().UnixMilli()
			if p.clock.CompareAndSwap(0, now) {
				goSafe(p.runTicker)
			}
		}
		if now-createdAt > p.maxage {
			return
		}
	}
}

// drainTasks 清空队列存量任务（Close 排空路径与临时 worker 退出路径共用）。
func (p *Pool) drainTasks() {
	for {
		select {
		case t := <-p.tasks:
			if t.f == nil {
				continue
			}
			p.runTask(t.ctx, t.f)
		default:
			return
		}
	}
}

func (p *Pool) runTicker() {
	// 退出即清零，下次有活跃 worker 时重新拉起
	defer p.clock.Store(0)

	// 每 WorkerMaxAge/100 唤醒一次 worker 查龄；下限 1ms 防极端值空转
	d := time.Duration(p.maxage) * time.Millisecond / 100
	if d < time.Millisecond {
		d = time.Millisecond
	}
	tk := time.NewTicker(d)
	defer tk.Stop()

	for now := range tk.C {
		if p.CurrentWorkers() == 0 {
			return
		}
		p.clock.Store(now.UnixMilli())
		select {
		case p.tasks <- task{}:
		default:
			// 队列满时丢弃唤醒信号：worker 处理完真实任务后照样查龄
		}
	}
}

// goSafe 池内部 goroutine 兜底 recover（内部代码不回调用户逻辑，纯保险）。
func goSafe(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[gopool] internal goroutine panic: %v\n%s", r, debug.Stack())
			}
		}()
		fn()
	}()
}

// ---- 包级默认池 ----

var defaultPool = NewPool("default")

// Go 在默认池中后台执行 f。
func Go(f func()) { defaultPool.Go(f) }

// CtxGo 在默认池中后台执行 f。
func CtxGo(ctx context.Context, f func()) { defaultPool.CtxGo(ctx, f) }

// SetPanicHandler 设置默认池的 panic 处理器。
func SetPanicHandler(h PanicHandler) { defaultPool.SetPanicHandler(h) }

// CurrentWorkers 返回默认池当前存活 worker 数。
func CurrentWorkers() int { return defaultPool.CurrentWorkers() }
