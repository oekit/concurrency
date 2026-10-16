package batch

import (
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// idleExitRounds 后台循环连续空闲多少个 interval 后自退出（下次 Add 重新
// 拉起），闲时不留常驻 goroutine。
const idleExitRounds = 10

// TaskContainer 攒批容器扩展点：执行器只负责"何时触发 flush"，
// "怎么攒、怎么执行"由容器实现决定。Bulk/Chunk 变体均构建其上。
type TaskContainer interface {
	// AddTask 收纳一个任务，返回 true 表示需要立即触发一次 flush
	// （例如攒满条数或字节数）。
	AddTask(task any) bool
	// Execute 执行一批任务。执行器保证同一执行器同一时刻至多一路调用
	// （后台循环串行），panic 由执行器隔离，实现方无需自行 recover。
	Execute(tasks []any)
	// RemoveAll 取走并清空当前积压的全部任务；在执行器内部锁内调用，
	// 实现方无需自行加锁。
	RemoveAll() []any
}

// Ticker 周期信号源抽象。注入手动实现即可确定性地驱动批量时序测试。
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// TickerFactory 按给定间隔构造 Ticker。
type TickerFactory func(d time.Duration) Ticker

// BatchOption 攒批执行器的可选参数。
type BatchOption func(*batchOptions)

type batchOptions struct {
	tickerFactory TickerFactory
	now           func() time.Time
}

// WithBatchTicker 注入周期信号源工厂（测试用手动 Ticker 即可确定性地驱动
// flush 时序，无需真实睡眠）。nil 工厂被忽略。
func WithBatchTicker(f TickerFactory) BatchOption {
	return func(o *batchOptions) {
		if f != nil {
			o.tickerFactory = f
		}
	}
}

// WithBatchClock 注入时间源（LessExecutor 等按墙钟判定的执行器使用）。
// nil 函数被忽略。
func WithBatchClock(now func() time.Time) BatchOption {
	return func(o *batchOptions) {
		if now != nil {
			o.now = now
		}
	}
}

// PeriodicalExecutor 攒批执行器：把零散任务攒进 TaskContainer，按间隔
// 周期或容器要求立即批量执行。
//
// 并发模型：
//   - 首次 Add 按需拉起唯一后台循环；连续 idleExitRounds 个 interval 无积压
//     时自退出，下次 Add 重新拉起（退出与 Add 在同一临界区内判定，任务不会
//     滞留无人消费）；
//   - 取走积压（RemoveAll）在锁内完成，Execute 在锁外串行执行且 panic 被隔离；
//   - Add/Flush/Wait 均可在多 goroutine 并发调用。
type PeriodicalExecutor struct {
	interval   time.Duration
	container  TaskContainer
	tickerFunc TickerFactory

	mu            sync.Mutex
	background    bool // 后台循环是否在跑（含正在退出收尾）
	stopRequested bool

	wake chan struct{} // cap 1：立即 flush 信号（非阻塞投递）
	exit chan struct{} // 后台循环退出时 close
}

// NewPeriodicalExecutor 创建攒批执行器。interval 为批量触发周期（<=0 表示
// 不设周期，仅容器主动要求或 Flush/Wait 驱动）；container 为攒批容器。
func NewPeriodicalExecutor(interval time.Duration, container TaskContainer, opts ...BatchOption) *PeriodicalExecutor {
	if container == nil {
		panic("batch: container must not be nil")
	}
	o := &batchOptions{now: time.Now}
	for _, opt := range opts {
		opt(o)
	}
	if o.tickerFactory == nil {
		o.tickerFactory = defaultTickerFactory
	}
	return &PeriodicalExecutor{
		interval:   interval,
		container:  container,
		tickerFunc: o.tickerFactory,
		wake:       make(chan struct{}, 1),
	}
}

// Add 收纳一个任务；容器要求立即 flush（AddTask 返回 true）时唤醒后台。
func (e *PeriodicalExecutor) Add(task any) {
	e.mu.Lock()
	needsFlush := e.container.AddTask(task)
	e.ensureBackgroundLocked()
	e.mu.Unlock()
	if needsFlush {
		e.trigger()
	}
}

// Flush 同步排空：立即取走当前积压并在调用方 goroutine 执行。与后台循环
// 并发调用安全（取走原子，双方各执行一半也保证不重不漏）。
func (e *PeriodicalExecutor) Flush() {
	if tasks := e.swap(); len(tasks) > 0 {
		runSafe(e.container, tasks)
	}
}

// Wait 请求后台排空积压后退出并阻塞至退出完成（幂等；退出后继续 Add 会
// 重新拉起后台）。适合进程退出前兜底，防止尾部批次丢失。
func (e *PeriodicalExecutor) Wait() {
	e.mu.Lock()
	if !e.background {
		e.mu.Unlock()
		return
	}
	e.stopRequested = true
	exit := e.exit
	e.mu.Unlock()
	e.trigger()
	<-exit
}

// ensureBackgroundLocked 后台循环不在跑时拉起；调用方必须持有 e.mu。
func (e *PeriodicalExecutor) ensureBackgroundLocked() {
	if e.background {
		return
	}
	e.background = true
	e.stopRequested = false
	e.exit = make(chan struct{})
	async.GoSafe(e.backgroundLoop)
}

// trigger 非阻塞投递立即 flush 信号（cap 1，已有待处理信号时合并）。
func (e *PeriodicalExecutor) trigger() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// backgroundLoop 后台主循环：等待周期或唤醒信号，取走积压批量执行；
// 停机请求或空闲到期时退出（见 round/roundTick）。
func (e *PeriodicalExecutor) backgroundLoop() {
	defer close(e.exit)

	var tickerC <-chan time.Time
	var ticker Ticker
	if e.interval > 0 {
		ticker = e.tickerFunc(e.interval)
		tickerC = ticker.C()
		defer ticker.Stop()
	}

	idleRounds := 0
	for {
		select {
		case <-e.wake:
			if e.round() {
				return
			}
			idleRounds = 0
		case <-tickerC:
			if e.roundTick(&idleRounds) {
				return
			}
		}
	}
}

// round 取走全部积压逐批执行；取空且收到停机请求时摘除后台标记并返回 true。
// 摘牌与"取空"在同一临界区完成：此后任何 Add 都能看到 background=false
// 并重新拉起，不存在任务滞留窗口。
func (e *PeriodicalExecutor) round() (exit bool) {
	for {
		e.mu.Lock()
		tasks := e.container.RemoveAll()
		if len(tasks) == 0 && e.stopRequested {
			e.background = false
			e.mu.Unlock()
			return true
		}
		e.mu.Unlock()
		if len(tasks) == 0 {
			return false
		}
		runSafe(e.container, tasks)
	}
}

// roundTick 周期触发版 round：取空且空闲满 idleExitRounds 轮时自退出。
func (e *PeriodicalExecutor) roundTick(idleRounds *int) (exit bool) {
	for {
		e.mu.Lock()
		tasks := e.container.RemoveAll()
		if len(tasks) == 0 {
			*idleRounds++
			if *idleRounds >= idleExitRounds {
				e.background = false
				e.mu.Unlock()
				return true
			}
			e.mu.Unlock()
			return false
		}
		e.mu.Unlock()
		*idleRounds = 0
		runSafe(e.container, tasks)
	}
}

// swap 取走当前积压（锁内），供 Flush 与后台循环共用。
func (e *PeriodicalExecutor) swap() []any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.container.RemoveAll()
}

// runSafe panic 隔离地执行一批任务：单批失败不影响执行器与后续批次。
func runSafe(c TaskContainer, tasks []any) {
	async.RunSafe(func() { c.Execute(tasks) })
}

// defaultTickerFactory 标准库 time.Ticker 适配。
func defaultTickerFactory(d time.Duration) Ticker {
	t := time.NewTicker(d)
	return tickerAdapter{t}
}

type tickerAdapter struct{ *time.Ticker }

func (a tickerAdapter) C() <-chan time.Time { return a.Ticker.C }
