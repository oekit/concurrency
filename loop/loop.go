package loop

import (
	"context"
	"time"

	"github.com/oekit/concurrency/async"
)

// Config 周期循环参数。
type Config struct {
	// Interval 轮次间隔。<=0 视为 0（只跑首跑轮，避免忙轮询）。
	Interval time.Duration
	// FirstDelay 首跑延迟；0 = 立即首跑。延迟期间 ctx 取消可打断。
	FirstDelay time.Duration
	// Name 可选任务名：panic 汇报带 task 标签（async.RunSafeLabeled），便于按任务聚合。
	Name string
}

// Loop 一个正在运行的周期任务循环。
type Loop struct {
	done chan struct{}
}

// Run 启动周期循环，立即（或 FirstDelay 后）返回 *Loop，不阻塞调用方。
// 循环在后台 goroutine 中运行；生命周期由 ctx 控制，排水用 Wait。
func Run(ctx context.Context, cfg Config, fn func(ctx context.Context)) *Loop {
	if ctx == nil {
		ctx = context.Background()
	}
	l := &Loop{done: make(chan struct{})}
	async.GoSafeLabeled(cfg.Name, func() {
		defer close(l.done)
		l.loop(ctx, cfg, fn)
	})
	return l
}

// Wait 阻塞至循环排水完成（ctx 已取消 且 在飞轮次收尾返回）。
// 超时预算由调用方配：需要限时排水时用 select 包一层 Done() 或外层 ctx。
// 可安全重复调用。
func (l *Loop) Wait() {
	<-l.done
}

// Done 返回排水完成信号（Wait 的非阻塞形态）。
func (l *Loop) Done() <-chan struct{} {
	return l.done
}

func (l *Loop) loop(ctx context.Context, cfg Config, fn func(ctx context.Context)) {
	// 首跑延迟：select 保证可被停机打断（不要 time.Sleep）
	if cfg.FirstDelay > 0 {
		timer := time.NewTimer(cfg.FirstDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	// 立即首跑；此后按 Interval 丢 tick 迭代
	l.runOnce(cfg.Name, ctx, fn)

	if cfg.Interval <= 0 {
		// 非正间隔：只跑首跑轮（避免无意义忙轮询拖死 CPU）；
		// 要持续轮询应给正间隔
		return
	}

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// 取消拦下一轮；在飞已在 runOnce 内同步完成，直接退出
			return
		case <-ticker.C:
			// 忙时丢 tick：runOnce 同步执行，期间 tick 积压在 ticker
			// 缓冲（cap=1）自然合并，不重叠
			l.runOnce(cfg.Name, ctx, fn)
		}
	}
}

// runOnce 执行一轮。panic 经 RunSafe 收编，循环存活。
// 轮次内传入 ctx，调用方可感知取消做「尽快收尾」——loop 不会腰斩在飞轮次。
func (l *Loop) runOnce(name string, ctx context.Context, fn func(ctx context.Context)) {
	async.RunSafeLabeled(name, func() {
		fn(ctx)
	})
}
