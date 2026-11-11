package loop

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oekit/concurrency/async"
)

// IntervalMode 周期循环的间隔模式（#140）。
type IntervalMode int

const (
	// IntervalFixed 固定节奏（默认）：按 Interval 的固定时刻表触发，忙时丢 tick——
	// fn 同步执行不重叠，执行期间到点的 tick 积压在 ticker 缓冲（cap=1）自然合并。
	// 关心「固定时刻表」的任务用这档（心跳 / 快照落盘 / 配置轮询）。
	IntervalFixed IntervalMode = iota

	// IntervalAfterCompletion 轮次完成后间隔：本轮 fn 返回（含 panic 收编）后再等
	// Interval 才跑下一轮——本轮跑得久，下一轮相应推迟。关心「两次执行之间至少
	// 隔开 Interval」的任务用这档（清理 / 巡检 / 对账 / 认领-投递类 worker）：
	// 固定节奏语义下「轮次间隔 ≥ Interval」没有保证（长轮次只丢 tick 不后移），
	// 而这类任务对时序的隐含假设恰恰是间隔下限。
	//
	// 间隔从「本轮结束」起算：fn 返回 → 重置计时器 → 等 Interval → 下一轮。
	// panic 收编（RunSafe）发生在 fn 外层，收编完成后同样重新计时。
	IntervalAfterCompletion
)

// Config 周期循环参数。
type Config struct {
	// Interval 轮次间隔。<=0 视为 0（只跑首跑轮，避免忙轮询）。
	// IntervalMode 两种模式下语义不同：IntervalFixed 是固定节奏的周期；
	// IntervalAfterCompletion 是「本轮结束到下一轮开始」的间隔。
	Interval time.Duration
	// IntervalMode 间隔模式，缺省 IntervalFixed（保持既有语义，#140）。
	IntervalMode IntervalMode
	// FirstDelay 首跑延迟；0 = 立即首跑。延迟期间 ctx 取消可打断。
	// 两种模式下都作用于首跑之前。
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
//
// ⚠️ 生命周期脚枪（#154）：Run 非阻塞、循环存活完全挂在 ctx 上——「短函数里
// 派生 ctx + defer cancel」的顺手写法会在函数返回的瞬间静默退役循环（fn 仍会
// 跑完第一轮，冒烟测试发现不了，周期行为已死）：
//
//	// ❌ 反例：函数返回即 cancel，循环存活不足一秒、零报错零日志
//	func startWorkers(rootCtx context.Context) {
//	    ctx, cancel := context.WithCancel(rootCtx)
//	    defer cancel()
//	    loop.Run(ctx, cfg, fn) // 非阻塞，函数随即 return
//	}
//
//	// ✅ 正例一：长命 ctx（main/service 层持有，随进程/服务关闭取消）
//	func main() {
//	    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	    defer stop()
//	    l := loop.Run(ctx, cfg, fn)
//	    defer l.Wait() // 停机排水：在飞轮次收尾
//	    ...
//	}
//
//	// ✅ 正例二：后台巡检/清扫/指标类任务直接用 RunBackground（进程级生命周期；
//	// 会接管 SIGINT/SIGTERM 的进程默认终止行为——副作用与 opt-out 见 RunBackground）
//	loop.RunBackground(cfg, fn)
//
// 入口已取消的 ctx 会被拒绝启动并打一行可定位的 warn 日志（连首跑都不执行，
// 与 FirstDelay 路径的取消语义对齐）。
func Run(ctx context.Context, cfg Config, fn func(ctx context.Context)) *Loop {
	if ctx == nil {
		ctx = context.Background()
	}
	l := &Loop{done: make(chan struct{})}
	async.GoSafeLabeled(cfg.Name, func() {
		defer close(l.done)
		if err := ctx.Err(); err != nil {
			// #154：入口已取消的 ctx 多半来自「短函数 + defer cancel」脚枪——
			// 此前循环静默退役（首跑后即退出），零日志零报错，生产超时清扫
			// 启动即夭折的事故只能靠人工排查。留下可定位的一行。
			log.Printf("[loop:%s] ctx 已取消，循环不启动: %v", cfg.Name, err)
			return
		}
		l.loop(ctx, cfg, fn)
	})
	return l
}

// procCtxOnce/procCtx/procCtxStop 为 RunBackground 惰性创建的进程级 ctx：
// SIGINT/SIGTERM 到达时取消，全部 RunBackground 循环共享。procCtxStop 为
// 信号注册的撤销函数（未安装时 nil），供 DisableBackgroundSignalHandling
// 恢复进程默认信号行为。procSignalWatchDone 为信号观察 goroutine 的退出
// 信号（#270）：opt-out 关停注册后关闭，观察 goroutine 经此分支退出——
// 此前它永久阻塞在 select 上（ctx 无人取消、ch 永不关闭），每次进程泄漏
// 一个 goroutine；测试借它断言泄漏已修。
var (
	procCtxOnce         sync.Once
	procCtx             context.Context
	procCtxStop         func()
	procSignalWatchDone <-chan struct{}
)

// signalHandlingDisabled 为 DisableBackgroundSignalHandling 的进程级开关（#218）。
var signalHandlingDisabled atomic.Bool

// watchProcessSignals 观察进程信号并取消 ctx（#270 拆出的独立单元，可测）：
// 信号到达取消 ctx；stopCh 关闭（opt-out）则经该分支退出——不 cancel（
// 已启动的循环必须继续存活，#218 的语义钉点）。返回退出信号供测试断言
// goroutine 真正退出。
func watchProcessSignals(ctx context.Context, cancel context.CancelFunc, ch <-chan os.Signal, stopCh <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		case <-stopCh:
		}
	}()
	return done
}

// processSignalContext 按当前 opt-out 状态构造 RunBackground 的进程级 ctx：
// opt-out 时为永不取消的 Background（不注册任何信号）；否则注册
// SIGINT/SIGTERM 并返回撤销注册的 stop。
func processSignalContext() (context.Context, func()) {
	if signalHandlingDisabled.Load() {
		return context.Background(), nil
	}
	// 不用 signal.NotifyContext：其 stop 在撤销注册之外还会**连带 cancel ctx**
	// （stdlib 语义——「这个 ctx 用完了」）。opt-out 的「恢复默认终止行为」
	// 恰恰相反：已启动的循环必须继续存活（只是从此不再有任何信号可等），
	// 故手动组装注册与取消，stop 只做 signal.Stop（#218）——观察 goroutine
	// 的退出走 stopCh（#270），不 cancel。
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	stopCh := make(chan struct{})
	procSignalWatchDone = watchProcessSignals(ctx, cancel, ch, stopCh)
	var stopOnce sync.Once
	return ctx, func() {
		// 撤销注册：进程默认终止行为恢复（signal.Stop 幂等）+ 观察 goroutine
		// 退出（stopCh 关闭一次；DisableBackgroundSignalHandling 可重复调用，
		// stopOnce 保 close 幂等——#270）
		stopOnce.Do(func() {
			signal.Stop(ch)
			close(stopCh)
		})
	}
}

func processContext() context.Context {
	procCtxOnce.Do(func() {
		procCtx, procCtxStop = processSignalContext()
	})
	return procCtx
}

// DisableBackgroundSignalHandling 关闭 RunBackground 的进程级 SIGINT/SIGTERM
// 接管（#218 的 opt-out）。
//
// 背景：RunBackground 首次调用会注册 SIGINT/SIGTERM（signal.Notify）
// ——stdlib 语义下 Notify 一旦安装，进程对所注册信号的**默认终止行为**随即
// 失效（直至 Stop/Reset）：信号只取消后台循环，主流程若阻塞在 listener/
// wg.Wait 等原语上将不再因信号退出，运维只能 SIGKILL。这是进程级、godoc
// 之外不可见的副作用——自带完整停机处理（自己 signal.Notify / 用 Run +
// NotifyContext）而只想借 RunBackground 起后台巡检的调用方，在进程启动早期
// 调用本函数即可放弃该副作用：
//
//	func main() {
//	    loop.DisableBackgroundSignalHandling() // opt-out：进程信号语义保持原样
//	    loop.RunBackground(...)                // 循环 ctx 永不因信号取消
//	    // 停机由调用方自己的信号处理驱动
//	}
//
// 语义：
//   - 注册尚未发生（首个 RunBackground 之前调用）：此后**永不注册**——
//     RunBackground 的循环 ctx 即 context.Background()，进程默认信号行为
//     全程不变；
//   - 注册已发生（已有 RunBackground 调用）：立即撤销注册（signal.Stop），
//     恢复进程默认终止行为；**已启动的循环不受影响、也不再有任何信号可等**
//     ——它们持有的 ctx 从此不会被取消，会一直跑到进程退出（需要收尾的
//     调用方应在 opt-out 之前自行安排循环退出）；
//   - 幂等，可重复调用；并发安全。
func DisableBackgroundSignalHandling() {
	signalHandlingDisabled.Store(true)
	procCtxOnce.Do(func() {
		// 注册尚未发生：钉住「永不注册」分支（processContext 的 Once 在此消耗）
		procCtx, procCtxStop = processSignalContext()
	})
	// 注册已发生：撤销注册，恢复进程默认终止行为（signal.Stop 幂等）
	if stop := procCtxStop; stop != nil {
		stop()
	}
}

// RunBackground 启动**进程级**周期循环（#154）：生命周期不再挂在调用方 ctx 上
// ——内部使用共享的进程级 ctx（SIGINT/SIGTERM 取消），把「短函数 + defer cancel
// 令循环静默退役」的脚枪从 API 形态上消除。后台巡检/清扫/指标上报等「要一直
// 跑到停机」的任务九成要的就是这个；需要跟随请求/服务生命周期的循环仍用 Run。
//
// ⚠️ 进程级副作用（#218）：首次 RunBackground 会注册 SIGINT/SIGTERM 的信号
// 监听（signal.Notify）——进程对这两个信号的**默认终止行为**从那一刻起失效：
// 信号只取消后台循环，主流程若没有自己的信号处理（此前依赖默认终止），停机
// 信号到达后循环退了、进程却退不了（阻塞在 listener/wg.Wait 上），只能
// SIGKILL。调用方必须二选一：
//   - 自己处理停机（signal.NotifyContext / Run 的 ✅ 正例一），主流程收到
//     信号后自行收尾退出；
//   - 进程启动早期调用 DisableBackgroundSignalHandling 放弃信号接管，进程
//     默认终止行为保持原样（此时循环 ctx 永不因信号取消，停机由调用方驱动）。
//
// 排水：停机信号到达后在飞轮次收尾退出；调用方应在停机序列里（收到信号后）
// 对返回的 *Loop 调用 Wait，确保周期任务有界完成。
func RunBackground(cfg Config, fn func(ctx context.Context)) *Loop {
	return Run(processContext(), cfg, fn)
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

	// 立即首跑；此后按模式迭代
	l.runOnce(cfg.Name, ctx, fn)

	if cfg.Interval <= 0 {
		// 非正间隔：只跑首跑轮（避免无意义忙轮询拖死 CPU）；
		// 要持续轮询应给正间隔
		return
	}

	if cfg.IntervalMode == IntervalAfterCompletion {
		// 轮次完成后间隔（#140）：本轮结束 → 重置计时器 → 等 Interval → 下一轮。
		// 本轮跑得久，下一轮相应推迟——「轮次间隔 ≥ Interval」由构造保证。
		// timer 每轮新建：间隔从本轮结束起算，Ticker 的固定时刻表语义在这里不成立。
		for {
			timer := time.NewTimer(cfg.Interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			// 取消拦下一轮；在飞已在 runOnce 内同步完成——与固定节奏同契约
			l.runOnce(cfg.Name, ctx, fn)
		}
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
