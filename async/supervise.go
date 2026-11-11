package async

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SuperviseOption 是 Supervise 的函数选项。
type SuperviseOption func(*superviseConfig)

type superviseConfig struct {
	task           string
	initialBackoff time.Duration
	maxBackoff     time.Duration
	maxConsecutive int
	onRestart      func(restarts int64, cause error, wait time.Duration)
	onGiveUp       func(cause error)
}

// WithTask 给监督循环设任务名：panic 汇报的 ctx 携带 task 标签（GoSafeLabeled
// 同款，TaskFromContext 可取），便于按任务维度聚合告警。
func WithTask(name string) SuperviseOption {
	return func(c *superviseConfig) { c.task = name }
}

// WithRestartBackoff 配置重启节奏：第 k 次连续失败后等待
// initial*2^(k-1)，受 max 封顶（指数退避）；max <= 0 时退化为固定 initial。
// initial <= 0 = 失败后立即重启——**慎用**：loop 秒挂的常驻错误会退化成
// 热循环烧 CPU，除非调用方确有外部节流。
// 缺省：initial=1s、max=30s（指数退避封顶 30s）。
func WithRestartBackoff(initial, max time.Duration) SuperviseOption {
	return func(c *superviseConfig) {
		c.initialBackoff = initial
		c.maxBackoff = max
	}
}

// WithMaxConsecutiveFailures 配置连续失败上限：连续 max 轮失败（错误或 panic）
// 后放弃重启并调用 onGiveUp（告警钩子），监督结束。0（缺省）= 不限，一直重启
// 到 ctx 取消或 loop 优雅返回。
func WithMaxConsecutiveFailures(max int) SuperviseOption {
	return func(c *superviseConfig) { c.maxConsecutive = max }
}

// WithOnRestart 配置每次重启前的观测钩子：restarts 是重启序号（从 1 起），
// cause 是该轮失败原因（loop 返回的错误或 *PanicError），wait 是本次重启等待。
// hook 自身 panic 被收编上报，不影响监督循环。
func WithOnRestart(hook func(restarts int64, cause error, wait time.Duration)) SuperviseOption {
	return func(c *superviseConfig) { c.onRestart = hook }
}

// WithOnGiveUp 配置放弃重启时的告警钩子（需配合 WithMaxConsecutiveFailures）。
// hook 自身 panic 被收编上报，不影响监督循环。
func WithOnGiveUp(hook func(cause error)) SuperviseOption {
	return func(c *superviseConfig) { c.onGiveUp = hook }
}

// Supervision 是一个正在运行的监督循环句柄。
type Supervision struct {
	done     chan struct{}
	errMu    sync.Mutex
	err      error
	rounds   atomic.Int64 // 已完成的 loop 调用轮数（含失败轮）
	restarts atomic.Int64 // 因失败重启的次数
	consec   atomic.Int64 // 当前连续失败计数
}

// Done 返回监督结束的信号 channel。
func (s *Supervision) Done() <-chan struct{} {
	return s.done
}

// Wait 阻塞至监督结束并返回结束原因：
//   - nil：loop 优雅返回（返回值 nil）；
//   - ctx.Err()：ctx 取消（无论最后一轮 loop 是否报错——取消是调用方的停机
//     决定，业务错误不该伪装成监督的结束原因；调用方告警判据
//     「err != nil && !errors.Is(err, context.Canceled)」因此可靠）；
//   - loop 的错误 / *PanicError：连续失败达上限放弃重启时最后的失败原因。
func (s *Supervision) Wait() error {
	<-s.done
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Err 返回结束原因（非阻塞；监督未结束时返回 nil——与 Done 配合使用）。
func (s *Supervision) Err() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// Rounds 返回已完成的 loop 调用轮数（liveness 观测面：轮数停止增长 = 循环没在跑）。
func (s *Supervision) Rounds() int64 {
	return s.rounds.Load()
}

// Restarts 返回因失败重启的累计次数。
func (s *Supervision) Restarts() int64 {
	return s.restarts.Load()
}

// ConsecutiveFailures 返回当前连续失败计数。轮次语义下「成功轮」即监督结束，
// 不存在中途清零——监督进行中该值就是自启动以来的失败轮累计，重启成功与否
// 要看 Rounds 是否增长（liveness 观测：失败计数增长而轮数不增长 = 循环一直
// 没能跑完一轮）；优雅结束后归零（末轮成功），放弃重启后保留失败终值。
func (s *Supervision) ConsecutiveFailures() int64 {
	return s.consec.Load()
}

// Supervise 启动监督循环（后台运行，立即返回 *Supervision）。
//
// 消费方多处手写的「后台循环防护」收编为一个原语（#137）：GoSafe/GoSafeLabeled
// 只保进程不保循环——循环体 panic 后 goroutine 死亡、循环静默停摆（消费队列的
// worker 停摆=丢消息、巡检循环停摆=新对象永不启动、活性表里的僵尸条目永占位）。
// Supervise 提供监督语义三件套：
//
//  1. **单轮 panic 收编**：loop panic 转成 *PanicError（errors.As 可辨、携带栈）
//     并带 task 标签上报全局 PanicHandler（标签经 panic 汇报的 ctx 携带，
//     TaskFromContext 可取）；
//  2. **退避重启**：失败轮（错误或 panic）按指数退避重启（见 WithRestartBackoff），
//     重启等待可被 ctx 取消打断；
//  3. **失败上限 + 告警钩子**：连续失败达上限放弃重启（WithMaxConsecutiveFailures
//     + WithOnGiveUp），观测钩子见 WithOnRestart；Rounds/Restarts/ConsecutiveFailures
//     可做活性导出。
//
// 轮次语义：一次 loop 调用 = 一轮。
//   - loop 返回 nil = 优雅结束，监督随之结束（循环自己决定生命周期完成了；
//     常驻循环应在 ctx.Done 后返回）；
//   - loop 返回 error 或 panic = 失败轮，触发重启流程；
//   - ctx 取消优先于一切重启：取消后不再启动新一轮，监督尽快结束
//     （结束原因见 Wait/Err）。
//
// 与 loop.Run 的分工：loop.Run 管「周期性 tick」（固定节奏、单轮 panic 不打断
// 后续轮次）；Supervise 管「单个长驻循环活得好不好」（loop 自含迭代逻辑，整轮
// 死亡后从头重启——重启即丢失 loop 内部状态，有状态的循环应把状态外置或在
// loop 内部自旋、只在整轮确实无法自愈时返回错误）。
//
// 用法:
//
//	sup := async.Supervise(ctx, func(ctx context.Context) error {
//	    return consumeQueue(ctx) // panic/错误都会被监督重启
//	}, async.WithTask("outbox-worker"), async.WithMaxConsecutiveFailures(5))
//	async.GoSafe(func() {  // 需要告警时另行监听
//	    if err := sup.Wait(); err != nil { alert(err) }
//	})
func Supervise(ctx context.Context, loop func(ctx context.Context) error, opts ...SuperviseOption) *Supervision {
	if loop == nil {
		panic("async.Supervise: nil loop")
	}
	cfg := superviseConfig{
		initialBackoff: time.Second,
		maxBackoff:     30 * time.Second,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.initialBackoff < 0 {
		cfg.initialBackoff = 0
	}
	s := &Supervision{done: make(chan struct{})}
	GoSafeLabeled(cfg.task, func() {
		defer close(s.done)
		s.run(ctx, cfg, loop)
	})
	return s
}

// run 监督主循环（运行在 GoSafeLabeled 的 goroutine 内）。
func (s *Supervision) run(ctx context.Context, cfg superviseConfig, loop func(ctx context.Context) error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// 已取消的 ctx 不首跑：监督没有「跑一轮再说」的理由
	if ctxErr := ctx.Err(); ctxErr != nil {
		s.setErr(ctxErr)
		return
	}
	for {
		err := s.round(ctx, cfg.task, loop)
		s.rounds.Add(1)
		if err == nil {
			s.consec.Store(0) // 末轮成功：连续失败归零
			s.setErr(nil)
			return
		}
		// ctx 已取消：不再重启，以 ctx.Err() 告终（取消是停机决定，
		// 最后一轮的业务错误不该伪装成监督的结束原因）
		if ctxErr := ctx.Err(); ctxErr != nil {
			s.setErr(ctxErr)
			return
		}
		consec := s.consec.Add(1)
		if cfg.maxConsecutive > 0 && consec >= int64(cfg.maxConsecutive) {
			RunSafe(func() {
				if cfg.onGiveUp != nil {
					cfg.onGiveUp(err)
				}
			})
			s.setErr(err)
			return
		}
		s.restarts.Add(1)
		wait := restartBackoff(consec, cfg.initialBackoff, cfg.maxBackoff)
		if cfg.onRestart != nil {
			restarts := s.restarts.Load()
			RunSafe(func() { cfg.onRestart(restarts, err, wait) })
		}
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				s.setErr(ctx.Err())
				return
			case <-timer.C:
			}
		}
	}
}

// round 执行一轮：loop 的 panic 收编为 *PanicError（同 RunSafeErr，但汇报的
// ctx 携带 task 标签），业务错误原样透传。收编骨架走 guarded 单源（#261）。
func (s *Supervision) round(ctx context.Context, task string, loop func(ctx context.Context) error) error {
	var loopErr error
	if pe := guarded(globalReport(ctx, task), func() { loopErr = loop(ctx) }); pe != nil {
		return pe
	}
	return loopErr
}

func (s *Supervision) setErr(err error) {
	s.errMu.Lock()
	s.err = err
	s.errMu.Unlock()
}

// restartBackoff 第 k 次连续失败后的重启等待：initial*2^(k-1) 受 max 封顶；
// max <= 0 固定 initial（无封顶就不做指数——无界翻倍只会饱和成天文数字）；
// initial <= 0 恒 0（立即重启，调用方自担热循环风险）。
// 溢出防护对齐 retry.addJitter 的口径：再翻倍必溢出时直接取封顶值——
// 真实等待只会更大，封顶即正确答案；负等待（溢出回绕）等于零等待，会静默绕过退避。
func restartBackoff(k int64, initial, max time.Duration) time.Duration {
	if initial <= 0 {
		return 0
	}
	if max <= 0 {
		return initial
	}
	if initial >= max {
		return max
	}
	d := initial
	for i := int64(1); i < k; i++ {
		if d >= max {
			return max
		}
		if d >= time.Duration(1<<62) {
			return max
		}
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}
