package breaker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/window"
)

// 缺省值导出（#28）：此前全部小写未导出，下游只能把魔法数字各自复制一份——
// 上游哪天调了缺省，三处拷贝不会跟着动，而行为差异只在熔断阈值上体现，排查成本很高。
// 现导出供调用方**引用**而非复制。
//
// 注意本组只含「本包真实缺省」的四项。**刻意不导出**冷却期与计数型触发的缺省：
// 二者的缺省是「未配置」（cooldown 回落 DefaultProbeTimeout、计数型关闭），对应值为 0，
// 导出一个 0 值常量反而是陷阱——`WithCooldown(DefaultCooldown)` /
// `WithConsecutiveFailures(DefaultTripThreshold)` 会写成"显式传 0"，而这两个选项对 0 是
// panic（WithConsecutiveFailures(0) 例外 = 显式关闭计数型）。**"用缺省"的正确写法是
// 不传该选项**，没有可引用的名字可写。若确需一个非零的触发阈值或冷却，那是调用方的
// 业务取值，应留在调用方自己的配置里，不应冒充本包缺省。
const (
	// DefaultWindow 为缺省统计窗口总长（10s，对齐上游 googleBreaker）。
	DefaultWindow = time.Second * 10
	// DefaultBuckets 为缺省桶数（40），单桶宽度 DefaultWindow/DefaultBuckets = 250ms。
	DefaultBuckets = 40
	// DefaultForcePass 为缺省强制探活间隔（1s）。
	DefaultForcePass = time.Second
	// DefaultProbeTimeout 为缺省探测时限：open 态冷却期满放行半开探测的等待时长，
	// 同时是挂起探测未回报的回收上限（缺省 1m，WithProbeTimeout 可调；冷却期缺省与本值
	// 一致，可经 WithCooldown 独立覆盖——两者是业务冷却与在途上界两个独立决策）。
	DefaultProbeTimeout = time.Minute
	// k 为 Google SRE 自适应熔断的接受率放大系数（Client-Side Throttling 公式），
	// 客户端在服务端可承受量之上仍保留 k 倍的请求尝试。
	k = 1.5
	// minK 为 k 的退化下限，即使全窗失败也保留 1.1 倍的接受率（恢复通道）。
	minK = 1.1
	// protection 为保护阈值：窗口总样本数不超过该值时不触发概率丢弃。
	protection = 5
)

// 桶内样本标记：Add 按标记把样本归入 Success/Failure/Drop（与上游 bucket.go 一致）。
const (
	success = iota
	fail
	drop
)

// breakerState 为显式三态：closed 正常放行；open 按 Google SRE 概率丢弃放行；
// half-open 挂起探测等待回报。迁移只由放行判定（closed→open）与探测回报驱动
// （half-open→closed/open），公式愈合不自动关闸（见 googleBreaker 注释）。
type breakerState uint8

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

// bucket 为熔断统计桶：Sum 记录总样本数（成功+失败+丢弃），
// Success/Failure/Drop 分别记录对应样本数。
// 实现 window.BucketInterface[int64]，作为 RollingWindow 的自定义桶复用其
// 惰性重置与时间对齐语义。
type bucket struct {
	Sum     int64
	Success int64
	Failure int64
	Drop    int64
}

// Add 按样本标记累加：fail 归入 Failure、drop 归入 Drop、其余归入 Success；
// Sum 恒加一。
func (b *bucket) Add(v int64) {
	b.Sum++
	switch v {
	case fail:
		b.Failure++
	case drop:
		b.Drop++
	default:
		b.Success++
	}
}

// Reset 清零桶数据。
func (b *bucket) Reset() {
	b.Sum = 0
	b.Success = 0
	b.Failure = 0
	b.Drop = 0
}

// googleBreaker 为 Google SRE 概率式自适应熔断之上的显式三态状态机
// （closed/open/half-open）：
//
//   - closed：请求全量放行；窗口统计超阈值（丢弃概率 > 0，即接受预算耗尽）或
//     计数型触发（WithConsecutiveFailures 连续失败达阈值）切入 open；
//   - open：流量收敛到探测级——每隔 forcePass 强制放行一次探活，其余一律拒绝
//     （#25：不再按 SRE 公式概率丢弃/愈合放行——公式退役为 closed→open 触发器，
//     熔断期内不放流量）；冷却（自进入/重开 open 起冷却期——WithCooldown 可独立
//     配置，缺省沿用 probeTimeout）期满后，首个放行请求即半开探测
//     （携带新代 generation）；
//   - half-open：挂起探测等待回报——成功关闸（closed + 清窗，旧失败样本不得触发立即重开）；
//     失败重开并重置计时；超 probeTimeout 未回报视为失联回收，放行新探测；
//     陈旧代（gen < 当前代）回报整体作废，不计窗口、不迁移。
//
// 代（generation）仅在探测放行时自增，普通放行携带当前代——同一代内并发在途回报
// 全部有效，代差只在探测迁移边界产生，陈旧探测的迟到回报因此可被精确识别丢弃。
// 开放期普通失败只记窗口样本，不触碰冷却计时（openSince）——冷却不被延长。
// 参见 https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	stat         *window.RollingWindow[int64, *bucket]
	clock        func() time.Time
	windowSize   int
	forcePass    time.Duration
	probeTimeout time.Duration
	// cooldown 为冷却期：open 态自进入/重开起多久放行半开探测；0 = 沿用 probeTimeout
	// （WithCooldown 独立覆盖——冷却期与探测失联时限是两个独立决策，见 #19）。
	cooldown time.Duration
	// consecutive 为计数型触发阈值（WithConsecutiveFailures，与速率型并联）；0 = 关闭。
	// consecFails 为当前连续失败计数，与状态机字段同受 mu 串行化。
	consecutive int
	consecFails int
	// lastPass 记录最近一次强制探活放行的时刻（clock 的 UnixNano）；
	// 0 表示尚无任何放行——每次进入/重开 open 时初始化为进入时刻（#25：
	// forcePass 是熔断期唯一放行通道，不得因零值而失效）。
	lastPass atomic.Int64

	// 状态机字段由 mu 串行化（窗口统计走 RollingWindow 自身锁，两锁无嵌套）。
	mu        sync.Mutex
	state     breakerState
	gen       uint64 // 当前代：探测放行时自增；普通放行携带当前代
	probeGen  uint64 // 挂起探测的代；0 = 无挂起探测
	probeAt   int64  // 挂起探测的放行时刻（clock 的 UnixNano）
	openSince int64  // 进入/重开 open 的时刻（clock 的 UnixNano）
}

// windowResult 为一次窗口聚合的结果（速率型触发判定的输入；
// #25 起公式退役为 closed→open 触发器，不再参与 open 态放行）。
type windowResult struct {
	accepts        int64
	total          int64
	failingBuckets int64
}

// newGoogleBreaker 按 cfg 构造熔断核心；窗口、强制探活与探测调度共用 cfg.clock。
func newGoogleBreaker(cfg breakerConfig) *googleBreaker {
	st := window.NewRollingWindow[int64, *bucket](
		func() *bucket { return new(bucket) },
		cfg.windowSize, cfg.windowInterval,
		window.WithWindowClock[int64, *bucket](cfg.clock),
	)
	return &googleBreaker{
		stat:         st,
		clock:        cfg.clock,
		windowSize:   cfg.windowSize,
		forcePass:    cfg.forcePass,
		probeTimeout: cfg.probeTimeout,
		cooldown:     cfg.cooldown,
		consecutive:  cfg.consecutive,
	}
}

// allowGen 为统一放行入口：探测调度（超时回收 + 冷却期满放行半开探测）之后
// 按状态走放行策略，返回携带代的承诺。拒绝时记一次丢弃样本并返回 ErrCircuitOpen
// （此时不产生代，返回值为 0）。
func (b *googleBreaker) allowGen() (googlePromise, uint64, error) {
	now := b.clock().UnixNano()

	b.mu.Lock()
	// 挂起探测超时未回报：视为失联，回收探测位放行新探测（陈旧代回报随后被代校验丢弃）
	if b.probeGen != 0 && now-b.probeAt >= int64(b.probeTimeout) {
		b.probeGen = 0
		if b.state == stateHalfOpen {
			b.state = stateOpen
		}
	}
	// 冷却期满（自 openSince 起冷却期——WithCooldown 可独立配置，缺省沿用 probeTimeout）
	// 且无挂起探测：本次放行即半开探测——
	// 绕过概率公式必达；代自增产生新探测纪元
	if b.state != stateClosed && b.probeGen == 0 && now-b.openSince >= b.cooldownNanos() {
		b.gen++
		gen := b.gen
		b.probeGen = gen
		b.probeAt = now
		b.state = stateHalfOpen
		b.consecFails = 0 // 进入 half-open：连续失败计数归零
		b.mu.Unlock()
		b.lastPass.Store(now)
		return googlePromise{b: b, gen: gen}, gen, nil
	}
	gen := b.gen
	st := b.state
	b.mu.Unlock()

	// 窗口聚合一次：速率型触发判定（closed→open）的唯一输入
	// （#25 起公式退役为触发器——open 态放行只走 forcePass 与半开探测）
	history := b.history()
	w := k - (k-minK)*float64(history.failingBuckets)/float64(b.windowSize)
	if w < minK {
		w = minK
	}
	weightedAccepts := w * float64(history.accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	// for better performance, no need to care about the negative ratio
	dropRatio := (float64(history.total-protection) - weightedAccepts) / float64(history.total+1)

	// closed 态接受预算耗尽（错误率超阈值）→ 切入 open，本请求即按 open 策略判定；
	// 锁内复核——状态可能已被并发的探测回报/回收迁移
	open := st != stateClosed
	if st == stateClosed && dropRatio > 0 {
		b.mu.Lock()
		if b.state == stateClosed {
			b.state = stateOpen
			b.openSince = now
			b.probeGen = 0
			b.consecFails = 0     // 进入 open：连续失败计数归零
			b.lastPass.Store(now) // open 期 forcePass 自进入起算
		}
		open = b.state != stateClosed
		b.mu.Unlock()
	}

	// closed 态：预算内全量放行（速率型触发已由上面的判定接管）
	if !open {
		return googlePromise{b: b, gen: gen}, gen, nil
	}

	// open/half-open 放行策略（#25 语义修正）：熔断期流量收敛到探测级——
	// 每隔 forcePass 强制放行一次探活（恢复观察哨 + 样本通道），其余一律拒绝
	// 并记丢弃样本。dropRatio<=0 的公式愈合不再短路放行（旧版在此全量放行，
	// 计数型触发在低样本场景形同虚设——状态已 open 而流量照旧）；关闸只由
	// 半开探测成功驱动，熔断期内不放流量
	if lastPass := b.lastPass.Load(); lastPass > 0 && time.Duration(now-lastPass) > b.forcePass {
		b.lastPass.Store(now)
		return googlePromise{b: b, gen: gen}, gen, nil
	}
	b.markDrop()
	return googlePromise{}, 0, ErrCircuitOpen
}

// allow 判定放行并返回承诺；拒绝时返回 ErrCircuitOpen。等价于 AllowGen 丢弃返回的代。
func (b *googleBreaker) allow() (googlePromise, error) {
	p, _, err := b.allowGen()
	return p, err
}

// doReq 判定放行后执行 req 并按 acceptable 回填成败（经代绑定回报）；
// rejected 标记本次是否被熔断拒绝（拒绝时 req 不会执行）。
// req panic 时记一次失败样本后原样 re-panic（success 仍为 false，defer 先于 panic 传播执行）。
func (b *googleBreaker) doReq(req func() error, acceptable func(error) bool) (err error, rejected bool) {
	p, _, aerr := b.allowGen()
	if aerr != nil {
		// **不重复记 drop**：allowGen 在返回 ErrCircuitOpen 前已经记过一次
		// （让 Allow 与 Do* 的样本口径一致）。此前这里再记一次 ⇒ 每次拒绝
		// 记**两个**丢弃样本，而 Allow 只记一个 ⇒ history.total 在整个
		// 熔断窗口里被灌水（它是 SRE 触发器的输入）。
		return aerr, true
	}

	var succ bool
	defer func() {
		// if req() panic, success is false, mark as failure
		if succ {
			p.Pass()
		} else {
			p.Fail()
		}
	}()

	err = req()
	if acceptable(err) {
		succ = true
	}
	return err, false
}

// report 按代回填一次放行的结果：陈旧代（gen < 当前代）整体作废——不计窗口、不迁移、
// 不计入连续失败；命中挂起探测的回报驱动状态迁移（成功关闸清窗；失败重开重置计时）；
// 其余回报只记窗口样本并维护连续失败计数（计数型触发，见 accountConsec）。
func (b *googleBreaker) report(gen uint64, ok bool) {
	b.mu.Lock()
	if gen < b.gen {
		// 探测纪元已推进（新探测放行）：旧代回报作废，防止误算进新一代的账
		b.mu.Unlock()
		return
	}
	isProbe := b.probeGen != 0 && gen == b.probeGen
	if !isProbe {
		b.accountConsec(ok)
	}
	b.mu.Unlock()

	if !isProbe {
		if ok {
			b.markSuccess()
		} else {
			b.markFailure()
		}
		return
	}

	// 探测回报：先迁移再记样本——关闸清窗后，探测结果作为新窗口首个样本
	b.mu.Lock()
	if b.probeGen != gen {
		// 探测位已被超时回收/显式放弃：降级为普通样本（不得驱动迁移）
		b.accountConsec(ok)
		b.mu.Unlock()
		if ok {
			b.markSuccess()
		} else {
			b.markFailure()
		}
		return
	}
	b.probeGen = 0
	if ok {
		b.state = stateClosed
		b.consecFails = 0 // 关闸：连续失败计数归零
		b.mu.Unlock()
		// 关闸清窗：窗口里的旧失败样本不得让 closed 在下一次判定立即复开
		b.stat.Reset()
		b.markSuccess()
	} else {
		b.state = stateOpen
		b.openSince = b.clock().UnixNano()
		b.consecFails = 0             // 重开：连续失败计数归零
		b.lastPass.Store(b.openSince) // open 期 forcePass 自重开起算（#25）
		b.mu.Unlock()
		b.markFailure()
	}
}

// successGen 按代回填成功。
func (b *googleBreaker) successGen(gen uint64) {
	b.report(gen, true)
}

// failureGen 按代回填失败。
func (b *googleBreaker) failureGen(gen uint64) {
	b.report(gen, false)
}

// abandonGen 显式放弃挂起探测（调用方确认探针失联）：腾出探测位但不重置冷却——
// 冷却已满时下一放行即刻补放新探测。非探测代为 no-op。
func (b *googleBreaker) abandonGen(gen uint64) {
	b.mu.Lock()
	if b.probeGen != 0 && gen == b.probeGen {
		b.probeGen = 0
		if b.state == stateHalfOpen {
			b.state = stateOpen
		}
	}
	b.mu.Unlock()
}

// accountConsec 在 mu 内按一次非探测、非陈旧的回报维护连续失败计数，并在 closed 态
// 判定计数型触发：成功清零；连续失败达 WithConsecutiveFailures 阈值即切入 open
// （与速率型并联——openSince 自本次失败起算，探测位在 closed 恒空、保守复位与
// 速率型触发对齐）。仅 consecutive > 0 时维护，缺省零开销零行为变更。
func (b *googleBreaker) accountConsec(ok bool) {
	if b.consecutive <= 0 {
		return
	}
	if ok {
		b.consecFails = 0
		return
	}
	b.consecFails++
	if b.state == stateClosed && b.consecFails >= b.consecutive {
		b.state = stateOpen
		b.openSince = b.clock().UnixNano()
		b.probeGen = 0
		b.consecFails = 0
		b.lastPass.Store(b.openSince) // open 期 forcePass 自进入起算（#25）
	}
}

// cooldownNanos 返回生效的冷却期（UnixNano 口径）：WithCooldown 显式配置优先，
// 否则回落 probeTimeout（v0.9.0 起的缺省绑定行为）。
func (b *googleBreaker) cooldownNanos() int64 {
	if b.cooldown > 0 {
		return int64(b.cooldown)
	}
	return int64(b.probeTimeout)
}

func (b *googleBreaker) markDrop() {
	b.stat.Add(drop)
}

func (b *googleBreaker) markFailure() {
	b.stat.Add(fail)
}

func (b *googleBreaker) markSuccess() {
	b.stat.Add(success)
}

// history 聚合窗口内全部有效桶；failingBuckets 为从旧到新扫描时"连续纯失败桶"
// 的尾部长度（遇成功样本或混合桶清零，空桶延续），语义与上游 googlebreaker.go
// 保持一致。
func (b *googleBreaker) history() windowResult {
	var result windowResult

	b.stat.Reduce(func(bk *bucket) {
		result.accepts += bk.Success
		result.total += bk.Sum
		if bk.Success > 0 {
			result.failingBuckets = 0
		} else if bk.Failure > 0 {
			result.failingBuckets++
		}
	})

	return result
}

// googlePromise 为放行后返回的承诺：绑定本次放行的代，由调用方回填真实调用结果。
type googlePromise struct {
	b   *googleBreaker
	gen uint64
}

// Pass 告知熔断器本次调用成功（按代回报）。
func (p googlePromise) Pass() {
	p.b.report(p.gen, true)
}

// Fail 告知熔断器本次调用失败（按代回报）。
func (p googlePromise) Fail() {
	p.b.report(p.gen, false)
}
