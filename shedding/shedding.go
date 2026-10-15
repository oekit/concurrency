package shedding

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/window"
)

const (
	// defaultBuckets 为缺省桶数（对齐上游 adaptiveshedder）。
	defaultBuckets = 50
	// defaultWindow 为缺省统计窗口总长（5s，对齐上游），单桶宽度
	// defaultWindow/defaultBuckets = 100ms。
	defaultWindow = time.Second * 5
	// defaultCpuThreshold 为缺省 CPU 过载阈值（1000m 记法，900m 即 90%）。
	defaultCpuThreshold = 900
	// defaultMinRt 为缺省最小平均响应时间（毫秒）：窗口尚无请求时的回退值，
	// 取一个足够大的值避免误丢弃（对齐上游）。
	defaultMinRt = float64(time.Second / time.Millisecond)
	// flyingBeta 为在途并发均值 avgFlying 的 EMA 系数（对齐上游）：
	// avgFlying = avgFlying×flyingBeta + flying×(1-flyingBeta)，仅请求完成时更新。
	flyingBeta = 0.9
	// coolOffDuration 为冷却期：最近发生过丢弃后，即使 CPU 回落仍继续丢弃的时长。
	coolOffDuration = time.Second
	// cpuMax 为 CPU 使用率上限（millicpu 千分记法，1000m 即满载）。
	cpuMax = 1000
	// millisecondsPerSecond 用于把 maxPass×minRt 换算为在途并发上限。
	millisecondsPerSecond = 1000
	// overloadFactorLowerBound 为过载因子下限：CPU 无论多满至少保留 10% 容量。
	overloadFactorLowerBound = 0.1

	// cpuRefreshInterval 为缺省 CPU 采样周期（250ms + beta=0.95 约平滑近 5s 负载，
	// 对齐上游 stat 包）。
	cpuRefreshInterval = time.Millisecond * 250
	// samplerBeta 为缺省采样管线的 CPU 指数平滑系数（对齐上游 stat 包）。
	samplerBeta = 0.95
)

// ErrServiceOverloaded 是 Shedder.Allow 在服务过载拒绝时返回的哨兵，
// 错误消息与上游逐字一致（"service overloaded"），判定一律用 errors.Is。
var ErrServiceOverloaded = errors.New("service overloaded")

type (
	// Promise 是 Allow 放行后返回的承诺：调用方执行真实处理后二选一回填结果——
	// Pass 告知成功（驱动 RT/通过数统计），Fail 告知失败。与 breaker 包的
	// Promise 语义一致，但独立定义（两包不互相依赖）。
	Promise interface {
		// Pass tells the shedder that the processing is successful.
		Pass()
		// Fail tells the shedder that the processing is failed.
		Fail()
	}

	// Shedder 是降载器接口。
	Shedder interface {
		// Allow 判断请求是否放行：放行返回 Promise 供调用方回填 Pass/Fail，
		// 过载拒绝返回 ErrServiceOverloaded；ctx 已取消时返回 ctx.Err()
		// （不消耗统计）。
		Allow(ctx context.Context) (Promise, error)
	}

	// ShedderOption 用于定制 Shedder（函数选项模式；按本仓命名硬规则为
	// XxxOption，禁止裸 Option）。
	ShedderOption func(opts *shedderOptions)

	shedderOptions struct {
		window       time.Duration
		buckets      int
		cpuThreshold int64
		clock        func() time.Time
		onReject     func(reason string)
	}

	// adaptiveShedder 为 CPU 自适应降载器（移植上游 adaptiveShedder）。
	// 零值不可用，须经 NewAdaptiveShedder 构造。
	adaptiveShedder struct {
		cpuThreshold int64
		// windowScale = 每秒桶数 / 1000，把 maxPass×minRt（毫秒）换算为在途并发。
		windowScale     float64
		clock           func() time.Time
		onReject        func(reason string)
		flying          atomic.Int64
		avgFlyingLock   sync.Mutex
		avgFlying       float64
		overloadTime    atomic.Int64 // 最近一次 CPU 过载时刻（clock 的 UnixNano）；0 = 尚未过载
		droppedRecently atomic.Bool
		passCounter     *window.RollingWindow[int64, *window.Bucket[int64]]
		rtCounter       *window.RollingWindow[int64, *window.Bucket[int64]]
	}

	// shedPromise 为 adaptiveShedder 放行后返回的承诺。
	shedPromise struct {
		// start 记录放行时刻（clock 的 UnixNano），Pass 时计算 RT。
		start   int64
		shedder *adaptiveShedder
	}

	// nopShedder 恒放行（除 ctx 已取消外），无任何统计。
	nopShedder struct{}

	// nopPromise 为 nopShedder 返回的空承诺，Pass/Fail 均为 no-op。
	nopPromise struct{}
)

// NewAdaptiveShedder 返回 CPU 自适应降载器，opts 定制窗口/阈值/时间源/丢弃钩子。
// 缺省 CPU 源为平台采样（linux：/proc/stat + cgroup；其他平台恒错误 → 自动
// 禁用、全放行）；测试/自定义场景用 SetCPUUsageFunc 注入。
func NewAdaptiveShedder(opts ...ShedderOption) Shedder {
	options := shedderOptions{
		window:       defaultWindow,
		buckets:      defaultBuckets,
		cpuThreshold: defaultCpuThreshold,
		clock:        time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	bucketDuration := options.window / time.Duration(options.buckets)
	newBucket := func() *window.Bucket[int64] { return new(window.Bucket[int64]) }
	windowClock := window.WithWindowClock[int64, *window.Bucket[int64]](options.clock)
	return &adaptiveShedder{
		cpuThreshold: options.cpuThreshold,
		windowScale:  float64(time.Second) / float64(bucketDuration) / millisecondsPerSecond,
		clock:        options.clock,
		onReject:     options.onReject,
		passCounter: window.NewRollingWindow[int64, *window.Bucket[int64]](
			newBucket, options.buckets, bucketDuration,
			window.IgnoreCurrentBucket[int64, *window.Bucket[int64]](), windowClock),
		rtCounter: window.NewRollingWindow[int64, *window.Bucket[int64]](
			newBucket, options.buckets, bucketDuration,
			window.IgnoreCurrentBucket[int64, *window.Bucket[int64]](), windowClock),
	}
}

// NewNopShedder 返回恒放行的空降载器（测试/禁用降载用）：Allow 除 ctx 已取消外
// 恒返回空 Promise 与 nil error，不读 CPU、无统计。
func NewNopShedder() Shedder {
	return nopShedder{}
}

// Allow implements Shedder.Allow.
func (as *adaptiveShedder) Allow(ctx context.Context) (Promise, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if as.shouldDrop() {
		as.droppedRecently.Store(true)
		return nil, ErrServiceOverloaded
	}

	as.addFlying(1)
	return &shedPromise{start: as.clock().UnixNano(), shedder: as}, nil
}

// addFlying 调整在途并发计数；仅在请求完成（delta < 0）时用 EMA 更新
// avgFlying——该策略让均值相对真实值滞后且更平滑：并发骤升时均值爬得慢、
// 多放行；骤降时均值落得慢、少放行，尽量喂饱服务（对齐上游注释）。
func (as *adaptiveShedder) addFlying(delta int64) {
	flying := as.flying.Add(delta)
	if delta < 0 {
		as.avgFlyingLock.Lock()
		as.avgFlying = as.avgFlying*flyingBeta + float64(flying)*(1-flyingBeta)
		as.avgFlyingLock.Unlock()
	}
}

// avgFlyingSnapshot 原子读取 avgFlying 均值。
func (as *adaptiveShedder) avgFlyingSnapshot() float64 {
	as.avgFlyingLock.Lock()
	defer as.avgFlyingLock.Unlock()
	return as.avgFlying
}

// highThru 判断在途并发是否超过过载容量：flying 与滞后的 avgFlying 都须超过
// maxFlight×过载因子（双保险防瞬时毛刺，对齐上游）。
func (as *adaptiveShedder) highThru() bool {
	avgFlying := as.avgFlyingSnapshot()
	maxFlight := as.maxFlight() * as.overloadFactor()
	return avgFlying > maxFlight && float64(as.flying.Load()) > maxFlight
}

// maxFlight 用 Little's Law 估计在途并发上限：
//
//	windows = 每秒桶数；maxQPS = maxPass × windows；
//	allowedFlying = maxQPS × minRT(毫秒) / 1000
//
// 窗口尚无数据时 maxPass=1、minRt=defaultMinRt，得到一个合理的宽松上限。
func (as *adaptiveShedder) maxFlight() float64 {
	maxFlight := float64(as.maxPass()) * as.minRt() * as.windowScale
	return math.Max(maxFlight, 1)
}

// maxPass 返回窗口内单桶最大通过数（吞吐上界）；空窗口回退 1。
func (as *adaptiveShedder) maxPass() int64 {
	var result int64 = 1

	as.passCounter.Reduce(func(b *window.Bucket[int64]) {
		if b.Sum > result {
			result = b.Sum
		}
	})

	return result
}

// minRt 返回窗口内最小平均响应时间（毫秒，按桶平均后取最小）；
// 只统计有请求的桶，空窗口回退 defaultMinRt（宽松值，避免误丢弃）。
func (as *adaptiveShedder) minRt() float64 {
	result := defaultMinRt

	as.rtCounter.Reduce(func(b *window.Bucket[int64]) {
		if b.Count <= 0 {
			return
		}

		avg := math.Round(float64(b.Sum) / float64(b.Count))
		if avg < result {
			result = avg
		}
	})

	return result
}

// overloadFactor 返回过载因子 (cpuMax-cpu)/(cpuMax-threshold)，夹在
// [0.1, 1]：CPU 刚过阈值时约 1（几乎不缩量），越满越小，但无论多满
// 至少保留 10% 容量。CPU 源错误视为 0（因子取 1，fail-open）。
func (as *adaptiveShedder) overloadFactor() float64 {
	// as.cpuThreshold 构造时已保证在 (0, cpuMax) 内
	cpu, err := currentCPUUsage()
	if err != nil {
		cpu = 0
	}
	factor := (cpuMax - float64(cpu)) / (cpuMax - float64(as.cpuThreshold))
	// at least accept 10% of acceptable requests, even cpu is highly overloaded.
	return math.Min(math.Max(factor, overloadFactorLowerBound), 1)
}

// shouldDrop 判定是否丢弃：仅在 (CPU 过载 || 冷却期内) 且在途并发超限时丢弃。
// 纯确定性逻辑（无概率分支）。
func (as *adaptiveShedder) shouldDrop() bool {
	if as.systemOverloaded() || as.stillHot() {
		if as.highThru() {
			if as.onReject != nil {
				as.onReject(as.dropMsg())
			}
			return true
		}
	}

	return false
}

// dropMsg 生成上游同格式的丢弃诊断消息（供 WithShedderRejectHandler 消费）。
// 注意 stillHot 可能因冷却期满而复位 droppedRecently——与上游日志路径的
// 双重求值行为一致，且此时丢弃决策已定，不影响结果。
func (as *adaptiveShedder) dropMsg() string {
	cpu, _ := currentCPUUsage()
	return fmt.Sprintf(
		"dropreq, cpu: %d, maxPass: %d, minRt: %.2f, hot: %t, flying: %d, avgFlying: %.2f",
		cpu, as.maxPass(), as.minRt(), as.stillHot(), as.flying.Load(), as.avgFlyingSnapshot())
}

// stillHot 判断是否处于冷却期：最近发生过丢弃且距最近一次 CPU 过载不足
// coolOffDuration。冷却期满时复位 droppedRecently（下次丢弃重新计时）。
func (as *adaptiveShedder) stillHot() bool {
	if !as.droppedRecently.Load() {
		return false
	}

	overloadTime := as.overloadTime.Load()
	if overloadTime == 0 {
		return false
	}

	if as.now()-overloadTime < int64(coolOffDuration) {
		return true
	}

	as.droppedRecently.Store(false)
	return false
}

// systemOverloaded 判断 CPU 是否过载（usage >= threshold），过载时刷新
// overloadTime 作为冷却期起点。CPU 源错误视为不过载（fail-open）。
func (as *adaptiveShedder) systemOverloaded() bool {
	cpu, err := currentCPUUsage()
	if err != nil || cpu < as.cpuThreshold {
		return false
	}

	as.overloadTime.Store(as.now())
	return true
}

// now 返回当前时刻（clock 的 UnixNano，仅参与差值运算）。
func (as *adaptiveShedder) now() int64 {
	return as.clock().UnixNano()
}

// Pass 回填成功：递减在途并发，并把 RT（毫秒，向上取整）与通过数写入统计窗口。
func (p *shedPromise) Pass() {
	rt := float64(p.shedder.now()-p.start) / float64(time.Millisecond)
	p.shedder.addFlying(-1)
	p.shedder.rtCounter.Add(int64(math.Ceil(rt)))
	p.shedder.passCounter.Add(1)
}

// Fail 回填失败：仅递减在途并发（不产生 RT/通过统计，对齐上游）。
func (p *shedPromise) Fail() {
	p.shedder.addFlying(-1)
}

// Allow implements Shedder.Allow.
func (nopShedder) Allow(ctx context.Context) (Promise, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nopPromise{}, nil
}

// Pass implements Promise.Pass（no-op）。
func (nopPromise) Pass() {}

// Fail implements Promise.Fail（no-op）。
func (nopPromise) Fail() {}

// WithShedderCPUThreshold 定制 CPU 过载阈值（千分毫核记法，900 即 90%）。
// 缺省 900（对齐上游）；milli <= 0 或 >= 1000 时 panic（过载因子分母
// cpuMax-threshold 必须为正）。
func WithShedderCPUThreshold(milli int64) ShedderOption {
	if milli <= 0 || milli >= cpuMax {
		panic("shedding: cpu threshold must be in (0, 1000) millicpu")
	}
	return func(opts *shedderOptions) {
		opts.cpuThreshold = milli
	}
}

// WithShedderWindow 定制统计窗口：window 总长切成 buckets 个桶（上游缺省
// 5s × 50 桶 = 单桶 100ms）。window <= 0、buckets < 1 或 window < buckets
// （单桶宽度将被截断为 0）时 panic。
func WithShedderWindow(window time.Duration, buckets int) ShedderOption {
	if window <= 0 {
		panic("shedding: window must be greater than 0")
	}
	if buckets < 1 {
		panic("shedding: buckets must be greater than 0")
	}
	if window < time.Duration(buckets) {
		panic("shedding: window must be no less than buckets")
	}
	return func(opts *shedderOptions) {
		opts.window = window
		opts.buckets = buckets
	}
}

// WithShedderClock 注入自定义时间源（缺省 time.Now；nil 视为缺省）。
// 冷却期计时与 Promise RT 统计共用同一时钟，测试注入假时钟即可完全确定地
// 驱动降载状态。
func WithShedderClock(clock func() time.Time) ShedderOption {
	return func(opts *shedderOptions) {
		if clock != nil {
			opts.clock = clock
		}
	}
}

// WithShedderRejectHandler 注册丢弃回调（缺省 no-op，本包不强制接 logx/metrics，
// 由上层显式接入）。reason 为上游同格式的诊断消息（"dropreq, cpu: …, maxPass:
// …, minRt: …, hot: …, flying: …, avgFlying: …"）。每次丢弃事件触发一次。
func WithShedderRejectHandler(handler func(reason string)) ShedderOption {
	return func(opts *shedderOptions) {
		if handler != nil {
			opts.onReject = handler
		}
	}
}

// —— CPU 源注入与缺省采样管线 ————————————————————————————————

var (
	// cpuSource 为注入的 CPU 使用率源（nil = 用缺省平台采样管线）。
	cpuSource atomic.Pointer[func() (int64, error)]
	// cpuUsageVal 为缺省管线维护的 EMA 平滑 CPU 使用率（毫秒‰，0~1000）。
	cpuUsageVal atomic.Int64
	// samplerOnce 保证缺省采样 goroutine 全进程至多一个、惰性启动。
	samplerOnce sync.Once
)

// SetCPUUsageFunc 注入 CPU 使用率源（仿 async.SetPanicHandler 的全局替换模式），
// 返回值单位为毫秒‰（0~1000，1000 即满载）。测试注入定值/序列 stub 即可完全
// 确定地驱动降载逻辑；也可替换为自定义平台采样（如容器 runtime API）。
// 传 nil 清除注入、恢复缺省平台采样。注入函数返回错误时按 usage=0 处理
// （fail-open，全放行）；注入后缺省采样管线（含其后台 goroutine）不再被调用。
func SetCPUUsageFunc(f func() (int64, error)) {
	if f == nil {
		cpuSource.Store(nil)
		return
	}
	cpuSource.Store(&f)
}

// currentCPUUsage 返回当前 CPU 使用率（毫秒‰）：优先注入源；否则走缺省管线
// （惰性启动后台采样 goroutine，读取其 EMA 平滑值，恒不返回错误——采样错误
// 在管线内部按 0 处理并随 EMA 衰减，对齐上游 RefreshCpu 错误返 0 的语义）。
func currentCPUUsage() (int64, error) {
	if fp := cpuSource.Load(); fp != nil && *fp != nil {
		return (*fp)()
	}
	samplerOnce.Do(startSampler)
	return cpuUsageVal.Load(), nil
}

// startSampler 启动缺省 CPU 采样循环：每 cpuRefreshInterval 采样一次并做
// beta=0.95 指数平滑。进程级单例（惰性启动，随进程存活——与上游 stat 包的
// init goroutine 同款取舍，量级为 1 goroutine + 1 ticker）；每周期独立
// recover 兜底，单次采样 panic 不终止采样循环。
//
// 刻意不走 async.GoSafe：GoSafe recover 一次后 goroutine 即终结，常驻循环
// 会因单次 panic 永久停摆（降载随之失效）；每 tick 独立 recover 才能自愈。
// 循环体之外只有 ticker 构造与 channel 收发（interval 为常量，不会 panic）。
func startSampler() {
	go func() {
		ticker := time.NewTicker(cpuRefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("shedding: cpu sampler panic recovered: %v\n%s", r, debug.Stack())
					}
				}()
				refreshCPUUsage(defaultRawCPUUsage)
			}()
		}
	}()
}

// refreshCPUUsage 执行一次采样 + EMA 更新，返回平滑后的新值。
// 采样错误视为 0：EMA 逐步衰减至 0 → 降载自动失效（fail-open）。
func refreshCPUUsage(read func() (int64, error)) int64 {
	cur, err := read()
	if err != nil {
		cur = 0
	}
	next := emaCPUUsage(cpuUsageVal.Load(), cur)
	cpuUsageVal.Store(next)
	return next
}

// emaCPUUsage 为纯函数版 EMA 单步：next = prev×beta + cur×(1-beta)。
// 独立成纯函数便于无全局状态的确定性单测。
func emaCPUUsage(prev, cur int64) int64 {
	return int64(float64(prev)*samplerBeta + float64(cur)*(1-samplerBeta))
}
