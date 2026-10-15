package breaker

import (
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/window"
)

const (
	// defaultWindow 为缺省统计窗口总长（10s，对齐上游 googleBreaker）。
	defaultWindow = time.Second * 10
	// defaultBuckets 为缺省桶数（40），单桶宽度 defaultWindow/defaultBuckets = 250ms。
	defaultBuckets = 40
	// defaultForcePass 为缺省强制探活间隔（1s）。
	defaultForcePass = time.Second
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

// googleBreaker 为 Google SRE 概率式自适应熔断（Client-Side Throttling），
// 无"关闭-打开-半开"状态机：按滑动窗口统计接受/失败/丢弃样本，
// 失败占比越高丢弃概率越大，熔断后每隔 forcePass 强制放行一次探活请求。
// 参见 https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	stat       *window.RollingWindow[int64, *bucket]
	clock      func() time.Time
	windowSize int
	forcePass  time.Duration
	// lastPass 记录最近一次概率放行/强制探活的时刻（clock 的 UnixNano）；
	// 0 表示尚无任何概率放行——此时强制探活不生效（与上游 syncx.AtomicDuration 零值语义一致）。
	lastPass atomic.Int64
}

// windowResult 为一次窗口聚合的结果。
type windowResult struct {
	accepts        int64
	total          int64
	failingBuckets int64
	workingBuckets int64
}

// newGoogleBreaker 按 cfg 构造自适应熔断核心；窗口与强制探活共用 cfg.clock。
func newGoogleBreaker(cfg breakerConfig) *googleBreaker {
	st := window.NewRollingWindow[int64, *bucket](
		func() *bucket { return new(bucket) },
		cfg.windowSize, cfg.windowInterval,
		window.WithWindowClock[int64, *bucket](cfg.clock),
	)
	return &googleBreaker{
		stat:       st,
		clock:      cfg.clock,
		windowSize: cfg.windowSize,
		forcePass:  cfg.forcePass,
	}
}

// accept 按 Google SRE 公式判定本次请求是否放行：
//
//	w = max(minK, k - (k-minK)×failingBuckets/windowSize)
//	dropRatio = (total - protection - w×accepts) / (total + 1)
//
// dropRatio <= 0 直接放行；否则熔断期内每隔 forcePass 强制放行一次探活；
// 其余按 dropRatio×(空闲桶占比) 概率丢弃。
func (b *googleBreaker) accept() error {
	history := b.history()
	w := k - (k-minK)*float64(history.failingBuckets)/float64(b.windowSize)
	if w < minK {
		w = minK
	}
	weightedAccepts := w * float64(history.accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	// for better performance, no need to care about the negative ratio
	dropRatio := (float64(history.total-protection) - weightedAccepts) / float64(history.total+1)
	if dropRatio <= 0 {
		return nil
	}

	now := b.clock().UnixNano()
	if lastPass := b.lastPass.Load(); lastPass > 0 && time.Duration(now-lastPass) > b.forcePass {
		b.lastPass.Store(now)
		return nil
	}

	dropRatio *= float64(int64(b.windowSize)-history.workingBuckets) / float64(b.windowSize)
	if trueOnProba(dropRatio) {
		return ErrCircuitOpen
	}

	b.lastPass.Store(now)
	return nil
}

// allow 判定放行并返回承诺；拒绝时记一次丢弃样本并返回 ErrCircuitOpen。
func (b *googleBreaker) allow() (googlePromise, error) {
	if err := b.accept(); err != nil {
		b.markDrop()
		return googlePromise{}, err
	}
	return googlePromise{b: b}, nil
}

// doReq 判定放行后执行 req 并按 acceptable 回填成败；
// rejected 标记本次是否被熔断拒绝（拒绝时 req 不会执行）。
// req panic 时记一次失败样本后原样 re-panic（success 仍为 false，defer 先于 panic 传播执行）。
func (b *googleBreaker) doReq(req func() error, acceptable func(error) bool) (err error, rejected bool) {
	if aerr := b.accept(); aerr != nil {
		b.markDrop()
		return aerr, true
	}

	var succ bool
	defer func() {
		// if req() panic, success is false, mark as failure
		if succ {
			b.markSuccess()
		} else {
			b.markFailure()
		}
	}()

	err = req()
	if acceptable(err) {
		succ = true
	}
	return err, false
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

// history 聚合窗口内全部有效桶；failingBuckets/workingBuckets 为从旧到新
// 扫描时"连续纯失败/纯成功桶"的尾部长度（遇相反样本或混合桶清零，空桶延续），
// 语义与上游 googlebreaker.go 保持一致。
func (b *googleBreaker) history() windowResult {
	var result windowResult

	b.stat.Reduce(func(bk *bucket) {
		result.accepts += bk.Success
		result.total += bk.Sum
		if bk.Failure > 0 {
			result.workingBuckets = 0
		} else if bk.Success > 0 {
			result.workingBuckets++
		}
		if bk.Success > 0 {
			result.failingBuckets = 0
		} else if bk.Failure > 0 {
			result.failingBuckets++
		}
	})

	return result
}

// googlePromise 为 allow 放行后返回的承诺：由调用方回填真实调用结果。
type googlePromise struct {
	b *googleBreaker
}

// Pass 告知熔断器本次调用成功。
func (p googlePromise) Pass() {
	p.b.markSuccess()
}

// Fail 告知熔断器本次调用失败。
func (p googlePromise) Fail() {
	p.b.markFailure()
}

// trueOnProba 以概率 proba 返回 true（上游 mathx.Proba.TrueOnProba 的就地实现，
// 不为一个函数引依赖）。math/rand/v2 顶层函数并发安全，无需上游的互斥锁包装。
func trueOnProba(proba float64) bool {
	return rand.Float64() < proba
}
