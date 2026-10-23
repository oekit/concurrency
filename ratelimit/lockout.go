package ratelimit

import (
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// LockoutConfig 是 Lockout 的构造配置。
type LockoutConfig struct {
	// Window 为失败计数窗口：窗口内的失败事件按 key 累计（滑动窗口，
	// 半开区间 (now-Window, now]，窗口外样本自然老化）。
	Window time.Duration
	// Threshold 为锁定阈值：窗口内第 N 次失败触发锁定。
	Threshold int
	// LockDuration 为锁定时长：触发后 key 进入拒绝态持续 L 秒，期间 CheckLocked
	// 返回剩余时长（供对外 429 文案）；到期自动解除。后续失败不延长锁定。
	LockDuration time.Duration
	// MaxEntries 为容量护栏上限（0 = 不限，对齐 KeyedLimiterOf.WithMaxEntries）。
	// 表满且 key 未登记时失败样本被丢弃（fail-open + OverCapHits 观测）——
	// 不因内部容量问题拒绝认证尝试（那会放大成对合法用户的拒绝服务）。
	MaxEntries int
	// IdleTTL 为空闲回收周期；<=0 用默认 1 分钟。锁定中的条目免于回收
	// （提前驱逐等于提前解锁）；窗口内仍有有效失败样本的条目同样免于回收
	// （#83：空闲回收线不得短于失败窗口，否则滑窗语义被截短，阈值可被
	// 「间隔 > IdleTTL」的分布式攻击绕过）；回收不改变锁定到期语义。
	IdleTTL time.Duration
	// Now 为时钟注入（nil = time.Now；与 budget.WithClock 同款，
	// 假时钟测试完全确定）。清扫节奏仍走真实 ticker，到期/空闲判定一律用本时钟。
	Now func() time.Time
}

// lockoutEntry 是单个 key 的锁定状态（全部字段由 LockoutOf.mu 串行化）。
type lockoutEntry struct {
	fails       []time.Time // 窗口内失败样本，时间递增，长度封顶 Threshold
	lockedUntil time.Time   // 零值 = 未锁定；到期惰性判定，不主动清零
	lastSeen    time.Time   // 空闲回收计时
}

// LockoutOf 是按 key 隔离的防爆破锁（fail2ban 形态）：窗口内失败事件达阈值
// → key 进入锁定拒绝态持续 LockDuration（期间即使凭证正确也应拒绝）→ 成功操作
// Reset 清零。与家族原语的边界：KeyedLimiter 是速率型令牌桶（表达不了
// 「计数达标后进入持续拒绝态」）；breaker 是调用方视角的实例级熔断（无 key 隔离）；
// budget 是超限记账存档（无拒绝态与 Reset）。
//
// 语义细则：
//   - 失败样本滑动窗口计数，窗口外自然老化——锁定到期后若旧失败仍在窗口内，
//     首个新失败即重新锁定（重复违规者语义，对齐 fail2ban findtime 行为）；
//   - 锁定期间新失败只入窗口记账（封顶 Threshold 个），不延长当前锁定；
//   - 容量护栏 fail-open：表满且 key 未登记时丢弃失败样本并计入 OverCapHits；
//   - 锁定中的条目免于空闲回收（提前驱逐等于提前解锁）；窗口内仍有有效失败
//     样本的条目同样免于回收——空闲回收只清理「已无窗口内样本」的条目，
//     否则窗口语义会被 IdleTTL 截短（#83）。
//
// 典型用法（登录防爆破）：
//
//	lo := ratelimit.NewLockout(ratelimit.LockoutConfig{
//	    Window: 10 * time.Minute, Threshold: 5, LockDuration: 15 * time.Minute,
//	    MaxEntries: 10_000,
//	})
//	if locked, remain := lo.CheckLocked("user:42"); locked {
//	    return &LockedError{RemainingSeconds: int(remain.Seconds())} // 对外 429
//	}
//	if err := verifyCredential(); err != nil {
//	    if locked, _ := lo.RecordFailure("user:42"); locked { ... }
//	    return err
//	}
//	lo.Reset("user:42") // 登录成功
type LockoutOf[K comparable] struct {
	mu         sync.Mutex
	entries    map[K]*lockoutEntry
	window     time.Duration
	threshold  int
	lockDur    time.Duration
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
	overCap    int64 // 容量护栏丢弃失败样本的累计次数
	done       chan struct{}
	closeOnce  sync.Once
}

// Lockout 是 string key 的别名，兼容惯例（对齐 KeyedLimiter）。
type Lockout = LockoutOf[string]

// NewLockout 创建 string key 的防爆破锁。
// Window/Threshold/LockDuration 为必填语义参数，非法值直接 panic
// （配置错误宁可启动即暴露——锁静默失效等于没有防护）。
func NewLockout(cfg LockoutConfig) *Lockout {
	return NewLockoutOf[string](cfg)
}

// NewLockoutOf 创建任意 comparable key 的防爆破锁（IP、用户名、API Key 等）。
func NewLockoutOf[K comparable](cfg LockoutConfig) *LockoutOf[K] {
	if cfg.Window <= 0 {
		panic("ratelimit: lockout window must be greater than 0")
	}
	if cfg.Threshold < 1 {
		panic("ratelimit: lockout threshold must be greater than 0")
	}
	if cfg.LockDuration <= 0 {
		panic("ratelimit: lockout duration must be greater than 0")
	}
	if cfg.MaxEntries < 0 {
		cfg.MaxEntries = 0
	}
	ttl := cfg.IdleTTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	l := &LockoutOf[K]{
		entries:    make(map[K]*lockoutEntry),
		window:     cfg.Window,
		threshold:  cfg.Threshold,
		lockDur:    cfg.LockDuration,
		maxEntries: cfg.MaxEntries,
		ttl:        ttl,
		now:        now,
		done:       make(chan struct{}),
	}
	async.GoSafe(l.cleanup)
	return l
}

// RecordFailure 记一次失败事件。
//   - 触发锁定或 key 已处于锁定态：返回 (true, 剩余锁定时长)；
//   - 未触发：返回 (false, 0)；
//   - 容量护栏拒绝（表满且 key 未登记）：失败样本被丢弃，返回 (false, 0)，
//     可经 OverCapHits 观测。
//
// 锁定期间调用只入窗口记账，不延长当前锁定。
func (l *LockoutOf[K]) RecordFailure(key K) (locked bool, lockRemain time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	e, ok := l.entries[key]
	if !ok {
		if !l.allowNew(now) {
			l.overCap++
			return false, 0
		}
		e = &lockoutEntry{lastSeen: now}
		l.entries[key] = e
	}
	e.lastSeen = now
	l.prune(e, now)

	if remain := lockedRemain(e, now); remain > 0 {
		// 已锁定：样本封顶记账（保持窗口内最新 Threshold 个），锁定不延长
		if len(e.fails) < l.threshold {
			e.fails = append(e.fails, now)
		}
		return true, remain
	}
	e.fails = append(e.fails, now)
	if len(e.fails) >= l.threshold {
		e.lockedUntil = now.Add(l.lockDur)
		return true, l.lockDur
	}
	return false, 0
}

// CheckLocked 查询 key 是否处于锁定拒绝态：locked=true 时 lockRemain 为剩余
// 锁定时长（供对外 429 文案），未锁定或未登记返回 (false, 0)。查询刷新空闲计时。
// 返回序与 RecordFailure 统一为 (locked, lockRemain)——两个方法同序是刻意设计，
// 避免调用方在相邻调用间交换返回值顺序。
func (l *LockoutOf[K]) CheckLocked(key K) (locked bool, lockRemain time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	e, ok := l.entries[key]
	if !ok {
		return false, 0
	}
	e.lastSeen = now
	if remain := lockedRemain(e, now); remain > 0 {
		return true, remain
	}
	return false, 0
}

// Reset 清零 key 的失败计数并解除现存锁定（成功操作后调用；亦可用作管理员解封）。
// key 未登记为 no-op。
func (l *LockoutOf[K]) Reset(key K) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, key)
}

// Len 返回当前活跃条目数（含锁定中）。
func (l *LockoutOf[K]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// OverCapHits 返回容量护栏丢弃失败样本的累计次数（可观测性）。
func (l *LockoutOf[K]) OverCapHits() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.overCap
}

// Close 停止后台清扫 goroutine。可安全重复调用（KeyedLimiterOf 同款幂等语义）。
func (l *LockoutOf[K]) Close() {
	l.closeOnce.Do(func() {
		close(l.done)
	})
}

// cleanup 定期回收空闲条目；ticker 只定节奏，到期/空闲判定一律用注入时钟
// （假时钟下真实 ticker 的时间戳不可比）。锁定中的条目免于回收，
// 窗口内仍有有效失败样本的条目同样免于回收（#83：空闲回收线不得短于失败窗口）。
func (l *LockoutOf[K]) cleanup() {
	ticker := time.NewTicker(l.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			now := l.now()
			l.mu.Lock()
			for key, e := range l.entries {
				if l.reclaimable(e, now) {
					delete(l.entries, key)
				}
			}
			l.mu.Unlock()
		}
	}
}

// allowNew 判定容量护栏：表满时先回收空闲且未锁定的条目再判（锁定条目不参与回收
// ——提前驱逐等于提前解锁）。仍满则拒绝登记新 key。
func (l *LockoutOf[K]) allowNew(now time.Time) bool {
	if l.maxEntries <= 0 || len(l.entries) < l.maxEntries {
		return true
	}
	for key, e := range l.entries {
		if l.reclaimable(e, now) {
			delete(l.entries, key)
		}
	}
	return len(l.entries) < l.maxEntries
}

// reclaimable 判定条目是否可被空闲回收。三条约束：
//  1. 锁定中的条目免于回收——提前驱逐等于提前解锁；
//  2. **窗口内仍有有效失败样本的条目免于回收**（#83）——空闲回收线若短于失败
//     窗口，会把「窗口内失败事件按 key 累计」的滑窗语义截短成「空闲不超过
//     IdleTTL 的窗口」：攻击者只要把每次失败间隔拉到 IdleTTL 之上，失败计数
//     永远归零，防爆破阈值形同虚设；
//  3. 回收还要求空闲确实超 IdleTTL——IdleTTL 保留「样本全部老化后仍留一段
//     宽限期」的原语义（纯内存回收，不改变锁定到期语义）。
func (l *LockoutOf[K]) reclaimable(e *lockoutEntry, now time.Time) bool {
	if lockedRemain(e, now) > 0 {
		return false
	}
	if l.hasWindowSample(e, now) {
		return false
	}
	return now.Sub(e.lastSeen) > l.ttl
}

// hasWindowSample 判定条目是否仍有落在 (now-Window, now] 内的失败样本。
// 样本时间递增（prune 保序），故只看最后一个即可。
func (l *LockoutOf[K]) hasWindowSample(e *lockoutEntry, now time.Time) bool {
	if len(e.fails) == 0 {
		return false
	}
	cut := now.Add(-l.window)
	return e.fails[len(e.fails)-1].After(cut)
}

// prune 淘汰窗口外的失败样本（滑动窗口；样本时间递增，就地压缩保序）。
func (l *LockoutOf[K]) prune(e *lockoutEntry, now time.Time) {
	cut := now.Add(-l.window)
	keep := e.fails[:0]
	for _, t := range e.fails {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	e.fails = keep
}

// lockedRemain 返回剩余锁定时长；未锁定或已到期返回 0（到期惰性解除）。
func lockedRemain(e *lockoutEntry, now time.Time) time.Duration {
	if e.lockedUntil.IsZero() {
		return 0
	}
	if remain := e.lockedUntil.Sub(now); remain > 0 {
		return remain
	}
	return 0
}
