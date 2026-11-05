package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
)

// KeyedLimiterOf 是按 key 隔离的限流器管理器（key 类型可定制）。
// 每个 key 维护独立的限流器实例，支持空闲回收、容量护栏、速率热更。
type KeyedLimiterOf[K comparable] struct {
	mu          sync.RWMutex
	limiters    map[K]*limiterEntry
	factory     func(K) Limiter
	ttl         time.Duration // 空闲过期；<=0 用 defaultTTL
	maxEntries  int           // 0 = 不限
	retryAfter  time.Duration // 容量护栏拒绝时建议的 Retry-After
	evictFn     func(key K, idle time.Duration)
	done        chan struct{}
	closeOnce   sync.Once
	overCapHits atomic.Int64
	evictions   atomic.Int64
}

// KeyedLimiter 是 string key 的别名，兼容既有调用方。
type KeyedLimiter = KeyedLimiterOf[string]

// defaultTTL 是未提供或非法 ttl 时的兜底清理间隔。
const defaultTTL = time.Minute

// defaultOverCapRetryAfter 是容量护栏拒绝时建议的 Retry-After。
const defaultOverCapRetryAfter = time.Minute

// minCleanupInterval 是后台清扫 ticker 的间隔下限（panic 兜底档，对齐 pkg/cache
// 的 LocalCache.cleanup 口径）。
const minCleanupInterval = time.Second

// minTickInterval 是 ticker 的最小正间隔（1ms）——比 minCleanupInterval 更细一档：
// ttl 落在 ns 级时 ttl/2 会算出 1ns 这样的间隔，ticker 会以 GHz 频率空转烧核。
const minTickInterval = time.Millisecond

// cleanupInterval 返回后台清扫的 ticker 间隔（KeyedLimiterOf 与 LockoutOf 共用）。
//
// ttl/2 可能整除为 0（ttl=1ns 时 ttl/2=0），而 time.NewTicker(0) 直接 panic——
// panic 被 async.GoSafe 收编后进程不崩，但**后台清扫协程当场死亡**：
// 空闲条目永远不被回收（limiters / entries 两张 map 随 key 基数无界增长），
// 容量护栏 WithMaxEntries 也永远等不到回收腾位（表满后新 key 全被拒）。
//
// 下限兜底分两档：间隔整除为 0 ⇒ 1s（对齐 pkg/cache）；间隔为正但过小（ns 级）
// ⇒ 1ms（避免热循环烧核）。
//
// **ttl 语义不变**：过期/空闲判定仍按调用方给的 ttl 走，被兜底的只是清扫节奏。
func cleanupInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval <= 0 {
		return minCleanupInterval
	}
	if interval < minTickInterval {
		return minTickInterval
	}
	return interval
}

type limiterEntry struct {
	limiter  Limiter
	lastSeen atomic.Int64 // UnixNano，无锁更新
	rate     int64        // 速率元数据（WithRate 家族登记；见 ReplaceIfChanged）
	hasRate  bool         // rate=0 是合法元数据之外的「未登记」标记——不用 rate==0 判
}

// evictedEntry 是一次空闲回收的观测载荷（回收路径统一收集、锁外回调）。
type evictedEntry[K comparable] struct {
	key  K
	idle time.Duration
}

// KeyedOption 是 KeyedLimiterOf 的函数选项。
type KeyedOption[K comparable] func(*keyedConfig[K])

type keyedConfig[K comparable] struct {
	ttl        time.Duration
	maxEntries int
	retryAfter time.Duration
	evictFn    func(key K, idle time.Duration)
}

// WithIdleTTL 设置空闲回收周期；表满时也先触发回收再判满。
// <=0 使用默认 1 分钟。
//
// ⚠️ 不变量（#142）：**ttl 必须大于限流器的窗口长**。空闲回收回收的是整个
// Limiter 实例——下次该 key 再来时 factory 新建实例，**窗口计数清零**。对
// NewSlidingWindow 这类窗口语义实现，若 ttl ≤ window：key 每空闲 ttl 就被重建，
// 窗口计数永远攒不满 ⇒ **限流/预算闸静默失效**（不超任何请求、无任何报错——
// 失效方向是安全/预算面放松）。建议 ttl ≥ 2×窗口（一个窗口的回收余量）。
// TokenBucket 无固定窗口，不受此约束（余量清零即代价）。回收可观测面见
// WithEvictCallback / Evictions。
//
// 小于 2ns 的值不会让后台清扫 panic——清扫节奏有 1s 下限（见 cleanupInterval），
// 过期判定仍按本值走。
func WithIdleTTL[K comparable](ttl time.Duration) KeyedOption[K] {
	return func(c *keyedConfig[K]) { c.ttl = ttl }
}

// WithEvictCallback 注册空闲回收回调（#142）：key 空闲超 ttl 被回收时以
// (key, 空闲时长) 回调，让「计数清零」至少可观测（消费方可在回调里打日志/打点）。
// 回调在**库锁之外**同步执行（后台清扫与表满 sweep-on-pressure 两条路径统一），
// 回调内可安全重入本管理器的查询方法；回调自身 panic 经 async.RunSafeLabeled
// 收编（task=ratelimit.keyed.evict），不影响限流主路径。回调高频场景注意
// 同步执行的耗时——只做日志/计数这类轻活。
func WithEvictCallback[K comparable](fn func(key K, idle time.Duration)) KeyedOption[K] {
	return func(c *keyedConfig[K]) { c.evictFn = fn }
}

// WithMaxEntries 设置容量护栏上限（未登记 key 在表满时被拒绝，已登记 key 不受影响）。
// 0 = 不限（默认）。
func WithMaxEntries[K comparable](max int) KeyedOption[K] {
	return func(c *keyedConfig[K]) { c.maxEntries = max }
}

// WithOverCapRetryAfter 设置容量护栏拒绝时 AllowWithRetryAfter 返回的建议等待。
// <=0 使用默认 1 分钟。
func WithOverCapRetryAfter[K comparable](d time.Duration) KeyedOption[K] {
	return func(c *keyedConfig[K]) { c.retryAfter = d }
}

// NewKeyedLimiter 创建 string key 的限流器管理器（兼容旧 API）。
//
// 参数:
//   - factory: 创建新限流器的工厂函数
//   - ttl: 限流器空闲过期时间，超过此时间未使用的限流器会被清理
//
// 用法:
//
//	keyed := ratelimit.NewKeyedLimiter(func() ratelimit.Limiter {
//	    return ratelimit.NewTokenBucket(10, 20)
//	}, 10*time.Minute)
//
// ⚠️ ttl 必须大于限流器的窗口长（窗口语义实现空闲回收=计数清零，限流静默失效），
// 完整不变量与可观测面见 WithIdleTTL。
func NewKeyedLimiter(factory func() Limiter, ttl time.Duration) *KeyedLimiter {
	return NewKeyedLimiterOf[string](factory, WithIdleTTL[string](ttl))
}

// NewKeyedLimiterOf 创建按 key 隔离的限流器管理器（key 任意 comparable 类型），
// 所有 key 共用同一份限流配额。
//
// 需要**按 key 给不同配额**（不同接口不同档位）请用 NewKeyedLimiterByFunc（#31）。
//
// 用法:
//
//	keyed := ratelimit.NewKeyedLimiterOf[uint](
//	    func() ratelimit.Limiter { return ratelimit.NewTokenBucket(10, 20) },
//	    ratelimit.WithIdleTTL[uint](10*time.Minute),
//	    ratelimit.WithMaxEntries[uint](10_000),
//	)
func NewKeyedLimiterOf[K comparable](factory func() Limiter, opts ...KeyedOption[K]) *KeyedLimiterOf[K] {
	// 统一到按 key 的 factory（内部表示），旧 API 忽略 key 即共用配额
	return NewKeyedLimiterByFunc[K](func(K) Limiter { return factory() }, opts...)
}

// NewKeyedLimiterByFunc 创建按 key 隔离、且**配额由 key 决定**的限流器管理器（#31）。
//
// NewKeyedLimiterOf 的 factory 签名是 func() Limiter —— 不接受 key，因此一个管理器内
// 所有 key 只能共享同一份配额。业务上按接口维度分档位（同步类宽松 / 备份类严格 /
// 导入确认最严）是常见形态，此前只能把限流器实例的键从 (maxReq, window) 扩成
// (action, maxReq, window)，即每个 action 一个管理器 —— 那样按 key 隔离、空闲回收、
// 容量护栏、Len()/OverCapHits() 可观测出口这些价值基本被绕过，整体限流状态也没有
// 单一治理点。本构造函数是它的正解：一个管理器，按 key 定配额。
//
// 用法:
//
//	quota := map[string]struct {
//	    maxReq  int
//	    window  time.Duration
//	}{
//	    "backup/import/confirm": {2, 900 * time.Second},
//	    "car/syncAllTripData":   {3, 300 * time.Second},
//	}
//	keyed := ratelimit.NewKeyedLimiterByFunc[string](func(k string) ratelimit.Limiter {
//	    q := quota[k]
//	    return ratelimit.NewSlidingWindow(q.window, q.maxReq)
//	}, ratelimit.WithIdleTTL[string](10*time.Minute))
//	ok := keyed.Allow("car/syncAllTripData")   // 该 key 用自己那档配额
//
// ⚠️ 空闲回收不变量（#142）：factory 返回窗口语义实现（如上例 SlidingWindow）时，
// WithIdleTTL 的 ttl 必须**大于各 key 的窗口长**（建议 ≥ 2×窗口）——回收=实例重建=
// 窗口计数清零，ttl ≤ window 时限流/预算闸静默失效（无任何报错），完整说明见
// WithIdleTTL；多档位场景以最大窗口为准。
//
// ⚠️ 未在配额表里登记的 key 会拿到 factory 返回的值（若 factory 写了兜底默认值，
// 就是静默按兜底档限流）。这是 API 形状使然，不是库的 bug——但它曾导致一次迁移中
// 未登记的接口被按默认档限流。**配额表缺 key 时应让它显式失败**（factory 返回 nil
// 会让 Allow 返回 false，且 nil **不会被登记**为条目——见 Get 的说明），或在 Allow
// 处自行判空，避免静默降级。
func NewKeyedLimiterByFunc[K comparable](factory func(K) Limiter, opts ...KeyedOption[K]) *KeyedLimiterOf[K] {
	cfg := keyedConfig[K]{
		ttl:        defaultTTL,
		retryAfter: defaultOverCapRetryAfter,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.ttl <= 0 {
		cfg.ttl = defaultTTL
	}
	if cfg.retryAfter <= 0 {
		cfg.retryAfter = defaultOverCapRetryAfter
	}
	if cfg.maxEntries < 0 {
		cfg.maxEntries = 0
	}

	kl := &KeyedLimiterOf[K]{
		limiters:   make(map[K]*limiterEntry),
		factory:    factory,
		ttl:        cfg.ttl,
		maxEntries: cfg.maxEntries,
		retryAfter: cfg.retryAfter,
		evictFn:    cfg.evictFn,
		done:       make(chan struct{}),
	}
	async.GoSafe(kl.cleanup)
	return kl
}

// allowNew 判定是否允许登记新 key（容量护栏）。
// 表满时先回收空闲条目；仍满则拒绝未登记 key。
// 回收条目记录到 evicted（可 nil）供调用方锁外回调——本函数在持锁状态下运行，
// 回调不允许在锁内触发（消费方回调里重入本管理器是合理需求，#142）。
func (kl *KeyedLimiterOf[K]) allowNew(now time.Time, evicted *[]evictedEntry[K]) bool {
	if kl.maxEntries <= 0 || len(kl.limiters) < kl.maxEntries {
		return true
	}
	// 表满：触发一次空闲回收再判
	for key, entry := range kl.limiters {
		if now.Sub(time.Unix(0, entry.lastSeen.Load())) > kl.ttl {
			delete(kl.limiters, key)
			if evicted != nil {
				*evicted = append(*evicted, evictedEntry[K]{
					key:  key,
					idle: now.Sub(time.Unix(0, entry.lastSeen.Load())),
				})
			}
		}
	}
	return len(kl.limiters) < kl.maxEntries
}

// reportEvicts 记回收计数并在锁外逐个触发回调（#142）。回调 panic 按
// async.RunSafeLabeled 收编（task=ratelimit.keyed.evict），不影响主路径。
func (kl *KeyedLimiterOf[K]) reportEvicts(evicted []evictedEntry[K]) {
	if len(evicted) == 0 {
		return
	}
	kl.evictions.Add(int64(len(evicted)))
	if kl.evictFn == nil {
		return
	}
	for _, e := range evicted {
		async.RunSafeLabeled("ratelimit.keyed.evict", func() { kl.evictFn(e.key, e.idle) })
	}
}

// Get 获取指定 key 的限流器。以下两种情形返回 nil：
//   - 表满且 key 未登记（容量护栏拒绝，OverCapHits 计数）；
//   - factory 对该 key 返回 nil（#134）——此时**不登记**条目，对齐 Create/Replace
//     的拒 nil 契约：nil 若被注册成条目，会被后续 Get 的快路径持续刷新 lastSeen
//     ⇒ IdleTTL 永不回收（慢性泄漏），还会占住 WithMaxEntries 的名额把真 key
//     挤出护栏（OverCapHits 增长，症状与「限流命中」不可分辨）。
func (kl *KeyedLimiterOf[K]) Get(key K) Limiter {
	kl.mu.RLock()
	entry, ok := kl.limiters[key]
	kl.mu.RUnlock()
	if ok {
		entry.lastSeen.Store(time.Now().UnixNano())
		return entry.limiter
	}

	kl.mu.Lock()
	// 双重检查
	if cur, ok := kl.limiters[key]; ok {
		cur.lastSeen.Store(time.Now().UnixNano())
		kl.mu.Unlock()
		return cur.limiter
	}
	var evicted []evictedEntry[K]
	if !kl.allowNew(time.Now(), &evicted) {
		kl.overCapHits.Add(1)
		kl.mu.Unlock()
		kl.reportEvicts(evicted) // 拒绝路径的回收也要可观测
		return nil
	}

	limiter := kl.factory(key)
	if limiter == nil {
		kl.mu.Unlock()
		kl.reportEvicts(evicted)
		return nil
	}
	entry = &limiterEntry{limiter: limiter}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
	kl.mu.Unlock()
	kl.reportEvicts(evicted)
	return entry.limiter
}

// Allow 检查指定 key 是否允许请求。容量护栏拒绝未登记 key 时返回 false。
func (kl *KeyedLimiterOf[K]) Allow(key K) bool {
	l := kl.Get(key)
	if l == nil {
		return false
	}
	return l.Allow()
}

// AllowN 检查指定 key 是否允许 N 个请求。容量护栏拒绝未登记 key 时返回 false。
func (kl *KeyedLimiterOf[K]) AllowN(key K, n int) bool {
	l := kl.Get(key)
	if l == nil {
		return false
	}
	return l.AllowN(n)
}

// AllowWithRetryAfter 检查是否放行；拒绝时返回建议等待时长（不消耗令牌的探测）。
//
//   - ok=true：请求放行，wait=0
//   - ok=false：请求拒绝；wait 是下一个可用令牌的等待时长（Reserve→Delay→Cancel，
//     不消耗令牌），容量护栏拒绝时返回建议 Retry-After
//
// 探测语义：被拒请求不会自我消耗配额，避免放大限流（429 + Retry-After 场景）。
func (kl *KeyedLimiterOf[K]) AllowWithRetryAfter(key K) (wait time.Duration, ok bool) {
	l := kl.Get(key)
	if l == nil {
		return kl.retryAfter, false
	}
	if l.Allow() {
		return 0, true
	}
	return peekRetryAfter(l), false
}

// Replace 将指定 key 的限流器替换为新实例（速率热更）。
// 换桶即新配额，桶内余量归零（调用方接受）。key 不存在时等价登记新桶。
//
// Replace 不登记速率元数据——走 Replace 后，AllowWithRate / ReplaceIfChanged
// 下次调用会因「未登记元数据」重建一次。速率热更口径请统一走 WithRate 家族
// （ReplaceIfChanged / AllowWithRate，#141），元数据才是事实单源。
func (kl *KeyedLimiterOf[K]) Replace(key K, l Limiter) {
	if l == nil {
		return
	}
	kl.mu.Lock()
	defer kl.mu.Unlock()
	entry := &limiterEntry{limiter: l}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
}

// ReplaceIfChanged 带速率元数据的热更登记（#141）：以库内登记的元数据为事实
// 来源比对——key 未登记、或登记的元数据 ≠ rate 时换桶（并登记新元数据）返回
// true；元数据一致时**不动桶**返回 false（余量保留、零分配）。
//
// 这把「速率是否变更」的判断从消费方的影子表收回库侧：消费方每次把当前配置
// 速率传进来即可，无需自维护 rates 镜像（影子表与库内真实状态是两份事实，
// Replace 之外的任何路径都会让影子表撒谎且无信号）。
//
// 语义细节：
//   - key 未登记：护栏内登记（同 Create——表满先回收空闲条目，仍满返回 false 并计
//     一次 OverCapHits）；
//   - key 已登记但此前走 Create/Replace（无元数据）：视为变更，换桶并登记元数据；
//   - rate <= 0：panic（对齐 NewSlidingWindow「配置错误启动即暴露」——负号漏
//     不能静默变成恒拒桶）；l 为 nil：返回 false（对齐 Create/Replace 拒 nil）。
//
// 惰性构造（只在真正需要换桶时才建新桶）见 AllowWithRate。
func (kl *KeyedLimiterOf[K]) ReplaceIfChanged(key K, rate int64, l Limiter) bool {
	if rate <= 0 {
		panic(fmt.Sprintf("ratelimit: ReplaceIfChanged rate must be > 0, got %d", rate))
	}
	if l == nil {
		return false
	}
	kl.mu.Lock()
	if entry, ok := kl.limiters[key]; ok && entry.hasRate && entry.rate == rate {
		entry.lastSeen.Store(time.Now().UnixNano())
		kl.mu.Unlock()
		return false
	}
	var evicted []evictedEntry[K]
	if _, exists := kl.limiters[key]; !exists {
		if !kl.allowNew(time.Now(), &evicted) {
			kl.overCapHits.Add(1)
			kl.mu.Unlock()
			kl.reportEvicts(evicted)
			return false
		}
	}
	entry := &limiterEntry{limiter: l, rate: rate, hasRate: true}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
	kl.mu.Unlock()
	kl.reportEvicts(evicted)
	return true
}

// AllowWithRate 带速率热更的 Allow（#141，热更消费方的正解入口）：每次调用传入
// 当前配置速率 rate，库内比对 key 登记的元数据——一致直接 Allow（零分配、余量
// 保留）；不一致或未登记才调 newBucket 构造新桶并原子登记（元数据随之更新）。
//
// 典型形态（Nacos/DB 热更下沉到库内，消费方无需影子表也不需预建桶）:
//
//	ok := kl.AllowWithRate(key, perMin, func() ratelimit.Limiter {
//	    return ratelimit.NewSlidingWindow(window, perMin)
//	})
//
// 语义细节：
//   - newBucket 只在真正需要（重）建桶时调用，且在**库锁之外**执行（对齐 #33 的
//     锁序结论：消费方闭包里读自持锁的速率状态不会倒置）；并发下两方同时过慢路径
//     可能多建一次桶，落锁比对后只有一方真正安装——多余的桶被丢弃，无正确性影响；
//   - newBucket 返回 nil：返回 false 不登记（对齐 Get 拒 nil 契约——nil 条目会
//     占护栏名额且永不被空闲回收）；
//   - key 未登记且表满：护栏拒绝（计一次 OverCapHits）返回 false；
//   - rate <= 0 或 newBucket 为 nil：panic（配置错误启动即暴露）。
func (kl *KeyedLimiterOf[K]) AllowWithRate(key K, rate int64, newBucket func() Limiter) bool {
	if rate <= 0 {
		panic(fmt.Sprintf("ratelimit: AllowWithRate rate must be > 0, got %d", rate))
	}
	if newBucket == nil {
		panic("ratelimit: AllowWithRate newBucket must not be nil")
	}

	// 快路径：元数据一致直接用现桶
	kl.mu.RLock()
	entry, ok := kl.limiters[key]
	if ok && entry.hasRate && entry.rate == rate {
		entry.lastSeen.Store(time.Now().UnixNano())
		l := entry.limiter
		kl.mu.RUnlock()
		return l.Allow()
	}
	kl.mu.RUnlock()

	// 慢路径：锁外建桶，落锁比对安装（竞态方以库内元数据为准）
	l := newBucket()
	if l == nil {
		return false
	}
	kl.mu.Lock()
	if cur, ok := kl.limiters[key]; ok && cur.hasRate && cur.rate == rate {
		cur.lastSeen.Store(time.Now().UnixNano())
		kl.mu.Unlock()
		return cur.limiter.Allow() // 竞态方已装好同速率桶，白建的那只丢弃
	}
	var evicted []evictedEntry[K]
	if _, exists := kl.limiters[key]; !exists {
		if !kl.allowNew(time.Now(), &evicted) {
			kl.overCapHits.Add(1)
			kl.mu.Unlock()
			kl.reportEvicts(evicted)
			return false
		}
	}
	entry = &limiterEntry{limiter: l, rate: rate, hasRate: true}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
	kl.mu.Unlock()
	kl.reportEvicts(evicted)
	return l.Allow()
}

// CurrentRate 返回 key 当前登记的速率元数据（#141 的元数据出口）：
// ok=false 表示 key 不存在或登记时未带元数据（factory/Create/Replace 路径）。
// 用于消费方观测/对账「这个 key 现在生效的是哪一档」——事实以库内登记为准。
func (kl *KeyedLimiterOf[K]) CurrentRate(key K) (rate int64, ok bool) {
	kl.mu.RLock()
	defer kl.mu.RUnlock()
	entry, exists := kl.limiters[key]
	if !exists || !entry.hasRate {
		return 0, false
	}
	return entry.rate, true
}

// Create 在容量护栏内原子登记新 key 的限流器（[#33](https://github.com/oekit/concurrency/issues/33)）：
// 表满先回收空闲条目（与 Allow 建桶路径同一 sweep-on-pressure），仍满返回 false 并计一次
// OverCapHits。已登记的 key 返回 false 且**不覆盖**——速率热更请走 Replace（换桶不该被
// 护栏卡住）。l 为 nil 返回 false。
//
// 与 NewKeyedLimiterByFunc 互补：ByFunc 的 factory 在库内锁内被调用，消费方若在
// factory 里读自持锁的速率状态会形成「库锁→消费方锁」与正常路径「消费方锁→库锁」的
// 锁序倒置；「每次调用传入当前配置速率」的消费方（Nacos/DB 热更即时生效）应外部构造
// 桶后用 Create 登记 / Replace 热更，配额回收与护栏都留在库内。
func (kl *KeyedLimiterOf[K]) Create(key K, l Limiter) bool {
	if l == nil {
		return false
	}
	kl.mu.Lock()
	if _, exists := kl.limiters[key]; exists {
		kl.mu.Unlock()
		return false
	}
	var evicted []evictedEntry[K]
	if !kl.allowNew(time.Now(), &evicted) {
		kl.overCapHits.Add(1)
		kl.mu.Unlock()
		kl.reportEvicts(evicted)
		return false
	}
	entry := &limiterEntry{limiter: l}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
	kl.mu.Unlock()
	kl.reportEvicts(evicted)
	return true
}

// Delete 删除指定 key 的限流器。
func (kl *KeyedLimiterOf[K]) Delete(key K) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	delete(kl.limiters, key)
}

// Len 返回当前活跃的限流器数量。
func (kl *KeyedLimiterOf[K]) Len() int {
	kl.mu.RLock()
	defer kl.mu.RUnlock()
	return len(kl.limiters)
}

// OverCapHits 返回容量护栏拒绝未登记 key 的累计次数（可观测性）。
func (kl *KeyedLimiterOf[K]) OverCapHits() int64 {
	return kl.overCapHits.Load()
}

// Evictions 返回空闲回收累计条目数（#142 可观测面）：后台清扫与表满
// sweep-on-pressure 两条路径都计数。窗口语义实现配错 ttl（≤ 窗口）时，
// 该计数持续增长而「限流命中为零」正是静默失效的特征形态——见 WithIdleTTL。
func (kl *KeyedLimiterOf[K]) Evictions() int64 {
	return kl.evictions.Load()
}

// Close 关闭限流器管理器，停止清理 goroutine。可安全重复调用。
func (kl *KeyedLimiterOf[K]) Close() {
	kl.closeOnce.Do(func() {
		close(kl.done)
	})
}

// cleanup 定期清理过期的限流器。
func (kl *KeyedLimiterOf[K]) cleanup() {
	ticker := time.NewTicker(cleanupInterval(kl.ttl))
	defer ticker.Stop()

	for {
		select {
		case <-kl.done:
			return
		case now := <-ticker.C:
			var evicted []evictedEntry[K]
			kl.mu.Lock()
			for key, entry := range kl.limiters {
				if now.Sub(time.Unix(0, entry.lastSeen.Load())) > kl.ttl {
					delete(kl.limiters, key)
					evicted = append(evicted, evictedEntry[K]{
						key:  key,
						idle: now.Sub(time.Unix(0, entry.lastSeen.Load())),
					})
				}
			}
			kl.mu.Unlock()
			kl.reportEvicts(evicted) // 锁外回调（#142）
		}
	}
}

// retryProber 支持「预留→读延迟→归还」的限流器可实现此接口，
// 供 AllowWithRetryAfter 做不消耗令牌的探测。
type retryProber interface {
	// RetryAfter 探测下一个可用令牌的等待时长，不消耗令牌。
	RetryAfter() (time.Duration, bool)
}

// peekRetryAfter 尽力探测等待时长；不支持探测的 Limiter 返回 0。
func peekRetryAfter(l Limiter) time.Duration {
	if p, ok := l.(retryProber); ok {
		d, _ := p.RetryAfter()
		return d
	}
	// 退回 Reserve 语义会消耗令牌——只在无 prober 时用 Reserve+Cancel 等价探测
	if tb, ok := l.(*TokenBucket); ok {
		d, _ := tb.RetryAfter()
		return d
	}
	return 0
}
