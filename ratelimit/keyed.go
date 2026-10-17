package ratelimit

import (
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
	factory     func() Limiter
	ttl         time.Duration // 空闲过期；<=0 用 defaultTTL
	maxEntries  int           // 0 = 不限
	retryAfter  time.Duration // 容量护栏拒绝时建议的 Retry-After
	done        chan struct{}
	closeOnce   sync.Once
	overCapHits atomic.Int64
}

// KeyedLimiter 是 string key 的别名，兼容既有调用方。
type KeyedLimiter = KeyedLimiterOf[string]

// defaultTTL 是未提供或非法 ttl 时的兜底清理间隔。
const defaultTTL = time.Minute

// defaultOverCapRetryAfter 是容量护栏拒绝时建议的 Retry-After。
const defaultOverCapRetryAfter = time.Minute

type limiterEntry struct {
	limiter  Limiter
	lastSeen atomic.Int64 // UnixNano，无锁更新
}

// KeyedOption 是 KeyedLimiterOf 的函数选项。
type KeyedOption[K comparable] func(*keyedConfig[K])

type keyedConfig[K comparable] struct {
	ttl        time.Duration
	maxEntries int
	retryAfter time.Duration
}

// WithIdleTTL 设置空闲回收周期；表满时也先触发回收再判满。
// <=0 使用默认 1 分钟。
func WithIdleTTL[K comparable](ttl time.Duration) KeyedOption[K] {
	return func(c *keyedConfig[K]) { c.ttl = ttl }
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
func NewKeyedLimiter(factory func() Limiter, ttl time.Duration) *KeyedLimiter {
	return NewKeyedLimiterOf[string](factory, WithIdleTTL[string](ttl))
}

// NewKeyedLimiterOf 创建按 key 隔离的限流器管理器（key 任意 comparable 类型）。
//
// 用法:
//
//	keyed := ratelimit.NewKeyedLimiterOf[uint](
//	    func() ratelimit.Limiter { return ratelimit.NewTokenBucket(10, 20) },
//	    ratelimit.WithIdleTTL[uint](10*time.Minute),
//	    ratelimit.WithMaxEntries[uint](10_000),
//	)
func NewKeyedLimiterOf[K comparable](factory func() Limiter, opts ...KeyedOption[K]) *KeyedLimiterOf[K] {
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
		done:       make(chan struct{}),
	}
	async.GoSafe(kl.cleanup)
	return kl
}

// allowNew 判定是否允许登记新 key（容量护栏）。
// 表满时先回收空闲条目；仍满则拒绝未登记 key。
func (kl *KeyedLimiterOf[K]) allowNew(now time.Time) bool {
	if kl.maxEntries <= 0 || len(kl.limiters) < kl.maxEntries {
		return true
	}
	// 表满：触发一次空闲回收再判
	for key, entry := range kl.limiters {
		if now.Sub(time.Unix(0, entry.lastSeen.Load())) > kl.ttl {
			delete(kl.limiters, key)
		}
	}
	return len(kl.limiters) < kl.maxEntries
}

// Get 获取指定 key 的限流器。表满且 key 未登记时返回 nil。
func (kl *KeyedLimiterOf[K]) Get(key K) Limiter {
	kl.mu.RLock()
	entry, ok := kl.limiters[key]
	kl.mu.RUnlock()
	if ok {
		entry.lastSeen.Store(time.Now().UnixNano())
		return entry.limiter
	}

	kl.mu.Lock()
	defer kl.mu.Unlock()

	// 双重检查
	if cur, ok := kl.limiters[key]; ok {
		cur.lastSeen.Store(time.Now().UnixNano())
		return cur.limiter
	}
	if !kl.allowNew(time.Now()) {
		kl.overCapHits.Add(1)
		return nil
	}

	limiter := kl.factory()
	entry = &limiterEntry{limiter: limiter}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry
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

// Close 关闭限流器管理器，停止清理 goroutine。可安全重复调用。
func (kl *KeyedLimiterOf[K]) Close() {
	kl.closeOnce.Do(func() {
		close(kl.done)
	})
}

// cleanup 定期清理过期的限流器。
func (kl *KeyedLimiterOf[K]) cleanup() {
	ticker := time.NewTicker(kl.ttl / 2)
	defer ticker.Stop()

	for {
		select {
		case <-kl.done:
			return
		case now := <-ticker.C:
			kl.mu.Lock()
			for key, entry := range kl.limiters {
				if now.Sub(time.Unix(0, entry.lastSeen.Load())) > kl.ttl {
					delete(kl.limiters, key)
				}
			}
			kl.mu.Unlock()
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
