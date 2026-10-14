package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/yi-nology/ekit-concurrency/async"
)

// KeyedLimiter 是按 key 隔离的限流器管理器。
// 每个 key 维护独立的限流器实例，支持自动清理过期 key。
type KeyedLimiter struct {
	mu        sync.RWMutex
	limiters  map[string]*limiterEntry
	factory   func() Limiter
	ttl       time.Duration
	done      chan struct{}
	closeOnce sync.Once
}

// defaultTTL 是未提供或非法 ttl 时的兜底清理间隔。
const defaultTTL = time.Minute

type limiterEntry struct {
	limiter  Limiter
	lastSeen atomic.Int64 // UnixNano，无锁更新
}

// NewKeyedLimiter 创建按 key 隔离的限流器管理器。
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
//	if keyed.Allow("user:123") {
//	    // 处理请求
//	}
func NewKeyedLimiter(factory func() Limiter, ttl time.Duration) *KeyedLimiter {
	kl := &KeyedLimiter{
		limiters: make(map[string]*limiterEntry),
		factory:  factory,
		ttl:      ttl,
		done:     make(chan struct{}),
	}
	// ttl 非法会让 cleanup goroutine 在 NewTicker(ttl/2) 处 panic
	// （被 GoSafe 吞掉后清理永久失效），构造期直接兜底
	if kl.ttl <= 0 {
		kl.ttl = defaultTTL
	}

	// 启动清理 goroutine（带 panic recovery）
	async.GoSafe(kl.cleanup)

	return kl
}

// Get 获取指定 key 的限流器。
func (kl *KeyedLimiter) Get(key string) Limiter {
	kl.mu.RLock()
	entry, ok := kl.limiters[key]
	kl.mu.RUnlock()

	if ok {
		entry.lastSeen.Store(time.Now().UnixNano())
		return entry.limiter
	}

	// 创建新的限流器
	kl.mu.Lock()
	defer kl.mu.Unlock()

	// 双重检查
	if cur, ok := kl.limiters[key]; ok {
		cur.lastSeen.Store(time.Now().UnixNano())
		return cur.limiter
	}

	limiter := kl.factory()
	entry = &limiterEntry{limiter: limiter}
	entry.lastSeen.Store(time.Now().UnixNano())
	kl.limiters[key] = entry

	return limiter
}

// Allow 检查指定 key 是否允许请求。
func (kl *KeyedLimiter) Allow(key string) bool {
	return kl.Get(key).Allow()
}

// AllowN 检查指定 key 是否允许 N 个请求。
func (kl *KeyedLimiter) AllowN(key string, n int) bool {
	return kl.Get(key).AllowN(n)
}

// Delete 删除指定 key 的限流器。
func (kl *KeyedLimiter) Delete(key string) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	delete(kl.limiters, key)
}

// Len 返回当前活跃的限流器数量。
func (kl *KeyedLimiter) Len() int {
	kl.mu.RLock()
	defer kl.mu.RUnlock()
	return len(kl.limiters)
}

// Close 关闭限流器管理器，停止清理 goroutine。可安全重复调用。
func (kl *KeyedLimiter) Close() {
	kl.closeOnce.Do(func() {
		close(kl.done)
	})
}

// cleanup 定期清理过期的限流器。
func (kl *KeyedLimiter) cleanup() {
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
