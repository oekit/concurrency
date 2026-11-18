package ratelimit

import (
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// —— keyedTTLRegistry：键控 TTL 注册表骨架单源（#363）——
//
// KeyedLimiterOf（keyed.go）与 LockoutOf（lockout.go）各自手写了一份
// 「键控 map + TTL 回收 + 容量护栏 + Close(done)」骨架：后台清扫循环
// （ticker 只定节奏、过期判定走注入时钟）、表满 sweep-on-pressure、
// closeOnce 幂等 Close——三段代码逐行平行，口径靠人肉同步（#84、#142、
// #361 的修复都曾多处齐改）。本类型把骨架收成一份泛型实现，条目语义回调化：
//
//   - expired func(E, now)：条目过期判定。KeyedLimiter = 空闲超 ttl；
//     Lockout = 锁定中免回收 + 窗口内仍有样本免回收 + 空闲超 ttl（#83）——
//     两侧护栏/清扫的判定差异全部吸收进本回调；
//   - idleOf func(E, now)：回收 idle 计算（仅 onEvict 非 nil 时在锁内采集）；
//   - onEvict func([]keyedEviction)：回收观测回调，锁外同步执行——消费方
//     回调里重入 owner 是合理需求（#142），panic 由 owner 侧收编
//     （async.RunSafeLabeled）；nil = 无观测面（LockoutOf），sweep 纯删除
//     零分配。
//
// 容量护栏（allowNewLocked）两侧语义同形：未设上限或未满直接放行；表满先
// sweep-on-pressure 回收过期条目再判满，回收载荷交调用方锁外回调。
//
// 跨 module 同族：timertask/cron LocalMutex（leases map + TTL 惰性全扫）是
// 同一骨架的第三份手写副本——timertask 独立 module，不在本次收编范围，
// 跨 module 联动另行评估（#363 边界声明）。

// keyedEviction 是一次空闲回收的观测载荷（回收路径统一收集、锁外回调）。
type keyedEviction[K comparable] struct {
	key  K
	idle time.Duration
}

// keyedTTLConfig 是 keyedTTLRegistry 的构造配置（回调与护栏参数）。
type keyedTTLConfig[K comparable, E any] struct {
	// ttl 为空闲过期；后台清扫 ticker 间隔按 cleanupInterval(ttl) 兜底。
	ttl time.Duration
	// maxEntries 为容量护栏上限；0 = 不限。
	maxEntries int
	// now 为时钟注入（nil 回退 time.Now）：清扫节奏仍走真实 ticker，
	// 过期判定与 idle 计算一律用本时钟（#361 口径——假时钟下真实 ticker
	// 的时间戳不可比）。
	now func() time.Time
	// expired 判定条目是否过期（sweep 与护栏共用同一判定，单源）。
	expired func(e E, now time.Time) bool
	// idleOf 计算回收 idle（仅 onEvict 非 nil 时被调用，锁内采集）。
	idleOf func(e E, now time.Time) time.Duration
	// onEvict 为回收观测回调（锁外执行；nil = 无观测面）。
	onEvict func(evicted []keyedEviction[K])
}

// keyedTTLRegistry 收敛「键控 map + TTL 回收 + 容量护栏 + Close(done)」骨架
// （#363）。owner（KeyedLimiterOf / LockoutOf）以组合持有，条目语义经
// keyedTTLConfig 回调注入；复合操作（护栏+安装、双重检查）由 owner 持本类型
// 的锁面整体完成。
type keyedTTLRegistry[K comparable, E any] struct {
	mu         sync.RWMutex
	entries    map[K]E
	ttl        time.Duration
	maxEntries int
	now        func() time.Time
	expired    func(e E, now time.Time) bool
	idleOf     func(e E, now time.Time) time.Duration
	onEvict    func(evicted []keyedEviction[K])
	done       chan struct{}
	closeOnce  sync.Once
}

// newKeyedTTLRegistry 构造注册表（不启动清扫协程——expired/onEvict 闭包可能
// 反引 owner 字段（如 kl.reg.ttl），owner 必须 wiring 完成后再 start）。
func newKeyedTTLRegistry[K comparable, E any](cfg keyedTTLConfig[K, E]) *keyedTTLRegistry[K, E] {
	if cfg.now == nil {
		cfg.now = time.Now // nil 时钟回退缺省（LockoutConfig.Now 同口径）
	}
	return &keyedTTLRegistry[K, E]{
		entries:    make(map[K]E),
		ttl:        cfg.ttl,
		maxEntries: cfg.maxEntries,
		now:        cfg.now,
		expired:    cfg.expired,
		idleOf:     cfg.idleOf,
		onEvict:    cfg.onEvict,
		done:       make(chan struct{}),
	}
}

// start 启动后台清扫协程（async.GoSafe：panic 收编，进程不崩）。
func (r *keyedTTLRegistry[K, E]) start() {
	async.GoSafe(r.cleanup)
}

// —— 锁面（owner 复合操作须整体持锁；粒度与锁序由 owner 语义决定）——

// Lock 写锁（独占）。
func (r *keyedTTLRegistry[K, E]) Lock() { r.mu.Lock() }

// Unlock 释放写锁。
func (r *keyedTTLRegistry[K, E]) Unlock() { r.mu.Unlock() }

// RLock 读锁（共享；owner 快路径读条目用）。
func (r *keyedTTLRegistry[K, E]) RLock() { r.mu.RLock() }

// RUnlock 释放读锁。
func (r *keyedTTLRegistry[K, E]) RUnlock() { r.mu.RUnlock() }

// —— map 面（带锁变体供 owner 在复合操作内复用）——

// lookup 带读锁查条目（快路径）。lastSeen 刷新由调用方负责（E 的字段，
// 条目指针出锁后原子写/串行写均安全，与收编前一致）。
func (r *keyedTTLRegistry[K, E]) lookup(key K) (E, bool) {
	r.mu.RLock()
	e, ok := r.entries[key]
	r.mu.RUnlock()
	return e, ok
}

// lookupLocked 查条目（调用方已持读锁或写锁——双重检查/存在性判定用）。
func (r *keyedTTLRegistry[K, E]) lookupLocked(key K) (E, bool) {
	e, ok := r.entries[key]
	return e, ok
}

// putLocked 安装条目（调用方已持写锁）。
func (r *keyedTTLRegistry[K, E]) putLocked(key K, e E) {
	r.entries[key] = e
}

// deleteLocked 删除条目（调用方已持写锁）。
func (r *keyedTTLRegistry[K, E]) deleteLocked(key K) {
	delete(r.entries, key)
}

// len 带读锁返回条目数。
func (r *keyedTTLRegistry[K, E]) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// —— 骨架单源（#363）：sweep / 护栏 / 清扫循环 / Close 幂等 ——

// sweepLocked 遍历全表删除过期条目（调用方已持写锁）。onEvict 非 nil 时按
// idleOf 采集观测载荷交调用方锁外回调；nil 时纯删除零分配（LockoutOf 无
// 回收观测面，与收编前一致）。
func (r *keyedTTLRegistry[K, E]) sweepLocked(now time.Time) []keyedEviction[K] {
	var evicted []keyedEviction[K]
	for key, e := range r.entries {
		if !r.expired(e, now) {
			continue
		}
		var idle time.Duration
		if r.idleOf != nil {
			idle = r.idleOf(e, now)
		}
		delete(r.entries, key)
		if r.onEvict != nil {
			evicted = append(evicted, keyedEviction[K]{key: key, idle: idle})
		}
	}
	return evicted
}

// allowNewLocked 判定是否允许登记新 key（容量护栏，调用方已持写锁）：
// 未设上限或未满直接放行；表满先 sweep-on-pressure 回收过期条目再判。
// 返回回收载荷交调用方锁外回调——回收路径统一收集、回调不允许在锁内触发
// （消费方回调里重入 owner 是合理需求，#142）。
func (r *keyedTTLRegistry[K, E]) allowNewLocked(now time.Time) (evicted []keyedEviction[K], ok bool) {
	if r.maxEntries <= 0 || len(r.entries) < r.maxEntries {
		return nil, true
	}
	evicted = r.sweepLocked(now)
	return evicted, len(r.entries) < r.maxEntries
}

// report 锁外交还回收载荷（onEvict 为 nil 时丢弃——载荷本就未收集）。
func (r *keyedTTLRegistry[K, E]) report(evicted []keyedEviction[K]) {
	if r.onEvict != nil {
		r.onEvict(evicted)
	}
}

// cleanup 定期清扫过期条目。ticker 只定节奏（cleanupInterval 兜底防 0 间隔
// panic / 热循环，#84），过期判定一律用注入时钟 r.now——假时钟下真实 ticker
// 的时间戳不可比（KeyedLimiterOf.cleanup 与 LockoutOf.cleanup 收编前同形，
// #361/#341）。回收载荷锁外交 onEvict。
func (r *keyedTTLRegistry[K, E]) cleanup() {
	ticker := time.NewTicker(cleanupInterval(r.ttl))
	defer ticker.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			now := r.now()
			r.mu.Lock()
			evicted := r.sweepLocked(now)
			r.mu.Unlock()
			r.report(evicted) // 锁外回调（#142）
		}
	}
}

// Close 停止后台清扫 goroutine。可安全重复调用（closeOnce 幂等）。
func (r *keyedTTLRegistry[K, E]) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
	})
}
