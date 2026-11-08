package syncx

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// KeyedMutex 本地按键互斥原语（#195）：**同一 key 同刻至多一个持有者**——
// 「同名任务/通道/资源同刻只跑一个」的单进程内单源。
//
// 收编 git-ferry `localGuard` 式的 sync.Map 三件套手搓（LoadOrStore + 失败还
// 名额 + Delete 回调）——三步各自易错，且失效形态都是慢性的：
//
//   - LoadOrStore 失败（key 已在跑）时忘了归还先抢到的全局名额 ⇒ 每撞一次
//     同 key 竞争，全局并发名额永久 -1，最终「无流量也 ErrTooManyConcurrent」；
//   - release 回调里 Delete 抢跑（先 Delete 后做/漏做其它清理，或 Delete 了
//     新执行流刚 store 的标记）⇒ 互斥标记被误删，同 key 两个执行流并行。
//
// 本原语把三步收敛成一次固定顺序的临界区：命中即拒（ok=false，无名额可还）、
// 成功即持有（held 集合登记）、release 即注销（delete 与登记同一把段锁），
// 调用方没有「中间态」可以写错。
//
// # 与同仓近邻的分工
//
//   - singleflight：同 key 并发调用**合并**成一次 fn（所有调用者共享同一份
//     结果）——要的是省执行；
//   - KeyedMutex：同 key 并发**互斥**（后来者立刻失败，不共享、不排队）——
//     要的是「只跑一个」的排他性，两次调用是两次独立执行；
//   - KeyedGate（本包）：同 key **并发上限 N**（TryAcquire 即拒 / Acquire
//     排队）——N>1 的闸门；KeyedMutex 是它的 N=1 且语义为「锁」的特化形态，
//     返回的是锁句柄而不是名额。
//
// # 内存模型
//
// 持有标记是**逐段锁保护的 `map[K]struct{}` 集合**（16 段，key 哈希打散），
// 条目生命周期 = 持有期：release 注销即删除条目——表中只会存在**正在持有**
// 的 key，高基数/长尾 key 不残留（对比 KeyedGate 的槽位常驻、KeyedLimiterOf
// 的空闲 TTL 清扫：那两者缓存的是可复用的有状态实例，而互斥标记是无状态位，
// 持有结束即无信息量，保留只会变成慢性泄漏面）。无需清扫协程、无需 Close。
//
// 注意 Acquire/WithLock 不排队：key 忙时**立刻**返回（ok=false / false），
// 返回 409/429 由调用方决定；需要「排队等锁 + 超时放弃」的形态（等价
// TryAcquireWithTimeout）请用 KeyedGate.Acquire(ctx, key)（cap=1 即排队互斥）。
//
// KeyedMutex 按键互斥器（并发安全）。零值不可用，经 NewKeyedMutex 构造。
type KeyedMutex[K comparable] struct {
	shards [keyedMutexShards]keyedMutexShard[K]
	seed   maphash.Seed // key → 段的哈希种子（实例独立，防键分布被构造撞段）
}

// keyedMutexShard 是单个段：held 集合 + 段锁。集合中**存在即持有**（条目
// 生命周期 = 持有期，release 即删除，见 KeyedMutex 内存模型说明）。
type keyedMutexShard[K comparable] struct {
	mu   sync.Mutex
	held map[K]struct{}
}

// keyedMutexShards 是 KeyedMutex 的分段锁段数：不同 key 的 Acquire/release
// 各自只锁自己的段，避免单 map 在高基数 key 下成为全局热点。段数取固定小值
// ——互斥标记的临界区只有 map 读写两步，16 段对常见任务/通道键基数（数百到
// 数万）已足够打散。
const keyedMutexShards = 16

// NewKeyedMutex 构造按键互斥器。key 类型任意 comparable（任务键通常为 string）。
//
// 用法:
//
//	guard := syncx.NewKeyedMutex[string]()
//	if !guard.WithLock(taskKey, func() { runOnce(taskKey) }) {
//	    return ErrTaskRunning // 409 / 429
//	}
func NewKeyedMutex[K comparable]() *KeyedMutex[K] {
	m := &KeyedMutex[K]{seed: maphash.MakeSeed()}
	for i := range m.shards {
		m.shards[i].held = make(map[K]struct{})
	}
	return m
}

// shardOf 返回 key 所在段。maphash.Comparable（Go 1.24）对任意 comparable
// key 做一次均匀散列——不要求 key 约束 Hash 方法，也不用 fmt.Sprint 造字符串
// （每次 Acquire 一次分配）。
func (m *KeyedMutex[K]) shardOf(key K) *keyedMutexShard[K] {
	return &m.shards[maphash.Comparable(m.seed, key)%keyedMutexShards]
}

// Acquire 尝试持有 key 的互斥标记，**不等待**：该 key 已被持有 ⇒ 立即返回
// (nil, false)，不排队、不占任何名额。
//
// ok=false 只有一个含义：**该 key 正在执行中**（非阻塞互斥没有别的失败原因），
// 调用方可据此返回 409 / 429，而不是泛化成 500。
//
// 返回的 release 注销持有标记，**幂等**（release 两次第二次 no-op）——吃掉
// 「超时清理路径与正常返回路径同时释放」这类下游 Double-release bug：标记用
// atomic.Bool CAS 防护（同包 KeyedGate 同款做法），裸 bool 在两个 goroutine
// 并发调用同一 release 时是 data race，且第二次 delete 可能误删新执行流刚
// 登记的标记（即 git-ferry 注释自证的 ④ 号失效形态）。
//
// release 必须被调用：持有标记在 release 前一直阻塞同 key 的后续 Acquire
// （fn panic 不会自动释放——需要「panic 也放锁」的包裹语义用 WithLock）。
func (m *KeyedMutex[K]) Acquire(key K) (release func(), ok bool) {
	s := m.shardOf(key)
	s.mu.Lock()
	if _, held := s.held[key]; held {
		s.mu.Unlock()
		return nil, false
	}
	s.held[key] = struct{}{}
	s.mu.Unlock()

	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			return
		}
		s.mu.Lock()
		// 此刻集合里的 key 必是本持有者登记的那条：Acquire 到 release 之间
		// 没有任何路径能替换或删除它（互斥保证了重入不可能）。
		delete(s.held, key)
		s.mu.Unlock()
	}, true
}

// WithLock 是 Acquire 的闭包糖：key 空闲则以独占方式执行 fn 并返回 true；
// key 忙则 fn **一次不调**、立即返回 false（即拒语义，与 Acquire 一致）。
// fn 返回（含 panic 展开）后互斥标记必然已释放——release 挂 defer，fn 的
// panic 不吞不转、原样上抛给调用方（本方法运行在调用方自己的 goroutine，
// 不跨 goroutine 收编；需要把 panic 转错误用 async.RunSafeErr 包裹本调用）。
//
// 用法:
//
//	if !guard.WithLock(key, func() { syncMirror(key) }) {
//	    return ErrTaskRunning
//	}
func (m *KeyedMutex[K]) WithLock(key K, fn func()) bool {
	release, ok := m.Acquire(key)
	if !ok {
		return false
	}
	defer release()
	fn()
	return true
}

// Len 返回当前持有中的 key 数（快照值，返回即过期——仅观测用，不做同步依据）。
func (m *KeyedMutex[K]) Len() int {
	n := 0
	for i := range m.shards {
		m.shards[i].mu.Lock()
		n += len(m.shards[i].held)
		m.shards[i].mu.Unlock()
	}
	return n
}
