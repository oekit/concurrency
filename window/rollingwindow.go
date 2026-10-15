package window

import (
	"sync"
	"time"
)

type (
	// Numerical 限定桶内数值类型（整数与浮点，含基于它们的自定义类型）。
	// 与 go-zero mathx.Numerical 同形；本仓零第三方/矩阵内依赖，就地定义。
	Numerical interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
			~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
			~float32 | ~float64
	}

	// BucketInterface 定义桶的读写能力：Add 累加一个值，Reset 清零。
	// 自定义桶实现该接口后即可作为 RollingWindow 的桶类型。
	BucketInterface[T Numerical] interface {
		Add(v T)
		Reset()
	}

	// WindowOption 用于定制 RollingWindow（函数选项模式；
	// 按本仓命名硬规则为 XxxOption，禁止裸 Option）。
	WindowOption[T Numerical, B BucketInterface[T]] func(rollingWindow *RollingWindow[T, B])

	// RollingWindow 是按时间 interval 分桶的滑动窗口统计容器。
	// Add 写入当前桶；跨桶时惰性重置过期桶（无后台 goroutine）；
	// Reduce 遍历有效桶执行聚合。并发安全；零值不可用，须经 NewRollingWindow 构造。
	RollingWindow[T Numerical, B BucketInterface[T]] struct {
		lock          sync.RWMutex
		size          int
		win           *window[T, B]
		interval      time.Duration
		offset        int
		ignoreCurrent bool
		clock         func() time.Time
		lastTime      time.Duration // 最后一个桶的起始时刻（clock 的 UnixNano）
	}
)

// NewRollingWindow 返回带 size 个桶、单桶时间宽度 interval 的滑动窗口，
// newBucket 为桶工厂，opts 定制窗口行为。size < 1 时 panic。
func NewRollingWindow[T Numerical, B BucketInterface[T]](newBucket func() B, size int,
	interval time.Duration, opts ...WindowOption[T, B]) *RollingWindow[T, B] {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow[T, B]{
		size:     size,
		win:      newWindow[T, B](newBucket, size),
		interval: interval,
	}
	for _, opt := range opts {
		opt(w)
	}
	// 时间源在 opts 应用后确定（WithWindowClock 生效）；nil 视为缺省 time.Now。
	// lastTime 必须用最终时钟取值，保证与后续 span/updateOffset 同源。
	if w.clock == nil {
		w.clock = time.Now
	}
	w.lastTime = w.now()
	return w
}

// Add 把值累加进当前桶；跨桶时惰性重置过期桶并把时间对齐到 interval 边界。
func (rw *RollingWindow[T, B]) Add(v T) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	rw.updateOffset()
	rw.win.add(rw.offset, v)
}

// Reduce 对所有有效桶执行 fn 聚合；
// 构造时若启用 IgnoreCurrentBucket 则跳过当前桶（数据尚未完整）。
func (rw *RollingWindow[T, B]) Reduce(fn func(b B)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int
	span := rw.span()
	// ignore the current bucket, because of partial data
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1
	} else {
		diff = rw.size - span
	}
	if diff > 0 {
		offset := (rw.offset + span + 1) % rw.size
		rw.win.reduce(offset, diff, fn)
	}
}

// span 返回自 lastTime 以来跨过的桶数；超出窗口长度按整窗处理。
func (rw *RollingWindow[T, B]) span() int {
	offset := int((rw.now() - rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}

	return rw.size
}

// updateOffset 重置过期桶并推进 offset，把 lastTime 对齐到 interval 边界。
func (rw *RollingWindow[T, B]) updateOffset() {
	span := rw.span()
	if span <= 0 {
		return
	}

	offset := rw.offset
	// reset expired buckets
	for i := 0; i < span; i++ {
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}

	rw.offset = (offset + span) % rw.size
	now := rw.now()
	// align to interval time boundary
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

// now 返回当前时刻（clock 的 UnixNano，仅参与差值运算）。
func (rw *RollingWindow[T, B]) now() time.Duration {
	return time.Duration(rw.clock().UnixNano())
}

// Bucket 是默认桶实现，持有累加和 Sum 与累加次数 Count。
type Bucket[T Numerical] struct {
	Sum   T
	Count int64
}

// Add 累加一个值。
func (b *Bucket[T]) Add(v T) {
	b.Sum += v
	b.Count++
}

// Reset 清零桶数据。
func (b *Bucket[T]) Reset() {
	b.Sum = 0
	b.Count = 0
}

type window[T Numerical, B BucketInterface[T]] struct {
	buckets []B
	size    int
}

func newWindow[T Numerical, B BucketInterface[T]](newBucket func() B, size int) *window[T, B] {
	buckets := make([]B, size)
	for i := 0; i < size; i++ {
		buckets[i] = newBucket()
	}
	return &window[T, B]{
		buckets: buckets,
		size:    size,
	}
}

func (w *window[T, B]) add(offset int, v T) {
	w.buckets[offset%w.size].Add(v)
}

func (w *window[T, B]) reduce(start, count int, fn func(b B)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

func (w *window[T, B]) resetBucket(offset int) {
	w.buckets[offset%w.size].Reset()
}

// IgnoreCurrentBucket 让 Reduce 跳过当前桶（数据尚未完整）。
func IgnoreCurrentBucket[T Numerical, B BucketInterface[T]]() WindowOption[T, B] {
	return func(w *RollingWindow[T, B]) {
		w.ignoreCurrent = true
	}
}

// WithWindowClock 注入自定义时间源（缺省 time.Now）。
// 测试注入假时钟即可完全确定地驱动窗口推进，不依赖真实 sleep。
func WithWindowClock[T Numerical, B BucketInterface[T]](clock func() time.Time) WindowOption[T, B] {
	return func(w *RollingWindow[T, B]) {
		w.clock = clock
	}
}
