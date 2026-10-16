package batch

import (
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
)

// LessExecutor 窗口去重执行器：同一 interval 窗口内至多执行一次，
// 其余调用丢弃。适合告警/日志限频——同一事件风暴只放行第一条。
type LessExecutor struct {
	interval time.Duration
	do       func()
	now      func() time.Time
	ever     atomic.Bool // 是否已执行过（区分"从未执行"与时间零点）
	lastNano atomic.Int64
}

// NewLessExecutor 创建窗口去重执行器。interval <= 0 或 do 为 nil 属于
// 编程错误，直接 panic。
func NewLessExecutor(interval time.Duration, do func(), opts ...BatchOption) *LessExecutor {
	if interval <= 0 {
		panic("batch: interval must be positive")
	}
	if do == nil {
		panic("batch: do must not be nil")
	}
	o := &batchOptions{now: time.Now}
	for _, opt := range opts {
		opt(o)
	}
	if o.now == nil {
		o.now = time.Now
	}
	return &LessExecutor{interval: interval, do: do, now: o.now}
}

// DoOrDiscard 窗口内首次调用执行 do 并返回 true，其余丢弃返回 false。
// 并发调用下同一窗口保证恰有一路执行（CAS 裁决）。
func (e *LessExecutor) DoOrDiscard() bool {
	now := e.now()
	if !e.ever.Load() {
		if !e.ever.CompareAndSwap(false, true) {
			return false // 并发对手已抢先执行窗口首次
		}
		e.lastNano.Store(now.UnixNano())
		async.RunSafe(e.do)
		return true
	}
	last := e.lastNano.Load()
	if now.Sub(time.Unix(0, last)) < e.interval {
		return false
	}
	if !e.lastNano.CompareAndSwap(last, now.UnixNano()) {
		return false // 并发对手已胜出，本窗口去重
	}
	async.RunSafe(e.do)
	return true
}
