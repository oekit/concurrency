package timewheel

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// opKind 事件类型。
type opKind int

const (
	opSet opKind = iota
	opMove
	opRemove
	opHas
)

// timerEntry 槽位中的定时条目：circle 为触发前还需整轮的圈数。
type timerEntry struct {
	key    any
	task   func()
	pos    int
	circle int
}

// opEvent 串行化事件：所有状态变更与查询都交给事件循环统一处理（无锁）。
type opEvent struct {
	kind  opKind
	key   any
	task  func()
	delay time.Duration
	resp  chan bool // opHas 应答（cap 1）
}

// Ticker 周期信号源抽象（与 batch.Ticker 同形；各自独立定义避免子包耦合），
// 注入手动实现即可确定性地驱动时序测试。
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// TickerFactory 按给定间隔构造 Ticker。
type TickerFactory func(d time.Duration) Ticker

// WheelOption 时间轮的可选参数。
type WheelOption func(*wheelOptions)

type wheelOptions struct {
	tickerFactory TickerFactory
}

// WithWheelTicker 注入周期信号源工厂。nil 工厂被忽略。
func WithWheelTicker(f TickerFactory) WheelOption {
	return func(o *wheelOptions) {
		if f != nil {
			o.tickerFactory = f
		}
	}
}

// TimingWheel 单级时间轮：所有操作经事件 channel 串行到唯一后台循环
// （无锁），每 tick 推进一格并触发到期任务。
//
// 已到期未触发的任务在 Close 后丢弃（不保证触发）；需要持久化/重试语义
// 请用 github.com/oekit/timertask。
type TimingWheel struct {
	interval time.Duration
	numSlots int
	slots    []map[*timerEntry]struct{} // 槽位 → 条目集合
	timers   map[any]*timerEntry        // key → 条目（O(1) 移动/删除）
	current  int                        // 当前指针（指向"下一个要触发的槽位"减一语义，advance 时先推进）

	eventCh chan opEvent
	stopCh  chan struct{}
	done    chan struct{}
	closed  sync.Once

	tickerFunc TickerFactory
}

// NewTimingWheel 创建并启动时间轮。interval 为每格时长（必须 >0），
// numSlots 为格数（必须 >0）；二者决定可表达的单轮范围 interval×numSlots，
// 更长延迟由圈数表达。参数非法属于编程错误，直接 panic。
func NewTimingWheel(interval time.Duration, numSlots int, opts ...WheelOption) *TimingWheel {
	if interval <= 0 {
		panic("timewheel: interval must be positive")
	}
	if numSlots <= 0 {
		panic("timewheel: numSlots must be positive")
	}
	o := &wheelOptions{}
	for _, opt := range opts {
		opt(o)
	}
	if o.tickerFactory == nil {
		o.tickerFactory = defaultTickerFactory
	}
	tw := &TimingWheel{
		interval:   interval,
		numSlots:   numSlots,
		slots:      make([]map[*timerEntry]struct{}, numSlots),
		timers:     make(map[any]*timerEntry),
		eventCh:    make(chan opEvent, 64),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
		tickerFunc: o.tickerFactory,
	}
	for i := range tw.slots {
		tw.slots[i] = make(map[*timerEntry]struct{})
	}
	async.GoSafe(tw.loop)
	return tw
}

// SetTimer 注册定时任务：delay 后触发 task（key 已存在时替换旧任务）。
// delay 不足一格自动取一格；key 必须为可比较类型（编程错误直接 panic）。
func (tw *TimingWheel) SetTimer(key any, task func(), delay time.Duration) {
	if delay <= 0 {
		panic("timewheel: delay must be positive")
	}
	if task == nil {
		panic("timewheel: task must not be nil")
	}
	tw.checkKey(key)
	tw.send(opEvent{kind: opSet, key: key, task: task, delay: delay})
}

// MoveTimer 调整已存在 key 的触发延迟（保留原任务）；key 不存在为 no-op。
func (tw *TimingWheel) MoveTimer(key any, delay time.Duration) {
	if delay <= 0 {
		panic("timewheel: delay must be positive")
	}
	tw.checkKey(key)
	tw.send(opEvent{kind: opMove, key: key, delay: delay})
}

// RemoveTimer 删除已存在的定时任务；key 不存在为 no-op。
func (tw *TimingWheel) RemoveTimer(key any) {
	tw.checkKey(key)
	tw.send(opEvent{kind: opRemove, key: key})
}

// HasTimer 返回 key 是否有未触发的定时任务。
func (tw *TimingWheel) HasTimer(key any) bool {
	tw.checkKey(key)
	resp := make(chan bool, 1)
	tw.send(opEvent{kind: opHas, key: key, resp: resp})
	select {
	case v := <-resp:
		return v
	case <-tw.stopCh:
		return false
	}
}

// Close 幂等关闭：停止事件循环，未触发的任务被丢弃。等待退出最多到
// ctx 结束，超时返回 ctx.Err()。
func (tw *TimingWheel) Close(ctx context.Context) error {
	tw.closed.Do(func() {
		close(tw.stopCh)
	})
	select {
	case <-tw.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// checkKey key 必须可比较（作为 map key）；提前校验让编程错误在调用方
// 栈上 panic，而不是打死事件循环。
func (tw *TimingWheel) checkKey(key any) {
	if key == nil || !reflect.TypeOf(key).Comparable() {
		panic("timewheel: key must be non-nil and comparable")
	}
}

// send 投递事件；已关闭时静默丢弃（与 Close 后任务被丢弃的语义一致）。
func (tw *TimingWheel) send(ev opEvent) {
	select {
	case tw.eventCh <- ev:
	case <-tw.stopCh:
	}
}

// loop 事件主循环：操作事件与 tick 串行处理，全部状态无锁访问。
func (tw *TimingWheel) loop() {
	defer close(tw.done)

	var tickerC <-chan time.Time
	ticker := tw.tickerFunc(tw.interval)
	tickerC = ticker.C()
	defer ticker.Stop()

	for {
		select {
		case <-tw.stopCh:
			return
		case ev := <-tw.eventCh:
			tw.handleOp(ev)
		case <-tickerC:
			tw.advance()
		}
	}
}

// handleOp 处理一条操作事件（事件循环内调用，无锁）。
func (tw *TimingWheel) handleOp(ev opEvent) {
	switch ev.kind {
	case opSet:
		tw.removeLocked(ev.key)
		tw.insertLocked(ev.key, ev.task, ev.delay)
	case opMove:
		if old, ok := tw.timers[ev.key]; ok {
			tw.detachLocked(old)
			tw.insertLocked(ev.key, old.task, ev.delay)
		}
	case opRemove:
		tw.removeLocked(ev.key)
	case opHas:
		_, ok := tw.timers[ev.key]
		if ev.resp != nil {
			ev.resp <- ok
		}
	}
}

// advance 推进一格并触发到期任务（事件循环内调用，无锁）。
func (tw *TimingWheel) advance() {
	tw.current = (tw.current + 1) % tw.numSlots
	slot := tw.slots[tw.current]
	if len(slot) == 0 {
		return
	}
	var due []*timerEntry
	for entry := range slot {
		if entry.circle > 0 {
			entry.circle--
			continue
		}
		due = append(due, entry)
	}
	for _, entry := range due {
		delete(slot, entry)
		delete(tw.timers, entry.key)
		async.RunSafe(entry.task)
	}
}

// insertLocked 计算槽位与圈数并放入（事件循环内调用，无锁）。
//
// 延迟不足一格取一格：ticks = ceil 语义由 clamp + 整除近似——
// ticks = delay/interval（下取整，clamp 后 ≥1）；circle = (ticks-1)/numSlots，
// pos = (current+ticks) mod numSlots。任一 ticks 值下"circle 整圈 + 走到 pos"
// 恰等于 ticks 格。
func (tw *TimingWheel) insertLocked(key any, task func(), delay time.Duration) {
	if delay < tw.interval {
		delay = tw.interval
	}
	ticks := int(delay / tw.interval)
	if ticks < 1 {
		ticks = 1
	}
	entry := &timerEntry{
		key:    key,
		task:   task,
		pos:    (tw.current + ticks) % tw.numSlots,
		circle: (ticks - 1) / tw.numSlots,
	}
	tw.slots[entry.pos][entry] = struct{}{}
	tw.timers[key] = entry
}

// detachLocked 把条目从其槽位摘除（不动 timers 索引）。
func (tw *TimingWheel) detachLocked(entry *timerEntry) {
	delete(tw.slots[entry.pos], entry)
}

// removeLocked 按 key 摘除（槽位 + 索引）。
func (tw *TimingWheel) removeLocked(key any) {
	if old, ok := tw.timers[key]; ok {
		tw.detachLocked(old)
		delete(tw.timers, key)
	}
}

// defaultTickerFactory 标准库 time.Ticker 适配。
func defaultTickerFactory(d time.Duration) Ticker {
	t := time.NewTicker(d)
	return tickerAdapter{t}
}

type tickerAdapter struct{ *time.Ticker }

func (a tickerAdapter) C() <-chan time.Time { return a.Ticker.C }
