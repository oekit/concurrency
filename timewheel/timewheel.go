package timewheel

import (
	"context"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/concurrency/internal/ticker"
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

// Ticker 周期信号源抽象，注入手动实现即可确定性地驱动时序测试。
// 实现单源在 internal/ticker（#264，与 batch 共用），此处类型别名转发，
// 既有导入面零破坏。
type Ticker = ticker.Ticker

// TickerFactory 按给定间隔构造 Ticker。
type TickerFactory = ticker.TickerFactory

// WheelOption 时间轮的可选参数。
type WheelOption func(*wheelOptions)

type wheelOptions struct {
	tickerFactory TickerFactory
	taskExecutor  func(task func())
}

// WithWheelTicker 注入周期信号源工厂。nil 工厂被忽略。
func WithWheelTicker(f TickerFactory) WheelOption {
	return func(o *wheelOptions) {
		if f != nil {
			o.tickerFactory = f
		}
	}
}

// WithWheelTaskExecutor 注入到期任务的执行器，把任务触发派发出事件循环
// （#274）。缺省（不配置）任务在事件循环内同步执行——单线程时间轮的既有
// 语义：SetTimer/MoveTimer/RemoveTimer/HasTimer 与 tick 全部串行在同一
// goroutine 上，慢任务会直接变成 tick 漂移与操作阻塞。挂重 IO/RPC 的调用方
// 应注入执行器转后台，例如复用仓内 worker 池：
//
//	pool := gopool.NewPool("timewheel", gopool.PoolOption{MaxIdleWorkers: 32})
//	tw := timewheel.NewTimingWheel(time.Second, 60,
//	    timewheel.WithWheelTaskExecutor(pool.Go))
//
// 调度语义（与缺省同步态对齐的部分，向后兼容承诺）：
//   - **不丢**：任务从槽位摘除后即交付执行器，交付恰一次；交付后 wheel 不再
//     追踪——Close 的「未触发任务丢弃」语义以**交付**为界（已交付的任务由
//     执行器负责跑完，wheel 关闭不回收它们）；
//   - **不乱序于交付**：跨 tick 按时间轮推进顺序交付（早 tick 先交付）；
//     同一 tick 内同槽多条目按 map 遍历序（与缺省态一致，不保证确定序）；
//   - **并行度与顺序归执行器**：执行器以何种并发度、按什么顺序实际运行任务
//     由其实现决定（需要严格串行就注入单 worker 执行器，如
//     `func(t func()) { go serialQueue(t) }`）；
//   - **panic 收编不变**：交付的闭包内含 async.RunSafe 包裹，任务 panic 不会
//     打死执行器的 worker goroutine（gopool 自身的 recoverRun 之外的双保险）；
//   - 执行器自身应快速返回（入队即回）；在执行器里同步执行任务等于回到
//     缺省同步态的阻塞语义，还多了间接层。
//
// exec 为 nil 时忽略（保留缺省同步执行，nil 参数 = 回退默认值的总则口径）。
func WithWheelTaskExecutor(exec func(task func())) WheelOption {
	return func(o *wheelOptions) {
		if exec != nil {
			o.taskExecutor = exec
		}
	}
}

// TimingWheel 单级时间轮：所有操作经事件 channel 串行到唯一后台循环
// （无锁），每 tick 推进一格并触发到期任务。
//
// # 任务执行语义（#274）
//
// 缺省：到期任务在事件循环 goroutine 内**同步**执行（async.RunSafe 收编
// panic）——task 必须轻量，重 IO/RPC/长计算会直接阻塞 tick 推进与全部操作
// （含 HasTimer 的应答）。重活请用 WithWheelTaskExecutor 注入执行器把触发
// 派发出事件循环（可接 gopool），或按旧口径在 task 内自行转后台。
//
// 已到期未触发的任务在 Close 后丢弃（不保证触发）；配置执行器时以「交付给
// 执行器」为已触发界（见 WithWheelTaskExecutor 的调度语义）。需要持久化/
// 重试语义请用 github.com/oekit/timertask。
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
	// taskExecutor 到期任务执行器（WithWheelTaskExecutor）；nil = 缺省同步执行。
	taskExecutor func(task func())
}

// NewTimingWheel 创建并启动时间轮。interval 为每格时长（必须 >0），
// numSlots 为格数（必须 >0）；二者决定可表达的单轮范围 interval×numSlots，
// 更长延迟由圈数表达。参数非法属于编程错误，直接 panic。
//
// 到期任务缺省在事件循环内同步执行（task 必须轻量，语义详见 TimingWheel
// 与 WithWheelTaskExecutor 的 godoc，#274）。
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
		interval:     interval,
		numSlots:     numSlots,
		slots:        make([]map[*timerEntry]struct{}, numSlots),
		timers:       make(map[any]*timerEntry),
		eventCh:      make(chan opEvent, 64),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
		tickerFunc:   o.tickerFactory,
		taskExecutor: o.taskExecutor,
	}
	for i := range tw.slots {
		tw.slots[i] = make(map[*timerEntry]struct{})
	}
	async.GoSafe(tw.loop)
	return tw
}

// SetTimer 注册定时任务：delay 后触发 task（key 已存在时替换旧任务）。
// 延迟按格**向上取整**（#219）：不足一格自动取一格，非整数格不提前——
// 1.5 格等 2 格，任务不早于 delay 触发（「delay 后触发」的契约）。
// key 必须为可比较类型（编程错误直接 panic）。
//
// ⚠️ 缺省态 task 在事件循环 goroutine 内**同步**执行（#274）：task 必须轻量
// （微秒级），重 IO/RPC/长计算会阻塞 tick 与全部操作——重活用
// WithWheelTaskExecutor 注入执行器转后台，或在 task 内自行 go 转后台。
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
// 延迟取整口径同 SetTimer（向上取整，#219）。
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
	// 循环因任何原因死亡时顺手关停通道（#174）：此后 send 快速失败（不再把
	// 事件塞进无人消费的缓冲后「成功返回」——任务静默丢失）、HasTimer 立即
	// 返回 false 而非永久挂起。死亡信号不再只体现在 done 上。
	defer tw.closed.Do(func() { close(tw.stopCh) })

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
		tw.runTask(entry.task)
	}
}

// runTask 触发一个到期任务（事件循环内调用，#274）：
//   - 缺省（未配置执行器）：事件循环内同步执行（async.RunSafe 收编 panic）——
//     修复前的唯一形态，语义不变；
//   - 配置了执行器（WithWheelTaskExecutor）：交付执行器派发出事件循环，慢任务
//     不再阻塞 tick 与操作。交付闭包内含 async.RunSafe：无论执行器把任务派去
//     哪个 goroutine，panic 都被收编（打不死事件循环与执行器 worker）。
//
// 交付恰一次、从槽位摘除在前：交付后 wheel 不再追踪该任务（Close 的丢弃
// 语义以交付为界）。
func (tw *TimingWheel) runTask(task func()) {
	if tw.taskExecutor == nil {
		async.RunSafe(task)
		return
	}
	tw.taskExecutor(func() { async.RunSafe(task) })
}

// insertLocked 计算槽位与圈数并放入（事件循环内调用，无锁）。
//
// ticks = ceil(delay/interval)（#219）：「delay 后触发」的契约是不早于 delay，
// 非整数格向下取整会**恒提前**（1.999 格按 1 格计，最多早近一整格）——floor
// 不是 ceil 的近似，误差方向固定。ceil 对不足一格的 0.x 格给出 1，天然覆盖
// 「delay 不足一格自动取一格」的既有口径。circle = (ticks-1)/numSlots，
// pos = (current+ticks) mod numSlots。任一 ticks 值下"circle 整圈 + 走到 pos"
// 恰等于 ticks 格。
//
// ticks 上界钳制（#174）：interval 极小 + 巨大 delay（「传 MaxInt64 当永不超时」
// 是调用方自然写法）时 ticks 可达 MaxInt64，pos 计算的加法回绕成负、Go 负数
// 取模得负槽位下标 ⇒ 事件循环 goroutine panic 死亡——此后 SetTimer 静默丢任务、
// HasTimer 永久挂起。按整轮饱和（对齐 retry 退避的溢出饱和口径）：超出
// MaxInt64/numSlots 的延迟钉在该格数（≈ 不可达的远未来），不回绕；ceil 的
// +1 同样先判上界再递增，避免 MaxInt64 边界回绕成负（#219）。
func (tw *TimingWheel) insertLocked(key any, task func(), delay time.Duration) {
	ticks := int64(delay / tw.interval)
	if delay%tw.interval != 0 && ticks < math.MaxInt64-1 {
		ticks++ // 向上取整（#219）：非整数格不提前触发
	}
	if ticks < 1 {
		ticks = 1 // 不足一格取一格
	}
	if maxTicks := int64(math.MaxInt64) / int64(tw.numSlots); ticks > maxTicks {
		ticks = maxTicks
	}
	entry := &timerEntry{
		key:    key,
		task:   task,
		pos:    int((int64(tw.current) + ticks) % int64(tw.numSlots)),
		circle: int((ticks - 1) / int64(tw.numSlots)),
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

// defaultTickerFactory 标准库 time.Ticker 适配（internal/ticker 单源，#264）。
func defaultTickerFactory(d time.Duration) Ticker {
	return ticker.New(d)
}
