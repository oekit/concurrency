package supervisor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// GroupOption 配置 Group 的重启策略与观测钩子。
//
// 选项是**非泛型**的：Go 不为「类型参数只出现在返回值里」的函数做推断，泛型选项
// 会让所有不关心目标类型的选项（任务名/退避/上限/钩子）在每个调用点被迫写实参。
// 目标类型相关的能力（如「规格不完整的目标跳过」）交给调用方在传 targets 前过滤。
type GroupOption func(*groupConfig)

type groupConfig struct {
	taskPrefix     string
	backoffSet     bool
	initial        time.Duration
	max            time.Duration
	maxConsecutive int
	onRestart      func(id string, restarts int64, cause error, wait time.Duration)
	onGiveUp       func(id string, cause error)
}

// WithGroupTask 给组内每条循环设任务名：panic 汇报与重启日志带
// `task=<prefix>[<id>]` 标签，可按目标维度聚合告警。缺省不打标签。
func WithGroupTask(prefix string) GroupOption {
	return func(c *groupConfig) { c.taskPrefix = prefix }
}

// WithGroupRestartBackoff 配置组内单条循环的重启节奏（语义同 async.WithRestartBackoff：
// 第 k 次连续失败后等 initial*2^(k-1)，受 max 封顶）。缺省沿用 Supervise 的 1s/30s。
//
// initial <= 0 表示失败后立即重启——慎用：秒挂的常驻错误会退化成热循环。
func WithGroupRestartBackoff(initial, max time.Duration) GroupOption {
	return func(c *groupConfig) {
		c.backoffSet = true
		c.initial = initial
		c.max = max
	}
}

// WithGroupMaxConsecutiveFailures 配置组内单条循环的连续失败上限：超过后放弃重启
// 该目标并调用 WithGroupOnGiveUp。0（缺省）= 不限。
//
// 「放弃」是终态：该目标仍在目标集里、但已经没有循环在跑（见 Running）。要复活它，
// 先从目标集里摘掉再重新加入（让 Reconcile 当作新增目标从头启动）。
func WithGroupMaxConsecutiveFailures(max int) GroupOption {
	return func(c *groupConfig) { c.maxConsecutive = max }
}

// WithGroupOnRestart 配置组内单条循环的重启前观测钩子（id 是该目标经 IDOf 得到的
// 标识，restarts 从 1 起）。钩子自身 panic 被收编上报，不影响监督。
func WithGroupOnRestart(hook func(id string, restarts int64, cause error, wait time.Duration)) GroupOption {
	return func(c *groupConfig) { c.onRestart = hook }
}

// WithGroupOnGiveUp 配置放弃重启某目标时的告警钩子（需配合
// WithGroupMaxConsecutiveFailures）。钩子自身 panic 被收编上报。
func WithGroupOnGiveUp(hook func(id string, cause error)) GroupOption {
	return func(c *groupConfig) { c.onGiveUp = hook }
}

// Group 管理「按键常驻循环集合」与动态目标集的持续对齐（#143）。
//
// # 它补的是哪个缺口
//
// async.Supervise 管**单条**循环的监督重启，loop.Run 管**固定周期**任务，但
// 「目标集来自外部存储（DB / 配置中心 / 平台接口），运行中要 reconcile——新增目标
// 起循环、消失目标停循环、规格变更重启循环、意外死亡探活重启」这一类常见形态
// 没有原语：通道/连接/租户 worker 池类服务各手写一份 `map[K]entry{cancel, spec,
// done}` + mutex，而锁内 cancel/delete 顺序、快照比对、done 非阻塞探活全是
// 并发正确性敏感点，抄一遍就多漂移一次。
//
// Group 把这套收敛成两个动作：**对齐**（Reconcile）+ **停机排水**（Stop/Wait）。
// 组内单条循环的监督语义**完全复用** Supervise（panic 收编 + 指数退避重启 +
// 失败上限），Group 只负责集合对齐——两层职责不重叠。
//
// # 身份与「规格变更」
//
// IDOf 返回的不是「主键」而是**reconcile 键**：状态比较走这个字符串，组不做
// 结构比较。所以「哪些字段变了要重启」完全由调用方决定——把需要触发重启的规格
// 字段一并编进键里即可：
//
//	id := fmt.Sprintf("%d:%s", bot.ID, bot.Token) // token 变了 ⇒ 键变了 ⇒ 重启
//
// 这个设计是刻意的：拿 reflect.DeepEqual 比较规格会在「规格里含 mutex / 函数 /
// 时间戳字段」时给出假差异，进而变成重启风暴；而缺字段（该重启的没编进键）只是
// 少重启一次，两种失效方向不对称，故选宽容侧并由调用方掌握敏感度。
//
// # 语义契约
//
//   - Reconcile(targets) 由**单个 goroutine** 调用（控制器式 reconcile 循环）；
//     Len/Targets/Running/Draining/Wait/Stop 可并发调用。
//   - 新增目标 ⇒ 起一条监督循环；消失目标 ⇒ 取消其 ctx 并转入排水集合
//     （不阻塞 Reconcile：慢排水的循环不能拖住对齐）。
//   - start 返回 nil = 该目标的循环**优雅结束**，不重启（目标仍在集合里，
//     见 Running）；返回错误或 panic = 失败 ⇒ 按退避重启。
//   - start 收不到「该目标的 ctx」以外的任何东西：目标本身（含规格）由闭包捕获 ——
//     start 的第二个参数就是规格，规格变更经 IDOf 变键后自然重启。
//   - 目标 id 为空字符串或同一批 targets 里 id 重复都是调用方契约违反 ⇒ panic
//     （两者都会让「哪个循环属于谁」失去唯一性，静默容忍只会让错配在运行期才暴露）。
//   - Reconcile 不启动、不停止任何循环**在持锁状态下**：用户回调里读组状态
//     （Len/Running）不会自锁。
//
// # 典型用法
//
//	g := supervisor.NewGroup(ctx, func(b Bot) string { return fmt.Sprintf("%d:%s", b.ID, b.Token) },
//	    func(ctx context.Context, b Bot) error { return b.Run(ctx) }, // 常驻，ctx 取消后返回
//	    supervisor.WithGroupTask("bot"),
//	    supervisor.WithGroupValid(func(b Bot) bool { return b.Token != "" }),
//	)
//	defer g.Wait() // 配合 Stop 做排水
//	loop.Run(ctx, loop.Config{Interval: 30 * time.Second}, func(ctx context.Context) {
//	    bots, err := loadBots(ctx)
//	    if err != nil {
//	        return // 拉不到目标集就保持现状，别把全部循环停掉
//	    }
//	    g.Reconcile(bots)
//	})
type Group[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc
	idOf   func(T) string
	start  func(ctx context.Context, target T) error
	cfg    groupConfig

	mu       sync.Mutex
	slots    map[string]*slot[T]
	draining []*groupEntry
	stopped  bool
}

type slot[T any] struct {
	spec  T
	entry *groupEntry
}

// groupEntry 是组内一条监督循环的句柄。
type groupEntry struct {
	cancel context.CancelFunc
	sup    *async.Supervision
}

// NewGroup 构造受管组。组的生命周期由 ctx 决定：ctx 取消 ⇒ 所有目标循环一起取消。
//
// idOf 返回目标的 reconcile 键（见 Group 文档「身份与规格变更」）；start 是该目标
// 的常驻循环体——阻塞到 ctx 取消或该目标的连接确实结束为止，返回 nil 表示「优雅
// 结束、不要重启」，返回错误/panic 表示失败、按退避重启。
func NewGroup[T any](
	ctx context.Context,
	idOf func(T) string,
	start func(ctx context.Context, target T) error,
	opts ...GroupOption,
) *Group[T] {
	if idOf == nil {
		panic("supervisor.NewGroup: idOf 不能为 nil——没有身份就没有对齐")
	}
	if start == nil {
		panic("supervisor.NewGroup: start 不能为 nil——起了不跑的目标等于没起")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	groupCtx, cancel := context.WithCancel(ctx) //nolint:gosec // G118: cancel 存在 Group.cancel 里，由 Stop（或父 ctx 取消）调用
	g := &Group[T]{
		ctx:    groupCtx,
		cancel: cancel,
		idOf:   idOf,
		start:  start,
		slots:  make(map[string]*slot[T]),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&g.cfg)
		}
	}
	return g
}

// Reconcile 把运行中的循环集合对齐到 targets：新增的起、消失的停、已排水的回收。
//
// 目标经 IDOf 得到的键既作身份也作「是否需要重启」的判据（见 Group 文档）。
// 必须由单个 goroutine 调用。组已 Stop 后调用是空操作。
func (g *Group[T]) Reconcile(targets []T) {
	type pending struct {
		id   string
		spec T
	}

	// 先算 diff（持锁只做 map 操作，不跑用户代码）
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	wanted := make(map[string]T, len(targets))
	order := make([]string, 0, len(targets))
	for _, target := range targets {
		id := g.idOf(target)
		if id == "" {
			g.mu.Unlock()
			panic("supervisor: IDOf 返回空字符串——空身份会让所有这类目标争同一个槽位")
		}
		if _, dup := wanted[id]; dup {
			g.mu.Unlock()
			panic(fmt.Sprintf("supervisor: targets 里 id 重复（%q）——同一批目标里身份必须唯一", id))
		}
		wanted[id] = target
		order = append(order, id)
	}

	var toStart []pending
	for _, id := range order {
		if _, ok := g.slots[id]; !ok {
			toStart = append(toStart, pending{id: id, spec: wanted[id]})
		}
	}

	var toStop []*groupEntry
	for id, s := range g.slots {
		if _, ok := wanted[id]; !ok {
			toStop = append(toStop, s.entry)
			delete(g.slots, id)
		}
	}
	g.draining = append(g.draining, toStop...)
	g.pruneDrainingLocked()
	g.mu.Unlock()

	// 用户代码（cancel / start）一律在锁外：start 会立刻在监督 goroutine 里跑起来，
	// 持锁调用会让「start 里读组状态」自锁。
	for _, entry := range toStop {
		entry.cancel()
	}
	for _, p := range toStart {
		entry := g.startOne(p.id, p.spec)
		g.mu.Lock()
		if g.stopped {
			g.mu.Unlock()
			entry.cancel()
			continue
		}
		g.slots[p.id] = &slot[T]{spec: p.spec, entry: entry}
		g.mu.Unlock()
	}
}

// Targets 返回当前受管目标的 id 集（顺序不保证）。
func (g *Group[T]) Targets() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := make([]string, 0, len(g.slots))
	for id := range g.slots {
		ids = append(ids, id)
	}
	return ids
}

// Len 返回受管目标数（含已优雅结束、已放弃重启但仍留在目标集里的那些，
// 与它们的循环是否还在跑无关——「在跑几条」看 Running）。
func (g *Group[T]) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.slots)
}

// Running 返回**循环仍在跑**的目标数（非阻塞探活）。
//
// 小于 Len 说明有的目标优雅结束或连续失败后被放弃重启——这是「集合对齐了，
// 但实际没在服务」的唯一观测口。两项相等不代表健康（循环可能在空转），
// 但不等一定说明有目标缺位。
func (g *Group[T]) Running() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, s := range g.slots {
		if !isDone(s.entry) {
			n++
		}
	}
	return n
}

// Draining 返回已从目标集移除、但循环还没退出的数量（非阻塞）。
//
// 停机观察用：Stop 之后该值长时间不归零，说明有循环不肯收尾（ctx 没被透传下去，
// 或阻塞在不可取消的调用里）。
func (g *Group[T]) Draining() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pruneDrainingLocked()
	return len(g.draining)
}

// Stop 取消整组：所有目标循环（含仍在排水的）一起收到 ctx 取消。
//
// 幂等、非阻塞——排水靠 Wait。与 ctx 取消等效，提供它是为了在不方便取消父 ctx 的
// 场合显式停机。
func (g *Group[T]) Stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.stopped = true
	g.cancel()
	g.mu.Unlock()
}

// Wait 阻塞至调用时刻已登记的全部循环（含排水中的）退出。
//
// 停机顺序是先 Stop（或取消父 ctx）再 Wait；只 Wait 不 Stop 会一直等到循环自己结束。
// 需要限时排水时由调用方包一层 select 控制超时预算。
func (g *Group[T]) Wait() {
	g.mu.Lock()
	sups := make([]*async.Supervision, 0, len(g.slots)+len(g.draining))
	for _, s := range g.slots {
		sups = append(sups, s.entry.sup)
	}
	for _, entry := range g.draining {
		sups = append(sups, entry.sup)
	}
	g.mu.Unlock()

	for _, sup := range sups {
		// 逐目标的结束原因不进组级返回值：它们已经经 restart/give-up 钩子
		// 逐条送达（带目标 id），组级再做一次聚合只会变成第二份口径。
		_ = sup.Wait()
	}
}

// startOne 为单个目标起监督循环。调用方须在锁外调用。
func (g *Group[T]) startOne(id string, spec T) *groupEntry {
	targetCtx, cancel := context.WithCancel(g.ctx)
	sup := async.Supervise(targetCtx, func(ctx context.Context) error {
		return g.start(ctx, spec)
	}, g.superviseOpts(id)...)
	return &groupEntry{cancel: cancel, sup: sup}
}

// superviseOpts 把组级配置翻译成 Supervise 的逐目标选项（钩子带上目标 id）。
func (g *Group[T]) superviseOpts(id string) []async.SuperviseOption {
	cfg := g.cfg
	opts := make([]async.SuperviseOption, 0, 5)
	if cfg.taskPrefix != "" {
		opts = append(opts, async.WithTask(cfg.taskPrefix+"["+id+"]"))
	}
	if cfg.backoffSet {
		opts = append(opts, async.WithRestartBackoff(cfg.initial, cfg.max))
	}
	if cfg.maxConsecutive > 0 {
		opts = append(opts, async.WithMaxConsecutiveFailures(cfg.maxConsecutive))
	}
	if cfg.onRestart != nil {
		hook := cfg.onRestart
		opts = append(opts, async.WithOnRestart(func(restarts int64, cause error, wait time.Duration) {
			hook(id, restarts, cause, wait)
		}))
	}
	if cfg.onGiveUp != nil {
		hook := cfg.onGiveUp
		opts = append(opts, async.WithOnGiveUp(func(cause error) { hook(id, cause) }))
	}
	return opts
}

// pruneDrainingLocked 回收已退出的排水条目（原地过滤，调用方须持锁）。
func (g *Group[T]) pruneDrainingLocked() {
	kept := g.draining[:0]
	for _, entry := range g.draining {
		if !isDone(entry) {
			kept = append(kept, entry)
		}
	}
	g.draining = kept
}

// isDone 非阻塞探活（Supervision.Done 已关闭 = 循环已退出）。
func isDone(entry *groupEntry) bool {
	select {
	case <-entry.sup.Done():
		return true
	default:
		return false
	}
}
