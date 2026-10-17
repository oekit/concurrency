// Package budget 提供预算/配额记账原语（#16）：按维度累计消耗，超限内容
// 进存档而非丢弃，消费方可按引用取回、按龄清理。
//
// 与仓内流控面（ratelimit/shedding/breaker）的丢弃式语义互补：那些原语
// 保护资源（超限即拒），本包保全内容（超限存档可取回）。典型场景：LLM
// 输出预算（超限截断提示 + 完整原文按需拉取）、日志/审计留存配额、批量
// 抓取字节配额。
//
// 并发安全；按龄清理依赖可注入时钟（WithClock），确定性测试不依赖 sleep。
package budget

import (
	"errors"
	"sync"
	"time"
)

// ErrOverBudget 表示本次消耗超出维度预算：content 已存档，返回值 Ref 可取回。
var ErrOverBudget = errors.New("budget: over budget")

// ErrInvalidAmount 表示消耗量为负（非法记账）。
var ErrInvalidAmount = errors.New("budget: negative amount")

// Dimension 是预算维度（如 "bytes"/"calls"；调用方自定义）。
type Dimension string

// Ref 是存档内容的取回引用（0 表示无存档）。
type Ref uint64

// entry 是一份存档：内容 + 入档时间（按龄清理依据）。
type entry struct {
	content []byte
	at      time.Time
}

// Ledger 记账器：按维度累计消耗，超限内容进存档。
type Ledger struct {
	mu       sync.Mutex
	caps     map[Dimension]int64
	used     map[Dimension]int64
	archives map[Ref]entry
	next     Ref
	now      func() time.Time
}

// Option 配置 Ledger。
type Option func(*Ledger)

// WithClock 注入时钟（按龄清理与入档时间的确定性测试入口；
// 与 batch.WithBatchClock / shedding.WithShedderClock 同形）。
func WithClock(now func() time.Time) Option {
	return func(l *Ledger) { l.now = now }
}

// NewLedger 创建记账器。caps 是各维度预算上限（拷贝存储，调用方后续修改
// 无影响）；未声明的维度不限量（仅记账语义不存在，不会误拦）。
func NewLedger(caps map[Dimension]int64, opts ...Option) *Ledger {
	l := &Ledger{
		caps:     make(map[Dimension]int64, len(caps)),
		used:     make(map[Dimension]int64),
		archives: make(map[Ref]entry),
		next:     1, // 0 保留给「无存档」
		now:      time.Now,
	}
	for d, cap := range caps {
		l.caps[d] = cap
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Consume 记一笔消耗。
//   - 未超限：used[d] += n，返回 (0, nil)；
//   - 超限：消耗不计入（内容未通过预算框），content 存档，返回
//     (Ref, ErrOverBudget)——Ref 经 Get 取回完整内容；
//   - n < 0 返回 (0, ErrInvalidAmount)。
//
// 未声明维度不限量，恒返回 (0, nil)。
func (l *Ledger) Consume(d Dimension, n int64, content []byte) (Ref, error) {
	if n < 0 {
		return 0, ErrInvalidAmount
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	cap, tracked := l.caps[d]
	if !tracked {
		return 0, nil // 未声明维度不限量
	}
	if used := l.used[d]; used+n > cap {
		ref := l.next
		l.next++
		l.archives[ref] = entry{content: content, at: l.now()}
		return ref, ErrOverBudget
	}
	l.used[d] += n
	return 0, nil
}

// Get 按引用取回存档内容（只读，不删除；清理由 Sweep 负责）。
// 引用不存在（或已被清理）返回 (nil, false)。
func (l *Ledger) Get(ref Ref) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.archives[ref]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(e.content))
	copy(out, e.content)
	return out, true
}

// Sweep 清理入档时间早于 olderThan 的存档，返回清理条数。
// olderThan=0 清理全部。
func (l *Ledger) Sweep(olderThan time.Duration) int {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for ref, e := range l.archives {
		if now.Sub(e.at) >= olderThan {
			delete(l.archives, ref)
			removed++
		}
	}
	return removed
}
