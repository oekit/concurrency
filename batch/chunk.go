package batch

import "time"

// ChunkExecutor 满字节触发版攒批执行器：按任务声明的大小累计，
// 攒满 maxBytes 立即 flush，否则等 interval 周期。适合按负载体量
// （日志字节、消息字节数）控制批次大小的场景。
type ChunkExecutor struct {
	*PeriodicalExecutor
}

// NewChunkExecutor 创建满字节触发的攒批执行器。maxBytes <= 0 或 do 为 nil
// 属于编程错误，直接 panic。
func NewChunkExecutor(interval time.Duration, maxBytes int64, do func(tasks []any), opts ...BatchOption) *ChunkExecutor {
	if maxBytes <= 0 {
		panic("batch: maxBytes must be positive")
	}
	if do == nil {
		panic("batch: do must not be nil")
	}
	return &ChunkExecutor{
		PeriodicalExecutor: NewPeriodicalExecutor(interval, &chunkContainer{maxBytes: maxBytes, do: do}, opts...),
	}
}

// Add 收纳一个任务并声明其大小（字节）。
func (e *ChunkExecutor) Add(task any, size int64) {
	e.PeriodicalExecutor.Add(chunkItem{task: task, size: size})
}

// chunkItem 携带大小的内部任务包装（对容器私有，Execute 只交付裸任务）。
type chunkItem struct {
	task any
	size int64
}

// chunkContainer 满字节容器：tasks 与 sizes 平行，Execute 只交付裸任务。
type chunkContainer struct {
	maxBytes int64
	tasks    []any
	sizes    []int64
	total    int64
	do       func(tasks []any)
}

func (c *chunkContainer) AddTask(v any) bool {
	item := v.(chunkItem)
	c.tasks = append(c.tasks, item.task)
	c.sizes = append(c.sizes, item.size)
	c.total += item.size
	return c.total >= c.maxBytes
}

func (c *chunkContainer) Execute(tasks []any) { c.do(tasks) }

func (c *chunkContainer) RemoveAll() []any {
	if len(c.tasks) == 0 {
		return nil
	}
	out := c.tasks
	c.tasks = nil
	c.sizes = nil
	c.total = 0
	return out
}
