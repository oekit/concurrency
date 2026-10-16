package batch

import "time"

// BulkExecutor 满条数触发版攒批执行器：攒满 maxTasks 条立即 flush，
// 否则等 interval 周期。interval <= 0 表示只按条数触发。
type BulkExecutor struct {
	*PeriodicalExecutor
}

// NewBulkExecutor 创建满条数触发的攒批执行器。maxTasks <= 0 或 do 为 nil
// 属于编程错误，直接 panic。
func NewBulkExecutor(interval time.Duration, maxTasks int, do func(tasks []any), opts ...BatchOption) *BulkExecutor {
	if maxTasks <= 0 {
		panic("batch: maxTasks must be positive")
	}
	if do == nil {
		panic("batch: do must not be nil")
	}
	return &BulkExecutor{
		PeriodicalExecutor: NewPeriodicalExecutor(interval, &bulkContainer{max: maxTasks, do: do}, opts...),
	}
}

// bulkContainer 满条数容器。
type bulkContainer struct {
	max   int
	tasks []any
	do    func(tasks []any)
}

func (c *bulkContainer) AddTask(task any) bool {
	c.tasks = append(c.tasks, task)
	return len(c.tasks) >= c.max
}

func (c *bulkContainer) Execute(tasks []any) { c.do(tasks) }

func (c *bulkContainer) RemoveAll() []any {
	if len(c.tasks) == 0 {
		return nil
	}
	out := c.tasks
	c.tasks = nil
	return out
}
