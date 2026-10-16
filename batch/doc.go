// Package batch 提供攒批执行器：把零散任务攒成批次按条数/字节数/时间触发
// 批量执行，核心是 PeriodicalExecutor + TaskContainer 扩展点。
//
// 设计参考 go-zero core/executors（PeriodicalExecutor 的按需后台协程 +
// 空闲自退出模型），差异点：
//   - Ticker/时钟可注入，批量时序测试完全确定（无需真实睡眠）；
//   - Execute 统一经 async.RunSafe 隔离 panic，一批失败不影响后续；
//   - 变体精简为 Bulk（满 N 条）/ Chunk（满字节）/ Less（窗口去重）三个高频件。
//
// 常用形态：
//
//	// 攒满 64 条或每 100ms 批量下发
//	exe := batch.NewBulkExecutor(100*time.Millisecond, 64, func(tasks []any) {
//	    flush(tasks)
//	})
//	exe.Add(task)
//	defer exe.Wait() // 进程退出前排空积压
package batch
