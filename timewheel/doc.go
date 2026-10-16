// Package timewheel 提供单级时间轮：海量短延迟定时任务的高效调度结构，
// 加延迟/移动/删除均为 O(1)，触发扫描均摊 O(1)（每 tick 只扫一个槽位）。
//
// 设计参考 go-zero collection.TimingWheel（单级时间轮 + 圈数 circle 处理
// 超出一轮的延迟），差异点：
//   - 任务类型收敛为 func()（类型安全，触发经 async.RunSafe 隔离 panic）；
//   - Ticker 可注入（WithWheelTicker），时序测试完全确定；
//   - 生命周期遵循本仓硬规则：Close(ctx) error 幂等关闭。
//
// 与 timertask 仓的分工：本包是纯进程内延迟调度（无持久化、无分布式分片）；
// 需要持久化/死信/分片认领请用 github.com/oekit/timertask。
//
// 用法:
//
//	tw := timewheel.NewTimingWheel(100*time.Millisecond, 64)
//	defer tw.Close(context.Background())
//	tw.SetTimer("session:42", func() { expireSession(42) }, 30*time.Second)
package timewheel
