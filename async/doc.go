// Package async 提供安全的 goroutine 启动工具。
//
// 所有 enjoydream 后端服务共享此包，防止 goroutine panic 导致进程崩溃。
//
// # 核心特性
//
//   - 自动 recover panic 并记录堆栈
//   - 支持共享 channel 收集错误（GoSafeWithErrChannel）
//   - 一行代码替代 go func(){...}()
//
// # 快速接入
//
// 替代裸 go 关键字：
//
//	async.GoSafe(func() {
//	    doSomething()
//	})
//
// 需要收集多个 goroutine 的错误到同一 channel 时：
//
//	ch := make(chan error, 10)
//	async.GoSafeWithErrChannel(ch, func() error {
//	    return doSomething()
//	})
//
// 错误只需日志上报的后台任务：
//
//	async.GoSafeWithErr(func() error {
//	    return flushBatch(ctx)
//	})
//
// 携带 context 的安全 goroutine：
//
//	async.GoWithContext(ctx, func(ctx context.Context) { doWork(ctx) })
//	async.GoSafeWithContext(ctx, func(ctx context.Context) error { return doWork(ctx) })
//
// # 使用场景
//
//   - 后台任务：日志上报、指标采集、缓存预热
//   - 并发请求：同时调用多个下游服务
//   - 异步处理：消息发送、文件处理
//
// # Panic 处理
//
// panic 被 recover 后会：
//  1. 记录 panic 信息和完整堆栈到日志
//  2. GoSafeWithErrChannel 会将 panic 转为 error 发送到共享 channel
//  3. 不会影响主 goroutine 的正常运行
//
// # 为什么使用标准库 log 而非 logx
//
// 本包是 enjoydream 后端的 panic 兜底层，必须保持零内部依赖（依赖叶子节点）。
// logx 会间接依赖 middleware、ratelimit 等上层包，而这些包又依赖 async，
// 若 async 反向依赖 logx 将形成导入环。因此本包刻意使用标准库 log。
package async
