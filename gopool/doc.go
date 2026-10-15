// Package gopool 自适应 goroutine 池：任务入队后按需惰性拉起 worker，
// 队列积压时扩容、空闲 worker 按 WorkerMaxAge 老化退出、队列满时回退为
// 直接 `go`（不阻塞调用方、不丢任务），全程 panic 被 recover。
//
// 与 async.GoSafe 的分工：GoSafe 每次调用起一个一次性 goroutine（并发上限
// 即调用方自控），适合低频后台任务；gopool 面向高频、突发的大批量任务，
// 用有界 worker 集合摊薄 goroutine 创建与调度开销，给并发数一个软上限
// （MaxIdleWorkers 之上仍会临时超编，只是即用即走）。
//
// 典型接线（业务侧把 panic 汇入 logx）：
//
//	gopool.SetPanicHandler(func(ctx context.Context, r any, stack []byte) {
//	    logx.Error("gopool panic", "error", r, "stack", string(stack))
//	})
//	gopool.Go(func() { ... })                 // 包级默认池
//	p := gopool.NewPool("ingest", gopool.PoolOption{MaxIdleWorkers: 64})
//	p.CtxGo(ctx, func() { ... })              // panic 处理器可拿到 ctx
//
// 设计参考 cloudwego/gopkg/concurrency/gopool（Apache-2.0，kitex 同源），
// 本实现为独立重写（MIT，oekit/concurrency）：worker 惰性拉起、队列满回退、
// 时钟共享的老化检查思路一致；panic 处理器与老化时钟改用 atomic 消除上游的
// 数据竞争，并补充 Close(ctx) 排空语义（ekit 生命周期惯例）。
package gopool
