package async

import (
	"context"
)

// Runner 携带自己的 PanicHandler 的 GoSafe/RunSafe 家族（#13）。
//
// 服务形态消费方继续用包级函数 + SetPanicHandler（进程入口一次）；
// 库形态消费方（被宿主 import，不能动全局）在包级持有一个 Runner，
// panic 路由到自己的组件 logger，宿主的全局设置不受影响、互不覆盖。
// 零值 Runner 回退全局 handler（与包级函数行为一致）。
//
// 用法：
//
//	var runner = async.Runner{Handler: func(ctx context.Context, r any, stack []byte) {
//	    logx.CtxError(ctx, "agentkit.worker.task_panic", logx.ErrKey, r, "stack", string(stack))
//	}}
//
// （logx.ErrKey 为 oekit/logx 的错误字段常量——错误字段名统一走该常量，
// 禁止手写 "err" 字面量，消费方仓 AGENTS 同款红线，#288。）
//
//	runner.GoSafeLabeled("claim", func() { ... })
//	err := runner.RunSafeErr(func() error { ... })
type Runner struct {
	// Handler 本实例的 panic 汇报函数（签名与全局 PanicHandler 一致，#184）；
	// nil 回退全局 PanicHandler。
	Handler PanicHandler
}

// call 经本实例 handler 汇报一次 panic，是 Runner 全部方法的唯一汇报出口（#164）。
//
// handler 自身 panic 必须就地收编：本函数运行在 recover 闭包内（RunSafe/GoSafe
// 的 deferred recover），这里再 panic 不会被同一个 defer 恢复，直接打死进程——
// 与包级 callPanicHandler 同款双层 recover 防护（#261 起两处共用 safeCall
// 单一实现）。此前六个方法各自直接调 r.handler()，同一个库里「包级 GoSafe 的
// Handler panic 被收编、Runner 的 Handler panic 打死进程」自相矛盾。
// 零值 Runner（Handler == nil）回退全局 handler：callPanicHandler 自带同款防护。
func (r Runner) call(ctx context.Context, v any, stack []byte) {
	if r.Handler == nil {
		callPanicHandler(ctx, v, stack)
		return
	}
	safeCall(func() { r.Handler(ctx, v, stack) })
}

// RunSafe 同步运行 fn，panic 经本实例 handler 汇报（不重新 panic）。
func (r Runner) RunSafe(fn func()) {
	_ = guarded(r.report(context.Background(), ""), fn)
}

// RunSafeErr 同步运行 fn，panic 收编为 *PanicError 返回并经本实例 handler 汇报。
func (r Runner) RunSafeErr(fn func() error) (err error) {
	if pe := guarded(r.report(context.Background(), ""), func() { err = fn() }); pe != nil {
		err = pe
	}
	return
}

// GoSafe 启动带 panic 恢复的 goroutine（汇报走本实例 handler）。
func (r Runner) GoSafe(fn func()) {
	spawnGuarded(r.report(context.Background(), ""), fn)
}

// GoSafeLabeled 带 task 标签的 GoSafe：标签经 panic 汇报的 ctx 携带
// （TaskFromContext 可取，#184 前拼在 msg 前缀 "task=<name>: ..." 里）。
func (r Runner) GoSafeLabeled(task string, fn func()) {
	spawnGuarded(r.report(context.Background(), task), fn)
}

// GoWithContext 启动携带 ctx 的安全 goroutine（汇报走本实例 handler）。
// panic 汇报携带该 ctx（#184：此前经由 GoSafe 委托，ctx 到不了 handler）。
func (r Runner) GoWithContext(ctx context.Context, fn func(ctx context.Context)) {
	spawnGuarded(r.report(ctx, ""), func() { fn(ctx) })
}

// RunSafeRetWith 是 Runner 版的 RunSafeRet（方法不能带类型参数，
// 泛型形态以包级函数提供）。
func RunSafeRetWith[T any](r Runner, fn func() (T, error)) (val T, err error) {
	if pe := guarded(r.report(context.Background(), ""), func() { val, err = fn() }); pe != nil {
		err = pe
	}
	return
}
