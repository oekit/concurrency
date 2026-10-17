package async

import (
	"context"
	"fmt"
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
//	var runner = async.Runner{Handler: func(msg string, stack []byte) {
//	    logx.Error("agentkit.worker.task_panic", "msg", msg, "stack", string(stack))
//	}}
//
//	runner.GoSafeLabeled("claim", func() { ... })
//	err := runner.RunSafeErr(func() error { ... })
type Runner struct {
	// Handler 本实例的 panic 汇报函数；nil 回退全局 PanicHandler。
	Handler PanicHandler
}

// handler 返回生效的汇报函数（nil 安全）。
func (r Runner) handler() PanicHandler {
	if r.Handler != nil {
		return r.Handler
	}
	return callGlobalPanicHandler
}

// RunSafe 同步运行 fn，panic 经本实例 handler 汇报（不重新 panic）。
func (r Runner) RunSafe(fn func()) {
	defer func() {
		if rc := recover(); rc != nil {
			pe := newPanicError("", rc)
			r.handler()(fmt.Sprintf("%v", rc), pe.Stack)
		}
	}()
	fn()
}

// RunSafeErr 同步运行 fn，panic 收编为 *PanicError 返回并经本实例 handler 汇报。
func (r Runner) RunSafeErr(fn func() error) (err error) {
	defer func() {
		if rc := recover(); rc != nil {
			pe := newPanicError("", rc)
			r.handler()(fmt.Sprintf("%v", rc), pe.Stack)
			err = pe
		}
	}()
	return fn()
}

// GoSafe 启动带 panic 恢复的 goroutine（汇报走本实例 handler）。
func (r Runner) GoSafe(fn func()) {
	go func() {
		defer func() {
			if rc := recover(); rc != nil {
				pe := newPanicError("", rc)
				r.handler()(fmt.Sprintf("%v", rc), pe.Stack)
			}
		}()
		fn()
	}()
}

// GoSafeLabeled 带 task 标签的 GoSafe（msg 形如 "task=janitor: ..."）。
func (r Runner) GoSafeLabeled(task string, fn func()) {
	go func() {
		defer func() {
			if rc := recover(); rc != nil {
				pe := newPanicError(task, rc)
				r.handler()(labeledMsg(task, rc), pe.Stack)
			}
		}()
		fn()
	}()
}

// GoWithContext 启动携带 ctx 的安全 goroutine（汇报走本实例 handler）。
func (r Runner) GoWithContext(ctx context.Context, fn func(ctx context.Context)) {
	r.GoSafe(func() { fn(ctx) })
}

// RunSafeRetWith 是 Runner 版的 RunSafeRet（方法不能带类型参数，
// 泛型形态以包级函数提供）。
func RunSafeRetWith[T any](r Runner, fn func() (T, error)) (val T, err error) {
	defer func() {
		if rc := recover(); rc != nil {
			pe := newPanicError("", rc)
			r.handler()(fmt.Sprintf("%v", rc), pe.Stack)
			err = pe
		}
	}()
	return fn()
}

// callGlobalPanicHandler 委托全局 handler（零值 Runner 的回退目标）。
func callGlobalPanicHandler(msg string, stack []byte) {
	callPanicHandler(msg, stack)
}
