package async

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync/atomic"
)

// PanicHandler 是 panic 恢复时的日志处理函数。
// 默认使用标准库 log.Printf。可通过 SetPanicHandler 注入自定义实现（如 logx）。
type PanicHandler func(msg string, stack []byte)

var panicHandler atomic.Pointer[PanicHandler]

func init() {
	defaultHandler := PanicHandler(func(msg string, stack []byte) {
		log.Printf("[async] panic recovered: %s\n%s", msg, string(stack))
	})
	panicHandler.Store(&defaultHandler)
}

// SetPanicHandler 设置全局 panic 恢复日志处理器。
// 应在 main.go 初始化阶段调用，注入 logx 等结构化日志实现。
//
// 用法:
//
//	async.SetPanicHandler(func(msg string, stack []byte) {
//	    logx.Error("goroutine panic", "error", msg, "stack", string(stack))
//	})
func SetPanicHandler(handler PanicHandler) {
	panicHandler.Store(&handler)
}

func callPanicHandler(msg string, stack []byte) {
	fn := panicHandler.Load()
	if fn == nil || *fn == nil {
		return
	}
	// handler 自身 panic 必须就地收编：本函数运行在 GoSafe 的 deferred
	// recover 闭包内，这里再 panic 不会被同一个 defer 恢复，直接打死进程。
	defer func() {
		if r := recover(); r != nil {
			log.Printf("async: panic handler itself panicked: %v", r)
		}
	}()
	(*fn)(msg, stack)
}

// ReportPanic 将一次「已被调用方 recover 的 panic」上报给全局 PanicHandler，
// 供自管 recover 的编排原语（如 syncx.FanInErr 逐槽收编）保持与 GoSafe/RunSafe
// 统一的观测面——handler 注入方（日志/告警）不漏接任何恢复事件。
// msg 形如 "panic recovered: <payload>"；stack 取当前调用栈。
func ReportPanic(r any) {
	callPanicHandler(fmt.Sprintf("panic recovered: %v", r), debug.Stack())
}

// RunSafe 在当前 goroutine 同步运行 fn，panic 被 recover 并经 PanicHandler
// 汇报（不重新 panic）。适合"第三方回调不应打死调用方"的场景，如批量执行器
// 执行用户回调；需要独立 goroutine 用 GoSafe。
func RunSafe(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			callPanicHandler(fmt.Sprintf("%v", r), debug.Stack())
		}
	}()
	fn()
}

// GoSafe launches a goroutine with panic recovery.
func GoSafe(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				callPanicHandler(fmt.Sprintf("%v", r), debug.Stack())
			}
		}()
		fn()
	}()
}

// GoSafeWithErr 启动一个带 panic 恢复的 goroutine 执行 fn。
// fn 返回的 error（含 panic 转换的 error）经 PanicHandler 记录后丢弃——
// 适合"错误只需日志上报"的后台任务；需要收集错误到共享 channel 用
// GoSafeWithErrChannel，需要同步等待/级联取消用 concurrency/group。
func GoSafeWithErr(fn func() error) {
	GoSafe(func() {
		if err := fn(); err != nil {
			callPanicHandler(fmt.Sprintf("task error: %v", err), debug.Stack())
		}
	})
}

// GoWithContext 启动一个携带 ctx 的安全 goroutine（panic 恢复，无错误上报）。
// fn 收到的就是调用方传入的 ctx，取消/超时由调用方管理。
func GoWithContext(ctx context.Context, fn func(ctx context.Context)) {
	GoSafe(func() { fn(ctx) })
}

// GoSafeWithContext 启动一个携带 ctx 的安全 goroutine，fn 返回的 error
// （含 panic 转换的 error）经 PanicHandler 记录。等价 GoWithContext + 错误上报。
func GoSafeWithContext(ctx context.Context, fn func(ctx context.Context) error) {
	GoSafe(func() {
		if err := fn(ctx); err != nil {
			callPanicHandler(fmt.Sprintf("task error: %v", err), debug.Stack())
		}
	})
}

// GoSafeWithErrChannel 启动 fn 并把 panic 与 fn 返回值都发送到共享 channel。
//
// 用于需要收集多个 goroutine 的错误到同一 channel 的场景（如 concurrency/group）。
// 调用方需保证 ch 缓冲足够大（建议至少为参与 goroutine 的数量），避免阻塞。
// 发送始终非阻塞；若 channel 已满或已被调用方关闭，不阻塞、不崩溃，
// 相关 panic 经 PanicHandler 输出。调用方应避免在还有 goroutine 未返回时关闭 ch。
func GoSafeWithErrChannel(ch chan<- error, fn func() error) {
	go func() {
		// send 独立 recover：defer 恢复 fn panic 后若向已关闭 channel 发送，
		// 二次 panic 会绕过同一 defer 直接崩溃进程
		send := func(err error) {
			defer func() {
				if r := recover(); r != nil {
					callPanicHandler(fmt.Sprintf("send to error channel: %v", r), debug.Stack())
				}
			}()
			// 非阻塞发送，避免 channel 满时 goroutine 泄漏
			select {
			case ch <- err:
			default:
			}
		}
		defer func() {
			if r := recover(); r != nil {
				callPanicHandler(fmt.Sprintf("%v", r), debug.Stack())
				send(fmt.Errorf("panic: %v", r))
			}
		}()
		send(fn())
	}()
}

// RunSafeLabeled 与 RunSafe 相同，但 panic 汇报带任务归属标签。
// msg 形如 "task=janitor: boom"，便于按任务维度聚合告警，不依赖堆栈解析。
// task 为空时退化为 RunSafe 的消息格式。
func RunSafeLabeled(task string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			callPanicHandler(labeledMsg(task, r), debug.Stack())
		}
	}()
	fn()
}

// GoSafeLabeled 与 GoSafe 相同，但 panic 汇报带任务归属标签。
// 后台循环 / 心跳 / 清扫等命名任务用本变体，运维可直接按 task 聚合。
func GoSafeLabeled(task string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				callPanicHandler(labeledMsg(task, r), debug.Stack())
			}
		}()
		fn()
	}()
}

// GoSafeWithContextLabeled 启动带 ctx 的安全 goroutine，panic 汇报带任务归属标签。
func GoSafeWithContextLabeled(ctx context.Context, task string, fn func(ctx context.Context)) {
	GoSafeLabeled(task, func() { fn(ctx) })
}

// labeledMsg 组装带任务归属的 panic 消息。
func labeledMsg(task string, r any) string {
	if task == "" {
		return fmt.Sprintf("%v", r)
	}
	return fmt.Sprintf("task=%s: %v", task, r)
}
