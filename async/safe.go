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
