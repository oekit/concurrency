package async

import (
	"fmt"
	"runtime/debug"
)

// PanicError 是 panic 收编为 error 的统一载体（#12）。
//
// RunSafeErr / RunSafeRet / RunSafeRetLabeled 的 panic 分支、syncx.FanInErr
// 的逐槽收编、singleflight.Flight 的 waiter 错误路径均使用本类型——调用方
// 用 errors.As 即可区分「panic 失败」与「业务错误」，拿到原始 payload 与栈，
// 再按自己的 logger 补挂领域上下文后决定降级或透传。
//
// Error() 文案与历史实现一致（"panic: <Value>"；Labeled 变体
// "task=<Task>: panic: <Value>"），现有字符串匹配零破坏。
type PanicError struct {
	// Value 是 recover() 的原始值。
	Value any
	// Stack 是 panic 时的调用栈（debug.Stack()，与 PanicHandler 收到的同一份）。
	Stack []byte
	// Task 是 Labeled 变体的任务标签；非 Labeled 为空。
	Task string
}

func (e *PanicError) Error() string {
	if e.Task != "" {
		return fmt.Sprintf("task=%s: panic: %v", e.Task, e.Value)
	}
	return fmt.Sprintf("panic: %v", e.Value)
}

// NewPanicError 供包内其他收编点（syncx.FanInErr 逐槽、singleflight waiter）
// 构造统一载体；外部消费方一般不需要——错误已经从 RunSafe*/Slot.Err 拿到。
func NewPanicError(r any) *PanicError {
	return &PanicError{Value: r, Stack: debug.Stack()}
}

// newPanicError 组装 PanicError（内部统一入口，带任务标签）。
func newPanicError(task string, r any) *PanicError {
	return &PanicError{Value: r, Stack: debug.Stack(), Task: task}
}
