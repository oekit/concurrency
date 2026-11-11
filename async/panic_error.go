package async

import (
	"fmt"
	"runtime/debug"

	pkgerrors "github.com/oekit/pkg/errors"
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
//
// 与 org panic 收编单源（pkg/errors.PanicError，#381）的分工：捕获与转换
// 全部走 pkg errors.Recovered（全矩阵唯一实现——nil 判定、栈采集、error
// 身份内建）；本类型是本仓的兼容载体，额外携带 Task 标签并保留历史 Error()
// 文案，Unwrap（#267）与 pkg 同口径。
type PanicError struct {
	// Value 是 recover() 的原始值。
	Value any
	// Stack 是 panic 时的调用栈（pkg errors.Recovered 采集，与 PanicHandler
	// 收到的同一份）。
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

// Unwrap 返回 panic 值中的 error（panic 值不是 error 时返回 nil）（#267）：
// 业务侧 panic(err)（断言失败、require 类 panic 的常见形态）经收编后，
// 调用方仍能 errors.Is / errors.As 穿透本类型直达原始错误——错误身份全链
// 不再丢，与 pkg errors.PanicError 及同簇 fanout.ItemError 的 Unwrap 纪律
// 对齐。
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// NewPanicError 供包内其他收编点（syncx.FanInErr 逐槽、singleflight waiter）
// 构造统一载体；外部消费方一般不需要——错误已经从 RunSafe*/Slot.Err 拿到。
func NewPanicError(r any) *PanicError {
	return newPanicError("", r)
}

// newRecoveredPanicError 把 recover() 的返回值经 org 单源收编为本仓载体
// （#261）：pkg errors.Recovered 负责 r==nil 判定、栈采集与 error 身份
// 内建（全矩阵唯一实现），本函数只补 Task 标签。r 为 nil（无 panic）返回
// nil——defer 惯用法直接 `if pe := newRecoveredPanicError(recover()); pe != nil`。
func newRecoveredPanicError(task string, r any) *PanicError {
	perr := pkgerrors.Recovered(r)
	if perr == nil {
		return nil
	}
	// Recovered 非 nil 时恒为 *pkgerrors.PanicError
	pe, ok := perr.(*pkgerrors.PanicError)
	if !ok { // 防御：单源契约变更时退回裸构造，不 panic
		return &PanicError{Value: r, Stack: debug.Stack(), Task: task}
	}
	return &PanicError{Value: pe.Value, Stack: pe.Stack, Task: task}
}

// newPanicError 组装 PanicError（内部统一入口，带任务标签）。
func newPanicError(task string, r any) *PanicError {
	if pe := newRecoveredPanicError(task, r); pe != nil {
		return pe
	}
	// r 为 nil 的显式构造（Recovered 的 nil 语义是「没有 panic」，
	// 与本入口的「构造一个 Value 为 nil 的载体」不同）——保持历史行为
	return &PanicError{Stack: debug.Stack()}
}
