package async

import (
	"context"
)

// RunSafeCtx 在当前 goroutine **同步**运行 fn，并把 panic 收编成 *PanicError 返回。
//
// # 它补的是哪个缺口
//
// 三个既有形态都装不下「在别人的 goroutine 边界上就地收编 panic」：
//
//   - GoSafe / RunSafe：不返回任何值 ⇒ 调用方拿不到这次执行的结局；
//   - GoSafeWithContext：新开 goroutine，同样拿不到结局（#184 起 panic 汇报
//     虽已携带 ctx，结局仍不可知）；
//   - RunSafeErr：能返回 error，但 fn 收不到 ctx ⇒ 收编点自己记不了 ctx 维度的账。
//
// 典型用途是**第三方 SDK 回调**：ws 客户端的读循环在 SDK 自己的 goroutine 里分发
// 事件，外层 GoSafe/Supervise 覆盖不到它，panic 必须在回调内就地收编，否则一条
// 坏事件就打死整个进程；而回调里又确实需要 ctx（trace/租户/超时）才能把这条
// panic 记得有用。
//
// # 语义
//
//   - fn 正常返回 ⇒ 返回 nil。
//   - fn panic（含 fn 为 nil 的调用）⇒ 返回 *PanicError（带原始 Value 与 Stack），
//     errors.As 可辨；**同时**照 RunSafeErr 的口径向全局 PanicHandler 汇报一份——
//     全局观测面不能因为「调用方自己也要记一笔」而漏掉这类事件。
//   - 本函数不开 goroutine、不做 ctx 取消判定：ctx 原样交给 fn，生命周期由调用方管。
//
// 用法：
//
//	err = async.RunSafeCtx(ctx, func(ctx context.Context) {
//	    handleEvent(ctx, ev) // SDK 回调体
//	})
//	if pe := (*async.PanicError)(nil); errors.As(err, &pe) {
//	    log.ErrorContext(ctx, "事件处理 panic", "panic", pe.Value) // 带 ctx 记日志
//	}
func RunSafeCtx(ctx context.Context, fn func(context.Context)) (panicErr error) {
	if pe := guarded(globalReport(ctx, ""), func() { fn(ctx) }); pe != nil {
		panicErr = pe
	}
	return panicErr
}

// RunSafeCtxRet 同 RunSafeCtx，但 fn 带返回值——panic 时返回零值与 *PanicError。
//
// 单独一档而不是让 RunSafeCtx 泛型化：两者签名不同（一个无返回、一个 (T, error)），
// 合成一个会让「只要收编不要值」的调用点被迫写泛型实参。语义与 RunSafeRet 同款，
// 只多了 ctx 透传与「panic 也向全局汇报」。
func RunSafeCtxRet[T any](ctx context.Context, fn func(context.Context) (T, error)) (val T, err error) {
	if pe := guarded(globalReport(ctx, ""), func() { val, err = fn(ctx) }); pe != nil {
		err = pe
	}
	return
}
