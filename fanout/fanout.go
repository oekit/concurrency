package fanout

import (
	"context"
	"fmt"
	"strings"

	"github.com/oekit/concurrency/syncx"
)

// ItemError 是单个输入项执行失败时的错误。
//
// 下标是它存在的理由：Map 的返回值是 ([]R, error) 形态，R 里放不下「这一项失败
// 了」的信息——失败项的 R 是零值。带上 Index，调用方才能把失败定位回自己的输入
// 清单（哪台机器、哪个文件、哪条记录）。
type ItemError struct {
	// Index 是该失败项在输入切片里的下标。
	Index int
	// Err 是该项的原始错误：fn 返回的错误，或 fn panic 被收编后的 *async.PanicError。
	Err error
}

// Error 实现 error。
func (e *ItemError) Error() string {
	return fmt.Sprintf("第 %d 项失败: %v", e.Index, e.Err)
}

// Unwrap 让 errors.Is / errors.As 能识别原始错误。
func (e *ItemError) Unwrap() error { return e.Err }

// Error 是 Map 在有项失败时返回的汇总错误。
//
// 失败项按输入序排列；ctx 取消时**未开始执行**的项也以 context.Canceled 记入
// Failed（errors.Is(err, context.Canceled) 可辨），这样调用方既知道「整体被取消」，
// 也知道「哪些项根本没跑」。
type Error struct {
	// Failed 是失败项列表（按输入序）。
	Failed []ItemError
}

// Error 实现 error。消息里最多列前三项，其余只给计数——错误串常进日志，
// N 项全列会把日志撑爆；结构化的完整列表在 Failed 字段里。
func (e *Error) Error() string {
	const maxShown = 3
	parts := make([]string, 0, maxShown)
	for i, f := range e.Failed {
		if i >= maxShown {
			break
		}
		parts = append(parts, f.Error())
	}
	msg := fmt.Sprintf("%d 项失败", len(e.Failed))
	if len(parts) == 0 {
		return msg
	}
	if rest := len(e.Failed) - len(parts); rest > 0 {
		return fmt.Sprintf("%s（%s；另有 %d 项）", msg, strings.Join(parts, "; "), rest)
	}
	return fmt.Sprintf("%s（%s）", msg, strings.Join(parts, "; "))
}

// Unwrap 返回每个失败项的原始错误，让 errors.Is / errors.As 能穿透到任意一项
// （Go 1.20+ 的多错误解包）。
func (e *Error) Unwrap() []error {
	out := make([]error, 0, len(e.Failed))
	for _, f := range e.Failed {
		out = append(out, f.Err)
	}
	return out
}

// Map 以最多 concurrency 个并发执行 fn 于 items 的每个元素，结果**按输入顺序**返回。
//
// 语义：
//   - 并发上限：同时在跑的 fn 不超过 concurrency 个（concurrency <= 0 ⇒ 不设上限，
//     与 syncx.FanInLimit 的 limit<=0 口径一致；concurrency 大于项数时以项数为准）。
//   - 顺序：results[i] 恒对应 items[i]，与完成先后无关。
//   - 失败：一项失败**不**取消其余项——其余项照常跑完；所有失败项在最后汇总成
//     一个 *Error 返回（含下标）。
//   - ctx 取消：ctx 结束即停止调度**未开始**的项（这些项记为 context.Canceled
//     失败），已在跑的项等它自己结束（fn 自己决定是否 ctx-aware）。
//   - panic：fn panic 被收编进该项的失败（*async.PanicError），不影响其余项。
//   - 空输入：返回 nil 结果与 nil 错误。
//
// 返回值 results 恒与 items 等长；失败项的 R 为零值，失败原因在 err 里。
// fn 为 nil 时不会逃逸 panic：nil 函数调用被引擎的逐项 recover 收编成该项的
// 失败（*async.PanicError），与普通失败项一样汇总返回。
//
// 底层引擎是 syncx.FanInLimitCtx（固定 worker 池 + 原子取号 + panic 收编 + ctx
// fail-fast），本函数在其上补 item 口径与错误汇总。
func Map[T, R any](ctx context.Context, items []T, concurrency int,
	fn func(ctx context.Context, item T) (R, error)) ([]R, error) {
	if len(items) == 0 {
		return nil, nil
	}

	slots := syncx.FanInLimitCtx(ctx, len(items), concurrency,
		func(ctx context.Context, i int) (R, error) {
			return fn(ctx, items[i])
		})

	results := make([]R, len(items))
	failed := make([]ItemError, 0, len(slots))
	for i, s := range slots {
		results[i] = s.Val
		if s.Err != nil {
			failed = append(failed, ItemError{Index: i, Err: s.Err})
		}
	}
	if len(failed) > 0 {
		return results, &Error{Failed: failed}
	}
	return results, nil
}
