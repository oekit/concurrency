package async

import (
	"context"
	"time"
)

// Spawn 发射**带句柄的旁路任务**（#197）：脱离 parent 生命周期（WithoutCancel
// 语义内置——values 经 Value 链照常继承，#211 修正后与 stdlib 同语义）、
// 保留 trace、panic 收编进结果，返回句柄供限时等待。
//
// 「主路径旁路发一个任务、限时收结果」是请求处理器的高频形态（列表页的旁路
// 统计/预热/降级数据源）。既有原语盖不住它：
//
//   - GoDetached（#155）是 fire-and-forget——不返回句柄，「发出去了还想
//     限时等结果」表达不了；
//   - 手拼 channel 五件套（缓冲 1 的 chan + WithoutCancel + GoSafe +
//     defer send + 限时 receive，git-ferry operation_log 实测 15 行）四处易错：
//     缓冲 <1 且主路径提前 return ⇒ sender 永远阻塞（goroutine 泄漏）；
//     漏 WithoutCancel ⇒ 请求取消连带腰斩旁路任务；结果类型手写 channel
//     元素 ⇒ 类型错误 run-time 才发现；超时后「任务还在跑、结果已丢弃」
//     没有显式语义可观测。
//
// Spawn 把五件套收编成一个泛型调用：
//
//	h := async.Spawn(ctx, "operation-stats", func(ctx context.Context) (Stats, error) {
//	    return svc.Stats(ctx)
//	})
//	list, err := svc.ListOperations(ctx)   // 主路径并行跑
//	stats, serr, timedOut := h.WaitUpTo(context.Background(), 5*time.Second)
//	if timedOut {
//	    stats = degradedStats()            // 旁路超时 ⇒ 降级数据源（任务继续跑完，结果丢弃）
//	} else if serr != nil {
//	    stats = degradedStats()            // 旁路失败 ⇒ 降级数据源
//	}
//
// 收编与日志口径：fn 的 panic 经全局 PanicHandler 汇报（新签名 ctx/r/stack，
// #184——汇报 ctx 为富化后的后台 ctx 并携带 task 标签，与 GoDetached 同口径）
// 并收编为 *PanicError 从 Wait/WaitUpTo 返回（errors.As 可辨）。fn 为 nil 时
// 同样走 panic 收编路径（runtime 「nil 函数调用」panic 转结果），不另设特判。
//
// 与既有原语的分工：
//
//	GoSafeWithContext(Labeled) = 跟随请求生命周期（请求取消即止），无结果句柄；
//	GoDetached                 = 与请求解耦但可追踪，fire-and-forget，无结果句柄；
//	Spawn                      = 与请求解耦 + 可等待句柄（限时 join 主路径）。
func Spawn[T any](ctx context.Context, task string, fn func(ctx context.Context) (T, error)) *Handle[T] {
	bg := DetachedContext(ctx)
	h := &Handle[T]{done: make(chan struct{})}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// 收编口径对齐 RunSafeRetLabeled（panic → *PanicError 进结果），
				// 但汇报 ctx 透传富化后的后台 ctx（GoDetached 同款，#184）。
				pe := newPanicError(task, r)
				callPanicHandler(withTask(bg, task), r, pe.Stack)
				h.err = pe
			}
			// done 的 close 是结果字段的发布点：val/err 的全部写入都发生在
			// 本 defer 前，等待方经 <-done 获得可见性（channel close 的
			// happens-before），无需额外锁。
			close(h.done)
		}()
		h.val, h.err = fn(bg)
	}()
	return h
}

// Handle 是 Spawn 旁路任务的等待句柄。Done/Wait/WaitUpTo 并发安全，
// 可多调用方同时等待同一份结果（fan-in join 形态）。
// T 为旁路任务的返回值类型——channel 元素类型的编译期对位由泛型承担，
// 「结果类型错了 run-time 才发现」这类手拼问题不再存在。
type Handle[T any] struct {
	done chan struct{}
	val  T
	err  error
}

// Done 返回完成信号 channel（任务完成或 fn panic 后关闭），供调用方把
// 「旁路完成」对接进自己的 select（与 Wait/WaitUpTo 并用不冲突）。
func (h *Handle[T]) Done() <-chan struct{} {
	return h.done
}

// Wait 等到旁路任务完成，返回结果与错误；ctx 取消/超时（含包装链）提前
// 返回 (零值, ctx.Err())——**任务不会因此取消**（Spawn 已脱离 ctx 生命周期，
// 这只是放弃等待），任务继续跑完、结果丢弃。
//
// ctx 为 nil 按 context.Background() 处理（与 syncx/retry 等包同口径）。
// 任务已完成且 ctx 恰好也已取消时，返回已完成的结果而非 ctx.Err()
// （结果已就位时取消没有意义）。
func (h *Handle[T]) Wait(ctx context.Context) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-h.done:
		return h.val, h.err
	default:
	}
	select {
	case <-h.done:
		return h.val, h.err
	case <-ctx.Done():
		// 结果与取消同刻就绪时偏向结果（非阻塞补看一眼，抢不到才算放弃）
		select {
		case <-h.done:
			return h.val, h.err
		default:
		}
		var zero T
		return zero, ctx.Err()
	}
}

// WaitUpTo 限时等待旁路任务：
//
//   - d 内完成 ⇒ (结果, 错误, false)；
//   - d 内未完成 ⇒ (零值, nil, true)——timedOut 标记把「放弃后结果丢弃」
//     的语义显式化：任务继续在后台跑完（不受放弃影响、不会因超时被腰斩），
//     超时的这次调用不再收到结果；Handle 本身不关闭、不取消，**后来的**
//     Wait/WaitUpTo（fan-in join 的其它等待方）仍能拿到真实完成结果——
//     「迟到」只对放弃等待的那一路生效；
//   - ctx 先取消 ⇒ (零值, ctx.Err(), false)——与 Wait 的放弃等待同语义，
//     注意此时 timedOut 为 false（是「取消」不是「超时」，两条失败路径
//     调用方要能区分：超时可降级重试，取消应当级联收手）。
//
// d <= 0 退化为非阻塞探活：已完成返回结果，否则立即 (零值, nil, true)。
// ctx 为 nil 按 context.Background() 处理。
//
// 返回值序 (T, error, bool) 中 bool 置尾是刻意为之（区别于惯用的 ok 在尾
// 的单返回布尔）：timedOut 与 error 是**并列的两种「没拿到结果」**——
// timedOut=true 时 err 恒为 nil，调用方先判 timedOut 再判 err。
func (h *Handle[T]) WaitUpTo(ctx context.Context, d time.Duration) (T, error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	var timerC <-chan time.Time
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		timerC = timer.C
	}
	select {
	case <-h.done:
		return h.val, h.err, false
	default:
	}
	if timerC == nil {
		// d<=0：非阻塞探活落空 ⇒ 立即超时
		var zero T
		return zero, nil, true
	}
	select {
	case <-h.done:
		return h.val, h.err, false
	case <-timerC:
		// 结果与超时同刻就绪时偏向结果
		select {
		case <-h.done:
			return h.val, h.err, false
		default:
		}
		var zero T
		return zero, nil, true
	case <-ctx.Done():
		select {
		case <-h.done:
			return h.val, h.err, false
		default:
		}
		var zero T
		return zero, ctx.Err(), false
	}
}
