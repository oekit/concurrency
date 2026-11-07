package poll

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/concurrency/retry"
)

// ErrCursorStuck 拉取返回了**非空批次**，却没让游标前进。
//
// 这不是「暂时没数据」，而是调用方 fetch 实现的契约违反：下一轮会用同一个游标
// 拉到同一批，于是同一批被无限重复处理（同时把 CPU 打满）。它不会自愈，
// 故 RunCursor 带着本错误**返回**而不是退避重试——让它在第一次发生时就可见。
// errors.Is 可辨（错误串里带当前游标与本批条数）。
var ErrCursorStuck = errors.New("poll: 拉取返回非空批次但游标未前进")

// Config 长轮询批量拉取循环的参数。零值可用（立即拉取、失败固定零退避、空批立即重拉）。
type Config struct {
	// ErrorBackoff 失败续拉的退避配置，复用 retry.Config 的**全部**口径：
	// Delay/Linear/Exponential/MaxDelay/Jitter/JitterRatio/DelayHint。
	//
	// 唯一不参与的是 Attempts——本原语是常驻循环，失败会一直续拉到 ctx 取消，
	// 「要不要继续」由 Retryable 而非次数决定（见 Retryable 字段说明）。
	ErrorBackoff retry.Config
	// Retryable 判定拉取错误是否可续拉；nil = 全部可续拉。
	// 返回 false 的错误会让 RunCursor **带着该错误返回**（如鉴权失效、游标被服务端
	// 作废——这类错误重试一万次也不会好，只会把日志刷满）。
	// 优先级高于 ErrorBackoff.Retryable（这里的是循环级口径）。
	Retryable func(error) bool
	// IdleDelay 空批等待。拉取成功但返回 0 项、且游标没变化时，等这么久再拉下一批。
	//
	// <=0 = 立即再拉，正确性上只适用于**服务端 hold**（长轮询：请求会挂到有数据或
	// 超时才返回）的接口。服务端不 hold 的接口必须给正值，否则就是忙轮询——
	// 这一点没有安全的缺省值：hold 与不 hold 用同一个数字必然有一边是错的。
	IdleDelay time.Duration
	// AdvanceOnItemError 单项处理失败时**仍然**推进游标。
	//
	// false（缺省）= 本批不推进游标，退避后重拉同一批。语义是 at-least-once：
	// 已处理过的项会再处理一次（handle 须按项幂等），未处理的项不会被跳过。
	// 这是缺省档——丢消息比重复处理难查得多。
	//
	// true = 失败项由 handle 自己落账（死信表/重试表），库处理后继续、整批处理完
	// 推进游标。本档要求 handle 承担落账责任：库不会把该项错误再交给任何人。
	// 选本档的理由只能是「重复投递的代价高于漏处理」，例如平台侧消息 id 不可去重。
	AdvanceOnItemError bool
	// OnAdvance 游标推进回调：库在**推进之后**同步调用一次，供调用方持久化
	// （各消费方的存储不同，库不碰）。返回错误 ⇒ 视为本轮失败：游标退回旧值、
	// 按 ErrorBackoff 退避后重拉同一批——持久化不上却继续前进，重启后就会从
	// 一个从未落盘的游标继续，等于凭记忆跳读。
	// nil = 不做持久化（内存游标，进程重启即重置）。
	OnAdvance func(ctx context.Context, cursor string) error
}

// RunCursor 运行「带单调游标的长轮询批量拉取」循环，直到 ctx 取消或发生不可续拉的错误。
//
// 返回**当前游标**与退出原因：ctx 取消 ⇒ ctx.Err()；不可续拉 ⇒ 该错误；
// 契约违反 ⇒ ErrCursorStuck。调用方拿返回的游标做关机时的最后一次持久化。
//
// # 为什么需要它
//
// IM / 消息平台的收消息循环是同一个形状，逐平台复制：带游标拉一批 → 出错等固定
// 几秒 → 推进游标并持久化 → 逐条处理。复制出来的副本在三个地方各自漂移：
// 退避是手写的固定值（无指数、无抖动 ⇒ 两个平台同时撞故障会同步重试，把下游
// 按同一节奏再打一遍）；游标推进时机各写各的（先推进后处理 = 崩溃丢消息，先处理
// 后推进 = 崩溃重复投递，两版混在生产里）；批内一条坏数据 panic 打死整个循环。
//
// 本原语把这四件事钉死成一份实现：
//
//  1. **游标只前进**——非空批次必须带回不同游标（否则 ErrCursorStuck）；
//  2. **处理完才推进**——本批 handle 全过（或按 AdvanceOnItemError 落账）才推进
//     游标，语义恒为 at-least-once，不会出现「游标跑了、消息没处理」；
//  3. **失败退避可指数可抖动**——复用 retry 的退避公式与 DelayHint，不再是固定
//     几秒；不可续拉的错误直接带错误返回，不空转；
//  4. **单项 panic 不杀循环**——每项经 async.RunSafeCtx 就地收编成 *PanicError
//     （同时汇报全局 PanicHandler），循环存活、按失败档处理该项。
//
// # 语义契约
//
//   - fetch 同步执行，ctx 原样传入（长轮询要能被取消）；fetch panic 收编后当作
//     一次可续拉失败。
//   - fetch 返回 **0 项**：游标允许不变（没新数据），此时按 IdleDelay 等待；
//     next 非空且与当前不同则视为服务端把游标推到了当前位置，照常推进。
//   - fetch 返回 **非空批**：游标必须前进（见 ErrCursorStuck）。
//   - handle 逐项**串行**调用（不并发：消息处理通常有顺序语义），每项独立收编 panic。
//   - 缺省档下某一项失败即**停止处理本批**（后面的项留到重拉时再处理，不会被跳过）、
//     不推进游标、按退避重拉。
//   - 退避等待期间 ctx 取消 ⇒ 立即返回（不睡满）。
//   - 续拉失败次数在成功一轮后归零（退避不会一路涨到 MaxDelay 卡死）。
//
// # 用法
//
//	cursor, _ := store.Load(ctx) // 各自存储
//	final, err := poll.RunCursor(ctx, poll.Config{
//	    ErrorBackoff: retry.Config{Delay: 200 * time.Millisecond, Exponential: true, MaxDelay: 5 * time.Second, JitterRatio: 0.2},
//	    IdleDelay:    0, // 服务端 hold 的自适应长轮询
//	    OnAdvance:    store.Save,
//	}, cursor, fetchBatch, handleOne)
//	if err != nil && !errors.Is(err, context.Canceled) {
//	    log.Error("收消息循环退出", "err", err)
//	}
func RunCursor[T any](
	ctx context.Context,
	cfg Config,
	from string,
	fetch func(ctx context.Context, cursor string) (items []T, next string, err error),
	handle func(ctx context.Context, item T) error,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if fetch == nil {
		panic("poll: fetch 不能为 nil——没有拉取函数的循环只会空转")
	}
	if handle == nil {
		panic("poll: handle 不能为 nil——静默消费掉已拉到的批次是比 panic 更坏的失败形态")
	}

	cursor := from
	failures := 0

	for {
		if err := ctx.Err(); err != nil {
			return cursor, err
		}

		items, next, fetchErr := fetchBatch(ctx, fetch, cursor)
		if fetchErr != nil {
			if !retryable(cfg, fetchErr) {
				return cursor, fetchErr
			}
			failures++
			if err := waitBackoff(ctx, cfg.ErrorBackoff, failures, fetchErr); err != nil {
				return cursor, err
			}
			continue
		}

		if len(items) == 0 {
			// 空批：游标通常不动；但有的服务端会把游标推到「当前读到的位置」，
			// 那种形态下 next 会变，照常推进（否则下次重启会重扫）。
			if next != "" && next != cursor {
				advanced, advErr := advance(ctx, cfg, cursor, next)
				if advErr != nil {
					failures++
					if err := waitBackoff(ctx, cfg.ErrorBackoff, failures, advErr); err != nil {
						return cursor, err
					}
					continue
				}
				cursor = advanced
				failures = 0
			}
			if cfg.IdleDelay > 0 {
				if err := sleepCtx(ctx, cfg.IdleDelay); err != nil {
					return cursor, err
				}
			}
			continue
		}

		// 非空批必须带回**不同**游标，否则下一轮重复拉同一批（忙轮询 + 无限重复）。
		if next == "" || next == cursor {
			return cursor, fmt.Errorf("%w（当前游标 %q，本批 %d 项）", ErrCursorStuck, cursor, len(items))
		}

		if itemErr := dispatch(ctx, cfg, items, handle); itemErr != nil {
			failures++
			if err := waitBackoff(ctx, cfg.ErrorBackoff, failures, itemErr); err != nil {
				return cursor, err
			}
			continue
		}

		advanced, advErr := advance(ctx, cfg, cursor, next)
		if advErr != nil {
			failures++
			if err := waitBackoff(ctx, cfg.ErrorBackoff, failures, advErr); err != nil {
				return cursor, err
			}
			continue
		}
		cursor = advanced
		failures = 0
	}
}

// dispatch 逐项串行执行 handle，每项独立收编 panic。
//
// 缺省档下第一项失败即返回该错误（停止处理本批）——继续处理后面的项意味着重拉时
// 每一项都要再处理一遍（已成功的项被重复执行，副作用翻倍），而停在失败处则
// 「已处理的再处理一次、未处理的一项不丢」，重复面最小。
//
// AdvanceOnItemError 档下必须处理完**整批**：本档会推进游标，中途返回会让本批
// 剩余项再也不会被拉回来（这是真正的丢数据）。
func dispatch[T any](ctx context.Context, cfg Config, items []T, handle func(context.Context, T) error) error {
	for _, item := range items {
		err := handleSafe(ctx, handle, item)
		if err == nil {
			continue
		}
		if !cfg.AdvanceOnItemError {
			return err
		}
		// 本档由 handle 自己落账；库这边只保证「本批跑完再推进」。
	}
	return nil
}

// handleSafe 就地收编单项处理的 panic，转成 *async.PanicError 返回。
//
// 不用裸 defer recover：收编后必须走全局 PanicHandler，否则「哪条消息把 worker
// 打崩了」这件事只留在本函数的返回值里，谁也没看见。
func handleSafe[T any](ctx context.Context, handle func(context.Context, T) error, item T) error {
	var itemErr error
	if panicErr := async.RunSafeCtx(ctx, func(inner context.Context) {
		itemErr = handle(inner, item)
	}); panicErr != nil {
		return panicErr
	}
	return itemErr
}

// fetchBatch 拉一批，fetch panic 收编成 *async.PanicError（当作一次可续拉失败）。
func fetchBatch[T any](
	ctx context.Context,
	fetch func(ctx context.Context, cursor string) (items []T, next string, err error),
	cursor string,
) ([]T, string, error) {
	var (
		items  []T
		next   string
		errBuf error
	)
	if panicErr := async.RunSafeCtx(ctx, func(inner context.Context) {
		items, next, errBuf = fetch(inner, cursor)
	}); panicErr != nil {
		return nil, "", panicErr
	}
	return items, next, errBuf
}

// advance 推进到 next：先问调用方能不能持久化，落盘成功才算推进。
//
// 顺序是刻意的：先落盘再返回新游标。若反过来（内存先推进、落盘失败再说），
// 失败那一刻的返回值已经带着一个没落盘的游标——调用方拿它做关机持久化就把
// 「凭记忆跳读」写进了存储。
func advance(ctx context.Context, cfg Config, cur, next string) (string, error) {
	if cfg.OnAdvance == nil {
		return next, nil
	}
	var advErr error
	if panicErr := async.RunSafeCtx(ctx, func(inner context.Context) {
		advErr = cfg.OnAdvance(inner, next)
	}); panicErr != nil {
		return cur, panicErr
	}
	if advErr != nil {
		return cur, advErr
	}
	return next, nil
}

// retryable 循环级可续拉判定：Config.Retryable 优先于 ErrorBackoff.Retryable。
func retryable(cfg Config, err error) bool {
	if cfg.Retryable != nil {
		return cfg.Retryable(err)
	}
	return cfg.ErrorBackoff.Retryable == nil || cfg.ErrorBackoff.Retryable(err)
}

// waitBackoff 按退避公式等待一次；d <= 0 时不睡但仍探一次 ctx（零退避不能
// 变成「取消后继续空转一轮 fetch」）。
func waitBackoff(ctx context.Context, cfg retry.Config, failures int, cause error) error {
	// 长驻循环的失败次数可以累积得很大，先夹到 2^20 再交给公式：Linear 档的
	// attempt*Delay 在 attempt 极大时会把 duration 乘成负数，负延迟等于零等待
	// ⇒ 退化成忙轮询（与 retry #55 / #136 同一种失效形态：负延迟既绕过封顶
	// 又让该轮零等待）。夹住之后 Linear 档最坏 2^20 × Delay，不会溢出。
	const maxBackoffAttempt = 1 << 20
	if failures > maxBackoffAttempt {
		failures = maxBackoffAttempt
	}
	delay := retry.Delay(cfg, failures, cause)
	if delay <= 0 {
		return ctx.Err()
	}
	return sleepCtx(ctx, delay)
}

// sleepCtx 可被取消的等待（不用 time.Sleep：取消后要能立刻返回）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
