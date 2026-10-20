// Package retry 可取消的重试与轮询原语（stdlib-only）：带退避的动作重试 +
// 到 deadline 为止的条件轮询。收编自消费方三类手写形态——SQLite busy 退避
// 重试（线性递增）、双副本首启的固定间隔回读、并发败者/终态等待的条件轮询。
package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrTimeout Poll 超时（调用方按需区分「条件未达成」与真错误）。
var ErrTimeout = errors.New("retry: 轮询超时")

// Config 重试参数。零值 = 只执行一次、不重试。
type Config struct {
	// Attempts 总尝试次数（含首次）；<=1 视为 1。
	Attempts int
	// Delay 退避基数。Linear=false 固定间隔；Linear=true 第 i 次失败后等待 Delay*i。
	// Exponential=true 时第 i 次失败后等待 Delay*2^(i-1)（见 Exponential）。
	Delay time.Duration
	// MaxDelay 单次退避上限（<=0 不限）。
	MaxDelay time.Duration
	// Linear true=线性递增退避（Delay*i），false=固定间隔。
	// 与 Exponential 同时为 true 时 Exponential 优先。
	Linear bool
	// Exponential true=指数退避 Delay*2^(attempt-1)，受 MaxDelay 封顶。
	// 外部频控（429/5xx）的标准应对姿势。
	Exponential bool
	// Jitter 可选抖动：每次退避在计算结果上叠加 [0, Jitter) 随机量。
	// 0 = 关闭。用于打散同时失败的重试风暴。
	// JitterRatio > 0 时本字段不生效（比例抖动优先，见 JitterRatio）。
	Jitter time.Duration
	// JitterRatio 按当次退避幅度的比例抖动（单边向上）：最终退避 ∈ [d, d*(1+ratio))。
	// 固定 Jitter 在高 attempt 档占比趋零（指数退避下 32s 档 500ms 只占 1.5%），
	// 打散重试风暴失效——比例抖动让占比恒定。>0 时优先于固定 Jitter；0 = 关闭。
	JitterRatio float64
	// DelayHint 从本次错误提取服务端退避建议（如 429/503 的 Retry-After）；
	// ok=true 时建议值取代计算退避，仍受 MaxDelay 封顶（避免服务端小时级建议
	// 把调用方钉死）；ok=false 或 nil 走计算退避。
	// 「仅限流失败采信建议」等门控放在实现里（配合 retry/httpx 的
	// RetryAfterHint 一行接入）。
	DelayHint func(err error) (d time.Duration, ok bool)
	// Retryable 判定错误是否可重试；nil = 所有错误都重试。
	Retryable func(error) bool
}

// Do 执行 fn 至多 Attempts 次，失败按 Config 退避后重试；返回最后一次错误。
// 退避等待感知 ctx 取消（nil 视为 context.Background()）。
//
// 实现上**委托给 DoValue**——退避序列、抖动比例、MaxDelay 封顶、DelayHint 优先、
// Retryable 门控只有一份实现。若这里另写一份循环，任何退避规则的改动都要改两处，
// 早晚会漏（#48 的根因正是「同一个仓里两份实现」）。
func Do(ctx context.Context, cfg Config, fn func(attempt int) error) error {
	_, err := DoValue(ctx, cfg, func(attempt int) (struct{}, error) {
		return struct{}{}, fn(attempt)
	})
	return err
}

// DoValue 是 Do 的泛型伴生：执行 fn 至多 Attempts 次，失败按 Config 退避后重试；
// 成功返回 fn 的结果与 nil，失败返回**零值**与最后一次错误。
//
// 与 Do 同源——Do 即本函数的薄包装，因此「调用方自己重写一遍退避+抖动」导致的
// 语义漂移（#48 的诉求）不会发生：DelayHint 优先、MaxDelay 封顶、比例抖动、
// Retryable 门控、ctx 取消，全部只有一份实现。
//
// 典型用法（装饰器型：重试并返回结果）：
//
//	msg, err := retry.DoValue(ctx, cfg, func(attempt int) (*Message, error) {
//		return callModel(ctx, prompt)
//	})
//
// 契约：
//   - err == nil 时返回值有效；err != nil 时返回**零值**——失败那次的中间结果
//     不可用，返回它会诱导调用方误用未成功的产物。
//   - 退避等待期间 ctx 取消 → 返回零值与 ctx.Err()。
//   - cfg.Retryable 判为不可重试 → 立即返回零值与该错误。
//   - cfg 零值 = 只执行一次（同 Do）。
func DoValue[T any](ctx context.Context, cfg Config, fn func(attempt int) (T, error)) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	attempts := cfg.Attempts
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		v, err := fn(attempt)
		if err == nil {
			return v, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		if cfg.Retryable != nil && !cfg.Retryable(err) {
			var zero T
			return zero, err
		}
		delay := backoff(cfg, attempt, err)
		if delay <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(delay):
		}
	}
	var zero T
	return zero, lastErr
}

// backoff 计算第 attempt 次失败后的实际退避时长。
// 顺序：计算退避（Exponential > Linear > 固定间隔）→ DelayHint 建议覆盖 →
// MaxDelay 封顶 → 抖动（JitterRatio 比例抖动优先，否则固定 Jitter）。
func backoff(cfg Config, attempt int, err error) time.Duration {
	var delay time.Duration
	switch {
	case cfg.Exponential:
		// Delay * 2^(attempt-1)；溢出饱和而非回绕为负
		delay = cfg.Delay
		for i := 1; i < attempt; i++ {
			if delay <= 0 {
				break
			}
			if cfg.MaxDelay > 0 && delay >= cfg.MaxDelay {
				break
			}
			if delay > time.Duration(1<<62) {
				delay = time.Duration(1<<63 - 1)
				break
			}
			delay *= 2
		}
	case cfg.Linear:
		delay = time.Duration(attempt) * cfg.Delay
	default:
		delay = cfg.Delay
	}

	// 服务端退避建议覆盖计算退避（负值视为无效走计算值）
	if cfg.DelayHint != nil {
		if d, ok := cfg.DelayHint(err); ok && d >= 0 {
			delay = d
		}
	}

	if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
		delay = cfg.MaxDelay
	}

	// 抖动：比例抖动（单边向上 [d, d*(1+ratio))）优先于固定 Jitter
	if cfg.JitterRatio > 0 {
		delay += time.Duration(rand.Int63n(int64(float64(delay) * cfg.JitterRatio)))
	} else if cfg.Jitter > 0 {
		delay += time.Duration(rand.Int63n(int64(cfg.Jitter)))
	}
	return delay
}

// Poll 按 interval 间隔轮询 cond 直到返回 done 或超过 timeout：cond 返回错误
// 立即中止并透传；超时返回 ErrTimeout（调用方可继续走兜底路径）；ctx 取消
// 透传 ctx.Err()。cond 立即先执行一次（不改语义地覆盖「条件可能已满足」）。
func Poll(ctx context.Context, interval, timeout time.Duration, cond func() (bool, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(timeout)
	for {
		done, err := cond()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if !time.Now().Before(deadline) {
			return ErrTimeout
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
