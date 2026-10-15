// Package retry 可取消的重试与轮询原语（stdlib-only）：带退避的动作重试 +
// 到 deadline 为止的条件轮询。收编自消费方三类手写形态——SQLite busy 退避
// 重试（线性递增）、双副本首启的固定间隔回读、并发败者/终态等待的条件轮询。
package retry

import (
	"context"
	"errors"
	"time"
)

// ErrTimeout Poll 超时（调用方按需区分「条件未达成」与真错误）。
var ErrTimeout = errors.New("retry: 轮询超时")

// Config 重试参数。零值 = 只执行一次、不重试。
type Config struct {
	// Attempts 总尝试次数（含首次）；<=1 视为 1。
	Attempts int
	// Delay 退避基数。Linear=false 固定间隔；Linear=true 第 i 次失败后等待 Delay*i。
	Delay time.Duration
	// MaxDelay 单次退避上限（<=0 不限）。
	MaxDelay time.Duration
	// Linear true=线性递增退避（Delay*i），false=固定间隔。
	Linear bool
	// Retryable 判定错误是否可重试；nil = 所有错误都重试。
	Retryable func(error) bool
}

// Do 执行 fn 至多 Attempts 次，失败按 Config 退避后重试；返回最后一次错误。
// 退避等待感知 ctx 取消（nil 视为 context.Background()）。
func Do(ctx context.Context, cfg Config, fn func(attempt int) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	attempts := cfg.Attempts
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = fn(attempt)
		if err == nil {
			return nil
		}
		if attempt == attempts {
			break
		}
		if cfg.Retryable != nil && !cfg.Retryable(err) {
			return err
		}
		delay := cfg.Delay
		if cfg.Linear {
			delay = time.Duration(attempt) * cfg.Delay
		}
		if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		if delay <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
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
