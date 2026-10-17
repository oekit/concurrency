// Package httpx 提供 HTTP 场景的可重试性判定与退避建议提取（#15/#14 同一现场），
// 收编消费方各自手写的同一段分类逻辑（429/5xx 重试、其余 4xx 拒收不重试、
// ctx 取消不重试）。
//
// 零外部依赖（仅 errors/context/time 标准库）——不 import net/http：
// 状态码以 int 传入，调用方用自己的 HTTP 客户端类型。
//
// 接入形态（两问一体：「该不该重试」+「隔多久再试」）：
//
//	cfg := retry.Config{
//	    Attempts:  3,
//	    Retryable: httpx.Classifier(),
//	    DelayHint: httpx.RetryAfterHint(),
//	    Exponential: true,
//	}
package httpx

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNoRetryAfter 是 RetryAfterHint 在错误未携带建议时返回的包装哨兵
// （经 errors.Is 可探测）。
var ErrNoRetryAfter = errors.New("httpx: no retry-after hint")

// retryAfterKey 是错误携带 Retry-After 建议的标记接口（实现方见
// NewRetryAfter/NewRetryAfterError）。
type retryAfterCarrier interface {
	RetryAfter() (time.Duration, bool)
}

// Retryable 判定 HTTP 状态码是否值得重试：429（限流）与 5xx（服务端故障）
// 重试；其余（2xx 非 error、3xx、4xx 接收端明确拒收）不重试。
func Retryable(status int) bool {
	return status == 429 || (status >= 500 && status <= 599)
}

// HTTPError 携带 HTTP 状态码的错误（Unwrap 保留底层错误链）。
type HTTPError struct {
	// Status 是 HTTP 状态码；0 表示传输层失败（未收到响应）。
	Status int
	// Err 是底层错误（响应体摘要、传输错误等）；可为 nil。
	Err error
	// hint 是服务端退避建议（Retry-After 秒数）；0 = 未携带。
	hint time.Duration
}

func (e *HTTPError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("http %d: %v", e.Status, e.Err)
	}
	return fmt.Sprintf("http %d", e.Status)
}

// Unwrap 保留错误链——上层 errors.Is(err, context.Canceled) 等判断不受包装影响。
func (e *HTTPError) Unwrap() error { return e.Err }

// RetryAfter 实现 retryAfterCarrier：优先取自身 hint，未携带则向 Err 链
// 回退查找（fmt.Errorf("%w", httpx.NewRetryAfter(d)) 注入的内层建议可被发现）。
func (e *HTTPError) RetryAfter() (time.Duration, bool) {
	if e.hint > 0 {
		return e.hint, true
	}
	var c retryAfterCarrier
	if e.Err != nil && errors.As(e.Err, &c) {
		return c.RetryAfter()
	}
	return 0, false
}

// NewRetryAfterError 构造携带 Retry-After 建议的 HTTPError（status 应为 429/503）。
func NewRetryAfterError(status int, d time.Duration, err error) *HTTPError {
	return &HTTPError{Status: status, Err: err, hint: d}
}

// NewRetryAfter 构造仅携带建议的错误包装（用于把既有错误补上建议，
// 经 fmt.Errorf("%w", httpx.NewRetryAfter(2*time.Second)) 注入链）。
func NewRetryAfter(d time.Duration) error { return &hintOnly{d: d} }

type hintOnly struct{ d time.Duration }

func (h *hintOnly) Error() string                     { return fmt.Sprintf("retry-after %s", h.d) }
func (h *hintOnly) RetryAfter() (time.Duration, bool) { return h.d, true }

// hint 读取错误链中第一个携带建议的载体。
func hint(err error) (time.Duration, bool) {
	var c retryAfterCarrier
	if errors.As(err, &c) {
		return c.RetryAfter()
	}
	return 0, false
}

// IsTransportRetryable 判定传输层错误可否重试：
//   - context.Canceled / context.DeadlineExceeded（含包装链）→ false——
//     那是调用方自己在收手，重试无意义（否则关停时退避循环会重试到预算耗尽）；
//   - 其余（连接失败、超时等）→ true。
func IsTransportRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// Classifier 返回可直接塞进 retry.Config.Retryable 的判定函数：
//   - ctx 取消/超时（含包装链）→ false；
//   - *HTTPError → Retryable(status)（429/5xx true，其余 4xx false）；
//   - 其他错误 → true（视为传输层失败）。
func Classifier() func(error) bool {
	return func(err error) bool {
		if !IsTransportRetryable(err) {
			return false
		}
		var he *HTTPError
		if errors.As(err, &he) {
			return Retryable(he.Status)
		}
		return true
	}
}

// RetryAfterHint 返回可直接塞进 retry.Config.DelayHint 的建议提取函数：
// 从错误链读取 Retry-After 建议（HTTPError 的 hint 字段或 retryAfterCarrier），
// 未携带返回 ok=false 走计算退避。建议值仍受 Config.MaxDelay 封顶。
func RetryAfterHint() func(err error) (time.Duration, bool) {
	return func(err error) (time.Duration, bool) {
		d, ok := hint(err)
		if !ok {
			return 0, false
		}
		return d, true
	}
}
