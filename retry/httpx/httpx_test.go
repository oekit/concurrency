package httpx_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oekit/concurrency/retry"
	"github.com/oekit/concurrency/retry/httpx"
)

func TestRetryable(t *testing.T) {
	cases := map[int]bool{
		429:  true,
		500:  true,
		502:  true,
		503:  true,
		599:  true,
		400:  false,
		401:  false,
		403:  false,
		404:  false,
		422:  false,
		200:  false,
		302:  false,
		499:  false,
		1001: false, // 6xx 不是标准 5xx → 不重试
	}
	for status, want := range cases {
		if got := httpx.Retryable(status); got != want {
			t.Fatalf("Retryable(%d) = %v, want %v", status, got, want)
		}
	}
}

func TestHTTPError_Unwrap(t *testing.T) {
	sentinel := errors.New("connection reset")
	e := &httpx.HTTPError{Status: 503, Err: sentinel}
	if !errors.Is(e, sentinel) {
		t.Fatal("Unwrap broken")
	}
	if got := e.Error(); got != "http 503: connection reset" {
		t.Fatalf("Error() = %q", got)
	}
}

func TestIsTransportRetryable(t *testing.T) {
	if httpx.IsTransportRetryable(context.Canceled) {
		t.Fatal("ctx cancel should NOT be retryable")
	}
	if httpx.IsTransportRetryable(context.DeadlineExceeded) {
		t.Fatal("ctx deadline should NOT be retryable")
	}
	if fmt.Errorf("dial tcp: %w", context.DeadlineExceeded) != nil {
		// 包装后的 ctx 错误也要识别（errors.Is 链）
		if httpx.IsTransportRetryable(fmt.Errorf("do: %w", context.Canceled)) {
			t.Fatal("wrapped ctx cancel should NOT be retryable")
		}
	}
	if !httpx.IsTransportRetryable(errors.New("connection reset by peer")) {
		t.Fatal("connection error should be retryable")
	}
}

func TestClassifier(t *testing.T) {
	classify := httpx.Classifier()

	// ctx 取消 → 不重试
	if classify(context.Canceled) {
		t.Fatal("ctx cancel should not be retryable")
	}
	// 429/5xx → 重试
	if !classify(&httpx.HTTPError{Status: 429, Err: errors.New("rate limited")}) {
		t.Fatal("429 should be retryable")
	}
	if !classify(&httpx.HTTPError{Status: 503, Err: errors.New("unavailable")}) {
		t.Fatal("503 should be retryable")
	}
	// 其余 4xx → 不重试
	if classify(&httpx.HTTPError{Status: 404, Err: errors.New("nf")}) {
		t.Fatal("404 should not be retryable")
	}
	// 其他错误（传输层）→ 重试
	if !classify(errors.New("connection reset")) {
		t.Fatal("transport error should be retryable")
	}
}

// TestClassifier_EndToEndWithRetry 钉住接入形态：Classifier 直接塞进
// Config.Retryable，429 重试、404 立即返回。
func TestClassifier_EndToEndWithRetry(t *testing.T) {
	cfg := retry.Config{
		Attempts:    3,
		Delay:       time.Millisecond,
		Retryable:   httpx.Classifier(),
		Exponential: true,
	}

	calls := 0
	err := retry.Do(context.Background(), cfg, func(int) error {
		calls++
		return &httpx.HTTPError{Status: 429, Err: errors.New("rl")}
	})
	var he *httpx.HTTPError
	if !errors.As(err, &he) || calls != 3 {
		t.Fatalf("429 should retry to exhaustion: calls=%d err=%v", calls, err)
	}

	calls = 0
	_ = retry.Do(context.Background(), cfg, func(int) error {
		calls++
		return &httpx.HTTPError{Status: 400, Err: errors.New("bad")}
	})
	if calls != 1 {
		t.Fatalf("400 should fail fast: calls=%d", calls)
	}
}

// TestDelayHintHTTPError 钉住 #14+#15 同一现场：Retry-After 建议经
// Config.DelayHint 接入（webhook 推送「该不该重试 + 隔多久」两问一体）。
func TestDelayHintHTTPError(t *testing.T) {
	cfg := retry.Config{
		Attempts:  2,
		Delay:     time.Millisecond,
		MaxDelay:  30 * time.Millisecond, // 服务端建议 10s → 钳到 30ms
		Retryable: httpx.Classifier(),
		DelayHint: httpx.RetryAfterHint(),
	}
	start := time.Now()
	_ = retry.Do(context.Background(), cfg, func(int) error {
		return &httpx.HTTPError{Status: 429, Err: fmt.Errorf("retry-after: %w", httpx.NewRetryAfter(10*time.Second))}
	})
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("hint should be clamped, elapsed %v", elapsed)
	}
	// 链内层建议真实生效：钳到 30ms（而非 1ms 计算退避）
	if elapsed < 25*time.Millisecond {
		t.Fatalf("hint not applied from wrapped chain, elapsed %v", elapsed)
	}
}
