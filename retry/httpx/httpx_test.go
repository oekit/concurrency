package httpx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/concurrency/breaker"
	"github.com/oekit/concurrency/retry"
	"github.com/oekit/concurrency/retry/httpx"
	pkgerrors "github.com/oekit/pkg/errors"
)

// TestRetryAfterZeroHintPreserved 回归测试（#228）：Retry-After: 0 是服务端合法
// 建议（0 = 立即重试），显式构造进 HTTPError 后不得被 `hint > 0` 当作「未携带」
// 降级成计算退避——同一建议在 ParseRetryAfter / hintOnly 载体里是 (0, true)，
// HTTPError 载体必须同口径（同一建议两种载体两种重试行为即契约破裂）。
func TestRetryAfterZeroHintPreserved(t *testing.T) {
	e := httpx.NewRetryAfterError(429, 0, errors.New("rate limited"))

	// 载体直取：0 必须以 ok=true 返回。
	if d, ok := e.RetryAfter(); !ok || d != 0 {
		t.Fatalf("HTTPError.RetryAfter() = (%v, %v), want (0, true)", d, ok)
	}
	// DelayHint 通路：ok=true 且值为 0，retry 侧即立即重试；ok=false 才走计算退避。
	if d, ok := httpx.RetryAfterHint()(e); !ok || d != 0 {
		t.Fatalf("RetryAfterHint(HTTPError) = (%v, %v), want (0, true)", d, ok)
	}
	// 对照：hintOnly 载体对 0 本就保留。
	if d, ok := httpx.RetryAfterHint()(httpx.NewRetryAfter(0)); !ok || d != 0 {
		t.Fatalf("RetryAfterHint(hintOnly) = (%v, %v), want (0, true)", d, ok)
	}
	// hasHint 优先：显式建议不被 Err 链内层建议覆盖。
	inner := pkgerrors.Wrapf(httpx.NewRetryAfter(7*time.Second), "wrap")
	e2 := httpx.NewRetryAfterError(503, 2*time.Second, inner)
	if d, ok := e2.RetryAfter(); !ok || d != 2*time.Second {
		t.Fatalf("hint precedence = (%v, %v), want (2s, true)", d, ok)
	}
}

// TestHTTPErrorWithoutHintFallsBackToChain 未携带建议的口径不变（向后兼容）：
// hasHint=false（零值构造）时回落 Err 链找内层建议；链上没有就 (0, false)。
func TestHTTPErrorWithoutHintFallsBackToChain(t *testing.T) {
	e := &httpx.HTTPError{Status: 429}
	if d, ok := e.RetryAfter(); ok || d != 0 {
		t.Fatalf("hintless HTTPError.RetryAfter() = (%v, %v), want (0, false)", d, ok)
	}
	inner := pkgerrors.Wrapf(httpx.NewRetryAfter(3*time.Second), "wrap")
	e2 := &httpx.HTTPError{Status: 429, Err: inner}
	if d, ok := e2.RetryAfter(); !ok || d != 3*time.Second {
		t.Fatalf("chain fallback = (%v, %v), want (3s, true)", d, ok)
	}
}

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
	if pkgerrors.Wrapf(context.DeadlineExceeded, "dial tcp") != nil {
		// 包装后的 ctx 错误也要识别（errors.Is 链）
		if httpx.IsTransportRetryable(pkgerrors.Wrapf(context.Canceled, "do")) {
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
		return &httpx.HTTPError{Status: 429, Err: pkgerrors.Wrapf(httpx.NewRetryAfter(10*time.Second), "retry-after")}
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

func TestClassifier_TransportFailureRetryable(t *testing.T) {
	// #325：HTTPError{Status:0} 是文档定义的传输层失败（未收到响应）——
	// 此前落 Retryable(0)=false，连接错误被静默放弃重试。
	c := httpx.Classifier()
	if !c(&httpx.HTTPError{Status: 0, Err: errors.New("dial timeout")}) {
		t.Fatal("Status=0(传输层失败) 应可重试")
	}
	if c(&httpx.HTTPError{Status: 403}) {
		t.Fatal("403 仍应不可重试")
	}
	if !c(&httpx.HTTPError{Status: 429}) {
		t.Fatal("429 应可重试")
	}
}

// TestClassifier_CircuitOpenNotRetryable 回归测试（#377）：breaker.ErrCircuitOpen
// 是「现在别发」而非「发失败了」，Classifier 默认判不可重试——裸哨兵、包装链、
// 以及被 *HTTPError 包裹（含 Status=0 传输层形态）都必须命中，哨兵判定优先于
// 状态码判定。
func TestClassifier_CircuitOpenNotRetryable(t *testing.T) {
	c := httpx.Classifier()
	cases := []struct {
		name string
		err  error
	}{
		{"bare", breaker.ErrCircuitOpen},
		{"wrapped", pkgerrors.Wrapf(breaker.ErrCircuitOpen, "send sms")},
		{"http status 0", &httpx.HTTPError{Status: 0, Err: breaker.ErrCircuitOpen}},
		{"http status 429", &httpx.HTTPError{Status: 429, Err: breaker.ErrCircuitOpen}},
		{"http status 503", &httpx.HTTPError{Status: 503, Err: pkgerrors.Wrapf(breaker.ErrCircuitOpen, "upstream")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if c(tc.err) {
				t.Fatalf("Classifier(%v) = true, want false（开闸期不得重试）", tc.err)
			}
		})
	}
}

// TestClassifierWithNotRetryable_CustomSentinel 组合口排除业务哨兵（errors.Is
// 语义、含包装链）；nil 哨兵忽略；未传入的哨兵不影响 Classifier 既有判定。
func TestClassifierWithNotRetryable_CustomSentinel(t *testing.T) {
	perm := errors.New("permission denied")
	c := httpx.ClassifierWithNotRetryable(perm)

	if c(perm) {
		t.Fatal("自定义哨兵应不可重试")
	}
	if c(pkgerrors.Wrapf(perm, "send")) {
		t.Fatal("包装链上的自定义哨兵应不可重试")
	}
	if !c(errors.New("connection reset")) {
		t.Fatal("未命中哨兵的传输层错误仍应可重试")
	}
	if !c(&httpx.HTTPError{Status: 503, Err: errors.New("unavailable")}) {
		t.Fatal("未命中哨兵的 5xx 仍应可重试")
	}
	// 默认 Classifier 不认识自定义哨兵（其余错误按传输层失败处理）。
	if !httpx.Classifier()(perm) {
		t.Fatal("默认 Classifier 不应改动对业务哨兵的既有判定")
	}
	// nil 哨兵不得 panic，也不得把 nil 当通配符排除一切错误。
	withNil := httpx.ClassifierWithNotRetryable(nil, perm)
	if withNil(perm) {
		t.Fatal("nil 之后的哨兵仍应生效")
	}
	if !withNil(errors.New("connection reset")) {
		t.Fatal("nil 哨兵不得误伤其他错误")
	}
	// 开闸哨兵在组合口下依然默认排除（重复传入无副作用）。
	if httpx.ClassifierWithNotRetryable(breaker.ErrCircuitOpen)(breaker.ErrCircuitOpen) {
		t.Fatal("ErrCircuitOpen 在组合口下仍应默认不可重试")
	}
}

// TestClassifier_CircuitOpenEndToEndWithBreaker 钉住 #377 的消费方现场：真实
// 熔断器开闸后，retry.Do + Classifier 只尝试一次，退避循环不再空烧——execute
// 一次都不执行，错误原样返回。
func TestClassifier_CircuitOpenEndToEndWithBreaker(t *testing.T) {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	brk := breaker.NewBreaker("httpx-open-e2e",
		breaker.WithBreakerClock(func() time.Time { return fixed }),
		breaker.WithConsecutiveFailures(1),
	)
	ctx := context.Background()
	boom := errors.New("boom")
	if err := brk.Do(ctx, func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("tripping Do err = %v, want boom", err)
	}

	calls, executed := 0, 0
	err := retry.Do(ctx, retry.Config{
		Attempts:  5,
		Delay:     time.Millisecond,
		Retryable: httpx.Classifier(),
	}, func(int) error {
		calls++
		return brk.Do(ctx, func() error { executed++; return nil })
	})

	if !errors.Is(err, breaker.ErrCircuitOpen) {
		t.Fatalf("retry err = %v, want ErrCircuitOpen", err)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1（开闸期不重试）", calls)
	}
	if executed != 0 {
		t.Fatalf("execute calls = %d, want 0（开闸拒绝不执行真实调用）", executed)
	}
}
