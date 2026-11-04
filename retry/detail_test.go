package retry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestNegativeDelayPanics 锁定 #136：Config.Delay 负值必须当场暴露——负退避等于
// 零退避，会静默消灭「打散重试风暴」这道包内核心防线。守卫在公式唯一实现处
// （computedDelay），Do/DoValue/Backoff/TotalBackoff 四个面同口径 panic。
func TestNegativeDelayPanics(t *testing.T) {
	cases := map[string]struct {
		cfg Config
	}{
		"Do 固定间隔":     {Config{Attempts: 3, Delay: -5 * time.Second}},
		"Do 线性":       {Config{Attempts: 3, Delay: -time.Second, Linear: true}},
		"Do 指数":       {Config{Attempts: 3, Delay: -time.Second, Exponential: true}},
		"Backoff 预计算": {Config{Delay: -1}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("Delay<0 应 panic（启动即暴露），实际静默执行")
				}
				if msg, ok := r.(string); !ok || !strings.Contains(msg, "Delay") {
					t.Fatalf("panic 消息应点名 Config.Delay，got %v", r)
				}
			}()
			switch name {
			case "Backoff 预计算":
				Backoff(tc.cfg, 1)
			default:
				_ = Do(context.Background(), tc.cfg, func(int) error {
					return errors.New("恒败")
				})
			}
		})
	}
}

// TestNegativeDelayDoesNotCallFn panic 守卫必须在 fn 首次调用前生效——配置错误
// 不该等到「已经打出去一发」才暴露。
func TestNegativeDelayDoesNotCallFn(t *testing.T) {
	defer func() { _ = recover() }()
	called := 0
	_ = Do(context.Background(), Config{Attempts: 3, Delay: -time.Millisecond},
		func(int) error { called++; return errors.New("boom") })
	if called != 0 {
		t.Fatalf("Delay<0 应在 fn 调用前 panic，实际已调用 %d 次", called)
	}
}

// TestZeroDelayStillWorks 零退避是合法显式配置（零值 Config 即只跑一次），
// 守卫不得误伤。
func TestZeroDelayStillWorks(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Config{Attempts: 3},
		func(int) error { calls++; return errors.New("boom") })
	if err == nil || calls != 3 {
		t.Fatalf("Delay=0 零退避重试应正常：calls=%d err=%v", calls, err)
	}
}

// TestDetailError_RoundTrip 锁定 #138：fn 把失败伴生数据装进 DetailError 后，
// DoValue 零值契约不动，伴生数据随 lastErr 送达——octopus sendWithRetry 形态。
func TestDetailError_RoundTrip(t *testing.T) {
	type sendResult struct {
		StatusCode int
	}
	cfg := Config{Attempts: 3, Delay: time.Millisecond}
	var lastAttempt int
	_, err := DoValue(context.Background(), cfg, func(attempt int) (*sendResult, error) {
		lastAttempt = attempt
		res := &sendResult{StatusCode: 429}
		return res, WithDetail(errors.New("send failed"), res)
	})
	if err == nil {
		t.Fatal("恒败应返回错误")
	}
	if lastAttempt != 3 {
		t.Fatalf("应重试到第 3 次，got %d", lastAttempt)
	}
	// 零值契约不动：DoValue 的 T 仍是 nil
	// （上面用 *_ = 忽略返回值本身即说明调用方拿不到成功产物）
	res, ok := Detail[*sendResult](err)
	if !ok {
		t.Fatal("应能从错误链提取伴生数据")
	}
	if res == nil || res.StatusCode != 429 {
		t.Fatalf("伴生数据应是最后一次尝试的 res，got %+v", res)
	}
	// errors.Is/As 链零破坏：Unwrap 透传原始错误
	sentinel := errors.New("sentinel")
	_, err = DoValue(context.Background(), Config{Attempts: 1},
		func(int) (*struct{}, error) { return nil, WithDetail(sentinel, "detail") })
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is 应穿透 DetailError，got %v", err)
	}
	// 错误文案透传原始错误
	if !strings.Contains(err.Error(), "sentinel") {
		t.Fatalf("Error() 应透传原始文案，got %q", err.Error())
	}
}

// TestDetail_MissingOrMismatched 提取面的两个 false 分支：链上没有容器、类型不符。
func TestDetail_MissingOrMismatched(t *testing.T) {
	if _, ok := Detail[string](errors.New("plain")); ok {
		t.Fatal("无 DetailError 时应 ok=false")
	}
	wrapped := WithDetail(errors.New("boom"), 42)
	if _, ok := Detail[string](wrapped); ok {
		t.Fatal("类型不符时应 ok=false，而不是 panic/强转")
	}
	if n, ok := Detail[int](wrapped); !ok || n != 42 {
		t.Fatalf("类型匹配时应取回，got %d %v", n, ok)
	}
	// WithDetail(nil, ...) 原样返回 nil——fn 成功短路路径可无判空统一走本函数
	if WithDetail(nil, "x") != nil {
		t.Fatal("WithDetail(nil,…) 应返回 nil")
	}
}
