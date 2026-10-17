package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoRetriesWithLinearBackoff(t *testing.T) {
	calls := 0
	start := time.Now()
	err := Do(context.Background(), Config{Attempts: 3, Delay: 10 * time.Millisecond, Linear: true}, func(attempt int) error {
		calls++
		if attempt < 3 {
			return errors.New("busy")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("期望成功，得到 %v", err)
	}
	if calls != 3 {
		t.Fatalf("期望调用 3 次，实际 %d", calls)
	}
	// 线性退避：第 1 次失败后 10ms + 第 2 次失败后 20ms
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("退避未生效，耗时 %v", elapsed)
	}
}

func TestDoStopsOnNonRetryable(t *testing.T) {
	calls := 0
	sentinel := errors.New("fatal")
	err := Do(context.Background(), Config{Attempts: 5, Delay: time.Millisecond, Retryable: func(err error) bool {
		return !errors.Is(err, sentinel)
	}}, func(attempt int) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("期望透传原错误，得到 %v", err)
	}
	if calls != 1 {
		t.Fatalf("不可重试错误应立即返回，实际调用 %d 次", calls)
	}
}

func TestDoReturnsLastErrorAfterExhausted(t *testing.T) {
	sentinel := errors.New("still busy")
	calls := 0
	err := Do(context.Background(), Config{Attempts: 3, Delay: time.Millisecond}, func(attempt int) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) || calls != 3 {
		t.Fatalf("期望耗尽后返回最后错误（3 次调用），得到 %v / %d 次", err, calls)
	}
}

func TestDoContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := Do(ctx, Config{Attempts: 3, Delay: time.Second}, func(attempt int) error {
		return errors.New("busy")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("期望 ctx 取消透传，得到 %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("退避未感知取消")
	}
}

func TestDoZeroConfigRunsOnce(t *testing.T) {
	calls := 0
	if err := Do(context.Background(), Config{}, func(attempt int) error { calls++; return nil }); err != nil {
		t.Fatalf("零值配置应执行一次成功：%v", err)
	}
	if calls != 1 {
		t.Fatalf("期望 1 次，实际 %d", calls)
	}
}

func TestPollConditionMetImmediately(t *testing.T) {
	err := Poll(context.Background(), 10*time.Millisecond, time.Second, func() (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("条件立即可达应成功：%v", err)
	}
}

func TestPollTimeout(t *testing.T) {
	err := Poll(context.Background(), 5*time.Millisecond, 30*time.Millisecond, func() (bool, error) {
		return false, nil
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，得到 %v", err)
	}
}

func TestPollCondErrorAborts(t *testing.T) {
	sentinel := errors.New("db down")
	calls := 0
	err := Poll(context.Background(), 5*time.Millisecond, time.Second, func() (bool, error) {
		calls++
		return false, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("cond 错误应立即透传，得到 %v", err)
	}
	if calls != 1 {
		t.Fatalf("cond 错误不应轮询第二次，实际 %d 次", calls)
	}
}

func TestPollEventuallyDone(t *testing.T) {
	n := 0
	err := Poll(context.Background(), 2*time.Millisecond, time.Second, func() (bool, error) {
		n++
		return n >= 3, nil
	})
	if err != nil || n != 3 {
		t.Fatalf("期望第 3 次达成，得到 err=%v n=%d", err, n)
	}
}

func TestDoExponentialBackoff(t *testing.T) {
	var delays []time.Duration
	last := time.Now()
	err := Do(context.Background(), Config{Attempts: 4, Delay: 5 * time.Millisecond, Exponential: true}, func(attempt int) error {
		if attempt > 1 {
			delays = append(delays, time.Since(last))
		}
		last = time.Now()
		if attempt < 4 {
			return errors.New("busy")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("期望成功，得到 %v", err)
	}
	// 指数退避：5ms → 10ms → 20ms；允许调度误差，但必须严格递增
	if len(delays) != 3 {
		t.Fatalf("期望 3 段退避，实际 %d", len(delays))
	}
	if delays[0] < 4*time.Millisecond || delays[1] < 8*time.Millisecond || delays[2] < 16*time.Millisecond {
		t.Fatalf("指数退避未生效：%v", delays)
	}
	if delays[0] >= delays[1] || delays[1] >= delays[2] {
		t.Fatalf("退避未递增：%v", delays)
	}
}

func TestDoExponentialCappedByMaxDelay(t *testing.T) {
	start := time.Now()
	err := Do(context.Background(), Config{Attempts: 5, Delay: 10 * time.Millisecond, Exponential: true, MaxDelay: 15 * time.Millisecond}, func(attempt int) error {
		return errors.New("busy")
	})
	if err == nil {
		t.Fatal("期望最终失败")
	}
	// 10 + 15 + 15 + 15 ≈ 55ms；指数 10/20/40/80 被 MaxDelay=15 封顶后应显著小于全指数
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("MaxDelay 封顶未生效，耗时 %v", elapsed)
	}
}

func TestDoJitterAddsNoise(t *testing.T) {
	// 固定 Delay + 大 Jitter：总耗时应大于纯固定间隔（3×1ms）
	start := time.Now()
	_ = Do(context.Background(), Config{Attempts: 3, Delay: time.Millisecond, Jitter: 20 * time.Millisecond}, func(attempt int) error {
		return errors.New("busy")
	})
	if elapsed := time.Since(start); elapsed < 3*time.Millisecond {
		t.Fatalf("固定基线异常：%v", elapsed)
	}
}

func TestBackoffUnit(t *testing.T) {
	// 纯函数级验证，避免测试抖动
	cfg := Config{Delay: 10 * time.Millisecond, Exponential: true}
	if d := backoff(cfg, 1, nil); d != 10*time.Millisecond {
		t.Fatalf("attempt=1 期望 10ms，得到 %v", d)
	}
	if d := backoff(cfg, 3, nil); d != 40*time.Millisecond {
		t.Fatalf("attempt=3 期望 40ms，得到 %v", d)
	}
	cfg.MaxDelay = 25 * time.Millisecond
	if d := backoff(cfg, 3, nil); d != 25*time.Millisecond {
		t.Fatalf("MaxDelay 封顶期望 25ms，得到 %v", d)
	}
	cfg = Config{Delay: 10 * time.Millisecond, Linear: true}
	if d := backoff(cfg, 2, nil); d != 20*time.Millisecond {
		t.Fatalf("线性 attempt=2 期望 20ms，得到 %v", d)
	}
	cfg = Config{Delay: 10 * time.Millisecond, Jitter: 5 * time.Millisecond}
	for i := 0; i < 20; i++ {
		d := backoff(cfg, 1, nil)
		if d < 10*time.Millisecond || d >= 15*time.Millisecond {
			t.Fatalf("抖动越界：%v", d)
		}
	}
}

// TestDo_DelayHintClampedByMaxDelay 钉住 #14：服务端退避建议优先于计算退避，
// 但仍受 MaxDelay 封顶。
func TestDo_DelayHintClampedByMaxDelay(t *testing.T) {
	cfg := Config{
		Attempts:    2,
		Exponential: true,
		Delay:       10 * time.Millisecond,
		MaxDelay:    20 * time.Millisecond,
		DelayHint: func(err error) (time.Duration, bool) {
			return 10 * time.Second, true // 服务端给小时级建议 → 钳到 MaxDelay
		},
	}
	start := time.Now()
	var lastErr error
	_ = Do(context.Background(), cfg, func(attempt int) error {
		lastErr = errors.New("hint-me")
		return lastErr
	})
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("hint should be clamped by MaxDelay, elapsed %v", elapsed)
	}
	if elapsed < 15*time.Millisecond {
		t.Fatalf("hint should win over computed backoff, elapsed %v", elapsed)
	}
}

// TestDo_DelayHintNotProvided_FallsBackToBackoff 钉住 ok=false 走计算退避。
func TestDo_DelayHintNotProvided_FallsBackToBackoff(t *testing.T) {
	cfg := Config{
		Attempts: 2,
		Delay:    30 * time.Millisecond,
		DelayHint: func(err error) (time.Duration, bool) {
			return 0, false
		},
	}
	start := time.Now()
	_ = Do(context.Background(), cfg, func(int) error { return errors.New("x") })
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("fallback backoff not applied, elapsed %v", elapsed)
	}
}

// TestDo_JitterRatioBounded 钉住 JitterRatio：单边向上按当次 delay 比例抖动，
// 上界 delay*(1+ratio)；ratio 生效时固定 Jitter 不叠加。
func TestDo_JitterRatioBounded(t *testing.T) {
	cfg := Config{
		Attempts:    50, // 采样足量，全部落在固定 delay 上界内
		Delay:       5 * time.Millisecond,
		JitterRatio: 0.5,         // [5ms, 7.5ms)
		Jitter:      time.Second, // 若错误叠加会远超上界 → 用上界检出
	}
	start := time.Now()
	_ = Do(context.Background(), cfg, func(int) error { return errors.New("x") })
	elapsed := time.Since(start)
	// 50 次平均应落在 ~6.25ms/次 → 总 250-375ms；若固定 Jitter 叠加则为 50s+
	if elapsed > 2*time.Second {
		t.Fatalf("ratio jitter should replace fixed jitter, elapsed %v", elapsed)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("jitter floor too low, elapsed %v", elapsed)
	}
}

// TestDo_JitterRatioStaysAboveBase 钉住单边向上：不含向下抖动。
func TestDo_JitterRatioStaysAboveBase(t *testing.T) {
	cfg := Config{
		Attempts:    3,
		Delay:       20 * time.Millisecond,
		JitterRatio: 0.5,
	}
	start := time.Now()
	_ = Do(context.Background(), cfg, func(int) error { return errors.New("x") })
	// Attempts=3 → 2 次退避等待；单边向上 ⇒ ≥ 40ms（无向下抖动）
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("2 waits with 20ms base should be >= 40ms, got %v", elapsed)
	}
}
