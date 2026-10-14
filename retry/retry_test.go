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
