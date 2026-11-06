package retry

import (
	"testing"
	"time"
)

// TestDelay_FollowsConfiguredFormula 实跑口径：固定/线性/指数三档与 Do 同一公式。
func TestDelay_FollowsConfiguredFormula(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		attempt int
		want    time.Duration
	}{
		{"固定间隔", Config{Delay: 100 * time.Millisecond}, 3, 100 * time.Millisecond},
		{"线性递增", Config{Delay: 100 * time.Millisecond, Linear: true}, 3, 300 * time.Millisecond},
		{"指数递增", Config{Delay: 100 * time.Millisecond, Exponential: true}, 3, 400 * time.Millisecond},
		{"MaxDelay 封顶", Config{Delay: 100 * time.Millisecond, Exponential: true, MaxDelay: 250 * time.Millisecond}, 3, 250 * time.Millisecond},
		{"attempt<1 视为 1", Config{Delay: 100 * time.Millisecond}, 0, 100 * time.Millisecond},
		{"零退避", Config{}, 5, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Delay(tc.cfg, tc.attempt, nil); got != tc.want {
				t.Fatalf("Delay = %s，期望 %s", got, tc.want)
			}
		})
	}
}

// TestDelay_DelayHintOverrides 服务端建议优先，仍受 MaxDelay 封顶。
func TestDelay_DelayHintOverrides(t *testing.T) {
	errRateLimited := &strErr{"429"}
	cfg := Config{
		Delay:     time.Millisecond,
		MaxDelay:  2 * time.Second,
		DelayHint: func(error) (time.Duration, bool) { return 500 * time.Millisecond, true },
	}
	if got := Delay(cfg, 1, errRateLimited); got != 500*time.Millisecond {
		t.Fatalf("Delay = %s，期望采信服务端建议 500ms", got)
	}

	cfg.DelayHint = func(error) (time.Duration, bool) { return time.Hour, true }
	if got := Delay(cfg, 1, errRateLimited); got != 2*time.Second {
		t.Fatalf("Delay = %s，期望被 MaxDelay 封到 2s", got)
	}
}

// TestDelay_JitterStaysWithinCeiling 抖动落在 [计算值, 上界) 内，且不恒等于上界。
//
// 两条都断言：只测「在下界以上」会放过「抖动恒取满」的实现——那等于没抖动
// （同一批同时失败的实例仍撞在同一点上）。
func TestDelay_JitterStaysWithinCeiling(t *testing.T) {
	const (
		base   = 100 * time.Millisecond
		jitter = 50 * time.Millisecond
	)
	cfg := Config{Delay: base, Jitter: jitter}

	sawBelowCeiling := false
	for i := 0; i < 200; i++ {
		got := Delay(cfg, 1, nil)
		if got < base || got >= base+jitter {
			t.Fatalf("Delay = %s，越出 [%s, %s)", got, base, base+jitter)
		}
		if got < base+jitter {
			sawBelowCeiling = true
		}
		if ceiling := Backoff(cfg, 1); got >= ceiling {
			t.Fatalf("Delay = %s 不应达到或超过 Backoff 上界 %s", got, ceiling)
		}
	}
	if !sawBelowCeiling {
		t.Fatal("抖动恒取上界——等于没有抖动，打散重试风暴失效")
	}
}

// TestDelay_NegativeDelayPanics 负退避是配置错误，当场暴露（与 Do 同口径）。
func TestDelay_NegativeDelayPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("期望 panic：负退避等于零退避")
		}
	}()
	_ = Delay(Config{Delay: -time.Second}, 1, nil)
}

type strErr struct{ s string }

func (e *strErr) Error() string { return e.s }
