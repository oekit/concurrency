package retry

import (
	"math"
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
	for range 200 {
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

// TestDelay_LinearOverflowSaturates 线性退避裸乘溢出饱和（#166）：
// attempt 增长超过 MaxInt64/Delay 后乘积回绕成负——负 delay 穿过封顶/抖动
// 不变，到 DoValue 变成 delay<=0 直通，无界重试退化成紧循环；修复后饱和到
// MaxInt64（与指数路径同口径），实跑/预计算/总量三个面一致为正。
func TestDelay_LinearOverflowSaturates(t *testing.T) {
	const maxDur = time.Duration(math.MaxInt64)
	cfg := Config{Delay: time.Second, Linear: true}

	attempt := math.MaxInt
	// 期望值按同一判据推导（32 位平台 int 较窄时走精确乘积分支，不特判平台）
	var want time.Duration
	if int64(attempt) > int64(math.MaxInt64)/int64(cfg.Delay) {
		want = maxDur
	} else {
		want = time.Duration(attempt) * cfg.Delay
	}
	if want <= 0 {
		t.Fatal("测试前提不成立：期望值应为正")
	}

	if got := Delay(cfg, attempt, nil); got != want || got <= 0 {
		t.Fatalf("Delay(线性溢出) = %s，期望饱和 %s", got, want)
	}
	if got := Backoff(cfg, attempt); got != want || got <= 0 {
		t.Fatalf("Backoff(线性溢出) = %s，期望饱和 %s", got, want)
	}
	// 未溢出区间保持精确（线性语义不变）
	if got := Delay(cfg, 3, nil); got != 3*time.Second {
		t.Fatalf("Delay(线性 attempt=3) = %s，期望 3s", got)
	}
}

// TestDelay_ExponentialOverflowAtExactBoundary 指数护栏的差一位洞（#283）：
// delay 恰为 1<<62 时旧护栏（严格 `>`）放行翻倍 ⇒ 1<<63 在 int64 回绕成负
// ——负 delay 穿过封顶/抖动原样直达，到 DoValue 变成 delay<=0 直通，退避
// 静默消失（#166 认定「指数路径有防御」的那道防线实测差一位）。修复后
// `>=` 含边界：恰好 1<<62 的 delay 第一次翻倍前即饱和到 MaxInt64；
// attempt=1（不翻倍）的 1<<62 本身可表示，保持原值。
func TestDelay_ExponentialOverflowAtExactBoundary(t *testing.T) {
	const maxDur = time.Duration(math.MaxInt64)
	cfg := Config{Delay: time.Duration(1) << 62, Exponential: true}

	// attempt=1：循环体不执行，delay 保持 1<<62（可表示、为正，不触发护栏）
	if got := Delay(cfg, 1, nil); got != time.Duration(1)<<62 {
		t.Fatalf("Delay(指数 attempt=1) = %s，期望原值 1<<62", got)
	}
	// attempt>=2：任一次翻倍都越过护栏，饱和到 MaxInt64，恒为正
	for _, attempt := range []int{2, 3, 10, math.MaxInt} {
		if got := Delay(cfg, attempt, nil); got != maxDur || got <= 0 {
			t.Fatalf("Delay(指数 attempt=%d) = %d，期望饱和 MaxInt64（旧实现此处回绕成负）", attempt, got)
		}
		if got := Backoff(cfg, attempt); got != maxDur || got <= 0 {
			t.Fatalf("Backoff(指数 attempt=%d) = %d，期望饱和 MaxInt64", attempt, got)
		}
		if got := TotalBackoff(cfg, attempt+1); got <= 0 {
			t.Fatalf("TotalBackoff(指数 attempt=%d) = %d，期望为正", attempt, got)
		}
	}
	// 实跑面：DoValue 的退避等待同样为正（负 delay 会变直通紧循环）
	cfgNoHint := cfg
	if got := backoff(cfgNoHint, 2, nil); got != maxDur || got <= 0 {
		t.Fatalf("backoff(实跑 attempt=2) = %d，期望饱和 MaxInt64", got)
	}

	// 对照：MaxDelay 封顶优先于饱和护栏——配置了封顶时在封顶处停下
	capped := Config{Delay: time.Duration(1) << 62, Exponential: true, MaxDelay: time.Minute}
	if got := Delay(capped, 2, nil); got != time.Minute {
		t.Fatalf("Delay(指数+封顶 attempt=2) = %s，期望封顶 1m", got)
	}
}
