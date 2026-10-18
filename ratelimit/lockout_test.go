package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// lockoutStart 假时钟起点：非零真实量级时刻。
var lockoutStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fakeLockClock 可手动推进的假时钟（与 budget/breaker 的确定性测试手法一致）。
type fakeLockClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeLockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeLockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestLockout 构造注入假时钟的防爆破锁（窗口 10m / 阈值 3 / 锁定 5m 为缺省口径）。
func newTestLockout(clock *fakeLockClock, mutate func(*LockoutConfig)) *Lockout {
	cfg := LockoutConfig{
		Window:       10 * time.Minute,
		Threshold:    3,
		LockDuration: 5 * time.Minute,
		Now:          clock.Now,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return NewLockout(cfg)
}

// TestLockout_ConfigValidation 必填语义参数非法值 panic（配置错误启动即暴露）。
func TestLockout_ConfigValidation(t *testing.T) {
	base := LockoutConfig{Window: time.Minute, Threshold: 3, LockDuration: time.Minute}
	bad := []LockoutConfig{
		{Window: 0, Threshold: 3, LockDuration: time.Minute},
		{Window: -time.Second, Threshold: 3, LockDuration: time.Minute},
		{Window: time.Minute, Threshold: 0, LockDuration: time.Minute},
		{Window: time.Minute, Threshold: -1, LockDuration: time.Minute},
		{Window: time.Minute, Threshold: 3, LockDuration: 0},
		{Window: time.Minute, Threshold: 3, LockDuration: -time.Second},
	}
	for i, cfg := range bad {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("bad config #%d should panic: %+v", i, cfg)
				}
			}()
			NewLockoutOf[uint](cfg)
		}()
	}
	_ = NewLockout(base) // 合法配置不 panic
}

// TestLockout_TripsAndExpires 阈值前不锁定；第 N 次失败触发锁定（剩余=全额）；
// 剩余时长随时间衰减；到期自动解除。
func TestLockout_TripsAndExpires(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, nil)
	defer lo.Close()

	if locked, remain := lo.RecordFailure("k"); locked {
		t.Fatalf("failure 1 locked=%v remain=%v, want unlocked", locked, remain)
	}
	if locked, _ := lo.RecordFailure("k"); locked {
		t.Fatal("failure 2 must not lock")
	}
	if locked, remain := lo.CheckLocked("k"); locked {
		t.Fatalf("CheckLocked below threshold = (%v, %v), want unlocked", locked, remain)
	}

	locked, remain := lo.RecordFailure("k")
	if !locked || remain != 5*time.Minute {
		t.Fatalf("failure 3 = (%v, %v), want locked with full 5m remain", locked, remain)
	}
	if locked, remain := lo.CheckLocked("k"); !locked || remain != 5*time.Minute {
		t.Fatalf("CheckLocked = (%v, %v), want locked/5m", locked, remain)
	}

	clock.Advance(time.Minute)
	if locked, remain := lo.CheckLocked("k"); !locked || remain != 4*time.Minute {
		t.Fatalf("CheckLocked after 1m = (%v, %v), want locked/4m", locked, remain)
	}

	clock.Advance(4 * time.Minute) // 锁定到点
	if locked, remain := lo.CheckLocked("k"); locked {
		t.Fatalf("CheckLocked after expiry = (%v, %v), want unlocked", locked, remain)
	}
	if lo.Len() != 1 {
		t.Fatalf("len = %d, want 1 (expired entry kept until idle reclaim)", lo.Len())
	}
}

// TestLockout_WindowSliding 滑动窗口计数：窗口内样本累计触发；窗口外样本自然老化
// 不计数。
func TestLockout_WindowSliding(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, nil)
	defer lo.Close()

	// 老化路径：t0 失败 → 推进 11m（超窗口 10m，样本老化）→ 两次失败仍不锁定
	if locked, _ := lo.RecordFailure("age"); locked {
		t.Fatal("failure 1 must not lock")
	}
	clock.Advance(11 * time.Minute)
	if locked, _ := lo.RecordFailure("age"); locked {
		t.Fatal("aged-out window must restart counting")
	}
	if locked, _ := lo.RecordFailure("age"); locked {
		t.Fatal("only 2 samples in window, must not lock")
	}

	// 滑动路径：t0/9m 失败两次 → 9m+1s 第三次（三样本全在 10m 窗口内）→ 锁定
	clock2 := &fakeLockClock{t: lockoutStart}
	lo2 := newTestLockout(clock2, nil)
	defer lo2.Close()
	_, _ = lo2.RecordFailure("slide")
	clock2.Advance(9 * time.Minute)
	if locked, _ := lo2.RecordFailure("slide"); locked {
		t.Fatal("2 samples must not lock")
	}
	clock2.Advance(time.Minute - time.Second) // 距首个样本 9m+1s < 10m
	if locked, remain := lo2.RecordFailure("slide"); !locked || remain != 5*time.Minute {
		t.Fatalf("sliding window 3rd failure = (%v, %v), want locked/5m", locked, remain)
	}
}

// TestLockout_ResetClearsFailuresAndLock Reset 清零失败计数（重新累计），
// 亦解除现存锁定（管理员解封）；未登记 key 为 no-op。
func TestLockout_ResetClearsFailuresAndLock(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, nil)
	defer lo.Close()

	lo.Reset("ghost") // no-op，不 panic

	// 清零计数：2 次失败 → Reset → 再 2 次失败不锁定
	_, _ = lo.RecordFailure("k")
	_, _ = lo.RecordFailure("k")
	lo.Reset("k")
	if lo.Len() != 0 {
		t.Fatalf("len = %d, want 0 after reset", lo.Len())
	}
	if locked, _ := lo.RecordFailure("k"); locked {
		t.Fatal("count must restart from 0 after reset")
	}
	if locked, _ := lo.RecordFailure("k"); locked {
		t.Fatal("2 failures after reset must not lock")
	}

	// 解除锁定：触发后 Reset 立即解锁
	_, _ = lo.RecordFailure("k")
	if locked, _ := lo.RecordFailure("k"); !locked {
		t.Fatal("3rd failure must lock")
	}
	lo.Reset("k")
	if locked, remain := lo.CheckLocked("k"); locked {
		t.Fatalf("CheckLocked after reset = (%v, %v), want unlocked (admin unban)", locked, remain)
	}
}

// TestLockout_RepeatOffenderReLocks 重复违规者语义：锁定到期后旧失败仍在窗口内，
// 首个新失败即重新锁定（对齐 fail2ban findtime 行为，文档化于类型注释）。
func TestLockout_RepeatOffenderReLocks(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, func(c *LockoutConfig) { c.LockDuration = 2 * time.Minute })
	defer lo.Close()

	for i := 0; i < 3; i++ {
		locked, _ := lo.RecordFailure("k")
		if locked != (i == 2) {
			t.Fatalf("failure %d locked=%v, want %v", i+1, locked, i == 2)
		}
	}
	clock.Advance(2 * time.Minute) // 锁定到期；窗口 10m 内旧样本仍有效

	if locked, remain := lo.RecordFailure("k"); !locked || remain != 2*time.Minute {
		t.Fatalf("post-expiry failure = (%v, %v), want re-locked/2m (repeat offender)", locked, remain)
	}
}

// TestLockout_NoExtendWhileLocked 锁定不因后续失败延长：锁定 1m 后再失败，
// 剩余时长保持衰减口径而非重新全额。
func TestLockout_NoExtendWhileLocked(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, nil)
	defer lo.Close()

	for i := 0; i < 3; i++ {
		_, _ = lo.RecordFailure("k")
	}
	clock.Advance(time.Minute)
	if locked, remain := lo.RecordFailure("k"); !locked || remain != 4*time.Minute {
		t.Fatalf("failure while locked = (%v, %v), want locked/4m (no extension)", locked, remain)
	}
}

// TestLockout_MaxEntriesFailsOpen 容量护栏 fail-open：表满时未登记 key 的失败样本
// 丢弃（不误伤合法 key 的认证可用性）并计入 OverCapHits；已登记 key 照常触发锁定。
func TestLockout_MaxEntriesFailsOpen(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, func(c *LockoutConfig) {
		c.MaxEntries = 1
		c.IdleTTL = time.Hour // 防清扫抢先
	})
	defer lo.Close()

	_, _ = lo.RecordFailure("a") // 登记 "a"
	if locked, _ := lo.RecordFailure("b"); locked {
		t.Fatal("over-cap key must fail open (sample dropped)")
	}
	if lo.OverCapHits() == 0 {
		t.Fatal("over-cap drop must be counted")
	}
	if locked, remain := lo.CheckLocked("b"); locked {
		t.Fatalf("over-cap key CheckLocked = (%v, %v), want unlocked", locked, remain)
	}
	// 已登记 key 不受影响，照常触发
	_, _ = lo.RecordFailure("a")
	if locked, remain := lo.RecordFailure("a"); !locked || remain != 5*time.Minute {
		t.Fatalf("registered key trip = (%v, %v), want locked/5m", locked, remain)
	}
}

// TestLockout_LockedEntrySurvivesIdleSweep 空闲回收不驱逐锁定条目（提前驱逐等于
// 提前解锁——安全语义）；锁定解除后条目按空闲计时正常回收、容量腾出。
// 判定全部走注入时钟（确定性），真实 ticker 只定清扫节奏。
func TestLockout_LockedEntrySurvivesIdleSweep(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := newTestLockout(clock, func(c *LockoutConfig) {
		c.Threshold = 1
		c.LockDuration = time.Minute
		c.MaxEntries = 1
		c.IdleTTL = 30 * time.Millisecond
	})
	defer lo.Close()

	if locked, _ := lo.RecordFailure("a"); !locked {
		t.Fatal("threshold=1 must lock on first failure")
	}
	// 空闲已超 TTL 但锁定仍有效：清扫不得驱逐（容量仍被占用）
	clock.Advance(100 * time.Millisecond)
	time.Sleep(80 * time.Millisecond) // 真实 ticker（ttl/2 = 15ms）多轮清扫
	if locked, remain := lo.CheckLocked("a"); !locked || remain <= 0 {
		t.Fatalf("locked entry reclaimed early: (%v, %v) — eviction must skip locked entries", locked, remain)
	}
	if locked, _ := lo.RecordFailure("b"); locked {
		t.Fatal("over-cap drop expected: capacity held by locked entry")
	}
	if lo.OverCapHits() != 1 {
		t.Fatalf("overCapHits = %d, want 1", lo.OverCapHits())
	}

	// 锁定到期且空闲超时：清扫回收，容量腾出，新 key 正常登记（阈值 1 → 登记即锁定）
	clock.Advance(time.Minute + 100*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	if locked, _ := lo.RecordFailure("b"); !locked {
		t.Fatal("b must register after reclaim (threshold=1 locks it immediately)")
	}
	if lo.OverCapHits() != 1 {
		t.Fatalf("overCapHits = %d, want 1 (b must register after reclaim, not drop)", lo.OverCapHits())
	}
}

// TestLockout_GenericKey 泛型 key：uint 主键直用（对齐 KeyedLimiterOf 惯例）。
func TestLockout_GenericKey(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	lo := NewLockoutOf[uint](LockoutConfig{
		Window: time.Minute, Threshold: 2, LockDuration: time.Minute, Now: clock.Now,
	})
	defer lo.Close()

	_, _ = lo.RecordFailure(42)
	if locked, _ := lo.RecordFailure(42); !locked {
		t.Fatal("uint key must trip at threshold")
	}
	if locked, _ := lo.CheckLocked(7); locked {
		t.Fatal("other uint key must be independent")
	}
}

// TestLockout_ConcurrentHammer 并发失败记录/查询/清零 + 时钟推进（-race 看门）：
// 全程无数据竞争、无 panic（状态全部由单一 mu 串行化）。
func TestLockout_ConcurrentHammer(t *testing.T) {
	clock := &fakeLockClock{t: lockoutStart}
	cfg := LockoutConfig{
		Window: 10 * time.Minute, Threshold: 3, LockDuration: 5 * time.Minute,
		MaxEntries: 64, Now: clock.Now,
	}
	lo := NewLockoutOf[int](cfg)
	defer lo.Close()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clock.Advance(time.Second)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := i % 16
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch key % 4 {
				case 0:
					_, _ = lo.RecordFailure(key)
				case 1:
					_, _ = lo.CheckLocked(key)
				case 2:
					lo.Reset(key)
				default:
					_, _ = lo.RecordFailure(key)
					_, _ = lo.CheckLocked(key)
				}
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
