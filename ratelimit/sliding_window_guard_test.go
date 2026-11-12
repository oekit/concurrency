package ratelimit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestNewSlidingWindow_NegativeMaxCountPanics 锁定 #135：负配额没有任何可辩护
// 语义，必须启动即暴露（对齐 Lockout「非法值 panic」哲学）——静默恒拒限流器
// 的症状（「全被拒」）与「真被打满」不可分辨，值班先去查流量而不会怀疑配置。
func TestNewSlidingWindow_NegativeMaxCountPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("maxCount<0 应 panic（启动即暴露）")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "maxCount") {
			t.Fatalf("panic 消息应点名 maxCount，got %v", r)
		}
	}()
	NewSlidingWindow(time.Minute, -3)
}

// TestNewSlidingWindow_NonPositiveWindowPanics 同口径：窗口为 0 的滑窗没有语义，
// 对齐 Lockout 的 Window 守卫。
func TestNewSlidingWindow_NonPositiveWindowPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("window<=0 应 panic（启动即暴露）")
		}
	}()
	NewSlidingWindow(0, 10)
}

// TestSlidingWindow_ZeroMaxCountUnlimited 锁定 #135 推荐形态的另一半：
// maxCount==0 = 不限流（恒放行、不记账），对齐同包 WithMaxEntries /
// LockoutConfig.MaxEntries 的 0=不限口径与宿主配置面直觉。
func TestSlidingWindow_ZeroMaxCountUnlimited(t *testing.T) {
	sw := NewSlidingWindow(time.Minute, 0)

	// 大量请求全部放行——修复前这里是静默恒拒
	for i := range 1000 {
		if !sw.Allow() {
			t.Fatalf("maxCount=0 是不限流档：第 %d 次 Allow 不应被拒", i+1)
		}
	}
	if !sw.AllowN(500) {
		t.Fatal("不限流档 AllowN 应恒放行")
	}
	// 不记账：Count 恒 0（不限流档没有「窗口内请求数」可言）
	if n := sw.Count(); n != 0 {
		t.Fatalf("不限流档不应记账：Count=%d，want 0", n)
	}
	// Reserve / Wait 同口径立即放行
	if d, err := sw.Reserve(); err != nil || d != 0 {
		t.Fatalf("不限流档 Reserve 应立即放行，got (%v, %v)", d, err)
	}
	if err := sw.Wait(context.Background()); err != nil {
		t.Fatalf("不限流档 Wait 应立即返回 nil，got %v", err)
	}
}

// TestSlidingWindow_ReserveBoundedByMaxCount 锁定 #190 的记账不变量：
// 修复前每笔 Reserve 无条件 append 且不清理，1000 次后 Count 在 maxCount=2 下
// 涨到 1002；修复后任意时刻 Count ≤ maxCount（预占未到期不计入），在册预占
// 达 maxCount 后 Reserve 返回 -1（不可预留）而非无限透支未来窗口。
func TestSlidingWindow_ReserveBoundedByMaxCount(t *testing.T) {
	sw := NewSlidingWindow(time.Hour, 2)

	negative := 0
	for i := range 1000 {
		_, err := sw.Reserve()
		if errors.Is(err, ErrNotReservable) {
			negative++
		}
		if n := sw.Count(); n > 2 {
			t.Fatalf("第 %d 次 Reserve 后 Count=%d，超过 maxCount=2（记账失真）", i+1, n)
		}
	}
	// 2 个立即放行 + 2 个预占（cap = maxCount），此后全部不可预留
	if negative != 996 {
		t.Fatalf("不可预留（ErrNotReservable）次数 = %d，期望 996（在册预占封顶 maxCount）", negative)
	}
	if n := sw.Count(); n != 2 {
		t.Fatalf("1000 次 Reserve 后 Count=%d，期望 2（预占未到期不计入已落账）", n)
	}
	// 满账 + 满预占：Allow 也必须拒绝
	if sw.Allow() {
		t.Fatal("已满额 + 预占满额时 Allow 不应放行")
	}
}

// TestSlidingWindow_ReserveDistinctSlots 锁定 #190 的「槽位互不相同」承诺：
// 预占时刻由 timestamps[len-maxCount]+window 边界推进——初始槽位错开 70ms 的
// 两笔预占必须拿到错开 ~70ms 的两个槽位；修复前两笔都从最老一条（ts[0]）算
// wait，拿到**同一时刻**（集体踩点）。
func TestSlidingWindow_ReserveDistinctSlots(t *testing.T) {
	const window = 200 * time.Millisecond
	sw := NewSlidingWindow(window, 2)

	if !sw.Allow() {
		t.Fatal("首个 Allow 应放行")
	}
	time.Sleep(70 * time.Millisecond) // 让第二个槽位比第一个晚 70ms
	if !sw.Allow() {
		t.Fatal("第二个 Allow 应放行")
	}

	w1, err1 := sw.Reserve() // 槽位 = ts[0]+window（wait ≈ 130ms）
	w2, err2 := sw.Reserve() // 槽位 = ts[1]+window（wait ≈ w1+70ms）
	if err1 != nil || err2 != nil {
		t.Fatalf("满窗后的两笔预占都应受理：(%v, %v)", err1, err2)
	}

	if w1 <= 0 || w2 <= 0 {
		t.Fatalf("满窗后的两笔预占都应等待：w1=%v w2=%v", w1, w2)
	}
	gap := w2 - w1
	if gap < 50*time.Millisecond || gap > 90*time.Millisecond {
		t.Fatalf("两笔预占的槽位应错开 ~70ms（互不相同的槽位），实测差 %v（w1=%v w2=%v）", gap, w1, w2)
	}
}

// TestSlidingWindow_ConcurrentReserveNoStampede 并发预留者拿到互不相同的槽位：
// 预占时刻按 timestamps[len-maxCount]+window 边界推进——满窗 4 个错开 2ms 的
// 槽位后，4 个并发 Reserve 分别落到这 4 个槽位的到期时刻，两两不同；
// 修复前 wait 恒由 ts[0] 算出，四者同刻（集体踩点）。
func TestSlidingWindow_ConcurrentReserveNoStampede(t *testing.T) {
	const maxCount = 4
	sw := NewSlidingWindow(time.Minute, maxCount)
	for i := range maxCount {
		if !sw.Allow() {
			t.Fatalf("第 %d 个 Allow 应放行", i+1)
		}
		time.Sleep(2 * time.Millisecond) // 4 个槽位两两错开 2ms，断言窗口就宽了
	}

	const callers = maxCount
	waits := make([]time.Duration, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Go(func() {
			<-start
			waits[i], _ = sw.Reserve()
		})
	}
	close(start)
	wg.Wait()

	seen := make(map[time.Duration]bool, callers)
	for i, w := range waits {
		if w < 0 {
			t.Fatalf("第 %d 个并发预留者不应被拒（预占未达 cap）：%v", i, w)
		}
		if seen[w] {
			t.Fatalf("并发预留者拿到同一槽位：wait=%v 出现多次（全部 waits=%v）", w, waits)
		}
		seen[w] = true
	}
	// 第 5 笔：在册预占已达 maxCount，不可再预留（规范哨兵 errors.Is 可判定）
	if _, err := sw.Reserve(); !errors.Is(err, ErrNotReservable) {
		t.Fatalf("预占满额后 Reserve 应返回 ErrNotReservable（不可预留），got %v", err)
	}
}

// TestSlidingWindow_ReserveLinearNotQuadratic 锁定 #190 的复杂度回归：
// 修复前 cleanOld 每次全表扫描 + timestamps 无界增长，40000 次连续 Reserve
// 实测 4.5s（调用量翻倍耗时 ×7.6，二次方）；修复后列表长度恒 ≤ 2*maxCount，
// 每次调用 O(maxCount)——总量线性。2s 预算是为 CI 慢机留的裕量（修复前
// 本用例 ~4.5s 起步，判红可靠）。
func TestSlidingWindow_ReserveLinearNotQuadratic(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过性能回归用例")
	}
	sw := NewSlidingWindow(time.Hour, 4)

	start := time.Now()
	const calls = 40000
	for range calls {
		sw.Reserve()
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("%d 次 Reserve 耗时 %v（>2s）——列表无界增长/全表扫描的二次方退化复现", calls, elapsed)
	}
}

// TestSlidingWindow_RetryAfter_NonConsumingProbe 锁定 #246 的实现面：RetryAfter
// 是不消耗配额的探测——空位回 (0, true)、满窗回最早槽位滑出时刻（与 Wait 的
// 空位计算同一口径）、探测前后账面不变。429 + Retry-After 场景的服务端据此
// 回填等待时长，不再恒 0 引发重试风暴。
func TestSlidingWindow_RetryAfter_NonConsumingProbe(t *testing.T) {
	sw := NewSlidingWindow(time.Hour, 2)

	// 不限流档恒 (0, true)
	unl := NewSlidingWindow(time.Minute, 0)
	if d, ok := unl.RetryAfter(); !ok || d != 0 {
		t.Fatalf("不限流档 RetryAfter = (%v, %v), want (0, true)", d, ok)
	}

	// 有空位：立即可用
	if d, ok := sw.RetryAfter(); !ok || d != 0 {
		t.Fatalf("空窗口 RetryAfter = (%v, %v), want (0, true)", d, ok)
	}

	// 打满：探测到真实等待，且账面不变（不消耗）
	sw.Allow()
	sw.Allow()
	d, ok := sw.RetryAfter()
	if !ok {
		t.Fatal("满窗探测 ok 应为 true（滑窗无「限流器不可用」形态）")
	}
	if d <= 0 || d > time.Hour {
		t.Fatalf("满窗探测应返回 (0, window] 内的真实等待，got %v", d)
	}
	if d2, _ := sw.RetryAfter(); d2 <= 0 {
		t.Fatal("重复探测不得消耗配额（第二次仍应有等待）")
	}
	if n := sw.Count(); n != 2 {
		t.Fatalf("探测后 Count=%d, want 2（探测不落账）", n)
	}
	if sw.Allow() {
		t.Fatal("探测不得消耗配额（满窗 Allow 仍应被拒）")
	}
}
