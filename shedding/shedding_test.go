package shedding

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/window"
)

// 测试窗口参数：500ms 切 5 桶（单桶 100ms，windowScale = 10/1000 = 0.01）。
// 假时钟推进，无真实 sleep。
const (
	testWindow         = 500 * time.Millisecond
	testBuckets        = 5
	testBucketInterval = testWindow / testBuckets // 100ms
)

// fakeClock 可手动推进的假时钟（原子计数，供并发测试安全读写）。
// 从真实量级的时刻起步：overloadTime 以 UnixNano 的 0 值为"从未过载"哨兵，
// 停在 epoch 0 会让冷却期判定失效（与 breaker 侧 lastPass 同款约束）。
type fakeClock struct {
	nanos atomic.Int64
}

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.Set(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	return c
}

func (c *fakeClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

func (c *fakeClock) Advance(d time.Duration) {
	c.nanos.Add(int64(d))
}

func (c *fakeClock) Set(t time.Time) {
	c.nanos.Store(t.UnixNano())
}

// cpuStub 可控的 CPU 使用率源 stub：val 运行期原子可变；err 构造后不可变
// （非 nil 时 get 恒返回该错误）。
type cpuStub struct {
	val atomic.Int64
	err error
}

func (s *cpuStub) set(v int64) { s.val.Store(v) }

func (s *cpuStub) get() (int64, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.val.Load(), nil
}

// useCPUStub 注入 stub 并注册测试结束时恢复原 CPU 源的清理。
func useCPUStub(t *testing.T, s *cpuStub) {
	t.Helper()
	prev := cpuSource.Load()
	SetCPUUsageFunc(s.get)
	t.Cleanup(func() { cpuSource.Store(prev) })
}

// newTestShedder 构造注入假时钟与 CPU stub 的小窗口降载器（白盒断言用）。
func newTestShedder(t *testing.T, clock *fakeClock, stub *cpuStub, opts ...ShedderOption) *adaptiveShedder {
	t.Helper()
	useCPUStub(t, stub)
	all := append([]ShedderOption{
		WithShedderWindow(testWindow, testBuckets),
		WithShedderClock(clock.Now),
	}, opts...)
	return NewAdaptiveShedder(all...).(*adaptiveShedder)
}

// primeThroughput 用连续成功请求把窗口统计打到确定状态：每轮先跨入新桶，
// 再放行 perBucket 个请求、每个 RT 为 rt（1µs 步进 → ceil 后 1ms）。
// 完成后：maxPass=perBucket、minRt=1（毫秒）、maxFlight=perBucket×1×0.01。
// rt 步进取 1µs 保证单轮全部请求落在同一桶内（perBucket×1µs ≪ 100ms 桶宽）。
func primeThroughput(t *testing.T, as *adaptiveShedder, clock *fakeClock, perBucket []int, rt time.Duration) {
	t.Helper()
	ctx := context.Background()
	for round, n := range perBucket {
		clock.Advance(testBucketInterval) // 跨入新桶：已完成的桶才计入 maxPass/minRt
		for i := 0; i < n; i++ {
			p, err := as.Allow(ctx)
			if err != nil {
				t.Fatalf("prime round %d request %d err = %v", round, i, err)
			}
			clock.Advance(rt)
			p.Pass()
		}
	}
}

// overloadShedder 把降载器打到"已发生首次丢弃"的确定性状态：
// prime(3×1000) → maxFlight=10；cpu=950 → 过载因子 0.5 → 容量上限 5；
// 悬挂 150 个在途 → 完成 1 个 → flying=150、avgFlying=15（> 5）→ 下一请求被丢弃。
// 悬挂 150（avgFlying=15）而非 100（=10）是刻意的：冷却期用例在 CPU 回落到 500
// 时过载因子夹到 1、容量上限回升到 maxFlight=10，须 avgFlying>10 才能继续丢弃。
func overloadShedder(t *testing.T, clock *fakeClock, stub *cpuStub, opts ...ShedderOption) *adaptiveShedder {
	t.Helper()
	as := newTestShedder(t, clock, stub, opts...)
	stub.set(100)
	primeThroughput(t, as, clock, []int{1000, 1000, 1000}, time.Microsecond)

	ctx := context.Background()
	// CPU 正常期悬挂 150 个在途请求（不回填）
	for i := 0; i < 150; i++ {
		if _, err := as.Allow(ctx); err != nil {
			t.Fatalf("inflight build allow %d err = %v", i, err)
		}
	}
	// CPU 过载（950 ≥ 900），但 avgFlying 仍为 0：双保险未同时满足 → 放行
	stub.set(950)
	p, err := as.Allow(ctx)
	if err != nil {
		t.Fatalf("allow before first completion err = %v, want pass (avgFlying lag)", err)
	}
	p.Pass() // flying 151→150，avgFlying = 0×0.9 + 150×0.1 = 15 > 5

	// 下一请求：过载 && 超限 → 确定性丢弃
	p2, err := as.Allow(ctx)
	if err != ErrServiceOverloaded {
		t.Fatalf("first overload drop err = %v, want bare ErrServiceOverloaded", err)
	}
	if p2 != nil {
		t.Fatal("dropped Allow must return nil promise")
	}
	return as
}

// sumWindow 聚合窗口全部有效桶的 Sum/Count（调用方须先跨桶使数据可见，
// IgnoreCurrentBucket 会跳过当前桶）。
func sumWindow(rw *window.RollingWindow[int64, *window.Bucket[int64]]) (sum, count int64) {
	rw.Reduce(func(b *window.Bucket[int64]) {
		sum += b.Sum
		count += b.Count
	})
	return sum, count
}

// TestShedder_LowCPUAlwaysAllows CPU 低于阈值时无论并发多高全放行
// （丢弃要求 (过载 || 冷却) && 超限 同时成立，低 CPU 直接短路）。
func TestShedder_LowCPUAlwaysAllows(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub)
	stub.set(500)

	primeThroughput(t, as, clock, []int{1000, 1000, 1000}, time.Microsecond)

	ctx := context.Background()
	for i := 0; i < 200; i++ {
		p, err := as.Allow(ctx)
		if err != nil {
			t.Fatalf("allow %d err = %v, want nil (low cpu never sheds)", i, err)
		}
		_ = p // 悬挂在途，flying 一路涨到 200+
	}
	if as.flying.Load() != 200 {
		t.Fatalf("flying = %d, want 200", as.flying.Load())
	}
	if got := as.avgFlyingSnapshot(); got != 0 {
		t.Fatalf("avgFlying = %v, want 0 (EMA updates only on completion)", got)
	}
}

// TestShedder_OverloadDropsExcess CPU 过载且在途并发超 maxFlight×过载因子 时
// 确定性丢弃；丢弃不消耗在途计数、错误为裸哨兵。
func TestShedder_OverloadDropsExcess(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := overloadShedder(t, clock, stub)

	// 白盒确认容量估计：maxPass=1000、minRt=1 → maxFlight=1000×1×0.01=10
	if got := as.maxFlight(); math.Abs(got-10) > 1e-9 {
		t.Fatalf("maxFlight = %v, want 10", got)
	}
	if got := as.maxPass(); got != 1000 {
		t.Fatalf("maxPass = %d, want 1000", got)
	}
	if got := as.minRt(); got != 1 {
		t.Fatalf("minRt = %v, want 1", got)
	}

	// 丢弃不改在途计数
	if got := as.flying.Load(); got != 150 {
		t.Fatalf("flying after drop = %d, want 150 (drop must not consume inflight)", got)
	}
	// 后续请求持续丢弃（仍过载且超限）
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if p, err := as.Allow(ctx); err != ErrServiceOverloaded || p != nil {
			t.Fatalf("drop %d = (%v, %v), want bare ErrServiceOverloaded + nil promise", i, p, err)
		}
	}
}

// TestShedder_LowTrafficAtOverloadPasses CPU 过载但并发低于容量上限时放行：
// 过载因子下限 0.1 保证至少 10% 容量永远可用（此处 avgFlying=0 双保险不成立）。
func TestShedder_LowTrafficAtOverloadPasses(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub)
	stub.set(100)
	primeThroughput(t, as, clock, []int{1000, 1000, 1000}, time.Microsecond)

	stub.set(999) // 重度过载：过载因子 = 1/100 → 夹到 0.1，容量上限 = 10×0.1 = 1
	ctx := context.Background()
	p, err := as.Allow(ctx)
	if err != nil {
		t.Fatalf("allow at overload with low inflight err = %v, want pass", err)
	}
	if as.flying.Load() != 1 {
		t.Fatalf("flying = %d, want 1", as.flying.Load())
	}
	p.Fail()
}

// TestShedder_CoolOff 最近发生过丢弃后，即使 CPU 已回落，冷却期（1s）内仍丢弃；
// 冷却期满恢复放行。冷却起点为最后一次 CPU 过载时刻（CPU 正常期间的丢弃
// 不刷新它）。
func TestShedder_CoolOff(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := overloadShedder(t, clock, stub)

	stub.set(500) // CPU 回落
	ctx := context.Background()

	// 冷却期内（+400ms < 1s）：仍丢弃
	clock.Advance(400 * time.Millisecond)
	if p, err := as.Allow(ctx); err != ErrServiceOverloaded || p != nil {
		t.Fatalf("allow within cool-off = (%v, %v), want drop", p, err)
	}
	// 仍在冷却期内（+800ms < 1s）：仍丢弃
	clock.Advance(400 * time.Millisecond)
	if p, err := as.Allow(ctx); err != ErrServiceOverloaded || p != nil {
		t.Fatalf("allow within cool-off (2nd) = (%v, %v), want drop", p, err)
	}
	// 冷却期满（累计 +1.1s > 1s，CPU 全程正常、未刷新冷却起点）：恢复放行
	clock.Advance(300 * time.Millisecond)
	p, err := as.Allow(ctx)
	if err != nil {
		t.Fatalf("allow after cool-off err = %v, want pass", err)
	}
	p.Pass()
}

// TestShedder_EMAFlyingLags avgFlying 只在完成（delta<0）时更新；并发骤降后
// 均值滞后偏高（少放行方向），且始终被夹在 (0, 初始并发) 内。
func TestShedder_EMAFlyingLags(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub)
	stub.set(100)

	ctx := context.Background()
	const n = 10
	promises := make([]Promise, 0, n)
	for i := 0; i < n; i++ {
		p, err := as.Allow(ctx)
		if err != nil {
			t.Fatalf("allow %d err = %v", i, err)
		}
		promises = append(promises, p)
	}
	if got := as.avgFlyingSnapshot(); got != 0 {
		t.Fatalf("avgFlying after allows = %v, want 0 (no completion yet)", got)
	}

	want := 0.0
	for k, p := range promises {
		p.Pass()
		want = want*flyingBeta + float64(n-k-1)*(1-flyingBeta)
		got := as.avgFlyingSnapshot()
		if math.Abs(got-want) > 1e-9 {
			t.Fatalf("after pass %d avgFlying = %v, want %v", k, got, want)
		}
		if got <= 0 || got >= n {
			t.Fatalf("avgFlying %v out of (0, %d)", got, n)
		}
	}
	if as.flying.Load() != 0 {
		t.Fatalf("flying = %d, want 0 after all passes", as.flying.Load())
	}
	// 骤降滞后：全部完成后 flying=0，均值仍显著高于 0（少放行）
	if got := as.avgFlyingSnapshot(); got <= 1 {
		t.Fatalf("avgFlying after rapid drop = %v, want > 1 (lag)", got)
	}
}

// TestShedder_PromiseMarking Pass 回填 RT（毫秒，向上取整）与通过数；
// Fail 只递减在途、不产生统计。
func TestShedder_PromiseMarking(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub)
	stub.set(100)

	ctx := context.Background()
	p1, err := as.Allow(ctx)
	if err != nil {
		t.Fatalf("allow err = %v", err)
	}
	clock.Advance(50 * time.Millisecond) // RT = 50ms
	p1.Pass()

	p2, err := as.Allow(ctx)
	if err != nil {
		t.Fatalf("allow err = %v", err)
	}
	p2.Fail() // 无统计样本

	if as.flying.Load() != 0 {
		t.Fatalf("flying = %d, want 0", as.flying.Load())
	}
	// 跨桶使当前桶数据对 Reduce 可见（IgnoreCurrentBucket 语义）
	clock.Advance(testBucketInterval)
	rtSum, rtCount := sumWindow(as.rtCounter)
	if rtSum != 50 || rtCount != 1 {
		t.Fatalf("rtCounter = (%d, %d), want (50, 1)", rtSum, rtCount)
	}
	passSum, passCount := sumWindow(as.passCounter)
	if passSum != 1 || passCount != 1 {
		t.Fatalf("passCounter = (%d, %d), want (1, 1)", passSum, passCount)
	}
}

// TestShedder_CtxCancelled ctx 已取消时 Allow 返回 ctx.Err()，不消耗统计。
func TestShedder_CtxCancelled(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub)
	stub.set(950) // 过载也不影响：ctx 检查在最前

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, err := as.Allow(ctx)
	if !errors.Is(err, context.Canceled) || p != nil {
		t.Fatalf("Allow on done ctx = (%v, %v), want (nil, context.Canceled)", p, err)
	}
	if as.flying.Load() != 0 {
		t.Fatalf("flying = %d, want 0 (done ctx must not consume stats)", as.flying.Load())
	}
}

// TestNopShedder_AlwaysAllows 空降载器恒放行、Promise 为 no-op；ctx 已取消时
// 仍遵循 ctx-aware 约定返回错误。
func TestNopShedder_AlwaysAllows(t *testing.T) {
	nop := NewNopShedder()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		p, err := nop.Allow(ctx)
		if err != nil || p == nil {
			t.Fatalf("nop allow %d = (%v, %v), want (promise, nil)", i, p, err)
		}
		p.Pass()
		p.Fail()
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if p, err := nop.Allow(cancelCtx); !errors.Is(err, context.Canceled) || p != nil {
		t.Fatalf("nop allow on done ctx = (%v, %v), want (nil, context.Canceled)", p, err)
	}
}

// TestShedderOption_Validation 非法选项值必须 panic（构造期快速失败）。
func TestShedderOption_Validation(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"cpu threshold 0", func() { WithShedderCPUThreshold(0) }},
		{"cpu threshold negative", func() { WithShedderCPUThreshold(-100) }},
		{"cpu threshold 1000", func() { WithShedderCPUThreshold(1000) }},
		{"cpu threshold over max", func() { WithShedderCPUThreshold(1500) }},
		{"window 0", func() { WithShedderWindow(0, 5) }},
		{"window negative", func() { WithShedderWindow(-time.Second, 5) }},
		{"buckets 0", func() { WithShedderWindow(time.Second, 0) }},
		{"buckets negative", func() { WithShedderWindow(time.Second, -1) }},
		{"window narrower than buckets", func() { WithShedderWindow(3, 5) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.fn()
		})
	}

	// 合法边界：window 恰等于 buckets（单桶 1ns）
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub, WithShedderWindow(5, 5), WithShedderCPUThreshold(1))
	if p, err := as.Allow(context.Background()); err != nil {
		t.Fatalf("boundary options allow err = %v", err)
	} else {
		p.Pass()
	}
}

// TestShedder_OverloadFactorClamp 过载因子公式与 [0.1, 1] 夹取。
func TestShedder_OverloadFactorClamp(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}
	as := newTestShedder(t, clock, stub) // 缺省阈值 900

	cases := []struct {
		cpu  int64
		want float64
	}{
		{0, 1},   // (1000-0)/100 = 10 → 夹到 1
		{900, 1}, // 恰在阈值：(100)/100 = 1
		{950, 0.5},
		{990, 0.1},  // 0.1 恰在下限
		{999, 0.1},  // 0.01 → 夹到 0.1
		{1000, 0.1}, // 0 → 夹到 0.1
		{1100, 0.1}, // 负值 → 夹到 0.1
	}
	for _, tc := range cases {
		stub.set(tc.cpu)
		if got := as.overloadFactor(); math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("cpu=%d overloadFactor = %v, want %v", tc.cpu, got, tc.want)
		}
	}
}

// TestShedder_CPUErrorFailOpen CPU 源错误时按 0 处理（fail-open）：
// 即使并发爆表也全放行；currentCPUUsage 透传注入源的错误。
func TestShedder_CPUErrorFailOpen(t *testing.T) {
	boom := errors.New("proc missing")
	stub := &cpuStub{err: boom}
	useCPUStub(t, stub)
	if v, err := currentCPUUsage(); !errors.Is(err, boom) || v != 0 {
		t.Fatalf("currentCPUUsage = (%d, %v), want (0, boom)", v, err)
	}

	clock := newFakeClock()
	as := newTestShedder(t, clock, stub)
	primeThroughput(t, as, clock, []int{1000, 1000, 1000}, time.Microsecond)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := as.Allow(ctx); err != nil {
			t.Fatalf("allow %d err = %v, want pass (error source must fail open)", i, err)
		}
	}
}

// TestEmaCPUUsage 缺省采样管线的 EMA 单步公式（beta=0.95，整型截断）。
func TestEmaCPUUsage(t *testing.T) {
	cases := []struct {
		prev, cur, want int64
	}{
		{0, 1000, 50},  // 冷启动首样本
		{50, 1000, 97}, // 47.5 + 50 = 97.5 → 截断 97
		{97, 0, 92},    // 92.15 → 92（错误样本按 0，逐步衰减）
		{1000, 1000, 1000},
		{0, 0, 0},
	}
	for _, tc := range cases {
		if got := emaCPUUsage(tc.prev, tc.cur); got != tc.want {
			t.Fatalf("emaCPUUsage(%d, %d) = %d, want %d", tc.prev, tc.cur, got, tc.want)
		}
	}
}

// TestShedder_DropHandler 丢弃事件触发注册的回调，携带上游同格式诊断消息；
// 缺省（不注册）不触发即可用。
func TestShedder_DropHandler(t *testing.T) {
	clock := newFakeClock()
	stub := &cpuStub{}

	var mu sync.Mutex
	var reasons []string
	as := overloadShedder(t, clock, stub, WithShedderRejectHandler(func(reason string) {
		mu.Lock()
		defer mu.Unlock()
		reasons = append(reasons, reason)
	}))

	// overloadShedder 内含一次丢弃；再补一次
	if _, err := as.Allow(context.Background()); err != ErrServiceOverloaded {
		t.Fatalf("second drop err = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 2 {
		t.Fatalf("drop handler calls = %d, want 2", len(reasons))
	}
	for _, r := range reasons {
		if !strings.Contains(r, "dropreq") || !strings.Contains(r, "cpu: 950") {
			t.Fatalf("drop reason = %q, want upstream-format dropreq message with cpu: 950", r)
		}
	}
}

// TestSetCPUUsageFunc_ResetToDefault 传 nil 清除注入恢复缺省管线；
// 缺省管线恒不返回错误、值域 [0,1000]。放在文件末尾：唯一会惰性启动真实
// 平台采样 goroutine 的用例（其余用例全部走注入路径，不产生真实 250ms tick）。
func TestSetCPUUsageFunc_ResetToDefault(t *testing.T) {
	stub := &cpuStub{}
	stub.set(777)
	useCPUStub(t, stub)
	if v, err := currentCPUUsage(); err != nil || v != 777 {
		t.Fatalf("injected currentCPUUsage = (%d, %v), want (777, nil)", v, err)
	}

	// nil 清除注入 → 缺省管线
	SetCPUUsageFunc(nil)
	v, err := currentCPUUsage()
	if err != nil {
		t.Fatalf("default currentCPUUsage err = %v, want nil", err)
	}
	if v < 0 || v > cpuMax {
		t.Fatalf("default currentCPUUsage = %d, out of [0, %d]", v, cpuMax)
	}

	// 再注入立即生效
	useCPUStub(t, stub)
	if v, err := currentCPUUsage(); err != nil || v != 777 {
		t.Fatalf("re-injected currentCPUUsage = (%d, %v), want (777, nil)", v, err)
	}
}
