package window

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// bucketInterval 单桶时间宽度；测试用假时钟推进，不真实 sleep。
const bucketInterval = time.Millisecond * 50

// fakeClock 可手动推进的假时钟（原子计数，供并发测试安全读写）。
type fakeClock struct {
	nanos atomic.Int64
}

func (c *fakeClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load())
}

func (c *fakeClock) Advance(d time.Duration) {
	c.nanos.Add(int64(d))
}

func newTestBucket() *Bucket[float64] {
	return new(Bucket[float64])
}

func TestRollingWindow_New(t *testing.T) {
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, 10, time.Second)
	if r == nil {
		t.Fatal("expected non-nil RollingWindow")
	}

	for _, size := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("size %d should panic", size)
				}
			}()
			NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, time.Second)
		}()
	}
}

// TestRollingWindow_NilClockFallback 注入 nil 时钟应回退 time.Now，不 panic。
func TestRollingWindow_NilClockFallback(t *testing.T) {
	r := NewRollingWindow[int64, *Bucket[int64]](func() *Bucket[int64] { return new(Bucket[int64]) }, 2, bucketInterval,
		WithWindowClock[int64, *Bucket[int64]](nil))
	r.Add(1)
	var sum int64
	r.Reduce(func(b *Bucket[int64]) { sum += b.Sum })
	if sum != 1 {
		t.Errorf("sum = %d, want 1", sum)
	}
}

func TestRollingWindow_Add(t *testing.T) {
	const size = 3
	clk := &fakeClock{}
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, bucketInterval,
		WithWindowClock[float64, *Bucket[float64]](clk.Now))
	listBuckets := func() (sums []float64, counts []int64) {
		r.Reduce(func(b *Bucket[float64]) {
			sums = append(sums, b.Sum)
			counts = append(counts, b.Count)
		})
		return
	}
	assertBuckets := func(wantSums []float64, wantCounts []int64) {
		t.Helper()
		gotSums, gotCounts := listBuckets()
		if !equalValues(gotSums, wantSums) {
			t.Errorf("sums = %v, want %v", gotSums, wantSums)
		}
		if !equalValues(gotCounts, wantCounts) {
			t.Errorf("counts = %v, want %v", gotCounts, wantCounts)
		}
	}

	assertBuckets([]float64{0, 0, 0}, []int64{0, 0, 0})
	r.Add(1)
	assertBuckets([]float64{0, 0, 1}, []int64{0, 0, 1})
	clk.Advance(bucketInterval)
	r.Add(2)
	r.Add(3)
	assertBuckets([]float64{0, 1, 5}, []int64{0, 1, 2})
	clk.Advance(bucketInterval)
	r.Add(4)
	r.Add(5)
	r.Add(6)
	assertBuckets([]float64{1, 5, 15}, []int64{1, 2, 3})
	clk.Advance(bucketInterval)
	r.Add(7)
	assertBuckets([]float64{5, 15, 7}, []int64{2, 3, 1})
}

// TestRollingWindow_Reset 覆盖惰性重置与 IgnoreCurrentBucket：
// 推进后过期桶惰性清零、整窗过期后 Reduce 无有效桶。
func TestRollingWindow_Reset(t *testing.T) {
	const size = 3
	clk := &fakeClock{}
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, bucketInterval,
		WithWindowClock[float64, *Bucket[float64]](clk.Now),
		IgnoreCurrentBucket[float64, *Bucket[float64]]())
	listBuckets := func() []float64 {
		var buckets []float64
		r.Reduce(func(b *Bucket[float64]) {
			buckets = append(buckets, b.Sum)
		})
		return buckets
	}

	r.Add(1)
	clk.Advance(bucketInterval)
	if got := listBuckets(); !equalValues(got, []float64{0, 1}) {
		t.Errorf("buckets = %v, want [0 1]", got)
	}
	clk.Advance(bucketInterval)
	if got := listBuckets(); !equalValues(got, []float64{1}) {
		t.Errorf("buckets = %v, want [1]", got)
	}
	clk.Advance(bucketInterval)
	if got := listBuckets(); len(got) != 0 {
		t.Errorf("buckets = %v, want empty", got)
	}

	// cross window：整窗过期后再写入，旧数据全部被惰性重置
	r.Add(1)
	clk.Advance(bucketInterval * 10)
	if got := listBuckets(); len(got) != 0 {
		t.Errorf("buckets = %v, want empty after cross window", got)
	}
}

func TestRollingWindow_Reduce(t *testing.T) {
	const size = 4
	tests := []struct {
		name   string
		opts   []WindowOption[float64, *Bucket[float64]]
		expect float64
	}{
		{
			name:   "default",
			opts:   nil,
			expect: 10,
		},
		{
			name:   "ignoreCurrent",
			opts:   []WindowOption[float64, *Bucket[float64]]{IgnoreCurrentBucket[float64, *Bucket[float64]]()},
			expect: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clk := &fakeClock{}
			opts := append([]WindowOption[float64, *Bucket[float64]]{WithWindowClock[float64, *Bucket[float64]](clk.Now)}, test.opts...)
			r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, bucketInterval, opts...)
			for x := 0; x < size; x++ {
				for i := 0; i <= x; i++ {
					r.Add(float64(i))
				}
				if x < size-1 {
					clk.Advance(bucketInterval)
				}
			}
			var result float64
			r.Reduce(func(b *Bucket[float64]) {
				result += b.Sum
			})
			if result != test.expect {
				t.Errorf("result = %v, want %v", result, test.expect)
			}
		})
	}
}

// TestRollingWindow_BucketTimeBoundary 覆盖跨 interval 边界与时间对齐：
// 半个 interval 的推进不触发跨桶、跨桶后 lastTime 对齐到边界。
func TestRollingWindow_BucketTimeBoundary(t *testing.T) {
	const size = 3
	interval := time.Millisecond * 30
	clk := &fakeClock{}
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, interval,
		WithWindowClock[float64, *Bucket[float64]](clk.Now))
	listBuckets := func() []float64 {
		var buckets []float64
		r.Reduce(func(b *Bucket[float64]) {
			buckets = append(buckets, b.Sum)
		})
		return buckets
	}
	assertBuckets := func(want []float64) {
		t.Helper()
		if got := listBuckets(); !equalValues(got, want) {
			t.Errorf("buckets = %v, want %v", got, want)
		}
	}

	assertBuckets([]float64{0, 0, 0})
	r.Add(1)
	assertBuckets([]float64{0, 0, 1})
	clk.Advance(time.Millisecond * 45) // 1.5 个 interval：span=1，对齐到 30ms 边界
	r.Add(2)
	r.Add(3)
	assertBuckets([]float64{0, 1, 5})
	clk.Advance(time.Millisecond * 20) // 小于 interval，但距上次对齐已跨过边界
	r.Add(4)
	r.Add(5)
	r.Add(6)
	assertBuckets([]float64{1, 5, 15})
	clk.Advance(time.Millisecond * 100) // 整窗过期
	r.Add(7)
	r.Add(8)
	r.Add(9)
	assertBuckets([]float64{0, 0, 24})
}

// maxBucket 自定义桶：只追踪窗口内最大值与次数，验证桶工厂可定制。
type maxBucket struct {
	max   int64
	count int64
}

func (b *maxBucket) Add(v int64) {
	if v > b.max {
		b.max = v
	}
	b.count++
}

func (b *maxBucket) Reset() {
	b.max = 0
	b.count = 0
}

// TestRollingWindow_CustomBucket 覆盖自定义 bucket 工厂与 Reduce 聚合。
func TestRollingWindow_CustomBucket(t *testing.T) {
	const size = 3
	clk := &fakeClock{}
	r := NewRollingWindow[int64, *maxBucket](func() *maxBucket { return new(maxBucket) }, size, bucketInterval,
		WithWindowClock[int64, *maxBucket](clk.Now))
	aggregate := func() (maxV, total int64) {
		r.Reduce(func(b *maxBucket) {
			if b.max > maxV {
				maxV = b.max
			}
			total += b.count
		})
		return
	}

	r.Add(5)
	r.Add(2)
	clk.Advance(bucketInterval)
	r.Add(9)

	maxV, total := aggregate()
	if maxV != 9 {
		t.Errorf("max = %d, want 9", maxV)
	}
	if total != 3 {
		t.Errorf("total count = %d, want 3", total)
	}

	// 整窗过期：所有桶被惰性重置，聚合归零
	clk.Advance(bucketInterval * time.Duration(size))
	maxV, total = aggregate()
	if maxV != 0 || total != 0 {
		t.Errorf("after full window: max = %d, total = %d, want 0, 0", maxV, total)
	}
}

// TestRollingWindow_DataRace 并发 Add/Reduce/时钟推进，须在 -race 下无告警。
func TestRollingWindow_DataRace(t *testing.T) {
	const size = 3
	const iterations = 200
	clk := &fakeClock{}
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, bucketInterval,
		WithWindowClock[float64, *Bucket[float64]](clk.Now))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			r.Add(float64(i))
			clk.Advance(bucketInterval / 2)
			time.Sleep(time.Millisecond)
		}
		close(stop)
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.Reduce(func(b *Bucket[float64]) {})
			}
		}
	}()

	wg.Wait()
}

func equalValues[T comparable](got, want []T) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestRollingWindow_ExplicitReset 显式 Reset 清空全部桶数据：Reduce 立即为空，
// 时间簿记（offset/lastTime）保留——Reset 后继续 Add/跨桶推进语义与新建窗口一致。
func TestRollingWindow_ExplicitReset(t *testing.T) {
	const size = 3
	clk := &fakeClock{}
	r := NewRollingWindow[float64, *Bucket[float64]](newTestBucket, size, bucketInterval,
		WithWindowClock[float64, *Bucket[float64]](clk.Now))
	sum := func() float64 {
		var total float64
		r.Reduce(func(b *Bucket[float64]) { total += b.Sum })
		return total
	}

	r.Add(1)
	r.Add(2)
	if got := sum(); got != 3 {
		t.Fatalf("sum before reset = %v, want 3", got)
	}

	r.Reset()
	if got := sum(); got != 0 {
		t.Fatalf("sum after reset = %v, want 0", got)
	}

	// Reset 后继续写入、跨桶推进：行为与新建窗口一致
	r.Add(5)
	clk.Advance(bucketInterval)
	r.Add(7)
	if got := sum(); got != 12 {
		t.Fatalf("sum after post-reset writes = %v, want 12", got)
	}
	clk.Advance(bucketInterval * 10) // 整窗滚出
	if got := sum(); got != 0 {
		t.Fatalf("sum after window expiry = %v, want 0", got)
	}
}
