package scanrotation

import (
	"sync"
	"testing"
	"time"
)

// 契约测试（#339）：配对语义、预算语义、轮转公平性、attempted<=0 不动游标。

func fixedClock(at time.Time) func() time.Time {
	return func() time.Time { return at }
}

// 跨轮公平轮转：预算允许每轮 3 行、共 5 行——起点序列 0,3,1,4,2 恰好
// 每个下标各出现一次，第 6 轮回到 0。
func TestRotationFairness(t *testing.T) {
	rot := New(0, WithRotationClock(fixedClock(time.Unix(0, 0)))) // 不限时
	const n = 5
	got := make([]int, 0, 6)
	for range 6 {
		start, deadline := rot.Begin(n, 0)
		if !deadline.IsZero() {
			t.Fatalf("不限时轮的 deadline 应为零值: %v", deadline)
		}
		got = append(got, start)
		rot.Advance(start, 3, n)
	}
	want := []int{0, 3, 1, 4, 2, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("起点序列 = %v, want %v", got, want)
		}
	}
}

// 「一条都没试过」与「试完一轮」不是同一件事：attempted<=0 游标不动，
// 下一轮从同一位置重扫（静默漏推回归钉住）。
func TestAdvanceZeroAttemptedKeepsCursor(t *testing.T) {
	rot := New(0)
	rot.Advance(0, 3, 5) // 先转到 3
	rot.Advance(3, 0, 5) // 本轮一条没试
	if start, _ := rot.Begin(5, 0); start != 3 {
		t.Fatalf("attempted=0 后游标应停在 3, got %d", start)
	}
	rot.Advance(3, -1, 5)
	rot.Advance(3, 0, 0) // n<=0 同样不动
	if start, _ := rot.Begin(5, 0); start != 3 {
		t.Fatalf("n<=0 的 Advance 不得动游标, got %d", start)
	}
}

// 预算语义：构造 budget 优先，<=0 时用 def，两者都 <=0 不限时；
// n<=0 返回即刻过期的 deadline。
func TestBudgetSemantics(t *testing.T) {
	base := time.Unix(1000, 0)

	rot := New(20*time.Second, WithRotationClock(fixedClock(base)))
	_, dl := rot.Begin(10, 5*time.Second)
	if !dl.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("budget 优先: deadline = %v, want %v", dl, base.Add(20*time.Second))
	}

	rot2 := New(0, WithRotationClock(fixedClock(base)))
	_, dl2 := rot2.Begin(10, 5*time.Second)
	if !dl2.Equal(base.Add(5 * time.Second)) {
		t.Fatalf("budget<=0 用 def: deadline = %v, want %v", dl2, base.Add(5*time.Second))
	}

	_, dl3 := rot2.Begin(10, 0)
	if !dl3.IsZero() {
		t.Fatalf("budget 与 def 都 <=0 应不限时（零值 deadline）: %v", dl3)
	}

	_, dl4 := rot2.Begin(0, 20*time.Second)
	if !dl4.Equal(base) {
		t.Fatalf("n<=0 的 deadline 应即刻过期: %v, want %v", dl4, base)
	}
	if start, _ := rot2.Begin(0, 0); start != 0 {
		t.Fatalf("n<=0 起点 = %d, want 0", start)
	}
}

// 方法级并发安全：并发 Begin/Advance 无 data race（轮转顺序语义不承诺，
// 只锁内存安全）。
func TestConcurrentBeginAdvanceRaceFree(t *testing.T) {
	rot := New(time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				start, _ := rot.Begin(7, 0)
				rot.Advance(start, 3, 7)
			}
		})
	}
	wg.Wait()
	if start, _ := rot.Begin(7, 0); start < 0 || start >= 7 {
		t.Fatalf("游标越界: %d", start)
	}
}
