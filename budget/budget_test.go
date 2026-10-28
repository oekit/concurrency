package budget

import (
	"errors"
	"math"
	"testing"
	"time"
)

func newTestLedger() (*Ledger, *fakeClock) {
	clk := newFakeClock(time.Unix(0, 0))
	l := NewLedger(map[Dimension]int64{
		"bytes": 100,
		"calls": 5,
	}, WithClock(clk.Now))
	return l, clk
}

func TestConsume_WithinBudget(t *testing.T) {
	l, _ := newTestLedger()
	ref, err := l.Consume("bytes", 60, []byte("half"))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if ref != 0 {
		t.Fatalf("successful consume should return zero Ref, got %d", ref)
	}
	if _, err = l.Consume("bytes", 40, []byte("rest")); err != nil {
		t.Fatalf("consume to cap: %v", err)
	}
	if _, err = l.Consume("bytes", 1, nil); !errors.Is(err, ErrOverBudget) {
		t.Fatalf("over-cap consume should return ErrOverBudget, got %v", err)
	}
}

func TestConsume_OverBudgetArchives(t *testing.T) {
	l, _ := newTestLedger()
	_, _ = l.Consume("bytes", 100, nil)

	content := []byte("full-original-output")
	ref, err := l.Consume("bytes", 250, content)
	if !errors.Is(err, ErrOverBudget) {
		t.Fatalf("err = %v, want ErrOverBudget", err)
	}
	if ref == 0 {
		t.Fatal("over-budget consume should return archive Ref")
	}
	got, ok := l.Get(ref)
	if !ok {
		t.Fatal("archived content not retrievable")
	}
	if string(got) != string(content) {
		t.Fatalf("archive content mismatch: %q", got)
	}
	// 超限不计入消耗（内容未通过预算框）
	ref2, err := l.Consume("bytes", 1, []byte("small"))
	if !errors.Is(err, ErrOverBudget) {
		t.Fatalf("budget should remain full, got %v", err)
	}
	_ = ref2
}

func TestConsume_UnknownDimensionUnlimited(t *testing.T) {
	l, _ := newTestLedger()
	for i := 0; i < 100; i++ {
		if _, err := l.Consume("untracked", 1000, nil); err != nil {
			t.Fatalf("untracked dimension should be unlimited: %v", err)
		}
	}
}

func TestGet_UnknownRef(t *testing.T) {
	l, _ := newTestLedger()
	if _, ok := l.Get(Ref(999)); ok {
		t.Fatal("unknown ref should miss")
	}
}

func TestSweep_ByAge(t *testing.T) {
	l, clk := newTestLedger()
	_, _ = l.Consume("bytes", 100, nil) // 占满

	oldRef, err := l.Consume("bytes", 200, []byte("old"))
	if !errors.Is(err, ErrOverBudget) {
		t.Fatal("expected over budget")
	}
	clk.Advance(2 * time.Hour)
	newRef, err := l.Consume("bytes", 200, []byte("new"))
	if !errors.Is(err, ErrOverBudget) {
		t.Fatal("expected over budget")
	}

	if n := l.Sweep(time.Hour); n != 1 {
		t.Fatalf("swept %d entries, want 1", n)
	}
	if _, ok := l.Get(oldRef); ok {
		t.Fatal("old archive should be swept")
	}
	if got, ok := l.Get(newRef); !ok || string(got) != "new" {
		t.Fatal("fresh archive should survive sweep")
	}
}

func TestSweep_ZeroAge_SweepsAll(t *testing.T) {
	l, _ := newTestLedger()
	_, _ = l.Consume("bytes", 100, nil)
	ref, err := l.Consume("bytes", 50, []byte("x"))
	if !errors.Is(err, ErrOverBudget) {
		t.Fatal("expected over budget")
	}
	if n := l.Sweep(0); n != 1 {
		t.Fatalf("swept %d, want 1", n)
	}
	if _, ok := l.Get(ref); ok {
		t.Fatal("swept ref should miss")
	}
}

func TestConsume_NegativeAmount(t *testing.T) {
	l, _ := newTestLedger()
	if _, err := l.Consume("bytes", -1, nil); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("err = %v, want ErrInvalidAmount", err)
	}
}

func TestZeroCapDimension_AlwaysOver(t *testing.T) {
	l := NewLedger(map[Dimension]int64{"off": 0})
	if _, err := l.Consume("off", 1, []byte("x")); !errors.Is(err, ErrOverBudget) {
		t.Fatalf("zero cap should always over, got %v", err)
	}
}

// TestConsume_HugeAmountDoesNotOverflow 回归测试（#104）：判据曾是 int64 上的
// 先加后比 `used+n > cap` ⇒ n 接近 math.MaxInt64 时加法回绕成负数 ⇒ 判据恒为
// 「未超限」⇒ 该笔被**直接放行**且计数器被写成负数 ⇒ 此后该维度**永久**变成
// 「无限制」（连普通消耗都拦不住）。
func TestConsume_HugeAmountDoesNotOverflow(t *testing.T) {
	l, _ := newTestLedger()
	if _, err := l.Consume("bytes", 1, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}

	ref, err := l.Consume("bytes", math.MaxInt64, []byte("huge"))
	if !errors.Is(err, ErrOverBudget) {
		t.Fatalf("huge amount should be rejected, got err=%v ref=%d", err, ref)
	}
	if ref == 0 {
		t.Fatal("over-budget consume should return archive Ref")
	}
	if got, ok := l.Get(ref); !ok || string(got) != "huge" {
		t.Fatal("archived content not retrievable")
	}

	// 计数器必须保持正值——变负即该维度永久失效。
	if used := l.Used("bytes"); used != 1 {
		t.Fatalf("used = %d, want 1（计数器被写成负数 ⇒ 该维度永久失效）", used)
	}
	// 预算仍然有效：后续任何消耗都应被拦下。
	if _, err := l.Consume("bytes", 1000, nil); !errors.Is(err, ErrOverBudget) {
		t.Fatalf("budget should still be enforced after overflow attempt, got %v", err)
	}
}

// TestConsume_UsedNeverExceedsCap 钉住「used 永不超过 cap」这条不变量：
// 判据改成不溢出的形式后，累加不可能越过 cap；而饱和钳制是第二道保险。
func TestConsume_UsedNeverExceedsCap(t *testing.T) {
	l, _ := newTestLedger()
	if _, err := l.Consume("bytes", 60, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := l.Consume("bytes", 40, nil); err != nil {
		t.Fatalf("consume to cap: %v", err)
	}
	if got := l.Used("bytes"); got != 100 {
		t.Fatalf("used = %d, want 100", got)
	}
	// 超限的那笔不计入消耗。
	if _, err := l.Consume("bytes", 1, nil); !errors.Is(err, ErrOverBudget) {
		t.Fatalf("err = %v, want ErrOverBudget", err)
	}
	if got := l.Used("bytes"); got != 100 {
		t.Fatalf("used = %d, want 100（超限不计入消耗）", got)
	}
}

// TestUsed_ReflectsConsumption 钉住观测口：没有 Used，计数器异常只能靠
// 「突然所有请求都不再被拦」反推（#104）。
func TestUsed_ReflectsConsumption(t *testing.T) {
	l, _ := newTestLedger()
	if got := l.Used("bytes"); got != 0 {
		t.Fatalf("used = %d, want 0", got)
	}
	if _, err := l.Consume("bytes", 60, nil); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got := l.Used("bytes"); got != 60 {
		t.Fatalf("used = %d, want 60", got)
	}
	if got := l.Used("unknown"); got != 0 {
		t.Fatalf("unknown dimension used = %d, want 0", got)
	}
}

// fakeClock 是可注入时钟的最小实现（与 batch/shedding 测试同形）。
type fakeClock struct{ t time.Time }

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{t: start} }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
