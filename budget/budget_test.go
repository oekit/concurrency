package budget

import (
	"errors"
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

// fakeClock 是可注入时钟的最小实现（与 batch/shedding 测试同形）。
type fakeClock struct{ t time.Time }

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{t: start} }

func (c *fakeClock) Now() time.Time { return c.t }

func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
