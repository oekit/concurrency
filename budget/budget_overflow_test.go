package budget

import (
	"math"
	"testing"
)

// TestConsume_OverflowDoesNotBypassBudget 回归测试：消耗量接近 int64 上限时，
// **不得**因为加法回绕而绕过预算判据。
//
// 缺陷成因（旧实现）：判据写成 `used+n > cap`——int64 上先加后比，
// n 接近 MaxInt64 时 `used+n` 回绕成负数 ⇒ 判据恒为「未超限」⇒
// 该笔消耗被直接放行（内容既没被拦截也没存档，调用方却拿到 err==nil），
// 且计数器被写成负数 ⇒ 此后**所有**后续消耗都无条件放行。
func TestConsume_OverflowDoesNotBypassBudget(t *testing.T) {
	l := NewLedger(map[Dimension]int64{"bytes": 100})

	// 先记一笔正常消耗，让 used 非零（回绕需要 used >= 1）。
	if _, err := l.Consume("bytes", 1, nil); err != nil {
		t.Fatalf("第一笔应通过: %v", err)
	}

	// 第二笔：n 接近 MaxInt64 ⇒ 旧实现会回绕成负数并被放行。
	ref, err := l.Consume("bytes", math.MaxInt64, nil)
	if err == nil {
		t.Fatalf("超限消耗未被拦截：ref=%d err=nil（内容既没被拦截也没存档，调用方却拿到 nil）", ref)
	}
	if err != ErrOverBudget {
		t.Fatalf("err = %v，期望 ErrOverBudget", err)
	}
	if ref == 0 {
		t.Fatal("超限内容应进存档并返回可取回的引用，ref=0 表示内容丢了")
	}
	if got, ok := l.Get(ref); !ok || len(got) != 0 {
		t.Fatalf("存档内容取不回：ok=%v len=%d", ok, len(got))
	}

	// 第三笔：正常量也必须照常被拦——计数器没被写坏。
	if _, err := l.Consume("bytes", 1000, nil); err != ErrOverBudget {
		t.Fatalf("计数器已损坏：Consume(1000) err=%v，期望 ErrOverBudget", err)
	}
}

// TestConsume_UsedNeverNegative 回归护栏：计数器不会被写成负数。
func TestConsume_UsedNeverNegative(t *testing.T) {
	l := NewLedger(map[Dimension]int64{"bytes": 100})
	_, _ = l.Consume("bytes", 1, nil)
	_, _ = l.Consume("bytes", math.MaxInt64, nil)
	if got := l.Used("bytes"); got < 0 {
		t.Fatalf("Used = %d，期望 >= 0（计数器被写坏了）", got)
	}
}

// TestConsume_ExactBoundary 回归护栏：判据改写后边界语义不变。
func TestConsume_ExactBoundary(t *testing.T) {
	l := NewLedger(map[Dimension]int64{"bytes": 100})
	if _, err := l.Consume("bytes", 100, nil); err != nil {
		t.Fatalf("恰好用满额度应通过: %v", err)
	}
	if _, err := l.Consume("bytes", 1, nil); err != ErrOverBudget {
		t.Fatalf("超出 1 应被拦：err=%v", err)
	}
	if got := l.Used("bytes"); got != 100 {
		t.Fatalf("Used = %d，期望 100", got)
	}
}

// TestUsed_UndeclaredDimension 回归护栏：未声明维度恒为 0。
func TestUsed_UndeclaredDimension(t *testing.T) {
	l := NewLedger(map[Dimension]int64{"bytes": 100})
	if got := l.Used("nope"); got != 0 {
		t.Fatalf("Used(未声明维度) = %d，期望 0", got)
	}
}
