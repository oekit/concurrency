package syncx

import (
	"errors"
	"fmt"
	"testing"
)

// TestFanInErr_NormalAndError 正常槽与 error 槽逐槽如实记录。
func TestFanInErr_NormalAndError(t *testing.T) {
	wantErr := errors.New("boom-1")
	slots := FanInErr(3, func(i int) (string, error) {
		switch i {
		case 0:
			return "ok-0", nil
		case 1:
			return "", wantErr
		default:
			return "ok-2", nil
		}
	})

	if len(slots) != 3 {
		t.Fatalf("应有 3 个槽，got %d", len(slots))
	}
	if slots[0].Val != "ok-0" || slots[0].Err != nil || slots[0].Panicked != nil {
		t.Errorf("槽 0 应为正常结果，got %+v", slots[0])
	}
	if !errors.Is(slots[1].Err, wantErr) || slots[1].Panicked != nil {
		t.Errorf("槽 1 应记录 error，got %+v", slots[1])
	}
	if slots[2].Val != "ok-2" || slots[2].Err != nil {
		t.Errorf("槽 2 应为正常结果，got %+v", slots[2])
	}
}

// TestFanInErr_PanicSlotIsMarked panic 槽显式标记（payload 直达调用侧）、
// Val 保持零值、其余槽位不受影响——零值歧义消失（concurrency#7）。
func TestFanInErr_PanicSlotIsMarked(t *testing.T) {
	type report struct{ Name string } // 合法零值 &ZeroValue{} 与 panic 空槽的场景
	slots := FanInErr(3, func(i int) (report, error) {
		if i == 1 {
			panic(fmt.Sprintf("branch-%d exploded", i))
		}
		return report{Name: fmt.Sprintf("branch-%d", i)}, nil
	})

	if slots[1].Panicked == nil {
		t.Fatal("panic 槽应标记 Panicked")
	}
	if fmt.Sprintf("%v", slots[1].Panicked) != "branch-1 exploded" {
		t.Errorf("Panicked 应携带 panic payload，got %v", slots[1].Panicked)
	}
	if slots[1].Val != (report{}) || slots[1].Err != nil {
		t.Errorf("panic 槽 Val 应为零值、Err 为 nil，got %+v", slots[1])
	}
	if slots[0].Val.Name != "branch-0" || slots[2].Val.Name != "branch-2" {
		t.Errorf("其余槽位不应受影响，got %+v", slots)
	}
}

// TestFanInErr_Empty n<=0 返回空切片（与 FanIn 契约一致）。
func TestFanInErr_Empty(t *testing.T) {
	if got := FanInErr(0, func(i int) (int, error) { return 0, nil }); got != nil {
		t.Errorf("n=0 应返回 nil，got %+v", got)
	}
	if got := FanInErr(-1, func(i int) (int, error) { return 0, nil }); got != nil {
		t.Errorf("n<0 应返回 nil，got %+v", got)
	}
}
