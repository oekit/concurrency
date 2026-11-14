package progress

import (
	"testing"
)

func TestNewTracker(t *testing.T) {
	phases := []PhaseConfig{
		{Name: "init", Title: "Initialization"},
		{Name: "process", Title: "Processing"},
	}
	tr := NewTracker(phases)

	if got := tr.GetStatus(); got != TaskRunning {
		t.Errorf("GetStatus() = %q, want %q", got, TaskRunning)
	}
	if tr.IsCompleted() {
		t.Error("new tracker should not be completed")
	}
	if got := tr.GetOverallProgress(); got != 0 {
		t.Errorf("GetOverallProgress() = %v, want 0", got)
	}

	all := tr.GetPhases()
	if len(all) != 2 {
		t.Fatalf("GetPhases() len = %d, want 2", len(all))
	}
	if all[0].Status != PhasePending {
		t.Errorf("phase[0].Status = %q, want %q", all[0].Status, PhasePending)
	}
}

func TestNewTracker_Empty(t *testing.T) {
	tr := NewTracker(nil)
	if got := tr.GetOverallProgress(); got != 0 {
		t.Errorf("empty tracker GetOverallProgress() = %v, want 0", got)
	}
	if got := tr.GetPhases(); len(got) != 0 {
		t.Errorf("empty tracker GetPhases() len = %d, want 0", len(got))
	}
}

func TestUpdatePhase_AutoCompletePredecessors(t *testing.T) {
	tr := NewTracker([]PhaseConfig{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	})

	// 直接激活第三个阶段，前两个应被自动标记完成
	tr.UpdatePhase("c", PhaseActive, 0)

	a, ok := tr.GetPhase("a")
	if !ok {
		t.Fatal("phase a not found")
	}
	if a.Status != PhaseCompleted {
		t.Errorf("predecessor a Status = %q, want %q", a.Status, PhaseCompleted)
	}
	if a.Progress != 100 {
		t.Errorf("predecessor a Progress = %v, want 100", a.Progress)
	}

	b, _ := tr.GetPhase("b")
	if b.Status != PhaseCompleted {
		t.Errorf("predecessor b Status = %q, want %q", b.Status, PhaseCompleted)
	}

	c, _ := tr.GetPhase("c")
	if c.Status != PhaseActive {
		t.Errorf("phase c Status = %q, want %q", c.Status, PhaseActive)
	}
	if c.StartTime.IsZero() {
		t.Error("active phase should have StartTime set")
	}
}

func TestUpdatePhase_UnknownPhase(t *testing.T) {
	tr := NewTracker([]PhaseConfig{{Name: "a"}})
	tr.UpdatePhase("nonexistent", PhaseActive, 50)
	// 应静默忽略，不 panic
	if got := tr.GetOverallProgress(); got != 0 {
		t.Errorf("GetOverallProgress() = %v, want 0", got)
	}
}

func TestUpdatePhase_CompletedRespectsProgress(t *testing.T) {
	// #332：PhaseCompleted 尊重调用方入参——godoc 契约是「更新状态**和进度**」，
	// 旧实现无条件覆写 100 把入参吞掉。调用方传 100 自然得到 100。
	tr := NewTracker([]PhaseConfig{{Name: "a"}})

	tr.UpdatePhase("a", PhaseActive, 50)
	tr.UpdatePhase("a", PhaseCompleted, 80)

	a, _ := tr.GetPhase("a")
	if a.Status != PhaseCompleted {
		t.Errorf("Status = %q, want %q", a.Status, PhaseCompleted)
	}
	if a.Progress != 80 {
		t.Errorf("Progress = %v, want 80(入参被尊重)", a.Progress)
	}
	if a.EndTime.IsZero() {
		t.Error("completed phase should have EndTime set")
	}
	// 注意：Duration 以毫秒计，快速完成的阶段可能为 0，此处只校验非负
	if a.Duration < 0 {
		t.Errorf("Duration = %d, want >= 0", a.Duration)
	}
}

func TestGetPhase_NotFound(t *testing.T) {
	tr := NewTracker([]PhaseConfig{{Name: "a"}})
	if _, ok := tr.GetPhase("missing"); ok {
		t.Error("GetPhase should return false for missing phase")
	}
}

func TestGetOverallProgress(t *testing.T) {
	tr := NewTracker([]PhaseConfig{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	})
	// 三个阶段各 0%，平均 0
	tr.UpdatePhase("a", PhaseActive, 0)
	// a 被设为 active, progress 0
	if got := tr.GetOverallProgress(); got != 0 {
		t.Errorf("GetOverallProgress() = %v, want 0", got)
	}

	// 把 a 完成（100），其余 0
	tr.UpdatePhase("a", PhaseCompleted, 100)
	got := tr.GetOverallProgress()
	// a=100, b=0, c=0 => 33.33...
	if got < 33 || got > 34 {
		t.Errorf("GetOverallProgress() = %v, want ~33.33", got)
	}
}

func TestComplete(t *testing.T) {
	tr := NewTracker([]PhaseConfig{
		{Name: "a"},
		{Name: "b"},
	})
	tr.Complete()

	if !tr.IsCompleted() {
		t.Error("Complete() should mark tracker as completed")
	}
	if got := tr.GetStatus(); got != TaskCompleted {
		t.Errorf("GetStatus() = %q, want %q", got, TaskCompleted)
	}
	for _, p := range tr.GetPhases() {
		if p.Status != PhaseCompleted {
			t.Errorf("phase %s Status = %q, want %q", p.Name, p.Status, PhaseCompleted)
		}
		if p.Progress != 100 {
			t.Errorf("phase %s Progress = %v, want 100", p.Name, p.Progress)
		}
	}
}

func TestFail(t *testing.T) {
	tr := NewTracker([]PhaseConfig{{Name: "a"}})
	tr.Fail()

	if !tr.IsCompleted() {
		t.Error("Fail() should mark tracker as completed (terminal state)")
	}
	if got := tr.GetStatus(); got != TaskFailed {
		t.Errorf("GetStatus() = %q, want %q", got, TaskFailed)
	}
}

// TestComplete_DoesNotOverrideFail 锁定 #193：Fail 是终态，之后的 Complete
// 是空操作——「defer t.Complete() + 错误分支 t.Fail()」这条最自然的消费方
// 写法不得把失败抹成 100% 成功。
func TestComplete_DoesNotOverrideFail(t *testing.T) {
	tr := NewTracker([]PhaseConfig{
		{Name: "init"},
		{Name: "work"},
	})
	tr.UpdatePhase("init", PhaseActive, 0)
	tr.UpdatePhase("init", PhaseCompleted, 100)
	tr.UpdatePhase("work", PhaseActive, 0)
	tr.UpdatePhase("work", PhaseActive, 50)

	tr.Fail()
	tr.Complete() // 修复前：无条件把 failed 阶段改写成 completed+100

	if got := tr.GetStatus(); got != TaskFailed {
		t.Fatalf("GetStatus() = %q, want %q（Fail 不可被 Complete 回退）", got, TaskFailed)
	}
	if !tr.IsCompleted() {
		t.Error("failed 任务也是已完成（成功或失败）")
	}

	work, ok := tr.GetPhase("work")
	if !ok {
		t.Fatal("phase work not found")
	}
	if work.Status != PhaseFailed {
		t.Errorf("失败阶段被改写：work.Status = %q, want %q", work.Status, PhaseFailed)
	}
	if work.Progress != 50 {
		t.Errorf("失败阶段进度被改写：work.Progress = %v, want 50", work.Progress)
	}

	if got := tr.GetOverallProgress(); got >= 100 {
		t.Errorf("GetOverallProgress() = %v, 失败任务不应被抹成 100%%", got)
	}
}

func TestGetPhases_ReturnsCopy(t *testing.T) {
	tr := NewTracker([]PhaseConfig{{Name: "a"}, {Name: "b"}})
	phases := tr.GetPhases()
	phases[0].Status = PhaseFailed

	// 修改副本不应影响内部状态
	got, _ := tr.GetPhase("a")
	if got.Status == PhaseFailed {
		t.Error("GetPhases() should return a copy; internal state was mutated")
	}
}

// TestUpdatePhase_DoesNotOverrideFail 锁定 #247（#193 残留）：UpdatePhase 的
// 「自动完成前置阶段」与 Complete() 同款不变量——失败阶段保留失败痕迹，
// 自动完成循环不得把 PhaseFailed 改写成 Completed/100（否则流程继续跑收尾
// 阶段时，失败痕迹被抹成 100% 成功，报表与 GetOverallProgress 系统性谎报）。
func TestUpdatePhase_DoesNotOverrideFail(t *testing.T) {
	tr := NewTracker([]PhaseConfig{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	})
	tr.UpdatePhase("a", PhaseActive, 0)
	tr.UpdatePhase("b", PhaseActive, 0)
	tr.UpdatePhase("b", PhaseFailed, 50)

	// 流程继续跑收尾阶段：自动完成循环触发（修复前把 b 改写成 completed+100）
	tr.UpdatePhase("c", PhaseActive, 0)

	b, ok := tr.GetPhase("b")
	if !ok {
		t.Fatal("phase b not found")
	}
	if b.Status != PhaseFailed {
		t.Errorf("失败阶段被自动完成改写：b.Status = %q, want %q", b.Status, PhaseFailed)
	}
	if b.Progress != 50 {
		t.Errorf("失败阶段进度被改写：b.Progress = %v, want 50", b.Progress)
	}
	if b.EndTime.IsZero() {
		t.Error("失败阶段的 EndTime 不应被清掉（报表要耗时数据）")
	}

	// 不变量不被破坏：失败痕迹之外，pending 前置仍按原语义自动完成
	a, _ := tr.GetPhase("a")
	if a.Status != PhaseCompleted || a.Progress != 100 {
		t.Errorf("前置 pending 阶段应照旧自动完成：a = (%q, %v), want (completed, 100)", a.Status, a.Progress)
	}
	c, _ := tr.GetPhase("c")
	if c.Status != PhaseActive {
		t.Errorf("目标阶段状态不应受影响：c.Status = %q, want %q", c.Status, PhaseActive)
	}

	if got := tr.GetOverallProgress(); got >= 100 {
		t.Errorf("GetOverallProgress() = %v, 含失败阶段的任务不应被抹成 100%%", got)
	}
}

func TestFailMarksAllActivePhases(t *testing.T) {
	// #331：并发/乱序下可能同时有多个 PhaseActive——Fail 必须全标，
	// 只标第一个会让其余阶段永卡 active。
	tr := NewTracker([]PhaseConfig{{Name: "a"}, {Name: "b"}, {Name: "c"}})
	tr.UpdatePhase("a", PhaseActive, 10)
	tr.UpdatePhase("b", PhaseActive, 20) // 自动完成 a
	tr.UpdatePhase("a", PhaseActive, 15) // 重标 a 活跃(并发/乱序复活的实际形态)
	tr.Fail()
	for _, name := range []string{"a", "b"} {
		p, _ := tr.GetPhase(name)
		if p.Status != PhaseFailed {
			t.Errorf("phase %q Status = %q, want %q", name, p.Status, PhaseFailed)
		}
	}
	if c, _ := tr.GetPhase("c"); c.Status != PhasePending {
		t.Errorf("pending 阶段不应被波及: %q", c.Status)
	}
}

func TestNewTrackerDuplicatePhaseNamePanics(t *testing.T) {
	// #333：重名 fail-fast（此前静默覆盖，前一个同名阶段成孤儿）
	defer func() {
		if recover() == nil {
			t.Fatal("重名 PhaseConfig 应 panic")
		}
	}()
	NewTracker([]PhaseConfig{{Name: "dup"}, {Name: "dup"}})
}
