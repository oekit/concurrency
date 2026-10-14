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

func TestUpdatePhase_CompletedSetsProgress100(t *testing.T) {
	tr := NewTracker([]PhaseConfig{{Name: "a"}})

	tr.UpdatePhase("a", PhaseActive, 50)
	tr.UpdatePhase("a", PhaseCompleted, 50)

	a, _ := tr.GetPhase("a")
	if a.Status != PhaseCompleted {
		t.Errorf("Status = %q, want %q", a.Status, PhaseCompleted)
	}
	if a.Progress != 100 {
		t.Errorf("Progress = %v, want 100", a.Progress)
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
