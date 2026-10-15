// Package progress 提供多阶段工作流的进度追踪模型。
// 适用于代码审查、构建流水线、数据处理等多步骤任务的进度管理。
package progress

import (
	"sync"
	"time"
)

// PhaseStatus 表示阶段的状态。
type PhaseStatus string

const (
	PhasePending   PhaseStatus = "pending"
	PhaseActive    PhaseStatus = "active"
	PhaseCompleted PhaseStatus = "completed"
	PhaseFailed    PhaseStatus = "failed"
	PhaseSkipped   PhaseStatus = "skipped"
)

// TaskStatus 表示整个追踪任务的状态。
type TaskStatus string

const (
	// TaskRunning 任务进行中。
	TaskRunning TaskStatus = "running"
	// TaskCompleted 任务已成功完成。
	TaskCompleted TaskStatus = "completed"
	// TaskFailed 任务失败。
	TaskFailed TaskStatus = "failed"
)

// PhaseConfig 定义一个阶段的配置。
type PhaseConfig struct {
	Name        string
	Title       string
	Description string
}

// PhaseInfo 表示单个阶段的运行时信息。
type PhaseInfo struct {
	Name        string      `json:"name"`
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Status      PhaseStatus `json:"status"`
	Progress    float64     `json:"progress"`
	StartTime   time.Time   `json:"start_time,omitempty"`
	EndTime     time.Time   `json:"end_time,omitempty"`
	Duration    int64       `json:"duration_ms,omitempty"`
}

// Tracker 是多阶段进度追踪器。
// 线程安全，支持并发更新。
//
// 用法:
//
//	t := progress.NewTracker([]progress.PhaseConfig{
//	    {Name: "init", Title: "Initialization"},
//	    {Name: "process", Title: "Processing"},
//	    {Name: "finish", Title: "Finalization"},
//	})
//	t.UpdatePhase("init", progress.PhaseActive, 0)
//	t.UpdatePhase("init", progress.PhaseCompleted, 100)
type Tracker struct {
	phases    []PhaseInfo
	phaseMap  map[string]int
	startTime time.Time
	status    TaskStatus
	mu        sync.RWMutex
}

// NewTracker 创建进度追踪器。phases 按顺序定义所有阶段。
func NewTracker(phases []PhaseConfig) *Tracker {
	infos := make([]PhaseInfo, len(phases))
	phaseMap := make(map[string]int, len(phases))
	for i, p := range phases {
		infos[i] = PhaseInfo{
			Name:        p.Name,
			Title:       p.Title,
			Description: p.Description,
			Status:      PhasePending,
		}
		phaseMap[p.Name] = i
	}
	return &Tracker{
		phases:    infos,
		phaseMap:  phaseMap,
		startTime: time.Now(),
		status:    TaskRunning,
	}
}

// UpdatePhase 更新指定阶段的状态和进度。
// 自动完成该阶段之前的所有阶段。
func (t *Tracker) UpdatePhase(name string, status PhaseStatus, progress float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	idx, ok := t.phaseMap[name]
	if !ok {
		return
	}

	now := time.Now()
	t.phases[idx].Status = status
	t.phases[idx].Progress = progress

	switch status {
	case PhaseActive:
		if t.phases[idx].StartTime.IsZero() {
			t.phases[idx].StartTime = now
		}
	case PhaseCompleted:
		t.phases[idx].EndTime = now
		t.phases[idx].Progress = 100
		if !t.phases[idx].StartTime.IsZero() {
			t.phases[idx].Duration = now.Sub(t.phases[idx].StartTime).Milliseconds()
		}
	case PhaseFailed:
		// 失败与完成一样要有终止时间与耗时，否则报表缺数据
		t.phases[idx].EndTime = now
		if !t.phases[idx].StartTime.IsZero() {
			t.phases[idx].Duration = now.Sub(t.phases[idx].StartTime).Milliseconds()
		}
	}

	// 自动完成之前的阶段
	for i := 0; i < idx; i++ {
		if t.phases[i].Status != PhaseCompleted && t.phases[i].Status != PhaseSkipped {
			t.phases[i].Status = PhaseCompleted
			t.phases[i].Progress = 100
			if t.phases[i].StartTime.IsZero() {
				t.phases[i].StartTime = now
			}
			t.phases[i].EndTime = now
			if !t.phases[i].StartTime.IsZero() {
				t.phases[i].Duration = now.Sub(t.phases[i].StartTime).Milliseconds()
			}
		}
	}
}

// GetPhase 返回指定阶段的信息（副本）。
func (t *Tracker) GetPhase(name string) (PhaseInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx, ok := t.phaseMap[name]
	if !ok {
		return PhaseInfo{}, false
	}
	return t.phases[idx], true
}

// GetPhases 返回所有阶段的信息（副本）。
func (t *Tracker) GetPhases() []PhaseInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]PhaseInfo, len(t.phases))
	copy(result, t.phases)
	return result
}

// GetOverallProgress 计算所有阶段的平均进度。
func (t *Tracker) GetOverallProgress() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.phases) == 0 {
		return 0
	}
	total := 0.0
	for _, p := range t.phases {
		total += p.Progress
	}
	return total / float64(len(t.phases))
}

// GetDuration 返回追踪器运行的总时长。
func (t *Tracker) GetDuration() time.Duration {
	return time.Since(t.startTime)
}

// Complete 标记所有阶段完成。
func (t *Tracker) Complete() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.status = TaskCompleted
	for i := range t.phases {
		if t.phases[i].Status != PhaseCompleted && t.phases[i].Status != PhaseSkipped {
			t.phases[i].Status = PhaseCompleted
			t.phases[i].Progress = 100
			if t.phases[i].StartTime.IsZero() {
				t.phases[i].StartTime = now
			}
			t.phases[i].EndTime = now
			t.phases[i].Duration = t.phases[i].EndTime.Sub(t.phases[i].StartTime).Milliseconds()
		}
	}
}

// Fail 标记任务失败。同时将当前活跃阶段标记为失败。
func (t *Tracker) Fail() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.status = TaskFailed
	for i := range t.phases {
		if t.phases[i].Status == PhaseActive {
			t.phases[i].Status = PhaseFailed
			t.phases[i].EndTime = now
			if !t.phases[i].StartTime.IsZero() {
				t.phases[i].Duration = t.phases[i].EndTime.Sub(t.phases[i].StartTime).Milliseconds()
			}
			break
		}
	}
}

// IsCompleted 返回任务是否已完成（成功或失败）。
func (t *Tracker) IsCompleted() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status == TaskCompleted || t.status == TaskFailed
}

// GetStatus 返回任务当前状态。
func (t *Tracker) GetStatus() TaskStatus {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}
