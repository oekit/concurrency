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

	// 自动完成之前的阶段（#247/#193 的「失败终态不可抹」语义由
	// completePhaseLocked 单点维持——#265 起与 Complete 共用同一回写实现）
	for i := range idx {
		t.completePhaseLocked(i, now)
	}
}

// completePhaseLocked 把 phases[i] 完结为 Completed/100 并补齐时间账
// （调用方已持锁；#265：UpdatePhase 自动完成循环与 Complete 的唯一状态回写
// 实现——「跳过失败/已终态 → 置 Completed/100 → 补 StartTime → EndTime →
// Duration」此前两处重复，终态保护判定随之单点）。now 为完结时刻。
func (t *Tracker) completePhaseLocked(i int, now time.Time) {
	p := &t.phases[i]
	// 失败终态不可抹（#247/#193）：「Fail 后继续跑收尾阶段」时，不得把失败
	// 抹成 Completed/100，否则进度条与 GetOverallProgress 系统性谎报 100% 成功；
	// Completed/Skipped 已是终态，不重复回写（避免覆盖真实时间账）。
	if p.Status == PhaseFailed || p.Status == PhaseCompleted || p.Status == PhaseSkipped {
		return
	}
	p.Status = PhaseCompleted
	p.Progress = 100
	if p.StartTime.IsZero() {
		p.StartTime = now
	}
	p.EndTime = now
	p.Duration = now.Sub(p.StartTime).Milliseconds()
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
//
// 不覆盖失败终态（#193）：Fail 之后 Complete 是空操作——「defer t.Complete() +
// 错误分支 t.Fail()」是消费方最自然的写法，若 Complete 无条件回写 TaskCompleted，
// 失败的任务会在报表里显示为 100% 成功。
func (t *Tracker) Complete() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status == TaskFailed {
		return // TaskFailed 是终态，不可回退到 TaskCompleted
	}
	now := time.Now()
	t.status = TaskCompleted
	for i := range t.phases {
		// 失败终态不可抹（#193/#247，防御：正常路径走不到这里）——判定在
		// completePhaseLocked 单点（#265）
		t.completePhaseLocked(i, now)
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
