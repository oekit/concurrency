// Package progress 提供多阶段工作流的进度追踪模型。
//
// Tracker 支持有序阶段的生命周期管理（pending → active → completed），
// 自动完成前置阶段，线程安全。适用于构建流水线、数据处理、代码审查等多步骤任务。
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
//	fmt.Println(t.GetOverallProgress()) // 33.33
package progress
