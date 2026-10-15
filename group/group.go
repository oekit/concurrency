package group

import (
	"github.com/oekit/concurrency/async"
)

// Group 收集多个 Actor（函数对）并并发执行。
// 当任意一个 Actor 返回时，所有其他 Actor 通过 interrupt 函数被中断。
// 零值可用。
//
// 典型用法：用于管理多个长期运行的 goroutine（HTTP server、gRPC server、信号监听等），
// 确保其中一个退出时，其他全部被通知停止。
//
//	g := &group.Group{}
//	g.Add(func() error { return httpServer.ListenAndServe() }, func(error) { httpServer.Close() })
//	g.Add(func() error { return grpcServer.Serve() }, func(error) { grpcServer.GracefulStop() })
//	g.Add(func() error { <-ctx.Done(); return nil }, func(error) {})
//	err := g.Run()
type Group struct {
	actors []actor
}

// Add 注册一个 Actor（execute + interrupt 函数对）。
//
// execute: 实际执行的函数，应在 interrupt 被调用时返回。
// interrupt: 通知 execute 停止的函数，即使 execute 已返回也必须安全可调用。
func (g *Group) Add(execute func() error, interrupt func(error)) {
	g.actors = append(g.actors, actor{execute, interrupt})
}

// Run 并发执行所有 Actor。
// 当第一个 Actor 返回时，其 error 传递给所有其他 Actor 的 interrupt 函数。
// Run 阻塞直到所有 Actor 退出，返回第一个退出的 Actor 的 error。
func (g *Group) Run() error {
	if len(g.actors) == 0 {
		return nil
	}

	errors := make(chan error, len(g.actors))
	for _, a := range g.actors {
		async.GoSafeWithErrChannel(errors, a.execute)
	}

	err := <-errors

	for _, a := range g.actors {
		a.interrupt(err)
	}

	for i := 1; i < cap(errors); i++ {
		<-errors
	}

	return err
}

type actor struct {
	execute   func() error
	interrupt func(error)
}
