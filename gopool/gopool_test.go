package gopool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolOption_Defaults(t *testing.T) {
	def := DefaultPoolOption()
	if def.MaxIdleWorkers != 1000 || def.WorkerMaxAge != time.Minute || def.TaskChanBuffer != 1000 {
		t.Fatalf("默认值异常: %+v", def)
	}
	// 零值字段回填默认
	p := NewPool("t", PoolOption{})
	if p.opt.MaxIdleWorkers != 1000 || p.opt.WorkerMaxAge != time.Minute || p.opt.TaskChanBuffer != 1000 {
		t.Fatalf("零值回填异常: %+v", p.opt)
	}
	// 定制字段生效
	p2 := NewPool("t2", PoolOption{MaxIdleWorkers: 8, WorkerMaxAge: time.Second, TaskChanBuffer: 16})
	if p2.opt.MaxIdleWorkers != 8 || p2.opt.WorkerMaxAge != time.Second || p2.opt.TaskChanBuffer != 16 {
		t.Fatalf("定制异常: %+v", p2.opt)
	}
	// 部分定制 + 部分零值
	p3 := NewPool("t3", PoolOption{MaxIdleWorkers: 4})
	if p3.opt.MaxIdleWorkers != 4 || p3.opt.WorkerMaxAge != time.Minute || p3.opt.TaskChanBuffer != 1000 {
		t.Fatalf("混合定制异常: %+v", p3.opt)
	}
}

func TestPool_GoCtxGo(t *testing.T) {
	p := NewPool("basic", PoolOption{TaskChanBuffer: 64})
	var n atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		p.Go(func() {
			defer wg.Done()
			n.Add(1)
		})
	}
	wg.Wait()
	if n.Load() != 100 {
		t.Fatalf("执行数 = %d, want 100", n.Load())
	}
	// ctx 透传：CtxGo 的 ctx 到达 panic 处理器（见 Panic 测试）
	if p.CurrentWorkers() < 1 {
		t.Fatalf("worker 未拉起: %d", p.CurrentWorkers())
	}
}

func TestPool_PanicRecovered(t *testing.T) {
	p := NewPool("panic", PoolOption{TaskChanBuffer: 8})
	type captured struct {
		ctx context.Context
		r   any
	}
	ch := make(chan captured, 1)
	p.SetPanicHandler(func(ctx context.Context, r any, _ []byte) {
		ch <- captured{ctx: ctx, r: r}
	})
	defer p.SetPanicHandler(nil)

	done := make(chan struct{})
	ctxKey := context.WithValue(context.Background(), "k", "v")
	p.CtxGo(ctxKey, func() {
		defer close(done)
		panic("boom")
	})

	<-done
	select {
	case got := <-ch:
		if got.r != "boom" {
			t.Fatalf("recover 值 = %v", got.r)
		}
		if got.ctx != ctxKey {
			t.Fatal("ctx 未透传到 panic 处理器")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic 处理器未被调用")
	}
}

// TestPool_PanicHandlerPanics 处理器自身 panic 被收编，进程不死。
func TestPool_PanicHandlerPanics(t *testing.T) {
	p := NewPool("handler-panic", PoolOption{TaskChanBuffer: 4})
	p.SetPanicHandler(func(context.Context, any, []byte) {
		panic("handler boom")
	})
	defer p.SetPanicHandler(nil)

	done := make(chan struct{})
	p.Go(func() {
		defer close(done)
		panic("orig")
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("任务未完成（处理器 panic 未被收编会打死进程）")
	}
}

// TestPool_QueueFullFallback 队列满回退：任务不丢、全部执行完。
func TestPool_QueueFullFallback(t *testing.T) {
	p := NewPool("fallback", PoolOption{TaskChanBuffer: 1})
	var n atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		p.Go(func() {
			defer wg.Done()
			<-release // 阻塞 worker 与队列，逼出回退路径
			n.Add(1)
		})
	}
	close(release)
	wg.Wait()
	if n.Load() != 200 {
		t.Fatalf("执行数 = %d, want 200", n.Load())
	}
}

// TestPool_TempWorkerBypass 超出 MaxIdleWorkers 的临时 worker 取完存量即退。
func TestPool_TempWorkerBypass(t *testing.T) {
	p := NewPool("temp", PoolOption{MaxIdleWorkers: 1, TaskChanBuffer: 256})
	var n atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		p.Go(func() {
			defer wg.Done()
			<-release
			n.Add(1)
		})
	}
	close(release)
	wg.Wait()
	if n.Load() != 100 {
		t.Fatalf("执行数 = %d, want 100", n.Load())
	}
}

// TestPool_WorkerMaxAge 老化退出：短龄 worker 处理完任务后应自行退出。
func TestPool_WorkerMaxAge(t *testing.T) {
	p := NewPool("age", PoolOption{WorkerMaxAge: 40 * time.Millisecond, TaskChanBuffer: 32})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		p.Go(func() { defer wg.Done() })
	}
	wg.Wait()
	// 老化检查间隔 max(maxage/100, 1ms)=1ms，放宽到 3s 等待全部退出
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.CurrentWorkers() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker 未老化退出: %d", p.CurrentWorkers())
}

func TestPool_CloseDrains(t *testing.T) {
	p := NewPool("close", PoolOption{TaskChanBuffer: 64})
	var n atomic.Int32
	for i := 0; i < 50; i++ {
		p.Go(func() {
			time.Sleep(2 * time.Millisecond)
			n.Add(1)
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n.Load() != 50 {
		t.Fatalf("Close 未排空: %d/50", n.Load())
	}
	if !p.Closed() {
		t.Fatal("Closed() = false")
	}
	// Close 幂等
	if err := p.Close(ctx); err != nil {
		t.Fatalf("二次 Close: %v", err)
	}
	// Close 后提交走旁路，仍然执行
	done := make(chan struct{})
	p.Go(func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close 后旁路任务未执行")
	}
}

func TestPool_CloseTimeout(t *testing.T) {
	p := NewPool("close-timeout", PoolOption{TaskChanBuffer: 8})
	release := make(chan struct{})
	p.Go(func() { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Close 超时 err = %v, want DeadlineExceeded", err)
	}
	close(release)
}

// TestDefaultPool 包级默认池基本功能与 panic 处理器注入。
func TestDefaultPool(t *testing.T) {
	var called atomic.Bool
	old := *defaultPool.panicHandler.Load()
	t.Cleanup(func() { defaultPool.SetPanicHandler(old) })
	SetPanicHandler(func(context.Context, any, []byte) { called.Store(true) })

	done := make(chan struct{})
	Go(func() { close(done) })
	<-done

	CtxGo(context.Background(), func() { panic("default-pool") })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !called.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if !called.Load() {
		t.Fatal("默认池 panic 处理器未被调用")
	}
	if CurrentWorkers() < 0 {
		t.Fatal("CurrentWorkers 异常")
	}
}

// TestPool_ConcurrentSubmitAndClose 并发提交下不 panic（提交与 Close 的
// 竞态窗口以 closed 标记兜底：旁路执行，不丢不炸）。
func TestPool_ConcurrentSubmitAndClose(t *testing.T) {
	p := NewPool("concurrent", PoolOption{TaskChanBuffer: 16})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 50; j++ {
				p.Go(func() { runtime.Gosched() })
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.Close(ctx) // 允许超时（并发提交方可能仍在灌任务），只验证不 panic
}

func BenchmarkGopool_Go(b *testing.B) {
	p := NewPool("bench", PoolOption{TaskChanBuffer: 1024})
	var n atomic.Int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Go(func() { n.Add(1) })
	}
}
