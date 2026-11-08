package async_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

func TestSpawnBasicResult(t *testing.T) {
	h := async.Spawn(context.Background(), "bypass-stats", func(ctx context.Context) (int, error) {
		return 42, nil
	})
	v, err := h.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait err = %v, want nil", err)
	}
	if v != 42 {
		t.Fatalf("Wait val = %d, want 42", v)
	}
	// 重复 Wait 拿同一份结果（句柄可多次读）；nil ctx 按 Background 处理
	// （经变量传参，SA1012 不让直接写 nil 字面量）
	var nilCtx context.Context = nil
	if v, _ = h.Wait(nilCtx); v != 42 {
		t.Fatalf("second Wait val = %d, want 42", v)
	}
	// Done 已关闭
	select {
	case <-h.Done():
	default:
		t.Fatal("Done must be closed after completion")
	}
}

func TestSpawnPropagatesError(t *testing.T) {
	want := errors.New("stats failed")
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (string, error) {
		return "", want
	})
	v, err := h.Wait(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if v != "" {
		t.Fatalf("val = %q, want zero", v)
	}
}

func TestSpawnPanicCollectedAsPanicError(t *testing.T) {
	var (
		mu       sync.Mutex
		reported any
	)
	async.SetPanicHandler(func(ctx context.Context, r any, stack []byte) {
		mu.Lock()
		reported = r
		mu.Unlock()
	})
	t.Cleanup(func() { async.SetPanicHandler(nil) })

	h := async.Spawn(context.Background(), "panicky-task", func(ctx context.Context) (int, error) {
		panic("bypass exploded")
	})
	v, err := h.Wait(context.Background())
	if v != 0 {
		t.Fatalf("val = %d, want zero on panic", v)
	}
	var pe *async.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want *PanicError", err)
	}
	if pe.Task != "panicky-task" {
		t.Fatalf("PanicError.Task = %q, want panicky-task", pe.Task)
	}
	// 汇报 ctx 携带 task 标签（#184 口径）：handler 侧经 TaskFromContext 可取
	// （TaskFromContext 对 nil ctx 容错返回空串，经变量传参避 SA1012）
	var nilCtx context.Context = nil
	if got := async.TaskFromContext(nilCtx); got != "" {
		t.Fatalf("TaskFromContext(nil) = %q, want empty", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if reported != "bypass exploded" {
		t.Fatalf("PanicHandler reported %v, want the panic value", reported)
	}
}

func TestSpawnDetachedFromParentCancel(t *testing.T) {
	// WithoutCancel 语义内置：parent 取消不波及旁路任务——fn 收到的 ctx
	// 与 parent 解耦（parent 取消后 fn 的 ctx 仍未 Done）
	ctx, cancel := context.WithCancel(context.Background())
	h := async.Spawn(ctx, "detached", func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond) // 留出 parent cancel 的窗口
		select {
		case <-ctx.Done():
			return "", ctx.Err() // 不应发生：ctx 已脱离 parent
		default:
		}
		return "finished", nil
	})
	cancel()
	v, err := h.Wait(context.Background())
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if v != "finished" {
		t.Fatalf("val = %q, want finished", v)
	}
}

func TestSpawnWaitTimesOutTaskKeepsRunning(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	h := async.Spawn(context.Background(), "slow", func(ctx context.Context) (int, error) {
		close(started)
		time.Sleep(50 * time.Millisecond) // 任务继续跑完
		close(finished)
		return 7, nil
	})
	<-started

	v, err, timedOut := h.WaitUpTo(context.Background(), time.Millisecond)
	if !timedOut {
		t.Fatalf("timedOut = false, want true")
	}
	if err != nil {
		t.Fatalf("timed-out err = %v, want nil", err)
	}
	if v != 0 {
		t.Fatalf("timed-out val = %d, want zero", v)
	}

	// 任务不受放弃影响，继续跑完；后续 Wait 拿到真实结果
	<-finished
	v, err = h.Wait(context.Background())
	if err != nil || v != 7 {
		t.Fatalf("late Wait = (%d, %v), want (7, nil)", v, err)
	}
}

func TestSpawnWaitUpToZeroIsProbe(t *testing.T) {
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (int, error) {
		time.Sleep(20 * time.Millisecond)
		return 1, nil
	})
	// d<=0：非阻塞探活——未完成立即 true
	if _, _, timedOut := h.WaitUpTo(context.Background(), 0); !timedOut {
		t.Fatal("probe before completion must report timedOut")
	}
	if _, _, timedOut := h.WaitUpTo(context.Background(), -time.Second); !timedOut {
		t.Fatal("negative d must behave as probe")
	}
	// 完成后探活拿到结果
	<-h.Done()
	v, err, timedOut := h.WaitUpTo(context.Background(), 0)
	if timedOut || err != nil || v != 1 {
		t.Fatalf("probe after completion = (%d, %v, %v), want (1, nil, false)", v, err, timedOut)
	}
}

func TestSpawnWaitUpToPrefersResultOnTie(t *testing.T) {
	// 结果与超时同刻就绪时偏向结果：任务已完成 ⇒ 任意 d 的 WaitUpTo 都返回结果
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (int, error) {
		return 3, nil
	})
	<-h.Done()
	for i := 0; i < 100; i++ {
		v, err, timedOut := h.WaitUpTo(context.Background(), 0)
		if timedOut || err != nil || v != 3 {
			t.Fatalf("completed handle probe = (%d, %v, %v), want (3, nil, false)", v, err, timedOut)
		}
	}
}

func TestSpawnWaitRespectsCtxCancel(t *testing.T) {
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	v, err := h.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if v != 0 {
		t.Fatalf("val = %d, want zero", v)
	}
	// 取消的只是等待：任务照常完成
	v, err = h.Wait(context.Background())
	if err != nil || v != 1 {
		t.Fatalf("task must finish despite abandoned wait, got (%d, %v)", v, err)
	}
}

func TestSpawnWaitUpToContextCancelVsTimeout(t *testing.T) {
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (int, error) {
		time.Sleep(200 * time.Millisecond)
		return 1, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	// ctx 先取消 ⇒ (零值, ctx.Err(), false)——取消不是超时
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	v, err, timedOut := h.WaitUpTo(ctx, time.Second)
	if timedOut {
		t.Fatal("context cancel must not be reported as timedOut")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want Canceled", err)
	}
	if v != 0 {
		t.Fatalf("val = %d, want zero", v)
	}
}

func TestSpawnConcurrentWaiters(t *testing.T) {
	// 多调用方同时等待同一份结果（fan-in join）
	h := async.Spawn(context.Background(), "t", func(ctx context.Context) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return 9, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := h.Wait(context.Background())
			if err != nil || v != 9 {
				t.Errorf("Wait = (%d, %v), want (9, nil)", v, err)
			}
			if _, _, timedOut := h.WaitUpTo(context.Background(), time.Second); timedOut {
				t.Errorf("WaitUpTo after completion must not report timedOut")
			}
		}()
	}
	wg.Wait()
}

func TestSpawnNilFnCollectedAsPanicError(t *testing.T) {
	h := async.Spawn[struct{}](context.Background(), "nil-fn", nil)
	_, err := h.Wait(context.Background())
	var pe *async.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("nil fn must surface as *PanicError, got %v", err)
	}
	select {
	case <-h.Done():
	default:
		t.Fatal("Done must be closed even on nil fn")
	}
}

func TestSpawnMainPathJoinPattern(t *testing.T) {
	// godoc 的 git-ferry operation_log 同款：主路径 + 旁路限时 join
	h := async.Spawn(context.Background(), "operation-stats", func(ctx context.Context) (int, error) {
		time.Sleep(30 * time.Millisecond) // 模拟旁路统计
		return 128, nil
	})
	mainTotal := 64 // 模拟主路径列表查询（先于旁路完成）

	v, err, timedOut := h.WaitUpTo(context.Background(), time.Second)
	if timedOut || err != nil {
		t.Fatalf("join failed (timedOut=%v err=%v)", timedOut, err)
	}
	if v+mainTotal != 192 {
		t.Fatalf("joined total = %d, want 192", v+mainTotal)
	}
}
