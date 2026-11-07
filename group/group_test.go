package group

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

func TestGroup_Run(t *testing.T) {
	g := &Group{}
	var called int32

	g.Add(func() error {
		atomic.AddInt32(&called, 1)
		return nil
	}, func(error) {})

	err := g.Run()
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestGroup_FirstErrorReturned(t *testing.T) {
	g := &Group{}
	expectedErr := errors.New("first error")

	g.Add(func() error {
		return expectedErr
	}, func(error) {})

	g.Add(func() error {
		time.Sleep(10 * time.Second)
		return nil
	}, func(error) {})

	err := g.Run()
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestGroup_InterruptCalled(t *testing.T) {
	g := &Group{}
	var interrupted int32

	g.Add(func() error {
		return errors.New("done")
	}, func(error) {})

	g.Add(func() error {
		time.Sleep(10 * time.Second)
		return nil
	}, func(err error) {
		atomic.AddInt32(&interrupted, 1)
	})

	_ = g.Run()
	if atomic.LoadInt32(&interrupted) != 1 {
		t.Errorf("expected interrupt called once, got %d", interrupted)
	}
}

// TestGroup_InterruptPanicDoesNotAbandonOthers 锁定 #194：interrupt 是消费方
// 回调，一个 interrupt 自身 panic 不得弃掉其余 Actor 的停机通知与排空等待——
// 修复前 panic 逃逸出 Run，后面的 Actor 既不被 interrupt 也不被等待（泄漏）。
func TestGroup_InterruptPanicDoesNotAbandonOthers(t *testing.T) {
	async.SetPanicHandler(func(context.Context, any, []byte) {}) // 收编上报走全局口子，这里静音
	t.Cleanup(func() { async.SetPanicHandler(nil) })             // nil = 恢复默认（#165）

	g := &Group{}
	wantErr := errors.New("actor-1 down")

	// actor-1：立即返回错误，其 interrupt 会 panic（坏回调）
	g.Add(func() error {
		return wantErr
	}, func(error) {
		panic("interrupt 自身坏了")
	})
	// actor-2：阻塞等 interrupt 通知，收到后退出
	stop2 := make(chan struct{})
	interrupted2 := make(chan struct{}, 1)
	g.Add(func() error {
		<-stop2
		return nil
	}, func(error) {
		select {
		case interrupted2 <- struct{}{}:
		default:
		}
		close(stop2)
	})

	runDone := make(chan error, 1)
	go func() { runDone <- g.Run() }()

	select {
	case err := <-runDone:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Run 返回 %v，期望首个退出的 actor 错误 %v", err, wantErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run 3s 未返回——interrupt panic 逃逸导致排空等待被弃")
	}

	// actor-2 确实收到了 interrupt（Run 已返回 ⇒ 排空完成 ⇒ actor-2 已退出，
	// 不是泄漏在后台继续跑）
	select {
	case <-interrupted2:
	case <-time.After(3 * time.Second):
		t.Fatal("actor-2 没有收到 interrupt——坏 interrupt 弃掉了其余 actor 的停机")
	}
}

func TestGroup_EmptyRun(t *testing.T) {
	g := &Group{}
	err := g.Run()
	if err != nil {
		t.Errorf("expected nil for empty group, got %v", err)
	}
}

func TestWaitGroupWrapper_Success(t *testing.T) {
	wg := NewWaitGroup(context.Background())

	wg.Wrap(func(ctx context.Context) error {
		return nil
	})
	wg.Wrap(func(ctx context.Context) error {
		return nil
	})

	err := wg.Wait()
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestWaitGroupWrapper_FirstError(t *testing.T) {
	wg := NewWaitGroup(context.Background())
	expectedErr := errors.New("test error")

	wg.Wrap(func(ctx context.Context) error {
		return expectedErr
	})
	wg.Wrap(func(ctx context.Context) error {
		time.Sleep(10 * time.Second)
		return nil
	})

	err := wg.Wait()
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

func TestWaitGroupWrapper_ContextCancel(t *testing.T) {
	wg := NewWaitGroup(context.Background())

	wg.Wrap(func(ctx context.Context) error {
		return errors.New("trigger cancel")
	})

	wg.Wrap(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})

	err := wg.Wait()
	if err == nil {
		t.Error("expected error")
	}
}

func TestWaitGroupWrapper_Timeout(t *testing.T) {
	wg := NewWaitGroup(context.Background(), WithTimeout(50*time.Millisecond))

	wg.Wrap(func(ctx context.Context) error {
		time.Sleep(1 * time.Second)
		return nil
	})

	err := wg.Wait()
	if err != nil {
		t.Errorf("expected nil (timeout cancels context), got %v", err)
	}
}

func TestWaitGroupWrapper_PanicPropagatedAsError(t *testing.T) {
	wg := NewWaitGroup(context.Background())

	wg.Wrap(func(ctx context.Context) error {
		panic("test panic")
	})

	// panic 应被 recover 并转换为 error，不应被静默当成成功
	err := wg.Wait()
	if err == nil {
		t.Fatal("expected non-nil error from panicked goroutine, got nil")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected error to mention panic, got %q", err.Error())
	}
}
