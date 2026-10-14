package group

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
