package singleflight

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoChan_SharedResult(t *testing.T) {
	f := New[string]()
	started := make(chan struct{})
	fn := func(ctx context.Context) (string, error) {
		close(started)
		time.Sleep(30 * time.Millisecond)
		return "v", nil
	}
	ch1 := f.DoChan(context.Background(), "k", fn)
	<-started
	ch2 := f.DoChan(context.Background(), "k", fn)
	r1, r2 := <-ch1, <-ch2
	if r1.Val != "v" || r2.Val != "v" {
		t.Fatalf("r1=%+v r2=%+v", r1, r2)
	}
	if !r1.Shared && !r2.Shared {
		t.Fatalf("one should be shared: %v %v", r1.Shared, r2.Shared)
	}
}

func TestDoChan_WaiterCtxExit(t *testing.T) {
	f := New[int]()
	block := make(chan struct{})
	defer close(block)
	go func() {
		_, _ = f.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
			<-block
			return 1, nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	wctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r := <-f.DoChan(wctx, "k", func(ctx context.Context) (int, error) {
		t.Fatal("waiter must not run fn")
		return 0, nil
	})
	if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Fatalf("r=%+v", r)
	}
}

func TestDoChan_LeaderImmuneWithWithoutCancel(t *testing.T) {
	f := New[string]()
	callCtx, cancel := context.WithCancel(context.Background())
	res := f.DoChan(context.WithoutCancel(callCtx), "k", func(ctx context.Context) (string, error) {
		select {
		case <-time.After(80 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	cancel()
	r := <-res
	if r.Err != nil || r.Val != "done" {
		t.Fatalf("leader should be immune, got %+v", r)
	}
}
