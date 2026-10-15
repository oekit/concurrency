package singleflight

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlight_Dedup(t *testing.T) {
	sf := New[string]()

	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "result", nil
	}

	var wg sync.WaitGroup
	results := make(chan string, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := sf.Do(context.Background(), "key1", fn)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			results <- val
		}()
	}
	wg.Wait()
	close(results)

	for r := range results {
		if r != "result" {
			t.Errorf("expected result, got %q", r)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 call, got %d", got)
	}
}

func TestFlight_DifferentKeys(t *testing.T) {
	sf := New[int]()

	var calls atomic.Int32
	fn := func(_ context.Context) (int, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return int(calls.Load()), nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, _ = sf.Do(context.Background(), key, fn)
		}(string(rune('a' + i)))
	}
	wg.Wait()

	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 calls for 3 keys, got %d", got)
	}
}

func TestFlight_Error(t *testing.T) {
	sf := New[string]()

	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "", context.DeadlineExceeded
	}

	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sf.Do(context.Background(), "err_key", fn)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		if e != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", e)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 call, got %d", got)
	}
}

func TestFlight_ContextCancel(t *testing.T) {
	sf := New[string]()

	started := make(chan struct{})
	fn := func(_ context.Context) (string, error) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		return "late", nil
	}

	go func() {
		_, _ = sf.Do(context.Background(), "slow_key", fn)
	}()

	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := sf.Do(ctx, "slow_key", fn)
	if err == nil {
		t.Error("expected context deadline error")
	}
}

func TestFlight_Forget(t *testing.T) {
	sf := New[string]()

	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		return "result", nil
	}

	_, _ = sf.Do(context.Background(), "k", fn)
	sf.Forget("k")
	_, _ = sf.Do(context.Background(), "k", fn)

	if got := calls.Load(); got != 2 {
		t.Errorf("expected 2 calls after Forget, got %d", got)
	}
}

func TestFlight_PanicPropagatesToCaller(t *testing.T) {
	sf := New[string]()

	started := make(chan struct{})
	fn := func(_ context.Context) (string, error) {
		close(started)
		time.Sleep(50 * time.Millisecond)
		panic("boom")
	}

	// caller goroutine：执行 fn，应收到 panic
	callerRecovered := make(chan any, 1)
	go func() {
		defer func() { callerRecovered <- recover() }()
		_, _ = sf.Do(context.Background(), "panic_key", fn)
	}()

	<-started // fn 已在 caller goroutine 中开始执行，意味着 key 已注册

	// waiter：应被唤醒（不阻塞），拿到由 panic 转化的 error
	// （零值 + nil error 是伪成功，调用方无法区分合法零值与执行失败）
	waiterDone := make(chan error, 1)
	go func() {
		_, err := sf.Do(context.Background(), "panic_key", fn)
		waiterDone <- err
	}()

	select {
	case err := <-waiterDone:
		if err == nil {
			t.Error("waiter expected non-nil error on fn panic, got nil")
		} else if !strings.Contains(err.Error(), "panic") {
			t.Errorf("waiter error should mention panic, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter blocked: fn panic did not wake waiters")
	}

	// 确认 panic 传播到了首个 caller
	if r := <-callerRecovered; r == nil {
		t.Error("caller expected to receive the panic, got nil")
	}
}

func TestFlight_PanicCleansMapEntry(t *testing.T) {
	sf := New[string]()

	// 第一次调用 fn panic，key 记录应被清理
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		_, _ = sf.Do(context.Background(), "k", func(_ context.Context) (string, error) {
			panic("cleanup-test")
		})
	}()

	if !panicked {
		t.Fatal("expected caller to panic")
	}

	// panic 后 key 应已从 map 移除：新的 Do 会真正执行 fn（而非永久阻塞）
	var calls atomic.Int32
	_, err := sf.Do(context.Background(), "k", func(_ context.Context) (string, error) {
		calls.Add(1)
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected err after panic cleanup: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 call after panic cleanup, got %d", calls.Load())
	}
}

func TestFlight_SequentialCalls(t *testing.T) {
	sf := New[string]()

	var calls atomic.Int32
	fn := func(_ context.Context) (string, error) {
		calls.Add(1)
		return "val", nil
	}

	val1, err := sf.Do(context.Background(), "k", fn)
	if err != nil || val1 != "val" {
		t.Fatalf("first call: val=%q err=%v", val1, err)
	}

	val2, err := sf.Do(context.Background(), "k", fn)
	if err != nil || val2 != "val" {
		t.Fatalf("second call: val=%q err=%v", val2, err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("expected 2 calls (no caching between sequential calls), got %d", got)
	}
}

func TestFlight_GenericInt(t *testing.T) {
	sf := New[int]()

	var calls atomic.Int32
	fn := func(_ context.Context) (int, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return 42, nil
	}

	var wg sync.WaitGroup
	results := make(chan int, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, _ := sf.Do(context.Background(), "int_key", fn)
			results <- val
		}()
	}
	wg.Wait()
	close(results)

	for r := range results {
		if r != 42 {
			t.Errorf("expected 42, got %d", r)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 call, got %d", got)
	}
}

func TestFlight_GenericStruct(t *testing.T) {
	type token struct {
		AccessToken string
		ExpiresAt   time.Time
	}

	sf := New[token]()

	var calls atomic.Int32
	fn := func(_ context.Context) (token, error) {
		calls.Add(1)
		time.Sleep(30 * time.Millisecond)
		return token{AccessToken: "abc", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}

	var wg sync.WaitGroup
	results := make(chan token, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, _ := sf.Do(context.Background(), "struct_key", fn)
			results <- val
		}()
	}
	wg.Wait()
	close(results)

	for r := range results {
		if r.AccessToken != "abc" {
			t.Errorf("expected abc, got %s", r.AccessToken)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 call, got %d", got)
	}
}
