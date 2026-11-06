package fanout

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/oekit/concurrency/async"
)

// TestMap_OnItemError_ReportsEachFailure 钉住：钩子对每个失败项各调一次，
// 下标与输入对齐，且**按升序**（聚合阶段逐项，不与 worker 完成序挂钩）。
func TestMap_OnItemError_ReportsEachFailure(t *testing.T) {
	items := []int{0, 1, 2, 3, 4}
	wantFail := map[int]bool{1: true, 3: true}

	var mu sync.Mutex
	var calls []int
	var errs []error

	results, err := Map(context.Background(), items, 3,
		func(_ context.Context, it int) (string, error) {
			if wantFail[it] {
				return "", errors.New("坏项")
			}
			return "ok", nil
		},
		WithOnItemError(func(i int, err error) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, i)
			errs = append(errs, err)
		}))

	if err == nil {
		t.Fatal("有失败项时 Map 应返回汇总错误")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || calls[0] != 1 || calls[1] != 3 {
		t.Fatalf("钩子应按下标升序对失败项各调一次，got %v", calls)
	}
	for i, e := range errs {
		if e == nil || e.Error() != "坏项" {
			t.Fatalf("第 %d 次钩子的 err 应是该项原始错误，got %v", i, e)
		}
	}
	// 成功项的结果不受影响
	if len(results) != len(items) || results[0] != "ok" || results[2] != "ok" {
		t.Fatalf("成功项结果应保持，got %v", results)
	}
}

// TestMap_OnItemError_SuccessItemsNotReported 钉住：全成功时钩子一次都不调。
func TestMap_OnItemError_SuccessItemsNotReported(t *testing.T) {
	called := 0
	_, err := Map(context.Background(), []int{1, 2}, 2,
		func(_ context.Context, it int) (int, error) { return it, nil },
		WithOnItemError(func(int, error) { called++ }))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if called != 0 {
		t.Fatalf("成功项不该触发钩子，got %d 次", called)
	}
}

// TestMap_OnItemError_PanicGoesThroughHook 钉住本选项存在的理由：**panic 也走
// 钩子**（err 是 *async.PanicError），消费方不必再在 fn 内层手写 recover 抢先。
func TestMap_OnItemError_PanicGoesThroughHook(t *testing.T) {
	async.SetPanicHandler(nil)
	defer async.SetPanicHandler(nil)

	var mu sync.Mutex
	gotIndex := -1
	var gotErr error

	_, err := Map(context.Background(), []int{10, 20}, 2,
		func(_ context.Context, it int) (int, error) {
			if it == 20 {
				panic("毒丸行")
			}
			return it, nil
		},
		WithOnItemError(func(i int, err error) {
			mu.Lock()
			defer mu.Unlock()
			gotIndex, gotErr = i, err
		}))

	mu.Lock()
	defer mu.Unlock()
	if gotIndex != 1 {
		t.Fatalf("钩子应拿到 panic 项的下标 1，got %d", gotIndex)
	}
	var pe *async.PanicError
	if !errors.As(gotErr, &pe) {
		t.Fatalf("钩子收到的应是 *async.PanicError，got %T: %v", gotErr, gotErr)
	}
	if pe.Value != "毒丸行" {
		t.Fatalf("panic payload 应保留，got %v", pe.Value)
	}
	// 库自身的汇总通道不能被钩子吃掉：Error.Failed 仍要有这一项
	var fe *Error
	if !errors.As(err, &fe) || len(fe.Failed) != 1 {
		t.Fatalf("汇总错误应仍含该项，got %v", err)
	}
}

// TestMap_OnItemError_CtxCanceledUnstarted 钉住：ctx 取消导致**未开始**的项
// 也进钩子（与 Error.Failed 同一口径），调用方不会漏掉「根本没跑」的那批。
func TestMap_OnItemError_CtxCanceledUnstarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 调用前就取消 ⇒ 全部项未启动

	var mu sync.Mutex
	var indexes []int
	var canceled int

	_, err := Map(ctx, []int{1, 2, 3}, 1,
		func(c context.Context, it int) (int, error) { return it, nil },
		WithOnItemError(func(i int, e error) {
			mu.Lock()
			defer mu.Unlock()
			indexes = append(indexes, i)
			if errors.Is(e, context.Canceled) {
				canceled++
			}
		}))

	mu.Lock()
	defer mu.Unlock()
	if len(indexes) != 3 || canceled != 3 {
		t.Fatalf("未启动项应全部以 context.Canceled 进钩子，got indexes=%v canceled=%d", indexes, canceled)
	}
	if err == nil {
		t.Fatal("应有汇总错误")
	}
}

// TestMap_OnItemError_HookPanicIsolated 钉住：钩子自己 panic 不打断整批聚合
// ——「某一项的钩子写坏了」不能升级成「整批结果全丢」。
func TestMap_OnItemError_HookPanicIsolated(t *testing.T) {
	var reported bool
	async.SetPanicHandler(func(string, []byte) { reported = true })
	defer async.SetPanicHandler(nil)

	results, err := Map(context.Background(), []int{1, 2}, 2,
		func(_ context.Context, it int) (int, error) {
			if it == 2 {
				return 0, errors.New("坏")
			}
			return it, nil
		},
		WithOnItemError(func(int, error) { panic("钩子也崩") }))

	if results[0] != 1 {
		t.Fatalf("钩子 panic 不该影响其它项结果，got %v", results)
	}
	var fe *Error
	if !errors.As(err, &fe) || len(fe.Failed) != 1 {
		t.Fatalf("汇总错误应保持，got %v", err)
	}
	if !reported {
		t.Fatal("钩子 panic 应经 async.ReportPanic 上报全局 handler")
	}
}

// TestMap_NilOptionTolerated 钉住：变参里混入 nil Option 不 panic（调用方常在
// 条件分支里传 opts...）。
func TestMap_NilOptionTolerated(t *testing.T) {
	_, err := Map(context.Background(), []int{1}, 1,
		func(_ context.Context, it int) (int, error) { return it, nil }, nil)
	if err != nil {
		t.Fatalf("nil Option 应被忽略，got %v", err)
	}
}
