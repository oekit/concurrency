package fanout

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// errBoom 是测试用的哨兵错误。
var errBoom = errors.New("boom")

// TestMap_Order 表驱动钉住「结果按输入顺序」：故意让后面的项先完成，
// 返回的 results[i] 仍必须对应 items[i]。
func TestMap_Order(t *testing.T) {
	cases := []struct {
		name        string
		items       []int
		concurrency int
	}{
		{name: "单并发", items: []int{0, 1, 2, 3, 4}, concurrency: 1},
		{name: "多并发", items: []int{0, 1, 2, 3, 4, 5, 6, 7}, concurrency: 4},
		{name: "并发大于项数", items: []int{0, 1, 2}, concurrency: 16},
		{name: "不限并发", items: []int{0, 1, 2, 3}, concurrency: 0},
		{name: "负并发按不限处理", items: []int{0, 1, 2, 3}, concurrency: -3},
		{name: "单元素", items: []int{7}, concurrency: 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 倒序 sleep：item 越小睡越久 ⇒ 完成顺序与输入顺序相反
			results, err := Map(context.Background(), tc.items, tc.concurrency,
				func(ctx context.Context, item int) (string, error) {
					time.Sleep(time.Duration(len(tc.items)-item) * 5 * time.Millisecond)
					return fmt.Sprintf("v%d", item), nil
				})
			if err != nil {
				t.Fatalf("Map 返回错误：%v", err)
			}
			if len(results) != len(tc.items) {
				t.Fatalf("结果长度 = %d, want %d", len(results), len(tc.items))
			}
			for i, item := range tc.items {
				if want := fmt.Sprintf("v%d", item); results[i] != want {
					t.Errorf("results[%d] = %q, want %q（结果必须按输入顺序）", i, results[i], want)
				}
			}
		})
	}
}

// TestMap_ConcurrencyLimit 钉住「并发上限真的生效」：记录同时在跑的最大数，
// 不得超过 concurrency。
func TestMap_ConcurrencyLimit(t *testing.T) {
	for _, concurrency := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("上限%d", concurrency), func(t *testing.T) {
			var (
				cur   atomic.Int64
				peak  atomic.Int64
				calls atomic.Int64
			)
			items := make([]int, 24)
			results, err := Map(context.Background(), items, concurrency,
				func(ctx context.Context, item int) (int, error) {
					calls.Add(1)
					n := cur.Add(1)
					for {
						p := peak.Load()
						if n <= p || peak.CompareAndSwap(p, n) {
							break
						}
					}
					time.Sleep(5 * time.Millisecond)
					cur.Add(-1)
					return item * 2, nil
				})
			if err != nil {
				t.Fatalf("Map 返回错误：%v", err)
			}
			if got := peak.Load(); got > int64(concurrency) {
				t.Errorf("同时在跑的最大数 = %d, 不得超过上限 %d", got, concurrency)
			}
			if got := calls.Load(); got != int64(len(items)) {
				t.Errorf("fn 调用次数 = %d, want %d", got, len(items))
			}
			if len(results) != len(items) {
				t.Errorf("结果长度 = %d, want %d", len(results), len(items))
			}
		})
	}
}

// TestMap_FailureDoesNotStopOthers 钉住「一项失败不影响其余项」：中间项失败，
// 其余项仍全部执行完，且错误被返回（失败项结果为零值）。
func TestMap_FailureDoesNotStopOthers(t *testing.T) {
	items := []int{0, 1, 2, 3, 4}
	var calls atomic.Int64
	results, err := Map(context.Background(), items, 2,
		func(ctx context.Context, item int) (string, error) {
			calls.Add(1)
			if item == 2 {
				return "", errBoom
			}
			return fmt.Sprintf("v%d", item), nil
		})
	if err == nil {
		t.Fatal("有一项失败时必须返回错误")
	}
	if got := calls.Load(); got != int64(len(items)) {
		t.Errorf("fn 调用次数 = %d, want %d（一项失败不得让其余项被跳过）", got, len(items))
	}
	if results[2] != "" {
		t.Errorf("失败项的结果 = %q, want 零值", results[2])
	}
	for _, i := range []int{0, 1, 3, 4} {
		if want := fmt.Sprintf("v%d", i); results[i] != want {
			t.Errorf("results[%d] = %q, want %q（成功项不受失败项影响）", i, results[i], want)
		}
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("errors.Is(err, errBoom) = false, want true（错误必须可穿透）")
	}
}

// TestMap_ErrorAggregatesAllFailures 钉住「失败项全部汇总」：多项失败时，
// *Error.Failed 里逐项列出下标与原始错误，且按输入序。
func TestMap_ErrorAggregatesAllFailures(t *testing.T) {
	errA := errors.New("A 失败")
	errB := errors.New("B 失败")
	items := []int{0, 1, 2, 3}
	_, err := Map(context.Background(), items, 2,
		func(ctx context.Context, item int) (int, error) {
			switch item {
			case 1:
				return 0, errA
			case 3:
				return 0, errB
			}
			return item, nil
		})
	if err == nil {
		t.Fatal("必须返回错误")
	}
	var agg *Error
	if !errors.As(err, &agg) {
		t.Fatalf("错误类型 = %T, want *fanout.Error", err)
	}
	if len(agg.Failed) != 2 {
		t.Fatalf("失败项数 = %d, want 2", len(agg.Failed))
	}
	if agg.Failed[0].Index != 1 || agg.Failed[1].Index != 3 {
		t.Errorf("失败项下标 = [%d %d], want [1 3]（须按输入序）",
			agg.Failed[0].Index, agg.Failed[1].Index)
	}
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Error("汇总错误必须同时可判出每一个原始错误")
	}
	if agg.Error() == "" {
		t.Error("汇总错误的消息不得为空")
	}
}

// TestMap_EmptyInput 空输入返回 nil 结果与 nil 错误，且不得 panic。
func TestMap_EmptyInput(t *testing.T) {
	results, err := Map(context.Background(), []int{}, 4,
		func(ctx context.Context, item int) (int, error) {
			t.Fatal("空输入不得调用 fn")
			return 0, nil
		})
	if err != nil {
		t.Errorf("空输入返回错误 = %v, want nil", err)
	}
	if len(results) != 0 {
		t.Errorf("空输入的结果长度 = %d, want 0", len(results))
	}
}

// TestMap_ContextCancel 钉住「ctx 取消后不再调度新任务」：取消后未开始执行的项
// 以 context.Canceled 记入失败，且 fn 不再被调用。
func TestMap_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	items := make([]int, 10)
	results, err := Map(ctx, items, 1,
		func(ctx context.Context, item int) (int, error) {
			calls.Add(1)
			cancel() // 第一项执行时取消
			return item, nil
		})
	if err == nil {
		t.Fatal("ctx 取消必须体现在错误里")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false, want true")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fn 调用次数 = %d, want 1（取消后不得再调度新任务）", got)
	}
	if results[0] != 0 {
		t.Errorf("首项结果 = %d, want 0", results[0])
	}
	var agg *Error
	if errors.As(err, &agg) {
		if len(agg.Failed) != len(items)-1 {
			t.Errorf("未执行项的失败数 = %d, want %d", len(agg.Failed), len(items)-1)
		}
	}
}

// TestMap_PanicIsolated 钉住「fn panic 不影响其余项」：panic 被收编成该项的失败
// （*async.PanicError），其余项照常完成。
func TestMap_PanicIsolated(t *testing.T) {
	items := []int{0, 1, 2}
	results, err := Map(context.Background(), items, 2,
		func(ctx context.Context, item int) (int, error) {
			if item == 1 {
				panic("fn 炸了")
			}
			return item, nil
		})
	if err == nil {
		t.Fatal("panic 必须体现在错误里")
	}
	var pe *async.PanicError
	if !errors.As(err, &pe) {
		t.Errorf("panic 未被收编成 *async.PanicError（errors.As 判不出）")
	}
	if results[0] != 0 || results[2] != 2 {
		t.Errorf("其余项结果 = [%d %d], want [0 2]（panic 不得影响其余项）", results[0], results[2])
	}
}

// TestMap_ItemPassedByValue 钉住「fn 收到的是输入项本身」（item 口径，不是下标口径）。
func TestMap_ItemPassedByValue(t *testing.T) {
	type asset struct {
		Host string
		Port int
	}
	items := []asset{{Host: "a", Port: 1}, {Host: "b", Port: 2}, {Host: "c", Port: 3}}
	results, err := Map(context.Background(), items, 2,
		func(ctx context.Context, a asset) (string, error) {
			return fmt.Sprintf("%s:%d", a.Host, a.Port), nil
		})
	if err != nil {
		t.Fatalf("Map 返回错误：%v", err)
	}
	for i, want := range []string{"a:1", "b:2", "c:3"} {
		if results[i] != want {
			t.Errorf("results[%d] = %q, want %q", i, results[i], want)
		}
	}
}

// TestItemError_Unwrap ItemError 自身可被 errors.Is 判出。
func TestItemError_Unwrap(t *testing.T) {
	ie := &ItemError{Index: 7, Err: errBoom}
	if !errors.Is(ie, errBoom) {
		t.Error("ItemError 必须能穿透到原始错误")
	}
	if ie.Error() == "" {
		t.Error("ItemError 的消息不得为空")
	}
}
