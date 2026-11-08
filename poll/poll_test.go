package poll

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/concurrency/retry"
)

// waitFor 等条件成立（有界等待，超时即失败）。
//
// 用「有界等待」而不是「睡一个估计出来的时长再断言」：后者在负载高时会假红
// （本仓 v0.23.1 / v0.26.0 两次实证）。条件通常是 atomic 计数或已关闭的通道。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("3s 内未等到：%s", what)
}

type runResult struct {
	cursor string
	err    error
}

// startRun 在独立 goroutine 里跑 RunCursor，返回结果通道与 cancel。
//
// 用例的读取纪律：共享切片只在收到结果之后读（通道接收建立 happens-before），
// 运行期的进度一律用 atomic 计数观测——否则 -race 下必报数据竞争。
func startRun[T any](
	cfg Config,
	from string,
	fetch func(context.Context, string) ([]T, string, error),
	handle func(context.Context, T) error,
) (<-chan runResult, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan runResult, 1)
	go func() {
		cursor, err := RunCursor(ctx, cfg, from, fetch, handle)
		ch <- runResult{cursor: cursor, err: err}
	}()
	return ch, cancel
}

// TestRunCursor_AdvancesAfterAllItemsHandled 钉死主链路：逐批拉取、按序逐项处理、
// 整批处理完才推进游标（推进先落盘、后算推进）。
func TestRunCursor_AdvancesAfterAllItemsHandled(t *testing.T) {
	script := []struct {
		items []string
		next  string
	}{
		{[]string{"a", "b"}, "c1"},
		{[]string{"c"}, "c2"},
	}

	var (
		calls    atomic.Int64
		advanced atomic.Int64
	)

	var got []string
	var advances []string

	fetch := func(ctx context.Context, _ string) ([]string, string, error) {
		i := int(calls.Add(1) - 1)
		if i < len(script) {
			return script[i].items, script[i].next, nil
		}
		<-ctx.Done() // 脚本跑完：模拟服务端 hold 住等新消息
		return nil, "", ctx.Err()
	}
	handle := func(_ context.Context, item string) error {
		got = append(got, item)
		return nil
	}
	cfg := Config{OnAdvance: func(_ context.Context, cursor string) error {
		advances = append(advances, cursor)
		advanced.Add(1)
		return nil
	}}

	resCh, cancel := startRun(cfg, "", fetch, handle)
	waitFor(t, "两批游标都已推进", func() bool { return advanced.Load() == 2 })
	cancel()
	res := <-resCh

	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v，期望 context.Canceled", res.err)
	}
	if res.cursor != "c2" {
		t.Fatalf("退出游标 = %q，期望 c2（返回值供关机持久化）", res.cursor)
	}
	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("处理顺序 = %v，期望 %v", got, want)
	}
	if want := []string{"c1", "c2"}; !equalStrings(advances, want) {
		t.Fatalf("推进序列 = %v，期望 %v（整批处理完才推进）", advances, want)
	}
}

// TestRunCursor_ItemErrorHoldsCursor 缺省档（at-least-once）：某一项失败 ⇒ 停止
// 处理本批、不推进游标、退避后重拉同一批。
func TestRunCursor_ItemErrorHoldsCursor(t *testing.T) {
	errItem := errors.New("这一项处理失败")

	var (
		advanced  atomic.Int64
		bFailed   atomic.Bool
		got       []string
		advances  []string
		fetchSeen []string
	)

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		fetchSeen = append(fetchSeen, cursor)
		if cursor != "" {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return []string{"a", "b", "c"}, "c1", nil
	}
	handle := func(_ context.Context, item string) error {
		got = append(got, item)
		if item == "b" && !bFailed.Load() {
			bFailed.Store(true)
			return errItem
		}
		return nil
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: time.Millisecond},
		OnAdvance: func(_ context.Context, cursor string) error {
			advances = append(advances, cursor)
			advanced.Add(1)
			return nil
		},
	}

	resCh, cancel := startRun(cfg, "", fetch, handle)
	waitFor(t, "失败后重拉并成功推进", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh

	// 关键断言：b 失败那轮**没有**处理 c（停在该项），重拉后才 a,b,c 全过。
	if want := []string{"a", "b", "a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("处理序列 = %v，期望 %v（失败即停本批，未处理项留待重拉）", got, want)
	}
	if want := []string{"c1"}; !equalStrings(advances, want) {
		t.Fatalf("推进序列 = %v，期望 %v（失败那轮不得推进）", advances, want)
	}
	if len(fetchSeen) < 2 || fetchSeen[0] != "" || fetchSeen[1] != "" {
		t.Fatalf("重拉游标 = %v，期望前两次都是空游标（未推进）", fetchSeen)
	}
	if res.cursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1", res.cursor)
	}
}

// TestRunCursor_AdvanceOnItemErrorHandlesWholeBatch 显式档：单项失败仍推进游标，
// 但**必须先把整批跑完**——中途返回会让本批剩余项再也不会被拉回来（真丢数据）。
func TestRunCursor_AdvanceOnItemErrorHandlesWholeBatch(t *testing.T) {
	errItem := errors.New("毒丸")

	var (
		advanced atomic.Int64
		got      []string
	)

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		if cursor != "" {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return []string{"a", "b", "c"}, "c1", nil
	}
	handle := func(_ context.Context, item string) error {
		got = append(got, item)
		if item == "b" {
			return errItem
		}
		return nil
	}
	cfg := Config{
		ErrorBackoff:       retry.Config{Delay: time.Millisecond},
		AdvanceOnItemError: true,
		OnAdvance: func(_ context.Context, _ string) error {
			advanced.Add(1)
			return nil
		},
	}

	resCh, cancel := startRun(cfg, "", fetch, handle)
	waitFor(t, "整批跑完并推进", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh

	if want := []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("处理序列 = %v，期望 %v（本档必须跑完整批）", got, want)
	}
	if res.cursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1（本档失败也推进）", res.cursor)
	}
}

// TestRunCursor_ItemPanicDoesNotKillLoop 单项 panic：循环存活、panic 上报全局
// PanicHandler、该项按失败档处理。
func TestRunCursor_ItemPanicDoesNotKillLoop(t *testing.T) {
	var panics atomic.Int64
	async.SetPanicHandler(func(context.Context, any, []byte) { panics.Add(1) })
	t.Cleanup(func() { async.SetPanicHandler(func(context.Context, any, []byte) {}) })

	var (
		advanced  atomic.Int64
		panicked  atomic.Bool
		got       []string
		resCursor string
	)

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		if cursor != "" {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return []string{"a", "b", "c"}, "c1", nil
	}
	handle := func(_ context.Context, item string) error {
		got = append(got, item)
		if item == "b" && !panicked.Load() {
			panicked.Store(true)
			panic("某条坏消息")
		}
		return nil
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: time.Millisecond},
		OnAdvance: func(_ context.Context, _ string) error {
			advanced.Add(1)
			return nil
		},
	}

	resCh, cancel := startRun(cfg, "", fetch, handle)
	waitFor(t, "panic 后重拉并推进", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh
	resCursor = res.cursor

	if panics.Load() == 0 {
		t.Fatal("单项 panic 没有上报全局 PanicHandler（观测面漏接）")
	}
	// panic 那轮停在 b（c 没跑过），重拉后 a,b,c 全过
	if want := []string{"a", "b", "a", "b", "c"}; !equalStrings(got, want) {
		t.Fatalf("处理序列 = %v，期望 %v", got, want)
	}
	if resCursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1（循环必须存活到推进）", resCursor)
	}
}

// TestRunCursor_FetchPanicSurvives fetch 自身 panic：收编成一次可续拉失败，循环存活。
func TestRunCursor_FetchPanicSurvives(t *testing.T) {
	var panics atomic.Int64
	async.SetPanicHandler(func(context.Context, any, []byte) { panics.Add(1) })
	t.Cleanup(func() { async.SetPanicHandler(func(context.Context, any, []byte) {}) })

	var (
		calls    atomic.Int64
		advanced atomic.Int64
	)

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		if calls.Add(1) == 1 {
			panic("拉取实现崩了")
		}
		if cursor != "" {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return []string{"a"}, "c1", nil
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: time.Millisecond},
		OnAdvance: func(_ context.Context, _ string) error {
			advanced.Add(1)
			return nil
		},
	}

	resCh, cancel := startRun(cfg, "", fetch, func(context.Context, string) error { return nil })
	waitFor(t, "panic 后重拉并推进", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh

	if panics.Load() == 0 {
		t.Fatal("fetch panic 没有上报全局 PanicHandler")
	}
	if res.cursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1", res.cursor)
	}
}

// TestRunCursor_NonRetryableReturns 不可续拉的错误：带着错误返回，不空转。
func TestRunCursor_NonRetryableReturns(t *testing.T) {
	errToken := errors.New("凭据失效")

	var calls atomic.Int64
	fetch := func(context.Context, string) ([]string, string, error) {
		calls.Add(1)
		return nil, "", errToken
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: time.Hour},
		Retryable:    func(error) bool { return false },
	}

	cursor, err := RunCursor(context.Background(), cfg, "c0", fetch,
		func(context.Context, string) error { return nil })

	if !errors.Is(err, errToken) {
		t.Fatalf("err = %v，期望原样返回 errToken", err)
	}
	if cursor != "c0" {
		t.Fatalf("游标 = %q，期望保持 c0（未推进）", cursor)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("fetch 调用 %d 次，期望 1 次（不可续拉就该立刻退出）", got)
	}
}

// TestRunCursor_StuckCursor 非空批却不推进游标：契约违反当场暴露，不无限重处理。
func TestRunCursor_StuckCursor(t *testing.T) {
	cases := []struct {
		name  string
		items []string
		next  string
	}{
		{name: "游标原样返回", items: []string{"a"}, next: "c0"},
		{name: "游标为空", items: []string{"a"}, next: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var handled atomic.Int64
			fetch := func(context.Context, string) ([]string, string, error) {
				return tc.items, tc.next, nil
			}
			_, err := RunCursor(context.Background(), Config{}, "c0", fetch,
				func(context.Context, string) error {
					handled.Add(1)
					return nil
				})

			if !errors.Is(err, ErrCursorStuck) {
				t.Fatalf("err = %v，期望 ErrCursorStuck", err)
			}
			if handled.Load() != 0 {
				t.Fatal("游标卡住时不该已经开始处理本批（先校验再处理）")
			}
		})
	}
}

// TestRunCursor_EmptyBatchAdvancesCursor 空批但游标前进（服务端把游标推到当前
// 读到的位置）：照常推进，否则重启会重扫。
func TestRunCursor_EmptyBatchAdvancesCursor(t *testing.T) {
	var (
		calls    atomic.Int64
		advanced atomic.Int64
	)
	var advances []string

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		if calls.Add(1) == 1 {
			return nil, "c1", nil // 0 项 + 游标前进
		}
		<-ctx.Done()
		return nil, "", ctx.Err()
	}
	cfg := Config{OnAdvance: func(_ context.Context, cursor string) error {
		advances = append(advances, cursor)
		advanced.Add(1)
		return nil
	}}

	resCh, cancel := startRun(cfg, "c0", fetch, func(context.Context, string) error { return nil })
	waitFor(t, "空批也推进游标", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh

	if want := []string{"c1"}; !equalStrings(advances, want) {
		t.Fatalf("推进序列 = %v，期望 %v", advances, want)
	}
	if res.cursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1", res.cursor)
	}
}

// TestRunCursor_OnAdvanceErrorHoldsCursor 持久化失败 ⇒ 游标不算推进（重拉同一批）。
//
// 代价是「已处理项被再处理一次」（at-least-once 的已知形态），远比「游标前进了但
// 没落盘，重启后从一个从未存在过的位置继续」便宜。
func TestRunCursor_OnAdvanceErrorHoldsCursor(t *testing.T) {
	errSave := errors.New("存储不可用")

	var (
		calls    atomic.Int64
		saves    atomic.Int64
		advanced atomic.Int64
		saveFail atomic.Bool
	)
	var fetchSeen []string

	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		fetchSeen = append(fetchSeen, cursor)
		if cursor != "" {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return []string{"a"}, "c1", nil
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: time.Millisecond},
		OnAdvance: func(_ context.Context, _ string) error {
			saves.Add(1)
			if !saveFail.Load() {
				saveFail.Store(true)
				return errSave
			}
			advanced.Add(1)
			return nil
		},
	}

	resCh, cancel := startRun(cfg, "", fetch, func(context.Context, string) error {
		calls.Add(1)
		return nil
	})
	waitFor(t, "落盘失败后重拉并最终推进", func() bool { return advanced.Load() == 1 })
	cancel()
	res := <-resCh

	if got := saves.Load(); got != 2 {
		t.Fatalf("OnAdvance 调用 %d 次，期望 2 次（失败后重试同一批）", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handle 调用 %d 次，期望 2 次（失败那批会再处理一遍）", got)
	}
	if len(fetchSeen) < 2 || fetchSeen[0] != "" || fetchSeen[1] != "" {
		t.Fatalf("重拉游标 = %v，期望前两次都是空游标", fetchSeen)
	}
	if res.cursor != "c1" {
		t.Fatalf("退出游标 = %q，期望 c1", res.cursor)
	}
}

// TestRunCursor_IdleDelaySpacesEmptyPolls 空批等待真的在等（下界断言）。
func TestRunCursor_IdleDelaySpacesEmptyPolls(t *testing.T) {
	const idle = 40 * time.Millisecond

	var calls atomic.Int64
	fetch := func(ctx context.Context, cursor string) ([]string, string, error) {
		if calls.Add(1) >= 3 {
			<-ctx.Done()
			return nil, "", ctx.Err()
		}
		return nil, cursor, nil // 空批、游标不动 ⇒ 走 IdleDelay
	}

	start := time.Now()
	resCh, cancel := startRun(Config{IdleDelay: idle}, "c0", fetch,
		func(context.Context, string) error { return nil })
	waitFor(t, "三次空批", func() bool { return calls.Load() >= 3 })
	elapsed := time.Since(start)
	cancel()
	<-resCh

	// 下界断言不会假红：time.Timer 从不提前触发，三次空批之间必然隔着两个 idle。
	if min := 2 * idle; elapsed < min {
		t.Fatalf("三次空批耗时 %s，小于下界 %s（IdleDelay 没生效，空批在忙轮询）", elapsed, min)
	}
}

// TestRunCursor_BackoffGrowsWithFailures 退避按失败次数增长（下界断言）。
func TestRunCursor_BackoffGrowsWithFailures(t *testing.T) {
	const base = 20 * time.Millisecond

	var calls atomic.Int64
	fetch := func(ctx context.Context, _ string) ([]string, string, error) {
		calls.Add(1)
		return nil, "", errors.New("下游不可用")
	}
	cfg := Config{ErrorBackoff: retry.Config{Delay: base, Linear: true}}

	start := time.Now()
	resCh, cancel := startRun(cfg, "", fetch, func(context.Context, string) error { return nil })
	waitFor(t, "三次续拉", func() bool { return calls.Load() >= 3 })
	elapsed := time.Since(start)
	cancel()
	<-resCh
	// 线性档：失败 1 次等 base、2 次等 2*base ⇒ 第三次拉取前至少过了 3*base。
	if min := 3 * base; elapsed < min {
		t.Fatalf("三次续拉耗时 %s，小于下界 %s（线性退避没生效）", elapsed, min)
	}
}

// TestRunCursor_EmptyBatchResetsBackoff 空批（fetch 成功、0 项、游标不变——
// 长轮询常态）是**成功轮**，失败计数必须归零（#191）。
//
// 序列：#1#2#3 失败 → #4 空批（游标不变）→ #5 失败 → #6 取消。
// 若 #5 按 attempt=1 起算（指数 40ms），#5→#6 间隔 ≈ 40ms；修复前计数没归零，
// #5 是 attempt=4 ⇒ 320ms——本用例以间隔上界钉住归零语义。
func TestRunCursor_EmptyBatchResetsBackoff(t *testing.T) {
	const base = 40 * time.Millisecond

	var (
		mu    sync.Mutex
		at    []time.Time
		empty atomic.Bool
	)
	fetch := func(_ context.Context, _ string) ([]string, string, error) {
		mu.Lock()
		at = append(at, time.Now())
		n := len(at)
		mu.Unlock()
		if n == 4 {
			// #4：成功但暂无新数据（空批、游标不动）——长轮询的常态轮
			empty.Store(true)
			return nil, "", nil
		}
		return nil, "", errors.New("下游抖动")
	}
	cfg := Config{ErrorBackoff: retry.Config{Delay: base, Exponential: true}}

	resCh, cancel := startRun(cfg, "", fetch, func(context.Context, string) error { return nil })
	waitFor(t, "#5 失败轮之后 #6 再拉一次", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(at) >= 6
	})
	if !empty.Load() {
		t.Fatal("#4 应是空批（用例编排失效）")
	}
	cancel()
	<-resCh

	mu.Lock()
	defer mu.Unlock()
	if len(at) < 6 {
		t.Fatalf("fetch 调用 %d 次，期望 ≥6", len(at))
	}
	gap := at[5].Sub(at[4])
	// attempt=1 ⇒ 40ms；未归零 ⇒ attempt=4 ⇒ 320ms。上下界都留足裕量。
	if gap < 30*time.Millisecond {
		t.Fatalf("#5→#6 间隔 %s，小于 30ms（#5 之前不该有等待之外的长退避编排）", gap)
	}
	if gap > 150*time.Millisecond {
		t.Fatalf("空批成功轮后 #5 的退避 = %s（>150ms）——失败计数没有被空批归零，attempt 仍按 4 起算（#191）", gap)
	}
}

// TestRunCursor_EmptyBatchAdvanceFailureBacksOff 空批推游标 + OnAdvance 持续失败
// （#216）：失败计数归零点在游标落定之后——advance 失败路径保持累计，退避照常
// 指数升级，与非空批口径一致；否则每轮从 0 计到 1 恒 attempt=1，对已死存储按
// 基础频率持续写打。
//
// 序列：服务端每轮返回空批且把游标推到新位置（poll.go 明确支持的形态），
// OnAdvance 恒失败。修复后第 3→4 次 OnAdvance 间隔 = attempt=3 的指数退避
// （40ms×4=160ms）；修复前每轮归零恒 40ms。下界断言（time.Timer 不提前触发）。
func TestRunCursor_EmptyBatchAdvanceFailureBacksOff(t *testing.T) {
	const base = 40 * time.Millisecond

	var (
		mu sync.Mutex
		at []time.Time // 每次 OnAdvance 失败的时刻
	)
	fetch := func(_ context.Context, _ string) ([]string, string, error) {
		// 空批 + 游标推到新位置：服务端「空批推游标」形态
		mu.Lock()
		next := fmt.Sprintf("c%d", len(at)+1)
		mu.Unlock()
		return nil, next, nil
	}
	cfg := Config{
		ErrorBackoff: retry.Config{Delay: base, Exponential: true},
		OnAdvance: func(context.Context, string) error {
			mu.Lock()
			at = append(at, time.Now())
			mu.Unlock()
			return errors.New("存储不可用")
		},
	}

	resCh, cancel := startRun(cfg, "c0", fetch, func(context.Context, string) error { return nil })
	waitFor(t, "四次 OnAdvance 失败", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(at) >= 4
	})
	cancel()
	<-resCh

	mu.Lock()
	defer mu.Unlock()
	if len(at) < 4 {
		t.Fatalf("OnAdvance 调用 %d 次，期望 ≥4", len(at))
	}
	gap := at[3].Sub(at[2])
	// attempt=3 ⇒ 160ms；每轮归零的旧实现 ⇒ 恒 attempt=1 ⇒ 40ms。下界 120ms 分离。
	if gap < 120*time.Millisecond {
		t.Fatalf("OnAdvance 持续失败间隔 %s，小于 attempt=3 退避下界 120ms——空批分支失败计数被提前归零，退避永不升级（#216）", gap)
	}
}

// TestRunCursor_CancelDuringBackoff 退避等待感知 ctx 取消（不睡满）。
func TestRunCursor_CancelDuringBackoff(t *testing.T) {
	fetched := make(chan struct{}, 1)
	fetch := func(context.Context, string) ([]string, string, error) {
		select {
		case fetched <- struct{}{}:
		default:
		}
		return nil, "", errors.New("下游不可用")
	}
	cfg := Config{ErrorBackoff: retry.Config{Delay: time.Hour}}

	resCh, cancel := startRun(cfg, "", fetch, func(context.Context, string) error { return nil })
	select {
	case <-fetched:
	case <-time.After(3 * time.Second):
		t.Fatal("第一次拉取没发生")
	}
	cancel()

	select {
	case res := <-resCh:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("err = %v，期望 context.Canceled", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("1h 退避没有感知取消（在 time.Sleep 而不是可取消等待）")
	}
}

// TestRunCursor_RejectsNilCallbacks 没有拉取/处理函数是编程错误，当场 panic。
func TestRunCursor_RejectsNilCallbacks(t *testing.T) {
	cases := []struct {
		name   string
		fetch  func(context.Context, string) ([]string, string, error)
		handle func(context.Context, string) error
	}{
		{name: "fetch 为 nil", handle: func(context.Context, string) error { return nil }},
		{
			name:   "handle 为 nil",
			fetch:  func(context.Context, string) ([]string, string, error) { return nil, "", nil },
			handle: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("期望 panic（静默空转/静默丢批次比 panic 更坏）")
				}
			}()
			_, _ = RunCursor(context.Background(), Config{}, "", tc.fetch, tc.handle)
		})
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
