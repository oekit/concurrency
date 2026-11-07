package supervisor

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
)

// waitFor 等条件成立（有界等待，超时即失败）。
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

// waitStarts 等指定目标各自真的进入过 start（按事件等，不看 Running 探针）。
func waitStarts(t *testing.T, started <-chan string, want ...string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	seen := make(map[string]bool, len(want))
	for len(seen) < len(want) {
		select {
		case id := <-started:
			seen[id] = true
		case <-deadline:
			t.Fatalf("3s 内未等到这些目标进入 start：%v（已见 %v）", want, seen)
		}
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("目标 %q 没有进入 start（已见 %v）", id, seen)
		}
	}
}

func identity(s string) string { return s }

// TestGroup_ReconcileStartsOnlyNewTargets 幂等对齐：同一集合重复 Reconcile 不得重启。
func TestGroup_ReconcileStartsOnlyNewTargets(t *testing.T) {
	var starts atomic.Int64
	started := make(chan string, 16)

	start := func(ctx context.Context, target string) error {
		starts.Add(1)
		started <- target
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start)

	g.Reconcile([]string{"a"})
	select {
	case id := <-started:
		if id != "a" {
			t.Fatalf("首个启动目标 = %q，期望 a", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Reconcile 没有启动目标 a")
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1", got)
	}

	g.Reconcile([]string{"a", "b"})
	select {
	case id := <-started:
		if id != "b" {
			t.Fatalf("新增启动目标 = %q，期望 b（a 已在跑，不该重启）", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Reconcile 没有启动新增目标 b")
	}
	if got := g.Len(); got != 2 {
		t.Fatalf("Len = %d，期望 2", got)
	}
	waitFor(t, "两条循环都在跑", func() bool { return g.Running() == 2 })

	// 同一集合再对齐一次：不得有任何新启动
	g.Reconcile([]string{"a", "b"})
	select {
	case id := <-started:
		t.Fatalf("重复 Reconcile 重启了 %q（应当幂等）", id)
	case <-time.After(150 * time.Millisecond):
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("start 调用 %d 次，期望 2 次", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_StopsRemovedTargets 消失的目标被取消并转入排水，Reconcile 不被排水阻塞。
func TestGroup_StopsRemovedTargets(t *testing.T) {
	exited := make(chan string, 16)

	started := make(chan string, 16)
	start := func(ctx context.Context, target string) error {
		started <- target
		<-ctx.Done()
		exited <- target
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start)

	g.Reconcile([]string{"a", "b"})
	// 等的是**真实的 start 事件**，不是 Running 探针：监督「尚未结束」不等于
	// start 已经跑过（cancel 早于首跑时 start 一次都不会执行）。
	waitStarts(t, started, "a", "b")

	g.Reconcile([]string{"a"})
	select {
	case id := <-exited:
		if id != "b" {
			t.Fatalf("被停掉的目标 = %q，期望 b", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("被移除的目标没有收到取消")
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（b 已移出目标集）", got)
	}
	waitFor(t, "排水集合清空", func() bool { return g.Draining() == 0 })

	g.Stop()
	g.Wait()
}

// TestGroup_RestartsOnUnexpectedExit 意外死亡（返回错误）按退避重启，集合不变。
func TestGroup_RestartsOnUnexpectedExit(t *testing.T) {
	var attempts atomic.Int64
	start := func(ctx context.Context, target string) error {
		if attempts.Add(1) == 1 {
			return errors.New("连接被对端断开")
		}
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start, WithGroupRestartBackoff(time.Millisecond, time.Millisecond))

	g.Reconcile([]string{"a"})
	waitFor(t, "失败后重启到第 2 轮", func() bool { return attempts.Load() >= 2 })
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（重启不改变集合）", got)
	}
	waitFor(t, "重启后循环仍在跑", func() bool { return g.Running() == 1 })

	g.Stop()
	g.Wait()
}

// TestGroup_NoRestartOnGracefulReturn 返回 nil = 优雅结束，不重启。
func TestGroup_NoRestartOnGracefulReturn(t *testing.T) {
	var attempts atomic.Int64
	start := func(context.Context, string) error {
		attempts.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start, WithGroupRestartBackoff(time.Millisecond, time.Millisecond))

	g.Reconcile([]string{"a"})
	waitFor(t, "循环退出", func() bool { return g.Running() == 0 })

	// 静默窗：优雅结束不得被当成失败重启
	time.Sleep(150 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("start 调用 %d 次，期望 1 次（返回 nil 不该重启）", got)
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（目标仍在集合里，只是没在跑）", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_InvalidTargetsFilteredByCaller 规格不完整的目标由调用方在传 targets 前
// 过滤掉——过滤掉就等于让对齐全停它，库不再单独提供 Valid 开关（少一个旋钮，
// 表达力不变：目标从集合里消失就是「停掉」的唯一语义）。
func TestGroup_InvalidTargetsFilteredByCaller(t *testing.T) {
	type bot struct {
		id    string
		token string
	}
	started := make(chan string, 16)
	exited := make(chan string, 16)

	start := func(ctx context.Context, b bot) error {
		started <- b.id
		<-ctx.Done()
		exited <- b.id
		return nil
	}
	// 调用方的职责：规格不完整（没有 token）的目标不进 targets
	live := func(all []bot) []bot {
		out := make([]bot, 0, len(all))
		for _, b := range all {
			if b.token != "" {
				out = append(out, b)
			}
		}
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, func(b bot) string { return b.id }, start)

	g.Reconcile(live([]bot{{id: "a", token: "t"}, {id: "bad"}}))
	select {
	case id := <-started:
		if id != "a" {
			t.Fatalf("启动目标 = %q，期望只有规格完整的 a", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("有效目标 a 没有启动")
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（不完整的目标不该进目标集）", got)
	}

	// a 的规格失效（token 被清空）⇒ 过滤后从集合消失 ⇒ 被停掉
	g.Reconcile(live([]bot{{id: "a"}, {id: "bad"}}))
	select {
	case id := <-exited:
		if id != "a" {
			t.Fatalf("停止目标 = %q，期望 a", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("规格失效的目标没有被停掉")
	}
	if got := g.Len(); got != 0 {
		t.Fatalf("Len = %d，期望 0", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_StopWaitsAllAndIsIdempotent Stop 取消全部并幂等，Wait 能排水返回。
func TestGroup_StopWaitsAllAndIsIdempotent(t *testing.T) {
	var exits atomic.Int64
	started := make(chan string, 16)
	start := func(ctx context.Context, target string) error {
		started <- target
		<-ctx.Done()
		exits.Add(1)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start)

	g.Reconcile([]string{"a", "b", "c"})
	waitStarts(t, started, "a", "b", "c")

	g.Stop()
	g.Stop() // 幂等，不得重复取消或死锁

	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop 后 Wait 没有返回（有循环不肯收尾）")
	}
	if got := exits.Load(); got != 3 {
		t.Fatalf("退出循环 %d 条，期望 3", got)
	}

	// 停机后 Reconcile 是空操作，不得再启新循环
	g.Reconcile([]string{"d"})
	if got := g.Len(); got != 3 {
		t.Fatalf("停机后 Len = %d，期望保持 3（Reconcile 应为空操作）", got)
	}
}

// TestGroup_StartMayReadGroupState start 一律在锁外被调用：回调里读组状态不会自锁。
//
// 这是「持锁只做 map 操作、用户代码一律锁外」的回归守卫。持锁启动的失效形态很坏：
// 回调里的读会卡在不可重入的 sync.Mutex 上，在生产里表现为「整组对齐从此停摆」，
// 而不是一次可见的报错——真发生时本用例会一直等不到读回，由 go test 超时判红。
func TestGroup_StartMayReadGroupState(t *testing.T) {
	readBack := make(chan int, 4)

	var g *Group[string]
	start := func(ctx context.Context, _ string) error {
		readBack <- g.Len()
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g = NewGroup(ctx, identity, start)

	g.Reconcile([]string{"a"})
	select {
	case n := <-readBack:
		if n != 1 {
			t.Fatalf("start 里读到的 Len = %d，期望 1", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("start 里读组状态卡住了（说明 start 被持锁调用）")
	}

	g.Stop()
	g.Wait()
}

// TestGroup_IDOfMayReadGroupState idOf 一律在锁外被调用：回调里读组状态（Len）
// 不会自锁（#188）。修复前 Reconcile 持 g.mu 调 idOf，idOf 里再调 Len 即死锁，
// 表现为「对齐循环还在跑但什么都不动」——本用例以 3s 超时断言钉住。
func TestGroup_IDOfMayReadGroupState(t *testing.T) {
	readBack := make(chan int, 4)

	var g *Group[string]
	idOf := func(s string) string {
		readBack <- g.Len() // 文档承诺安全的用法：回调里读组状态
		return s
	}
	start := func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g = NewGroup(ctx, idOf, start)

	done := make(chan struct{})
	go func() {
		defer close(done)
		g.Reconcile([]string{"a", "b"})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Reconcile 3s 未返回——idOf 里读组状态自死锁（持锁调用了用户回调）")
	}

	// 两次 idOf 读回都要真的发生（不是没跑就通过）
	for i := 0; i < 2; i++ {
		select {
		case <-readBack:
		case <-time.After(3 * time.Second):
			t.Fatalf("idOf 的第 %d 次读回没有送达", i+1)
		}
	}
	if got := g.Len(); got != 2 {
		t.Fatalf("Len = %d，期望 2", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_IDOfPanicKeepsGroupUsable idOf 自身 panic：经 RunSafe 收编为
// *PanicError 后上抛，上抛点不持锁——组不因一次坏回调永久死锁（#189）。
func TestGroup_IDOfPanicKeepsGroupUsable(t *testing.T) {
	var reported atomic.Int64
	async.SetPanicHandler(func(context.Context, any, []byte) { reported.Add(1) })
	t.Cleanup(func() { async.SetPanicHandler(nil) }) // nil = 恢复默认 handler（#165）

	start := func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, func(s string) string {
		if s == "bad" {
			panic("目标元数据解析出错")
		}
		return s
	}, start)

	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		g.Reconcile([]string{"bad"})
	}()

	var r any
	select {
	case r = <-panicked:
	case <-time.After(3 * time.Second):
		t.Fatal("Reconcile 3s 未返回——idOf panic 把 g.mu 悬空了")
	}
	if r == nil {
		t.Fatal("idOf panic 应回调失败上抛（契约违反 fail-fast），而不是静默吞掉")
	}
	// 收编路径应经全局 PanicHandler 上报（r 是 *async.PanicError 的包装）
	perr, ok := r.(error)
	if !ok {
		t.Fatalf("上抛 panic 应是 error（fmt.Errorf 包装 *PanicError），got %T", r)
	}
	var pe *async.PanicError
	if !errors.As(perr, &pe) {
		t.Fatalf("上抛 panic 应包装 *async.PanicError，got %T", r)
	}
	waitFor(t, "panic 已上报 PanicHandler", func() bool { return reported.Load() == 1 })

	// 组仍可用：Len 不阻塞、后续 Reconcile 正常工作
	lenDone := make(chan int, 1)
	go func() { lenDone <- g.Len() }()
	select {
	case n := <-lenDone:
		if n != 0 {
			t.Fatalf("Len = %d，期望 0（坏目标没有登记）", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idOf panic 后 g.Len() 永久阻塞——g.mu 未释放")
	}

	started := make(chan string, 4)
	g2 := NewGroup(ctx, identity, func(ctx context.Context, target string) error {
		started <- target
		<-ctx.Done()
		return nil
	})
	g2.Reconcile([]string{"good"})
	select {
	case id := <-started:
		if id != "good" {
			t.Fatalf("启动目标 = %q，期望 good", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("后续 Reconcile 没有正常启动目标")
	}
	g2.Stop()
	g2.Wait()
	g.Stop()
}

// TestGroup_PanicInStartRestartedAndLabeled 循环体 panic：收编上报（带 task 标签）并重启。
func TestGroup_PanicInStartRestartedAndLabeled(t *testing.T) {
	var (
		attempts atomic.Int64
		panics   atomic.Int64
	)
	msgs := make(chan string, 16)
	async.SetPanicHandler(func(ctx context.Context, r any, _ []byte) {
		panics.Add(1)
		if task := async.TaskFromContext(ctx); task != "" {
			msgs <- "task=" + task
		}
	})
	t.Cleanup(func() { async.SetPanicHandler(func(context.Context, any, []byte) {}) })

	start := func(ctx context.Context, _ string) error {
		if attempts.Add(1) == 1 {
			panic("连接对象为空")
		}
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start,
		WithGroupTask("bot"),
		WithGroupRestartBackoff(time.Millisecond, time.Millisecond),
	)

	g.Reconcile([]string{"7"})
	waitFor(t, "panic 后重启到第 2 轮", func() bool { return attempts.Load() >= 2 })

	if panics.Load() == 0 {
		t.Fatal("循环体 panic 没有上报全局 PanicHandler")
	}
	select {
	case msg := <-msgs:
		if !strings.Contains(msg, "task=bot[7]") {
			t.Fatalf("panic 消息 = %q，期望带 task=bot[7] 标签（按目标聚合告警）", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("没收到 panic 上报")
	}

	g.Stop()
	g.Wait()
}

// TestGroup_GiveUpStopsRestarting 连续失败达上限：放弃重启，目标仍在集合里但没在跑。
func TestGroup_GiveUpStopsRestarting(t *testing.T) {
	var (
		attempts atomic.Int64
		gaveUp   atomic.Int64
	)

	start := func(context.Context, string) error {
		attempts.Add(1)
		return errors.New("凭据被吊销")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGroup(ctx, identity, start,
		WithGroupRestartBackoff(time.Millisecond, time.Millisecond),
		WithGroupMaxConsecutiveFailures(3),
		WithGroupOnGiveUp(func(id string, cause error) {
			if id != "a" {
				t.Errorf("放弃回调 id = %q，期望 a", id)
			}
			if cause == nil {
				t.Error("放弃回调 cause 不应为 nil")
			}
			gaveUp.Add(1)
		}),
	)

	g.Reconcile([]string{"a"})
	waitFor(t, "放弃重启", func() bool { return gaveUp.Load() == 1 })
	waitFor(t, "循环已不在跑", func() bool { return g.Running() == 0 })

	if got := attempts.Load(); got != 3 {
		t.Fatalf("start 调用 %d 次，期望 3 次（连续失败上限）", got)
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（放弃重启不等于移出目标集）", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_RejectsBadIdentities 空身份 / 同批重复身份是调用方契约违反，当场 panic。
func TestGroup_RejectsBadIdentities(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("空身份", func(t *testing.T) {
		g := NewGroup(ctx, identity, func(context.Context, string) error { return nil })
		defer g.Stop()
		defer func() {
			if recover() == nil {
				t.Fatal("期望 panic：空身份会让所有这类目标争同一个槽位")
			}
		}()
		g.Reconcile([]string{""})
	})

	t.Run("同批重复身份", func(t *testing.T) {
		g := NewGroup(ctx, func(s string) string { return strings.ToLower(s) },
			func(context.Context, string) error { return nil })
		defer g.Stop()
		defer func() {
			if recover() == nil {
				t.Fatal("期望 panic：同一批目标里身份必须唯一")
			}
		}()
		g.Reconcile([]string{"A", "a"})
	})

	t.Run("idOf 为 nil", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("期望 panic：没有身份就没有对齐")
			}
		}()
		NewGroup[string](ctx, nil, func(context.Context, string) error { return nil })
	})

	t.Run("start 为 nil", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("期望 panic：起了不跑的目标等于没起")
			}
		}()
		NewGroup(ctx, identity, nil)
	})
}

// TestGroup_SpecChangeViaIdentityRestarts 身份键编码了规格 ⇒ 规格变更表现为重启。
func TestGroup_SpecChangeViaIdentityRestarts(t *testing.T) {
	type bot struct {
		id    string
		token string
	}
	started := make(chan string, 16)
	exited := make(chan string, 16)

	start := func(ctx context.Context, b bot) error {
		started <- b.token
		<-ctx.Done()
		exited <- b.token
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 把 token 编进 reconcile 键：token 变 ⇒ 键变 ⇒ 旧循环停、新循环起
	g := NewGroup(ctx, func(b bot) string { return b.id + ":" + b.token }, start)

	g.Reconcile([]bot{{id: "1", token: "old"}})
	select {
	case tok := <-started:
		if tok != "old" {
			t.Fatalf("首启 token = %q，期望 old", tok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("首个目标没有启动")
	}

	g.Reconcile([]bot{{id: "1", token: "new"}})
	select {
	case tok := <-exited:
		if tok != "old" {
			t.Fatalf("被停循环的 token = %q，期望 old", tok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("规格变更后旧循环没有停")
	}
	select {
	case tok := <-started:
		if tok != "new" {
			t.Fatalf("新起循环的 token = %q，期望 new", tok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("规格变更后新循环没有起")
	}
	if got := g.Len(); got != 1 {
		t.Fatalf("Len = %d，期望 1（同一 bot 的旧规格已被替换）", got)
	}

	g.Stop()
	g.Wait()
}

// TestGroup_TargetsReflectCurrentSet Targets/Len 反映当前目标集。
func TestGroup_TargetsReflectCurrentSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 16)
	g := NewGroup(ctx, identity, func(ctx context.Context, target string) error {
		started <- target
		<-ctx.Done()
		return nil
	})

	g.Reconcile([]string{"a", "b", "c"})
	waitStarts(t, started, "a", "b", "c")

	got := g.Targets()
	if len(got) != 3 {
		t.Fatalf("Targets = %v，期望 3 项", got)
	}
	seen := make(map[string]bool, 3)
	for _, id := range got {
		seen[id] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Fatalf("Targets = %v，缺少 %q", got, want)
		}
	}

	g.Reconcile([]string{"b"})
	waitFor(t, "目标集收缩", func() bool { return g.Len() == 1 })

	g.Stop()
	g.Wait()
}
