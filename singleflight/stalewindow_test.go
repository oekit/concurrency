package singleflight

// 窄窗口压测探针（#285）。
//
// 契约套件（pkg/singleflighttest/contract.go）的分工原文：「实现内部『先摘键后
// 唤醒』与『先唤醒后摘键』的差别落在飞行完成与唤醒之间的一个极窄窗口里，通过
// 公开 API 无法确定观测……窄窗口由实现仓自己的用例守住」。本文件就是那个
// 「实现仓自己的用例」——release 曾是 close→摘键序，留出一个 close-first 独有的
// 陈旧 join 面：ready 已关、key 还在，新到调用 LoadOrStore 命中已结束的 flight，
// 不执行 fn 直接拿到上一次的结果，且唤醒风暴会把 leader 的摘键推迟到毫秒级。
//
// 判据与 issue #285 的探针同构：不读任何实现内部状态，纯公开 API——返回值在
// 本次调用开始之前就已产出（doneAt[v] < t0）即计一次陈旧 join。阈值按构建模式
// 双档标定（同机实测，8 goroutine × 100k 次）：
//
//	形态                        非 race   race
//	delete-first（修复后 = pkg 参照）  ~0.5%   ~10%
//	close-first（缺陷形态）           ~25%    ~57–82%
//
// race 模式下正确实现也升到 ~10%：race 探测器拖慢 leader「fn 产出 → 摘键」的
// 收尾窗口（defer + 插桩 sync.Map），与参照实现 pkg/internal/singleflight 同款
// 探针同升（10.5%），属调度假象而非结构洞——两档阈值对两侧均保持 ≥2× 裕量。

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNarrowWindow_NoStaleJoin(t *testing.T) {
	const goroutines = 8
	const iters = 100_000

	f := New[int64]()
	var seq atomic.Int64
	// doneAt[id] = 第 id 个结果的产出时刻（UnixNano）；id 从 1 起，0 位弃用。
	doneAt := make([]int64, goroutines*iters+1)
	var stale atomic.Int64

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				t0 := time.Now().UnixNano()
				v, err := f.Do(context.Background(), "k", func(ctx context.Context) (int64, error) {
					id := seq.Add(1)
					atomic.StoreInt64(&doneAt[id], time.Now().UnixNano())
					return id, nil
				})
				if err != nil {
					t.Errorf("Do: %v", err)
					return
				}
				if v < 1 || int(v) >= len(doneAt) {
					t.Errorf("Do 返回非法结果 id %d", v)
					return
				}
				// 陈旧判定：返回值在本次调用开始前就已产出 ⇒ 命中的是已结束
				// 的旧 flight（join 了不该 join 的轮次）。
				if d := atomic.LoadInt64(&doneAt[v]); d != 0 && d < t0 {
					stale.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	total := int64(goroutines * iters)
	s := stale.Load()
	rate := float64(s) / float64(total) * 100
	if rate >= staleJoinLimitPct {
		t.Fatalf("陈旧 join 率超阈值：%d/%d（%.2f%% ≥ %.0f%%）——release 的先摘键防线失效"+
			"（#285，close-first 形态非 race ~25%% / race ~57-82%%；阈值见 staleJoinLimitPct 标定表）",
			s, total, rate, staleJoinLimitPct)
	}
	if seq.Load() == 0 {
		t.Fatal("fn 从未执行：探针失效")
	}
}

// TestAfterDoReturns_FreshExecution 是窄窗口的可观测互补面（契约套件
// runAfterFlightReruns 同款）：Do 返回后同 key 的下一次 Do 必须重新执行 fn
// ——先摘键序保证「返回即已摘键」，后续调用不可能再命中已结束的 flight。
func TestAfterDoReturns_FreshExecution(t *testing.T) {
	f := New[int]()
	var calls int
	for i := range 3 {
		got, err := f.Do(context.Background(), "k", func(ctx context.Context) (int, error) {
			calls++
			return calls, nil
		})
		if err != nil {
			t.Fatalf("Do %d: %v", i, err)
		}
		if got != i+1 {
			t.Fatalf("第 %d 次 Do 返回 %d，want %d——复用了已结束 flight 的陈旧结果（#285）", i, got, i+1)
		}
	}
}
