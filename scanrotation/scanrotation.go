// Package scanrotation 周期扫描作业的「单轮墙钟预算 + 跨轮轮转游标」记账件
// （#339，下沉自 bianque 的 scan_rotation.go——告警回推与自动化结局补推两个
// 独立 sweep 服务踩了同一个坑后收敛的家族件）。
//
// 与 loop 的边界（家族分工）：loop 管「何时跑」（Fixed/AfterCompletion 节奏
// 控制），本件管「一轮跑多久、下轮从哪起」——不驱动执行、不启 goroutine，
// 只记账。任何「周期扫描 + 每行一次外部投递 + 单轮墙钟预算 + 跨轮公平轮转」
// 的补推/清扫作业（webhook 重发、缓存预热、对账清扫）都是同一形态：
//
//	rot := scanrotation.New(20 * time.Second)
//	for { // 外层由 loop 驱动
//		rows := fetchRows()
//		start, deadline := rot.Begin(len(rows), 0)
//		attempted := 0
//		for i, row := range rows {
//			idx := (start + i) % len(rows)
//			if !deadline.IsZero() && time.Now().After(deadline) {
//				break // 预算耗尽收手：没扫到的行留给下一轮（游标停在本处）
//			}
//			deliver(row) // 每行一次外部投递
//			attempted = i + 1
//		}
//		rot.Advance(start, attempted, len(rows))
//	}
//
// 「预算用尽即让出本轮 + 下轮从停下位置接着转」与「一条都没试过 ≠ 试完
// 一轮」（Advance 的 attempted<=0 口径）是本件的两条实弹语义——后者按 0
// 推进会让本轮没轮到的行在下一轮被当成「已处理」跳过，静默漏推。
package scanrotation

import (
	"sync"
	"time"
)

// ScanRotation 周期扫描的轮转器：每轮 Begin 拿起点与预算截止，预算耗尽
// 收手，Advance 把游标停在收手处——下一轮从没扫到的行接着转。
//
// 方法级并发安全（互斥锁）；但一个实例只应服务**一个** sweep 循环
// （Begin/Advance 配对在同一 goroutine 里完成）——两个循环共用一个实例
// 时游标被互相追赶，轮转顺序语义未定义，各建各的实例。
type ScanRotation struct {
	mu     sync.Mutex
	budget time.Duration
	cursor int
	now    func() time.Time
}

// New 创建轮转器。budget 是每轮的墙钟预算；budget <= 0 时每轮 Begin 用
// 调用方传入的 def 兜底，def 也 <= 0 则该轮不限时（deadline 为零值）。
func New(budget time.Duration, opts ...RotationOption) *ScanRotation {
	r := &ScanRotation{budget: budget, now: time.Now}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RotationOption 定制轮转器（函数选项模式；按本仓命名硬规则为 XxxOption，
// 禁止裸 Option）。
type RotationOption func(*ScanRotation)

// WithRotationClock 注入自定义时间源（缺省 time.Now）。测试注入假时钟即可
// 确定地驱动预算判定，不依赖真实时间流逝。
func WithRotationClock(now func() time.Time) RotationOption {
	return func(r *ScanRotation) { r.now = now }
}

// Begin 开始一轮扫描：start 是本轮应从第几行扫起（∈ [0, n)，游标按上轮
// 收手处轮转），deadline 是本轮的墙钟预算截止——扫每一行**之前**检查
// time.Now().After(deadline)，超了收手，没扫到的行留给下一轮。
//
// n 是本轮待扫的总行数。n <= 0：本轮没有可扫的行——start=0、deadline 为
// 当前时刻（即刻过期，「本轮无预算」与「无限时」可由调用方区分）。
// 预算取值：构造时 budget > 0 用 budget；否则用本调用传入的 def；两者都
// <= 0 时 deadline 为零值 time.Time——本轮不限时，调用方无需检查截止。
func (r *ScanRotation) Begin(n int, def time.Duration) (start int, deadline time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if n <= 0 {
		return 0, now
	}
	b := r.budget
	if b <= 0 {
		b = def
	}
	if b > 0 {
		deadline = now.Add(b)
	}
	return r.cursor % n, deadline
}

// Advance 结束本轮：游标停在「起点 + 本轮实际尝试的行数」处。
//
// start 必须回传本轮 Begin 的返回值（配对契约——调用方拿游标与行数错位
// 即静默漏推）；attempted 是本轮**实际尝试**的行数（发起投递/处理才算，
// 被预算截止拦下的不算）。
//
// attempted <= 0 时游标**不动**：「一条都没试过」与「试完一轮」不是同一件
// 事——按 0 推进会让本轮没轮到的行在下一轮被当成「已处理」跳过（这正是
// 「静默漏推」的写法）。n <= 0 同理不动。
//
// n 在同一轮的 Begin 与 Advance 之间不得变化（行数固定的扫描集）；跨轮
// 行数变化时，下次 Begin 的 % n 自然重排，跨轮公平不保证（如实声明）。
func (r *ScanRotation) Advance(start, attempted, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 || attempted <= 0 {
		return
	}
	r.cursor = (start + attempted) % n
}
