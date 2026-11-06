// Package ratelimit 提供通用限流器实现。
//
// 支持令牌桶和滑动窗口两种限流算法，以及按 key 隔离的限流器管理；
// Lockout 提供按 key 的防爆破锁（fail2ban 形态：失败计数达阈值 → 锁定拒绝态）。
//
// 用法:
//
//	// 令牌桶限流器
//	limiter := ratelimit.NewTokenBucket(100, 200) // 100 QPS, burst 200
//	if limiter.Allow() {
//	    // 处理请求
//	}
//
//	// 按 key 隔离的限流器（如按用户 ID）
//	keyed := ratelimit.NewKeyedLimiter(func() ratelimit.Limiter {
//	    return ratelimit.NewTokenBucket(10, 20)
//	}, 10*time.Minute)
//	if keyed.Allow("user:123") {
//	    // 处理请求
//	}
//
//	// 防爆破锁（登录/API Key 防爆破、匿名接口防枚举）
//	lo := ratelimit.NewLockout(ratelimit.LockoutConfig{
//	    Window: 10 * time.Minute, Threshold: 5, LockDuration: 15 * time.Minute,
//	})
//	if locked, remain := lo.CheckLocked("user:42"); locked {
//	    // 对外 429 + 剩余秒数文案（remain 即剩余锁定时长）
//	}
//	if locked, _ := lo.RecordFailure("user:42"); locked {
//	    // 本次失败触发锁定（或已在锁定中）
//	}
//	lo.Reset("user:42") // 登录成功清零
//
// # 与 Throttle（写合并节流）的分工
//
// 上面的 Limiter / KeyedLimiter 是**速率配额**：单位时间允许通过多少次。
// Throttle 是另一件事——**窗口内去重合并**（first-then-suppress）：同一个 key
// 在窗口里只需生效一次，后续调用静默抑制、不排队、不报错、不占配额：
//
//	th := ratelimit.NewThrottle[uint64](time.Minute)
//	ran, err := th.Do(ctx, userID, func(ctx context.Context) error {
//	    return repo.TouchLastSeen(ctx, userID) // 每个请求都该 touch，真写库一分钟一次
//	})
//	_ = ran // false = 本窗口内已合并，不是失败
//
// 拿令牌桶硬凑这套语义会缺一笔账：Allow 返回 false 只说明「这次没通过」，
// 没有「抑制了几次」的口径（见 Throttle.Suppressed）。
package ratelimit
