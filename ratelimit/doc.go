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
package ratelimit
