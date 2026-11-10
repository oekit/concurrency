package ratelimit

import (
	"context"
	"errors"
	"time"
)

// ErrNotReservable 是 Limiter.Reserve 的「不可预留」规范哨兵（#248/#250）：
// 本请求在**任何未来时刻**都无法被预留——
//   - TokenBucket：1 个令牌超出 burst 上限（含 burst=0），重试也无用（与
//     WaitN 的 ErrBurstTooSmall 同因不同面）；
//   - SlidingWindow：整窗额度已被在册预占订满，再受理只会无限透支未来窗口。
//
// 判定一律 errors.Is(err, ErrNotReservable)，不要比对魔法数值——此前两个实现
// 各用一套互斥魔数（TokenBucket 回 time.Duration 最大值 ≈292 年、SlidingWindow
// 回 -1），接口契约零声明，按任一约定写的通用消费方（KeyedLimiter factory 的
// 返回类型是 Limiter 接口）换实现即静默失效：按 d<0 判拒绝的遇上 TokenBucket
// 被判成「等 292 年」挂死；按 d==最大值判拒绝的遇上 SlidingWindow 把 -1 当
// 立即可用，对满窗口的限流器紧循环锤击。
var ErrNotReservable = errors.New("ratelimit: not reservable")

// Limiter 是通用限流器接口。
//
// v0.21.0 起启用 Wait(ctx)——此前该行是注释掉的：接口上没有阻塞等待这一档，
// 消费方要么退化成「拿不到就拒」，要么在库外自建第二份令牌桶实现（重复实现 +
// 各自漂移，上游的时钟注入、Retry-After 探测语义同步不过去）。
//
// ⚠️ 启用该方法对**自行实现 Limiter 的调用方**是破坏性变更（接口多了一个方法，
// 旧实现不再满足）——0.x 阶段 minor 允许 breaking。仓内两个实现
// （TokenBucket / SlidingWindow）均已补齐。
type Limiter interface {
	// Allow 检查是否允许一个请求。
	Allow() bool

	// AllowN 检查是否允许 N 个请求。
	AllowN(n int) bool

	// Reserve 预留一个令牌，返回需要等待的时间与错误。
	//
	//   - (d, nil)：预留成功——d==0 表示立即可用；d>0 表示需等待 d 后放行
	//     （预留即消费，并发调用者拿到互不相同的槽位）。
	//   - (-1, ErrNotReservable)：不可预留——本请求在任何未来时刻都无法被
	//     预留。判定一律 errors.Is(err, ErrNotReservable)，不要比对 d 的
	//     魔法数值（d 仅为防御性占位：负值不会被误读成 0=立即可用）。
	//
	// ⚠️ v0.33.0 起改为 (time.Duration, error) 双返回，「不可预留」哨兵收敛为
	// ErrNotReservable——此前 TokenBucket 回最大 duration（≈292 年）、
	// SlidingWindow 回 -1，互斥魔数、接口零声明（#248/#250）。对自行实现
	// Limiter 的调用方是破坏性变更（签名变了，旧实现不再满足）——0.x 阶段
	// minor 允许 breaking。仓内两个实现均已对齐。
	Reserve() (time.Duration, error)

	// Wait 等待直到获取令牌或 ctx 取消。
	//
	// ctx 取消返回 ctx.Err()（且不消耗配额）；ctx 为 nil 时按
	// context.Background() 处理（永不取消）。
	Wait(ctx context.Context) error
}
