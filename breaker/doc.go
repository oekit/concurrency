// Package breaker 提供 Google SRE 概率式自适应熔断（移植 go-zero core/breaker
// 的 googleBreaker，算法忠实上游）。
//
// 与传统"关闭-打开-半开"状态机不同，本实现无状态机：按滑动窗口（缺省 40 桶 × 250ms）
// 统计接受/失败/丢弃样本，依 Google SRE Client-Side Throttling 公式计算丢弃概率——
// 请求量超过服务端可承受量（k=1.5 倍接受率，退化下限 minK=1.1）越多，丢弃概率越高，
// 过载呈概率衰减而非硬切换；protection=5 为保护阈值（窗口样本不足不丢弃）。
// 熔断后每隔 forcePass（缺省 1s）强制放行一次探活请求，服务恢复即自动愈合，
// 窗口历史随之滚出、丢弃概率归零。
//
// ErrCircuitOpen 为全库熔断打开的规范哨兵（httpx/nns-endpoint 经别名链共用同一
// sentinel）；拒绝时返回裸哨兵，判定一律 errors.Is。
//
// 算法与上游 go-zero 保持一致；差异点：
//   - API 全量 ctx-aware（Allow(ctx)/Do(ctx,…)），ctx 已取消直接返回 ctx.Err()；
//   - Promise 回填命名为 Pass/Fail（上游为 Accept/Reject(reason)）；
//   - 时间源可注入 WithBreakerClock（缺省 time.Now），窗口推进与强制探活共用；
//   - 可观测性零强制注入：拒绝走可选钩子 WithBreakerRejectHandler（缺省 no-op），
//     由上层显式接 logx/metrics；
//   - 统计容器复用本仓 window.RollingWindow（自定义桶 bucket 实现
//     BucketInterface[int64]），不镜像上游 bucket 布局。
//
// 用法:
//
//	b := breaker.GetBreaker("user-rpc")
//	err := b.Do(context.Background(), func() error { return callUserRPC() })
//	if errors.Is(err, breaker.ErrCircuitOpen) {
//	    // 熔断打开，快速失败/走降级
//	}
package breaker
