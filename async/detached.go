package async

import (
	"context"
	"sync/atomic"
)

// contextEnricher 为全局「后台 ctx 富化」钩子：把 parent 的观测信息
// （trace_id / 租户标识等）注入派生的 Background ctx。concurrency 自身
// 不依赖任何 tracing 实现——观测接线由上层模块（tracing/traceid）init 注册。
var contextEnricher atomic.Pointer[func(parent, bg context.Context) context.Context]

// SetContextEnricher 注册全局「后台 ctx 富化」钩子（#155）：GoDetached/DetachedContext
// 派生 Background ctx 时回调 fn，由其把 parent 的追踪信息（trace_id 等）注入 bg。
//
// 典型接线（tracing/traceid 模块 init，一次注册全局生效）：
//
//	func init() {
//	    async.SetContextEnricher(func(parent, bg context.Context) context.Context {
//	        if tid := traceid.FromContext(parent); tid != "" {
//	            return traceid.ContextWith(bg, tid)
//	        }
//	        return bg
//	    })
//	}
//
// fn 为 nil 清除钩子（回到裸 Background）。钩子在后台 goroutine 启动前同步调用，
// 注意自身不要 panic 或长时间阻塞（其自身 panic 由 GoSafe 家族收编，但观测注入
// 会丢失）。重复注册以后一次为准（进程级单钩子，与 SetPanicHandler 同形态）。
func SetContextEnricher(fn func(parent, bg context.Context) context.Context) {
	if fn == nil {
		contextEnricher.Store(nil)
		return
	}
	contextEnricher.Store(&fn)
}

// DetachedContext 返回与 parent 生命周期解耦、但保留观测信息的 ctx：
//
//   - 解耦：返回值自 Background 派生，parent 的取消/超时对它无效——
//     fire-and-forget 任务不会因请求结束被腰斩；
//   - 可追踪：经 SetContextEnricher 注册的钩子把 parent 的 trace_id 等注入
//     返回值（无钩子、parent 为 nil 或钩子返回 nil 时退回裸 Background），
//     日志仍能跟请求串起来。
//
// 与手拼五件套（Background + traceid 注入 + GoSafeLabeled）等价；
// 供需要自管 spawn 策略的调用方复用同一口径，常规后台任务直接用 GoDetached。
func DetachedContext(parent context.Context) context.Context {
	bg := context.Background()
	if parent == nil {
		return bg
	}
	if fn := contextEnricher.Load(); fn != nil && *fn != nil {
		if enriched := (*fn)(parent, bg); enriched != nil {
			return enriched
		}
	}
	return bg
}

// GoDetached 启动**脱离请求生命周期、保留追踪信息**的后台安全 goroutine（#155）：
//
//   - fn 收到的 ctx 与 parent 解耦（DetachedContext：Background 派生），
//     请求超时/断连不会腰斩在飞任务；
//   - 经 SetContextEnricher 注册的钩子保留 parent 的 trace_id（由 tracing/
//     traceid 模块 init 注册，concurrency 不依赖具体实现）；
//   - panic 收编并带 task 标签（与 GoSafeLabeled 同口径）。
//
// 与既有原语的分工：
//
//	GoSafeWithContext(Labeled) = 跟随请求生命周期（请求取消即止）；
//	GoDetached                = 与请求解耦但可追踪（fire-and-forget）。
//
// 适合构建/部署触发、通知下发、缓存失效等「异步但不该被请求取消牵死、日志又要
// 跟请求串起来」的调用点，替代各服务手抄的 Background+traceid+GoSafe 三件套。
func GoDetached(parent context.Context, task string, fn func(ctx context.Context)) {
	GoSafeLabeled(task, func() { fn(DetachedContext(parent)) })
}
