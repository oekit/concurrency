package async

import (
	"context"
	"runtime/debug"
	"sync/atomic"
)

// contextEnricher 为全局「后台 ctx 富化」钩子：把 parent 的观测信息
// （trace_id / 租户标识等）注入派生的 Background ctx。concurrency 自身
// 不依赖任何 tracing 实现——观测接线由上层模块（tracing/traceid）init 注册。
var contextEnricher atomic.Pointer[func(parent, bg context.Context) context.Context]

// SetContextEnricher 注册全局「后台 ctx 富化」钩子（#155）：GoDetached/DetachedContext
// 派生脱钩 ctx（WithoutCancel(parent)，values 已继承）时回调 fn，由其把 parent
// 的追踪信息（trace_id 等）注入 bg。
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

// DetachedContext 返回与 parent 生命周期解耦、但保留 values 与观测信息的
// ctx（#211）：实现为 context.WithoutCancel(parent)（go 地板 1.24 直接用
// stdlib），parent 的 values 经 Value 链原样继承——与 WithoutCancel 的
// 标准语义一致。
//
// 对照表（parent 的什么被保留/丢弃）：
//
//	维度        | 保留？
//	-----------+------------------------------------------------------
//	Done/Err   | 丢弃——返回值永不取消（Done 为 nil、Err 恒 nil），
//	           | parent 的 cancel/超时不波及 fire-and-forget 任务
//	Deadline   | 丢弃——WithoutCancel 不继承时限（连 values 一起留在
//	           | 脱钩面上）——后台任务不受请求超时牵连
//	Values     | 保留——Value 委托 parent（请求作用域：会话 scope、
//	           | 租户标识等照常可读，ScopeFrom 类收编件不再静默降级）
//	trace      | 保留——经 SetContextEnricher 注册的钩子把 parent 的
//	           | trace_id 等注入返回值（无钩子或钩子返回 nil 时退回
//	           | 脱钩面），日志仍能跟请求串起来
//
// 与手拼五件套（WithoutCancel + traceid 注入 + GoSafeLabeled）等价；
// 供需要自管 spawn 策略的调用方复用同一口径，常规后台任务直接用 GoDetached。
func DetachedContext(parent context.Context) context.Context {
	if parent == nil {
		return context.Background()
	}
	bg := context.WithoutCancel(parent)
	if fn := contextEnricher.Load(); fn != nil && *fn != nil {
		if enriched := (*fn)(parent, bg); enriched != nil {
			return enriched
		}
	}
	return bg
}

// GoDetached 启动**脱离请求生命周期、保留 values 与追踪信息**的后台安全
// goroutine（#155、#211）：
//
//   - fn 收到的 ctx 与 parent 解耦（DetachedContext：WithoutCancel 派生），
//     请求超时/断连不会腰斩在飞任务；
//   - parent 的 values 经 Value 链保留（#211：与 context.WithoutCancel
//     标准语义一致——请求作用域/会话 scope 在 fn 内照常可读，手拼
//     WithoutCancel 场景收编后不再静默丢 values）；
//   - 经 SetContextEnricher 注册的钩子保留 parent 的 trace_id（由 tracing/
//     traceid 模块 init 注册，concurrency 不依赖具体实现）；
//   - panic 收编并带 task 标签（与 GoSafeLabeled 同口径），汇报的 ctx 即
//     富化后的后台 ctx——trace 保留到 panic 日志里（#184）。
//
// 与既有原语的分工：
//
//	GoSafeWithContext(Labeled) = 跟随请求生命周期（请求取消即止）；
//	GoDetached                = 与请求解耦但可追踪（fire-and-forget）。
//
// 适合构建/部署触发、通知下发、缓存失效等「异步但不该被请求取消牵死、日志又要
// 跟请求串起来」的调用点，替代各服务手抄的 WithoutCancel+traceid+GoSafe 三件套。
func GoDetached(parent context.Context, task string, fn func(ctx context.Context)) {
	bg := DetachedContext(parent)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				callPanicHandler(withTask(bg, task), r, debug.Stack())
			}
		}()
		fn(bg)
	}()
}
