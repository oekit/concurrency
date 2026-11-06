package fanout

import "github.com/oekit/concurrency/async"

// MapOption 配置 Map 的可选行为。
//
// 走变参选项而不是新增必填参数：既有四参调用点一个都不用改（本包所有新增能力
// 都按这个口径加）。
type MapOption func(*options)

type options struct {
	onItemError func(index int, err error)
}

func newOptions(opts []MapOption) options {
	var o options
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// WithOnItemError 注册**逐项失败钩子**：每一项失败在聚合返回**之前**被同步调用一次，
// 按下标升序，逐项串行（不在 worker 里并发调用 ⇒ 钩子可以放心读写共享状态）。
//
// # 为什么需要它
//
// 库已经把逐项 panic 收编成 *async.PanicError 并经 Error.Failed 返还，但消费方想在
// 「该项失败那一刻」落业务终态时，此前只有两条别扭的路：
//
//   - 在 fn 内层自己 `defer recover()`——抢先于库的收编，于是库的 PanicError 通道
//     被吃掉，其余项继续跑、这一项的错误却再也汇不进 Error.Failed；
//   - 等 Map 返回后从 Failed 里 errors.As 分拣——能拿到下标，但「落终态」这一步
//     被推到了整批结束之后，与批量进度脱节（长批里毒丸行要等全批跑完才落库）。
//
// 钩子把「分拣 + 落终态」放回库的收编链路里：**panic 也走这里**（err 是该 panic 的
// *async.PanicError），调用方不必再内层 recover。
//
// # 语义
//
//   - err 覆盖三类失败：fn 返回的错误、fn panic 收编后的 *async.PanicError、ctx 取消
//     导致**未开始执行**的项的 context.Canceled（与 Error.Failed 同一口径）。
//   - 钩子跑在**调用 Map 的 goroutine** 上（聚合阶段），不在 worker 上 ⇒ 不并发。
//   - 钩子自己 panic 不会打死调用方、也不会改变该项的失败归属：就地收编并经
//     async.ReportPanic 上报全局 PanicHandler。
//   - 钩子拿到的下标恒指向 items 的原始位置，与 results 对齐。
//
// 用法（把毒丸行当场落终态）：
//
//	results, err := fanout.Map(ctx, rows, 8, deliver,
//	    fanout.WithOnItemError(func(i int, err error) {
//	        markTerminal(ctx, rows[i].ID, err) // 该项失败当场落库
//	    }))
func WithOnItemError(hook func(index int, err error)) MapOption {
	return func(o *options) { o.onItemError = hook }
}

// notifyItemError 调钩子并收编钩子自身的 panic。
//
// 钩子 panic 不能走「重新 panic」——此时已经离开 worker 的 recover 面，重新 panic
// 会打断整批的聚合返回，把「某一项的钩子写坏了」升级成「整批结果全丢」。
func notifyItemError(hook func(int, error), index int, err error) {
	if hook == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			async.ReportPanic(r)
		}
	}()
	hook(index, err)
}
