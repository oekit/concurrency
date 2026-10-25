// Package fanout 提供「限并发 + 结果保序 + 错误汇总」的批量执行原语。
//
// # 它解决什么
//
// 矩阵里做「同一件事在多台机器上并发跑、结果要能并排看」的场景，此前只有两种凑法：
//
//   - concurrency/syncx.FanInLimitCtx：限并发扇出的**引擎**（固定 worker 池、
//     按 index 序返回逐槽结果、ctx 取消不再调度新工作、fn panic 收编进槽），
//     但它是 **index 口径**的——fn 收到的是下标，要拿输入项得自己回查切片；
//     返回的是 []syncx.Slot[T] 逐槽结构，错误也散在各个槽里，调用方得自己扫。
//   - 调用方手搓 worker pool：语义拧、样板重复，还常漏掉「一项失败拖垮其余」。
//
// 本包在 syncx 引擎之上补一层**易用面**：fn 直接收输入项、结果按输入序返回、
// 失败项连同**下标**汇总成一个错误。
//
// # 典型用法
//
//	results, err := fanout.Map(ctx, assets, 10, func(ctx context.Context, a Asset) (Report, error) {
//	    return runCommand(ctx, a)
//	})
//	// results[i] 恒对应 assets[i]（失败项为零值）
//	// err 非 nil ⇒ *fanout.Error，Failed 列出每个失败项的下标与原始错误
//
// # 与 pkg/slicex.Map 的区别（语义相反，勿混）
//
// pkg/slicex.Map 是**串行**的逐项映射；本包的 Map 是**并发**的，且结果**按输入
// 顺序**返回。同名不同义：看到 fanout.Map 就是并发保序版，slicex.Map 是串行版。
//
// # 失败语义
//
// 一项失败**不**影响其余项——批量运维里「一台连不上」不该让另外九台的采集白跑。
// 所有失败项在最后汇总成一个 *Error 返回（含下标），errors.Is / errors.As 可穿透
// 到每一项的原始错误。
package fanout
