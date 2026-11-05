// Package loop 提供周期任务循环原语：ctx 感知 + 单轮 panic 收编 + 两种间隔模式 + 排水。
//
// 收编自消费方反复手搓的同一模式（过期清理 / 心跳 / 快照落盘 / 配置轮询等），
// 把五个易漂移语义一次钉死：
//
//  1. 忙时丢 tick——fn 同步执行、不重叠，执行期间的 tick 直接丢弃（IntervalFixed）
//  2. 轮次完成后间隔——本轮跑完再等 Interval，轮次间隔 ≥ Interval 由构造保证
//     （IntervalAfterCompletion，#140）
//  3. 单轮 panic 不打死循环——按 async.RunSafe 收编（走全局 PanicHandler），循环存活
//  4. 停机排水——ctx 取消拦「下一轮」，在飞轮次用独立 ctx 跑完；Wait() 等排水结束
//  5. 首跑延迟可被停机打断——FirstDelay 走 select 而非 time.Sleep
//
// # 用法
//
//	ctx, cancel := context.WithCancel(context.Background())
//	l := loop.Run(ctx, loop.Config{Interval: time.Hour, Name: "expire-sweep"}, func(ctx context.Context) {
//	    sweep(ctx)
//	})
//	// ... 服务运行 ...
//	cancel()      // 拦下一轮；在飞轮次跑完
//	l.Wait()      // 排水（超时预算由调用方包一层控制）
//
// # 间隔模式
//
//   - IntervalFixed（默认）：固定节奏 + 忙时丢 tick。关心「固定时刻表」的任务用这档。
//   - IntervalAfterCompletion：本轮 fn 返回（含 panic 收编）后再等 Interval。关心
//     「两次执行之间至少隔开 Interval」的任务用这档（清理 / 巡检 / 对账 / 认领-投递类
//     worker）——固定节奏语义下长轮次只丢 tick 不后移，「轮次间隔 ≥ Interval」无保证；
//     清理类任务用错模式会静默改变时序假设。
//
// # 语义契约
//
//   - fn 立即（或 FirstDelay 后）首跑，之后按 IntervalMode 迭代
//   - fn 内 panic 按 RunSafe 收编，循环存活；Config.Name 非空时汇报带 task 标签；
//     AfterCompletion 模式下 panic 收编完成后同样重新计时
//   - 忙时丢 tick 仅 IntervalFixed 有（fn 同步执行，不重叠）
//   - ctx 取消拦「下一轮」，在飞轮次跑完；Wait() 返回即排水完成
//   - Interval <= 0 时按 0 处理（忙轮询），调用方应给合理间隔
package loop
